from fastapi import FastAPI, Depends, HTTPException
from contextlib import asynccontextmanager, suppress
from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime, timezone
import os
import redis
import asyncio
from redis.exceptions import RedisError

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from database import engine, Base, get_db, AsyncSessionLocal
import models 
from concurrency_policy import cleanup_expired_rate_limit_records
from execution_recovery import recover_expired_executions
from execution_retry import requeue_eligible_retries
from queue_reconciliation import (
    deliver_executions_to_redis,
    enqueue_execution,
    reconcile_queued_executions,
)


RECOVERY_INTERVAL_SECONDS = float(
    os.getenv("EXECUTION_RECOVERY_INTERVAL_SECONDS", "10")
)
RATE_LIMIT_CLEANUP_INTERVAL_SECONDS = float(
    os.getenv("RATE_LIMIT_CLEANUP_INTERVAL_SECONDS", "60")
)
RATE_LIMIT_RETENTION_BUFFER_SECONDS = int(
    os.getenv("RATE_LIMIT_RETENTION_BUFFER_SECONDS", "3600")
)

class JobCreate(BaseModel):
    name: str
    payload: Optional[Dict[str, Any]] = None
    priority: int = 0

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("BOOTING UP API AND CHECKING DATABASE...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with AsyncSessionLocal() as session:
        reconciliation = await reconcile_queue(session)
        if reconciliation is not None:
            print(f"Startup queue reconciliation: {reconciliation.as_dict()}")
    recovery_task = asyncio.create_task(recovery_loop())
    try:
        yield
    finally:
        recovery_task.cancel()
        with suppress(asyncio.CancelledError):
            await recovery_task
        print("SHUTTING DOWN...")

app = FastAPI(title="FlowForge API", version="0.1.0", lifespan=lifespan)


async def reconcile_queue(db: AsyncSession, execution_ids: Optional[list[int]] = None):
    r_url = os.getenv("REDIS_URL")
    if not r_url:
        print("Redis is not configured; queued executions remain durable in Postgres.")
        return None

    redis_client = redis.from_url(r_url, decode_responses=True)
    if execution_ids is not None:
        return deliver_executions_to_redis(redis_client, execution_ids)
    return await reconcile_queued_executions(db, redis_client)


async def run_recovery_cycle(now: Optional[datetime] = None):
    current_time = now or datetime.now(timezone.utc)
    async with AsyncSessionLocal() as session:
        lease_recovered_ids = await recover_expired_executions(session, now=current_time)
        retry_requeued_ids = await requeue_eligible_retries(session, now=current_time)
        requeued_ids = lease_recovered_ids + retry_requeued_ids
        reconciliation = None
        if requeued_ids:
            try:
                reconciliation = await reconcile_queue(session, execution_ids=requeued_ids)
            except TypeError:
                reconciliation = await reconcile_queue(session)
    return requeued_ids, reconciliation


async def run_rate_limit_cleanup_cycle(
    now: Optional[datetime] = None,
    buffer_seconds: int = RATE_LIMIT_RETENTION_BUFFER_SECONDS,
) -> int:
    current_time = now or datetime.now(timezone.utc)
    try:
        async with AsyncSessionLocal() as session:
            async with session.begin():
                deleted = await cleanup_expired_rate_limit_records(
                    session, now=current_time, buffer_seconds=buffer_seconds
                )
            return deleted
    except Exception as error:
        print(f"Rate limit record cleanup failed: {error}")
        return 0


async def recovery_loop(
    interval_seconds: float = RECOVERY_INTERVAL_SECONDS,
    cleanup_interval_seconds: float = RATE_LIMIT_CLEANUP_INTERVAL_SECONDS,
):
    loop = asyncio.get_running_loop()
    last_cleanup_time = float("-inf")
    while True:
        try:
            recovered_ids, reconciliation = await run_recovery_cycle()
            if recovered_ids:
                print(f"Recovered/requeued executions: {recovered_ids}")
            if reconciliation is not None and reconciliation.redis_error:
                print(f"Queue reconciliation failed: {reconciliation.redis_error}")
        except Exception as error:
            print(f"Execution recovery cycle failed: {error}")

        current_monotonic = loop.time()
        if current_monotonic - last_cleanup_time >= cleanup_interval_seconds:
            try:
                deleted_count = await run_rate_limit_cleanup_cycle()
                if deleted_count > 0:
                    print(f"Cleaned up {deleted_count} expired rate limit records")
                last_cleanup_time = current_monotonic
            except Exception as error:
                print(f"Rate limit record cleanup failed: {error}")
                last_cleanup_time = current_monotonic

        await asyncio.sleep(interval_seconds)

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

@app.post("/jobs/")
async def create_job(job: JobCreate, db: AsyncSession = Depends(get_db)):
    new_job = models.JobDefinition(
        name=job.name, 
        payload=job.payload, 
        priority=job.priority
    )
    db.add(new_job)
    await db.commit()
    await db.refresh(new_job)
    return new_job

@app.post("/jobs/{job_id}/execute")
async def trigger_job(job_id: int, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.JobDefinition).where(models.JobDefinition.id == job_id))
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    new_execution = models.Execution(job_definition_id=job.id, status="QUEUED")
    db.add(new_execution)
    await db.commit()
    await db.refresh(new_execution)
    
    print(f"Queued execution {new_execution.id} in Postgres...")

    r_url = os.getenv("REDIS_URL")
    redis_client = redis.from_url(r_url, decode_responses=True)
    # Redis delivery follows the durable commit; reconciliation replays failures.
    try:
        enqueue_execution(redis_client, new_execution.id)
    except RedisError as error:
        print(f"Redis enqueue failed for execution {new_execution.id}: {error}")
    
    return new_execution

@app.get("/executions/")
async def list_executions(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(models.Execution))
    return result.scalars().all()


@app.post("/system/reconcile-queue")
async def reconcile_execution_queue(db: AsyncSession = Depends(get_db)):
    reconciliation = await reconcile_queue(db)
    if reconciliation is None:
        raise HTTPException(status_code=503, detail="Redis is not configured")
    return reconciliation.as_dict()

@app.post("/system/sweep")
async def sweep_dead_jobs(db: AsyncSession = Depends(get_db)):
    recovered_ids = await recover_expired_executions(db)
    if not recovered_ids:
        return {"message": "All clean! No dead jobs found."}

    reconciliation = await reconcile_queue(db)
    return {
        "message": f"Swept and recovered {len(recovered_ids)} jobs",
        "recovered_ids": recovered_ids,
        "reconciliation": reconciliation.as_dict() if reconciliation else None,
    }
