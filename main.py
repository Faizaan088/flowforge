from fastapi import FastAPI, Depends, HTTPException
from contextlib import asynccontextmanager
from pydantic import BaseModel
from typing import Optional, Dict, Any
import os
import redis
from redis.exceptions import RedisError
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import and_

from database import engine, Base, get_db, AsyncSessionLocal
import models 
from queue_reconciliation import enqueue_execution, reconcile_queued_executions

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
    yield
    print("SHUTTING DOWN...")

app = FastAPI(title="FlowForge API", version="0.1.0", lifespan=lifespan)


async def reconcile_queue(db: AsyncSession):
    r_url = os.getenv("REDIS_URL")
    if not r_url:
        print("Redis is not configured; queued executions remain durable in Postgres.")
        return None

    redis_client = redis.from_url(r_url, decode_responses=True)
    return await reconcile_queued_executions(db, redis_client)

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
    print("Sweeper running: Looking for dead workers...")
    now = datetime.now(timezone.utc)
    
    result = await db.execute(
        select(models.Execution).where(
            and_(
                models.Execution.status == "RUNNING",
                models.Execution.lease_until < now
            )
        )
    )
    dead_executions = result.scalars().all()
    
    if not dead_executions:
        return {"message": "All clean! No dead jobs found."}
        
    r_url = os.getenv("REDIS_URL")
    redis_client = redis.from_url(r_url, decode_responses=True)
    
    swept_ids = []
    for exec in dead_executions:
        print(f"Found dead execution {exec.id}! Re-queuing...")
        exec.status = "QUEUED"
        exec.worker_id = None
        exec.lease_until = None
        exec.attempt += 1
        
        redis_client.lpush("flowforge:queue", exec.id)
        swept_ids.append(exec.id)
        
    await db.commit()
    return {"message": f"Swept and recovered {len(swept_ids)} jobs", "recovered_ids": swept_ids}
