import json
import logging
from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager, suppress
from pydantic import BaseModel, ConfigDict
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
from concurrency_policy import (
    PolicyValidationError,
    cleanup_expired_rate_limit_records,
    delete_concurrency_limit_policy,
    delete_rate_limit_policy,
    get_concurrency_limit_policy,
    get_concurrency_limit_policy_by_id,
    get_rate_limit_policy,
    get_rate_limit_policy_by_id,
    list_concurrency_limit_policies,
    list_rate_limit_policies,
    set_concurrency_limit_policy,
    set_concurrency_limit_policy_enabled,
    set_rate_limit_policy,
    set_rate_limit_policy_enabled,
    update_concurrency_limit_policy,
    update_rate_limit_policy,
)
from execution_claim import cancel_execution, update_execution_priority
from execution_recovery import recover_expired_executions
from execution_retry import requeue_eligible_retries
from queue_reconciliation import (
    deliver_executions_to_redis,
    enqueue_execution,
    reconcile_queued_executions,
)
from workflow_engine import WorkflowValidationError, cancel_workflow_run
from auth import (
    ROLE_ADMIN,
    ROLE_OBSERVER,
    ROLE_OPERATOR,
    LoginRequest,
    TokenResponse,
    UserAlreadyExistsError,
    UserCreate,
    UserResponse,
    authenticate_user,
    bootstrap_admin,
    create_access_token,
    create_user,
    get_current_user,
    list_users,
    require_permission,
    require_role,
)
from events import (
    EVENT_EXECUTION_QUEUED,
    authenticate_websocket,
    create_event,
    manager,
    publish_event,
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


class ExecutionTrigger(BaseModel):
    priority: Optional[int] = None


class ExecutionCancelRequest(BaseModel):
    reason: Optional[str] = None


class ExecutionPriorityUpdate(BaseModel):
    priority: int


class WorkflowCancelRequest(BaseModel):
    reason: Optional[str] = None


class ConcurrencyPolicyCreate(BaseModel):
    target_type: str
    target_id: str
    max_concurrency: int = 1
    is_enabled: bool = True


class ConcurrencyPolicyUpdate(BaseModel):
    max_concurrency: Optional[int] = None
    is_enabled: Optional[bool] = None


class ConcurrencyPolicyResponse(BaseModel):
    id: int
    target_type: str
    target_id: str
    max_concurrency: int
    is_enabled: bool
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class RateLimitPolicyCreate(BaseModel):
    target_type: str = "CATEGORY"
    target_id: str
    max_requests: int = 10
    window_seconds: int = 60
    is_enabled: bool = True


class RateLimitPolicyUpdate(BaseModel):
    max_requests: Optional[int] = None
    window_seconds: Optional[int] = None
    is_enabled: Optional[bool] = None


class RateLimitPolicyResponse(BaseModel):
    id: int
    target_type: str
    target_id: str
    max_requests: int
    window_seconds: int
    is_enabled: bool
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("BOOTING UP API AND CHECKING DATABASE...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    async with AsyncSessionLocal() as session:
        await bootstrap_admin(session)
        reconciliation = await reconcile_queue(session)
        if reconciliation is not None:
            print(f"Startup queue reconciliation: {reconciliation.as_dict()}")
    r_url = os.getenv("REDIS_URL")
    if r_url:
        await manager.start_redis_listener(r_url)
    recovery_task = asyncio.create_task(recovery_loop())
    try:
        yield
    finally:
        recovery_task.cancel()
        with suppress(asyncio.CancelledError):
            await recovery_task
        await manager.stop_redis_listener()
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
    r_url = os.getenv("REDIS_URL")
    redis_client = redis.from_url(r_url, decode_responses=True) if r_url else None
    async with AsyncSessionLocal() as session:
        lease_recovered_ids = await recover_expired_executions(
            session, now=current_time, redis_client=redis_client
        )
        retry_requeued_ids = await requeue_eligible_retries(
            session, now=current_time, redis_client=redis_client
        )
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


# ---------------------------------------------------------------------------
# Authentication & User Management API
# ---------------------------------------------------------------------------
@app.post("/auth/login", response_model=TokenResponse)
async def login(
    payload: LoginRequest,
    db: AsyncSession = Depends(get_db),
):
    user = await authenticate_user(db, payload.username, payload.password)
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.is_active:
        raise HTTPException(
            status_code=401,
            detail="User account is disabled",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token(user)
    return TokenResponse(
        access_token=token,
        token_type="bearer",
        user=UserResponse.model_validate(user),
    )


@app.get("/auth/me", response_model=UserResponse)
async def get_me(
    current_user: models.User = Depends(get_current_user),
):
    return current_user


@app.post("/auth/users", response_model=UserResponse, status_code=201)
async def create_user_route(
    payload: UserCreate,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("users:manage")),
):
    try:
        user = await create_user(
            session=db,
            username=payload.username,
            password=payload.password,
            role=payload.role,
            email=payload.email,
        )
        await db.commit()
        await db.refresh(user)
        return user
    except UserAlreadyExistsError as err:
        raise HTTPException(status_code=409, detail=str(err))
    except ValueError as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.get("/auth/users", response_model=list[UserResponse])
async def list_users_route(
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("users:manage")),
):
    return await list_users(db)


# ---------------------------------------------------------------------------
# Jobs & Executions API (Protected by RBAC)
# ---------------------------------------------------------------------------
@app.post("/jobs/")
async def create_job(
    job: JobCreate,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:trigger")),
):
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
async def trigger_job(
    job_id: int,
    payload: Optional[ExecutionTrigger] = None,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:trigger")),
):
    result = await db.execute(select(models.JobDefinition).where(models.JobDefinition.id == job_id))
    job = result.scalar_one_or_none()
    
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    prio = payload.priority if payload and payload.priority is not None else (job.priority or 0)
    new_execution = models.Execution(
        job_definition_id=job.id,
        status="QUEUED",
        priority=prio,
        category=job.category,
    )
    db.add(new_execution)
    await db.commit()
    await db.refresh(new_execution)
    
    print(f"Queued execution {new_execution.id} in Postgres with priority {new_execution.priority}...")

    r_url = os.getenv("REDIS_URL")
    redis_client = redis.from_url(r_url, decode_responses=True) if r_url else None
    if redis_client:
        try:
            enqueue_execution(redis_client, new_execution.id)
        except RedisError as error:
            print(f"Redis enqueue failed for execution {new_execution.id}: {error}")
    
    publish_event(
        redis_client,
        create_event(
            EVENT_EXECUTION_QUEUED,
            execution_id=new_execution.id,
            job_id=new_execution.job_definition_id,
            status="QUEUED",
            metadata={"priority": new_execution.priority, "category": new_execution.category},
        ),
    )

    return new_execution


@app.get("/executions/")
async def list_executions(
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:read")),
):
    result = await db.execute(select(models.Execution).order_by(models.Execution.priority.desc(), models.Execution.id.asc()))
    return result.scalars().all()


@app.get("/executions/{execution_id}")
async def get_execution(
    execution_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:read")),
):
    execution = await db.get(models.Execution, execution_id)
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    return execution


@app.post("/executions/{execution_id}/cancel")
async def cancel_execution_route(
    execution_id: int,
    payload: Optional[ExecutionCancelRequest] = None,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:cancel")),
):
    r_url = os.getenv("REDIS_URL")
    redis_client = redis.from_url(r_url, decode_responses=True) if r_url else None
    reason = payload.reason if payload else None
    result = await cancel_execution(
        session=db,
        execution_id=execution_id,
        reason=reason,
        redis_client=redis_client,
    )
    if result.status == "NOT_FOUND":
        raise HTTPException(status_code=404, detail="Execution not found")
    if not result.cancelled:
        raise HTTPException(
            status_code=409,
            detail=result.message or f"Execution is already in terminal state '{result.status}'",
        )
    return {
        "message": result.message or "Execution cancelled",
        "execution_id": execution_id,
        "status": "CANCELLED",
    }


@app.patch("/executions/{execution_id}/priority")
@app.post("/executions/{execution_id}/priority", include_in_schema=False)
async def update_execution_priority_route(
    execution_id: int,
    payload: ExecutionPriorityUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:priority")),
):
    r_url = os.getenv("REDIS_URL")
    redis_client = redis.from_url(r_url, decode_responses=True) if r_url else None
    row = await db.get(models.Execution, execution_id)
    if not row:
        raise HTTPException(status_code=404, detail="Execution not found")
    if row.status != "QUEUED":
        raise HTTPException(
            status_code=409,
            detail=f"Cannot change priority of execution in status '{row.status}'",
        )
    updated = await update_execution_priority(db, execution_id, payload.priority, redis_client=redis_client)
    await db.commit()
    await db.refresh(updated)
    return updated


@app.post("/workflows/runs/{run_id}/cancel")
async def cancel_workflow_run_route(
    run_id: int,
    payload: Optional[WorkflowCancelRequest] = None,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("workflows:manage")),
):
    r_url = os.getenv("REDIS_URL")
    redis_client = redis.from_url(r_url, decode_responses=True) if r_url else None
    reason = payload.reason if payload else None
    try:
        run = await cancel_workflow_run(
            session=db,
            workflow_run_id=run_id,
            reason=reason,
            redis_client=redis_client,
        )
        return {
            "message": "Workflow run cancelled",
            "run_id": run.id,
            "status": run.status,
        }
    except WorkflowValidationError as err:
        raise HTTPException(status_code=404, detail=str(err))


@app.post("/system/reconcile-queue")
async def reconcile_execution_queue(
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("system:reconcile")),
):
    reconciliation = await reconcile_queue(db)
    if reconciliation is None:
        raise HTTPException(status_code=503, detail="Redis is not configured")
    return reconciliation.as_dict()

@app.post("/system/sweep")
async def sweep_dead_jobs(
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("system:reconcile")),
):
    r_url = os.getenv("REDIS_URL")
    redis_client = redis.from_url(r_url, decode_responses=True) if r_url else None
    recovered_ids = await recover_expired_executions(db, redis_client=redis_client)
    if not recovered_ids:
        return {"message": "All clean! No dead jobs found."}

    reconciliation = await reconcile_queue(db)
    return {
        "message": f"Swept and recovered {len(recovered_ids)} jobs",
        "recovered_ids": recovered_ids,
        "reconciliation": reconciliation.as_dict() if reconciliation else None,
    }


# ---------------------------------------------------------------------------
# Concurrency Limit Policy API (Protected by RBAC)
# ---------------------------------------------------------------------------
@app.post(
    "/policies/concurrency",
    response_model=ConcurrencyPolicyResponse,
    status_code=200,
)
@app.post(
    "/policies/concurrency/",
    response_model=ConcurrencyPolicyResponse,
    status_code=200,
    include_in_schema=False,
)
async def create_or_upsert_concurrency_policy(
    payload: ConcurrencyPolicyCreate,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:manage")),
):
    try:
        policy = await set_concurrency_limit_policy(
            session=db,
            target_type=payload.target_type,
            target_id=payload.target_id,
            max_concurrency=payload.max_concurrency,
            is_enabled=payload.is_enabled,
        )
        await db.commit()
        await db.refresh(policy)
        return policy
    except PolicyValidationError as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.get(
    "/policies/concurrency",
    response_model=list[ConcurrencyPolicyResponse],
)
@app.get(
    "/policies/concurrency/",
    response_model=list[ConcurrencyPolicyResponse],
    include_in_schema=False,
)
async def list_concurrency_policies(
    target_type: Optional[str] = None,
    target_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:read")),
):
    return await list_concurrency_limit_policies(
        session=db,
        target_type=target_type,
        target_id=target_id,
    )


@app.get(
    "/policies/concurrency/target/{target_type}/{target_id}",
    response_model=ConcurrencyPolicyResponse,
)
async def get_concurrency_policy_by_target(
    target_type: str,
    target_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:read")),
):
    policy = await get_concurrency_limit_policy(
        session=db, target_type=target_type, target_id=target_id
    )
    if not policy:
        raise HTTPException(status_code=404, detail="Concurrency policy not found")
    return policy


@app.get(
    "/policies/concurrency/{policy_id}",
    response_model=ConcurrencyPolicyResponse,
)
async def get_concurrency_policy(
    policy_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:read")),
):
    policy = await get_concurrency_limit_policy_by_id(session=db, policy_id=policy_id)
    if not policy:
        raise HTTPException(status_code=404, detail="Concurrency policy not found")
    return policy


@app.patch(
    "/policies/concurrency/{policy_id}",
    response_model=ConcurrencyPolicyResponse,
)
@app.put(
    "/policies/concurrency/{policy_id}",
    response_model=ConcurrencyPolicyResponse,
    include_in_schema=False,
)
async def update_concurrency_policy(
    policy_id: int,
    payload: ConcurrencyPolicyUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:manage")),
):
    try:
        policy = await update_concurrency_limit_policy(
            session=db,
            policy_id=policy_id,
            max_concurrency=payload.max_concurrency,
            is_enabled=payload.is_enabled,
        )
        if not policy:
            raise HTTPException(status_code=404, detail="Concurrency policy not found")
        await db.commit()
        await db.refresh(policy)
        return policy
    except PolicyValidationError as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.post(
    "/policies/concurrency/{policy_id}/enable",
    response_model=ConcurrencyPolicyResponse,
)
async def enable_concurrency_policy(
    policy_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:manage")),
):
    policy = await set_concurrency_limit_policy_enabled(
        session=db, policy_id=policy_id, is_enabled=True
    )
    if not policy:
        raise HTTPException(status_code=404, detail="Concurrency policy not found")
    await db.commit()
    await db.refresh(policy)
    return policy


@app.post(
    "/policies/concurrency/{policy_id}/disable",
    response_model=ConcurrencyPolicyResponse,
)
async def disable_concurrency_policy(
    policy_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:manage")),
):
    policy = await set_concurrency_limit_policy_enabled(
        session=db, policy_id=policy_id, is_enabled=False
    )
    if not policy:
        raise HTTPException(status_code=404, detail="Concurrency policy not found")
    await db.commit()
    await db.refresh(policy)
    return policy


@app.delete("/policies/concurrency/{policy_id}")
async def delete_concurrency_policy(
    policy_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:manage")),
):
    deleted = await delete_concurrency_limit_policy(session=db, policy_id=policy_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Concurrency policy not found")
    await db.commit()
    return {"message": "Concurrency policy deleted", "id": policy_id}


# ---------------------------------------------------------------------------
# Rate Limit Policy API (Protected by RBAC)
# ---------------------------------------------------------------------------
@app.post(
    "/policies/rate-limit",
    response_model=RateLimitPolicyResponse,
    status_code=200,
)
@app.post(
    "/policies/rate-limit/",
    response_model=RateLimitPolicyResponse,
    status_code=200,
    include_in_schema=False,
)
async def create_or_upsert_rate_limit_policy(
    payload: RateLimitPolicyCreate,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:manage")),
):
    try:
        policy = await set_rate_limit_policy(
            session=db,
            target_type=payload.target_type,
            target_id=payload.target_id,
            max_requests=payload.max_requests,
            window_seconds=payload.window_seconds,
            is_enabled=payload.is_enabled,
        )
        await db.commit()
        await db.refresh(policy)
        return policy
    except PolicyValidationError as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.get(
    "/policies/rate-limit",
    response_model=list[RateLimitPolicyResponse],
)
@app.get(
    "/policies/rate-limit/",
    response_model=list[RateLimitPolicyResponse],
    include_in_schema=False,
)
async def list_rate_limit_policies_route(
    target_type: Optional[str] = None,
    target_id: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:read")),
):
    return await list_rate_limit_policies(
        session=db,
        target_type=target_type,
        target_id=target_id,
    )


@app.get(
    "/policies/rate-limit/target/{target_type}/{target_id}",
    response_model=RateLimitPolicyResponse,
)
async def get_rate_limit_policy_by_target(
    target_type: str,
    target_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:read")),
):
    policy = await get_rate_limit_policy(
        session=db, target_type=target_type, target_id=target_id
    )
    if not policy:
        raise HTTPException(status_code=404, detail="Rate limit policy not found")
    return policy


@app.get(
    "/policies/rate-limit/{policy_id}",
    response_model=RateLimitPolicyResponse,
)
async def get_rate_limit_policy_route(
    policy_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:read")),
):
    policy = await get_rate_limit_policy_by_id(session=db, policy_id=policy_id)
    if not policy:
        raise HTTPException(status_code=404, detail="Rate limit policy not found")
    return policy


@app.patch(
    "/policies/rate-limit/{policy_id}",
    response_model=RateLimitPolicyResponse,
)
@app.put(
    "/policies/rate-limit/{policy_id}",
    response_model=RateLimitPolicyResponse,
    include_in_schema=False,
)
async def update_rate_limit_policy_route(
    policy_id: int,
    payload: RateLimitPolicyUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:manage")),
):
    try:
        policy = await update_rate_limit_policy(
            session=db,
            policy_id=policy_id,
            max_requests=payload.max_requests,
            window_seconds=payload.window_seconds,
            is_enabled=payload.is_enabled,
        )
        if not policy:
            raise HTTPException(status_code=404, detail="Rate limit policy not found")
        await db.commit()
        await db.refresh(policy)
        return policy
    except PolicyValidationError as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.post(
    "/policies/rate-limit/{policy_id}/enable",
    response_model=RateLimitPolicyResponse,
)
async def enable_rate_limit_policy(
    policy_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:manage")),
):
    policy = await set_rate_limit_policy_enabled(
        session=db, policy_id=policy_id, is_enabled=True
    )
    if not policy:
        raise HTTPException(status_code=404, detail="Rate limit policy not found")
    await db.commit()
    await db.refresh(policy)
    return policy


@app.post(
    "/policies/rate-limit/{policy_id}/disable",
    response_model=RateLimitPolicyResponse,
)
async def disable_rate_limit_policy(
    policy_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:manage")),
):
    policy = await set_rate_limit_policy_enabled(
        session=db, policy_id=policy_id, is_enabled=False
    )
    if not policy:
        raise HTTPException(status_code=404, detail="Rate limit policy not found")
    await db.commit()
    await db.refresh(policy)
    return policy


@app.delete("/policies/rate-limit/{policy_id}")
async def delete_rate_limit_policy_route(
    policy_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:manage")),
):
    deleted = await delete_rate_limit_policy(session=db, policy_id=policy_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Rate limit policy not found")
    await db.commit()
    return {"message": "Rate limit policy deleted", "id": policy_id}


# ---------------------------------------------------------------------------
# WebSocket Live Execution Events Stream
# ---------------------------------------------------------------------------
@app.websocket("/ws/events")
async def websocket_events_endpoint(
    websocket: WebSocket,
    session: AsyncSession = Depends(get_db),
):
    """Real-time live execution event stream over WebSocket.

    Protected with JWT authentication via query string (`?token=...`),
    Authorization header (`Bearer ...`), or subprotocol (`token.<jwt>`).
    Enforces RBAC (Observer, Operator, Admin) and supports subscription filtering,
    ping/pong keepalive, and initial state snapshots.
    """
    user = await authenticate_websocket(websocket, session)
    if user is None or user.role not in [ROLE_ADMIN, ROLE_OPERATOR, ROLE_OBSERVER]:
        await websocket.close(code=1008)
        return

    conn = await manager.connect(websocket, user)

    # Initial connection acknowledgment
    await websocket.send_text(
        json.dumps({
            "type": "connected",
            "user": conn.user.username,
            "role": conn.user.role,
            "subscription": conn.subscription.to_dict(),
        })
    )

    async def sender():
        try:
            while conn.is_active:
                event = await conn.queue.get()
                await websocket.send_text(event.to_json())
                conn.queue.task_done()
        except (WebSocketDisconnect, asyncio.CancelledError):
            pass
        except Exception as exc:
            logging.getLogger("flowforge.events").debug("WebSocket sender terminated: %s", exc)

    sender_task = asyncio.create_task(sender())

    try:
        while True:
            raw_text = await websocket.receive_text()
            try:
                data = json.loads(raw_text)
            except Exception:
                if raw_text.strip().lower() == "ping":
                    data = {"action": "ping"}
                else:
                    continue

            action = data.get("action") or data.get("type")
            if action == "ping":
                await websocket.send_text(
                    json.dumps({
                        "type": "pong",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
                )
            elif action == "subscribe":
                filters = data.get("filter") or data.get("filters") or {}
                if isinstance(filters, str):
                    if filters == "all":
                        filters = {"all": True}
                    else:
                        filters = {}
                conn.update_subscription(filters)
                await websocket.send_text(
                    json.dumps({
                        "type": "subscribed",
                        "subscription": conn.subscription.to_dict(),
                    })
                )
            elif action == "snapshot":
                stmt = (
                    select(models.Execution)
                    .order_by(models.Execution.id.desc())
                    .limit(50)
                )
                exec_id = data.get("execution_id")
                if exec_id is not None:
                    stmt = select(models.Execution).where(
                        models.Execution.id == int(exec_id)
                    )
                res = await session.execute(stmt)
                rows = res.scalars().all()
                snapshot = [
                    {
                        "id": r.id,
                        "job_definition_id": r.job_definition_id,
                        "status": r.status,
                        "priority": r.priority,
                        "category": r.category,
                        "attempt": r.attempt,
                    }
                    for r in rows
                ]
                await websocket.send_text(
                    json.dumps({
                        "type": "snapshot",
                        "executions": snapshot,
                    })
                )
    except (WebSocketDisconnect, asyncio.CancelledError):
        pass
    except Exception as exc:
        logging.getLogger("flowforge.events").debug("WebSocket session closed: %s", exc)
    finally:
        sender_task.cancel()
        with suppress(asyncio.CancelledError):
            await sender_task
        await manager.disconnect(conn)



