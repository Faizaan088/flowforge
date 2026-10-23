from fastapi import (
    FastAPI,
    Request,
    Depends,
    HTTPException,
    Response,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager, suppress
from pydantic import BaseModel, ConfigDict, Field
from typing import Optional, Dict, Any, Literal
from datetime import datetime, timezone, timedelta
import os
import hashlib
import logging
import redis
import asyncio
import json
from redis.exceptions import RedisError

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

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
from scheduler import scheduler_loop
from audit import record_audit_event, list_audit_events
from worker_registry import disable_worker, enable_worker, UnknownWorkerError
from task_registry import default_task_registry
from workflow_engine import (
    WorkflowValidationError,
    cancel_workflow_run,
    create_workflow,
    create_workflow_run,
    resolve_and_dispatch,
)
from auth import (
    ROLE_ADMIN,
    ROLE_OBSERVER,
    ROLE_OPERATOR,
    LoginRequest,
    TokenResponse,
    UserAlreadyExistsError,
    UserCreate,
    update_user,
    UserUpdate,
    UserResponse,
    authenticate_user,
    bootstrap_admin,
    create_access_token,
    create_user,
    get_current_user,
    list_users,
    require_permission,
    require_role,
    ForgotPasswordRequest,
    ResetPasswordRequest,
    VerifyEmailRequest,
    ResendVerificationRequest,
    ApiKeyCreate,
    ApiKeyResponse,
    ApiKeyCreatedResponse,
    AccountDeleteRequest,
    revoke_token,
    request_password_reset,
    reset_password_with_token,
    create_email_verification_token,
    verify_email_with_token,
    create_api_key,
    list_api_keys,
    revoke_api_key,
    delete_user_account,
    verify_password,
)
from events import (
    EVENT_EXECUTION_QUEUED,
    authenticate_websocket,
    create_event,
    manager,
    publish_event,
)
from metrics import (
    CONTENT_TYPE_LATEST,
    RequestCorrelationMiddleware,
    generate_latest,
    record_execution_transition,
    refresh_queue_gauges,
)
from config import get_cors_origins, validate_production_configuration, safe_int, safe_float


RECOVERY_INTERVAL_SECONDS = safe_float(
    os.getenv("EXECUTION_RECOVERY_INTERVAL_SECONDS", "10"), 10.0
)
RATE_LIMIT_CLEANUP_INTERVAL_SECONDS = safe_float(
    os.getenv("RATE_LIMIT_CLEANUP_INTERVAL_SECONDS", "60"), 60.0
)
RATE_LIMIT_RETENTION_BUFFER_SECONDS = safe_int(
    os.getenv("RATE_LIMIT_RETENTION_BUFFER_SECONDS", "3600"), 3600
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


class WorkerResponse(BaseModel):
    worker_id: str
    status: str
    created_at: datetime
    last_heartbeat_at: datetime
    is_alive: bool

    model_config = ConfigDict(from_attributes=True)


class WorkflowTaskSummary(BaseModel):
    id: int
    name: str
    task_type: str
    config: Optional[Dict[str, Any]] = None

    model_config = ConfigDict(from_attributes=True)


class WorkflowEdgeResponse(BaseModel):
    id: int
    upstream_task_id: int
    downstream_task_id: int

    model_config = ConfigDict(from_attributes=True)


class WorkflowDefinitionResponse(BaseModel):
    id: int
    name: str
    description: Optional[str] = None
    created_at: datetime
    tasks: list[WorkflowTaskSummary] = []
    edges: list[WorkflowEdgeResponse] = []

    model_config = ConfigDict(from_attributes=True)


class WorkflowCreateRequest(BaseModel):
    name: str
    description: Optional[str] = None
    tasks: list[Dict[str, Any]]
    edges: Optional[list[Any]] = None


class WorkflowRunTriggerRequest(BaseModel):
    triggered_by: Optional[str] = "MANUAL"


class WorkflowTaskExecutionResponse(BaseModel):
    id: int
    workflow_task_id: int
    task_name: Optional[str] = None
    task_type: Optional[str] = None
    execution_id: Optional[int] = None
    status: str
    attempt: int
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error_summary: Optional[str] = None

    model_config = ConfigDict(from_attributes=True)


class WorkflowRunResponse(BaseModel):
    id: int
    workflow_id: int
    status: str
    triggered_by: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error_summary: Optional[str] = None
    created_at: datetime
    task_executions: list[WorkflowTaskExecutionResponse] = []

    model_config = ConfigDict(from_attributes=True)


class ScheduleCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    schedule_type: Literal["recurring", "one_time", "delayed"]
    enabled: bool = True
    configuration: Optional[Dict[str, Any]] = None


class ScheduleUpdateRequest(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=200)
    enabled: Optional[bool] = None
    configuration: Optional[Dict[str, Any]] = None


class ScheduleResponse(BaseModel):
    id: int
    name: str
    schedule_type: str
    enabled: bool
    configuration: Optional[Dict[str, Any]] = None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class AuditEventResponse(BaseModel):
    id: int
    actor_id: str
    action: str
    target_type: str
    target_id: Optional[str] = None
    metadata_: Optional[Dict[str, Any]] = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("BOOTING UP API AND CHECKING CONFIGURATION...")
    validate_production_configuration()

    if os.getenv("AUTO_CREATE_TABLES", "false").lower() == "true":
        print("AUTO_CREATE_TABLES enabled: synchronizing schema via Base.metadata.create_all...")
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async with AsyncSessionLocal() as session:
        await bootstrap_admin(session)
        reconciliation = await reconcile_queue(session)
        if reconciliation is not None:
            print(f"Startup queue reconciliation: {reconciliation.as_dict()}")
    r_url = os.getenv("REDIS_URL")
    redis_client = redis.from_url(r_url, decode_responses=True) if r_url else None
    if r_url:
        await manager.start_redis_listener(r_url)
    recovery_task = asyncio.create_task(recovery_loop())
    scheduler_task = asyncio.create_task(
        scheduler_loop(
            AsyncSessionLocal,
            redis_client=redis_client,
            interval_seconds=safe_float(os.getenv("SCHEDULER_INTERVAL_SECONDS", "5.0"), 5.0),
        )
    )
    try:
        yield
    finally:
        scheduler_task.cancel()
        recovery_task.cancel()
        with suppress(asyncio.CancelledError):
            await scheduler_task
        with suppress(asyncio.CancelledError):
            await recovery_task
        await manager.stop_redis_listener()
        print("SHUTTING DOWN...")

app = FastAPI(title="FlowForge API", version="0.1.0", lifespan=lifespan)
cors_origins = get_cors_origins()
allow_wildcard = "*" in cors_origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=not allow_wildcard,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)
app.add_middleware(RequestCorrelationMiddleware)


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

        try:
            async with AsyncSessionLocal() as session:
                await refresh_queue_gauges(session)
        except Exception as error:
            print(f"Queue gauge refresh failed: {error}")

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


@app.get("/metrics")
async def prometheus_metrics():
    """Prometheus exposition format metrics endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health_check():
    """Standard healthcheck preserving existing Docker compatibility."""
    return {"status": "healthy"}


@app.get("/health/live")
async def liveness_check():
    """Liveness probe for orchestrators."""
    return {"status": "alive"}


@app.get("/health/ready")
async def readiness_check(
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    """Readiness probe verifying database and Redis connectivity."""
    db_ok = False
    try:
        await db.execute(select(1))
        db_ok = True
    except Exception:
        db_ok = False

    r_url = os.getenv("REDIS_URL")
    redis_ok = True
    if r_url:
        try:
            redis_client = redis.from_url(r_url, decode_responses=True, socket_timeout=1.0)
            redis_ok = bool(redis_client.ping())
        except Exception:
            redis_ok = False

    is_ready = db_ok and redis_ok
    if not is_ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return {
        "status": "ready" if is_ready else "not_ready",
        "database": "ok" if db_ok else "unhealthy",
        "redis": "ok" if redis_ok else "unhealthy",
    }


@app.get("/system/summary")
async def system_summary(
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:read")),
):
    """Operational summary endpoint exposing queue and worker state."""
    gauges = await refresh_queue_gauges(db, force=True)
    return {
        "status": "operational",
        "active_workers": gauges.get("active_workers", 0),
        "queued_executions": gauges.get("queued", 0),
        "running_executions": gauges.get("running", 0),
        "retry_waiting": gauges.get("retry_waiting", 0),
        "dead_letters": gauges.get("dead_lettered", 0),
    }


# ---------------------------------------------------------------------------
# Authentication & User Management API
# ---------------------------------------------------------------------------
LOGIN_RATE_LIMIT_ATTEMPTS = safe_int(os.getenv("LOGIN_RATE_LIMIT_ATTEMPTS", "10"), 10)
LOGIN_RATE_LIMIT_WINDOW_SECONDS = safe_int(os.getenv("LOGIN_RATE_LIMIT_WINDOW_SECONDS", "900"), 900)


def _login_rate_limit_key(request: Request, username: str) -> str:
    # The reverse proxy overwrites X-Real-IP. Hash identity data rather than
    # writing raw usernames into Redis keys.
    client_ip = request.headers.get("x-real-ip") or (request.client.host if request.client else "unknown")
    normalized_username = username.strip().casefold()
    identity = hashlib.sha256(f"{client_ip}\0{normalized_username}".encode("utf-8")).hexdigest()
    return f"flowforge:login-attempts:{identity}"


async def enforce_login_rate_limit(request: Request, username: str) -> str:
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        if os.getenv("ENVIRONMENT", "development").lower() == "production":
            raise HTTPException(status_code=503, detail="Login temporarily unavailable")
        return ""
    try:
        from redis.asyncio import Redis
        async with Redis.from_url(redis_url, decode_responses=True) as client:
            key = _login_rate_limit_key(request, username)
            client_ip = request.headers.get("x-real-ip") or (request.client.host if request.client else "unknown")
            ip_hash = hashlib.sha256(client_ip.encode("utf-8")).hexdigest()
            ip_key = f"flowforge:login-ip-attempts:{ip_hash}"
            username_attempts = await client.incr(key)
            ip_attempts = await client.incr(ip_key)
            if username_attempts == 1:
                await client.expire(key, LOGIN_RATE_LIMIT_WINDOW_SECONDS)
            if ip_attempts == 1:
                await client.expire(ip_key, LOGIN_RATE_LIMIT_WINDOW_SECONDS)
            if max(username_attempts, ip_attempts) > LOGIN_RATE_LIMIT_ATTEMPTS:
                raise HTTPException(
                    status_code=429,
                    detail="Too many login attempts. Try again later.",
                    headers={"Retry-After": str(LOGIN_RATE_LIMIT_WINDOW_SECONDS)},
                )
            return f"{key}|{ip_key}"
    except HTTPException:
        raise
    except Exception as exc:
        logging.getLogger("flowforge.auth").warning(
            "Login rate limiter unavailable (%s)", type(exc).__name__
        )
        if os.getenv("ENVIRONMENT", "development").lower() == "production":
            raise HTTPException(status_code=503, detail="Login temporarily unavailable") from exc
        return ""


async def clear_login_rate_limit(key: str) -> None:
    if not key:
        return
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return
    try:
        from redis.asyncio import Redis
        async with Redis.from_url(redis_url, decode_responses=True) as client:
            await client.delete(*[part for part in key.split("|") if part])
    except Exception as exc:
        logging.getLogger("flowforge.auth").debug(
            "Could not clear login attempt counter (%s)", type(exc).__name__
        )


async def enforce_auth_action_rate_limit(
    request: Request,
    action: str,
    identifier: str,
    max_attempts: int = 5,
    window_seconds: int = 300,
) -> str:
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return ""
    try:
        from redis.asyncio import Redis
        async with Redis.from_url(redis_url, decode_responses=True) as client:
            client_ip = request.headers.get("x-real-ip") or (request.client.host if request.client else "unknown")
            normalized = identifier.strip().casefold()
            identity = hashlib.sha256(f"{action}\0{client_ip}\0{normalized}".encode("utf-8")).hexdigest()
            key = f"flowforge:rate-limit:{action}:{identity}"
            attempts = await client.incr(key)
            if attempts == 1:
                await client.expire(key, window_seconds)
            if attempts > max_attempts:
                raise HTTPException(
                    status_code=429,
                    detail=f"Too many requests. Please wait before trying again.",
                    headers={"Retry-After": str(window_seconds)},
                )
            return key
    except HTTPException:
        raise
    except Exception as exc:
        logging.getLogger("flowforge.auth").warning(
            "Rate limiter error for %s (%s)", action, type(exc).__name__
        )
        return ""


@app.post("/auth/login", response_model=TokenResponse)
async def login(
    payload: LoginRequest,
    request: Request,
    response: Response,
    db: AsyncSession = Depends(get_db),
):
    rate_limit_key = await enforce_login_rate_limit(request, payload.username)
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
    await clear_login_rate_limit(rate_limit_key)
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
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
        await record_audit_event(
            db,
            actor_id=str(current_user.id),
            action="USER_CREATED",
            target_type="USER",
            target_id=str(user.id),
            metadata={"username": user.username, "role": user.role},
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


@app.patch("/auth/users/{user_id}", response_model=UserResponse)
async def update_user_route(
    user_id: int,
    payload: UserUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("users:manage")),
):
    changes = payload.model_dump(exclude_unset=True)

    if not changes:
        raise HTTPException(status_code=400, detail="No changes supplied")

    # Prevent an administrator from removing their own admin access
    # or disabling their own account through this endpoint.
    if user_id == current_user.id:
        if changes.get("role", current_user.role) != ROLE_ADMIN:
            raise HTTPException(
                status_code=400,
                detail="You cannot remove your own administrator role",
            )
        if changes.get("is_active") is False:
            raise HTTPException(
                status_code=400,
                detail="You cannot disable your own account",
            )

    target = await db.get(models.User, user_id)
    if target is None:
        raise HTTPException(status_code=404, detail="User not found")

    # Never allow the last active administrator to be demoted or disabled.
    removing_admin = (
        target.role == ROLE_ADMIN
        and target.is_active
        and (
            changes.get("role", ROLE_ADMIN) != ROLE_ADMIN
            or changes.get("is_active", True) is False
        )
    )

    if removing_admin:
        result = await db.execute(
            select(models.User).where(
                models.User.role == ROLE_ADMIN,
                models.User.is_active.is_(True),
            )
        )
        active_admins = result.scalars().all()

        if len(active_admins) <= 1:
            raise HTTPException(
                status_code=400,
                detail="Cannot remove or disable the last active administrator",
            )

    try:
        updated = await update_user(
            session=db,
            user_id=user_id,
            role=changes.get("role"),
            is_active=changes.get("is_active"),
            password=changes.get("password"),
        )
        if updated is None:
            raise HTTPException(status_code=404, detail="User not found")

        audit_changes = sorted(key for key in changes if key != "password")
        if "password" in changes:
            audit_changes.append("password_changed")
        await record_audit_event(
            db,
            actor_id=str(current_user.id),
            action="USER_UPDATED",
            target_type="USER",
            target_id=str(updated.id),
            metadata={"changed_fields": audit_changes},
        )
        await db.commit()
        await db.refresh(updated)
        return updated

    except ValueError as err:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(err))
    except Exception:
        await db.rollback()
        raise


@app.post("/auth/logout")
async def logout(
    request: Request,
    current_user: models.User = Depends(get_current_user),
):
    auth_header = request.headers.get("Authorization")
    if auth_header:
        parts = auth_header.strip().split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            revoke_token(parts[1])
    return {"message": "Logged out successfully"}


@app.post("/auth/forgot-password")
async def forgot_password_route(
    payload: ForgotPasswordRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    await enforce_auth_action_rate_limit(request, "forgot-password", payload.email_or_username, max_attempts=5, window_seconds=300)
    base_url = str(request.base_url).rstrip("/")
    origin = request.headers.get("origin")
    if origin:
        base_url = origin
    await request_password_reset(db, payload.email_or_username, base_url=base_url)
    await db.commit()
    return {"message": "If an account matches that email or username, password reset instructions have been sent."}


@app.post("/auth/reset-password")
async def reset_password_route(
    payload: ResetPasswordRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    await enforce_auth_action_rate_limit(request, "reset-password", payload.token[:16], max_attempts=5, window_seconds=300)
    try:
        user = await reset_password_with_token(db, payload.token, payload.new_password)
        await record_audit_event(
            db,
            actor_id=str(user.id),
            action="PASSWORD_RESET",
            target_type="USER",
            target_id=str(user.id),
        )
        await db.commit()
        return {"message": "Password reset successfully. You may now log in with your new password."}
    except ValueError as err:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(err))


@app.post("/auth/verify-email")
async def verify_email_route(
    payload: VerifyEmailRequest,
    db: AsyncSession = Depends(get_db),
):
    try:
        user = await verify_email_with_token(db, payload.token)
        await record_audit_event(
            db,
            actor_id=str(user.id),
            action="EMAIL_VERIFIED",
            target_type="USER",
            target_id=str(user.id),
        )
        await db.commit()
        return {"message": "Email verified successfully."}
    except ValueError as err:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(err))


@app.post("/auth/resend-verification")
async def resend_verification_route(
    request: Request,
    payload: ResendVerificationRequest,
    db: AsyncSession = Depends(get_db),
):
    user = None
    auth_header = request.headers.get("Authorization")
    if auth_header:
        try:
            user = await get_current_user(request, db)
        except Exception:
            pass
    if user is None and payload.email:
        from auth import get_user_by_email
        user = await get_user_by_email(db, payload.email)

    if user and not user.is_verified:
        await enforce_auth_action_rate_limit(request, "resend-verification", user.username, max_attempts=3, window_seconds=300)
        base_url = request.headers.get("origin") or str(request.base_url).rstrip("/")
        await create_email_verification_token(db, user, base_url=base_url)
        await db.commit()

    return {"message": "If an unverified account matches, verification instructions have been sent."}


@app.delete("/users/me")
async def delete_my_account(
    payload: AccountDeleteRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    if payload.confirm_username != current_user.username:
        raise HTTPException(status_code=400, detail="Username confirmation does not match")
    if not verify_password(payload.password, current_user.password_hash):
        raise HTTPException(status_code=400, detail="Incorrect password")

    user_id = current_user.id
    try:
        await delete_user_account(db, user_id)
        auth_header = request.headers.get("Authorization")
        if auth_header:
            parts = auth_header.strip().split()
            if len(parts) == 2 and parts[0].lower() == "bearer":
                revoke_token(parts[1])
        await db.commit()
        return {"message": "Account successfully deleted"}
    except ValueError as err:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(err))


# ---------------------------------------------------------------------------
# API Key Management
# ---------------------------------------------------------------------------
@app.get("/api-keys/", response_model=list[ApiKeyResponse])
async def list_user_api_keys(
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    return await list_api_keys(db, current_user.id)


@app.post("/api-keys/", response_model=ApiKeyCreatedResponse, status_code=status.HTTP_201_CREATED)
async def create_user_api_key(
    payload: ApiKeyCreate,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    try:
        key_record, raw_key = await create_api_key(
            session=db,
            user_id=current_user.id,
            name=payload.name,
            expires_in_days=payload.expires_in_days,
        )
        await record_audit_event(
            db,
            actor_id=str(current_user.id),
            action="API_KEY_CREATED",
            target_type="API_KEY",
            target_id=str(key_record.id),
            metadata={"name": key_record.name, "prefix": key_record.key_prefix},
        )
        await db.commit()
        await db.refresh(key_record)
        return ApiKeyCreatedResponse(
            api_key=ApiKeyResponse.model_validate(key_record),
            raw_key=raw_key,
        )
    except ValueError as err:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(err))


@app.delete("/api-keys/{key_id}")
async def revoke_user_api_key(
    key_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(get_current_user),
):
    revoked = await revoke_api_key(db, current_user.id, key_id)
    if not revoked:
        raise HTTPException(status_code=404, detail="API key not found")
    await record_audit_event(
        db,
        actor_id=str(current_user.id),
        action="API_KEY_REVOKED",
        target_type="API_KEY",
        target_id=str(key_id),
    )
    await db.commit()
    return {"message": "API key revoked successfully", "id": key_id}


# ---------------------------------------------------------------------------
# Jobs & Executions API (Protected by RBAC)
# ---------------------------------------------------------------------------
@app.get("/jobs/")
async def list_jobs_route(
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:read")),
):
    stmt = select(models.JobDefinition)
    if current_user.role not in (ROLE_ADMIN, ROLE_OBSERVER):
        stmt = stmt.where(
            (models.JobDefinition.user_id == current_user.id)
            | (models.JobDefinition.user_id.is_(None))
        )
    stmt = stmt.order_by(models.JobDefinition.id.desc())
    result = await db.execute(stmt)
    return result.scalars().all()


@app.get("/jobs/{job_id}")
async def get_job_route(
    job_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:read")),
):
    job = await db.get(models.JobDefinition, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if current_user.role not in (ROLE_ADMIN, ROLE_OBSERVER) and job.user_id is not None and job.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Forbidden: You do not own this job")
    return job


@app.post("/jobs/")
async def create_job(
    job: JobCreate,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:trigger")),
):
    new_job = models.JobDefinition(
        name=job.name, 
        payload=job.payload, 
        priority=job.priority,
        user_id=current_user.id,
    )
    db.add(new_job)
    await db.commit()
    await db.refresh(new_job)

    await record_audit_event(
        db,
        actor_id=str(current_user.id),
        action="JOB_CREATED",
        target_type="JOB_DEFINITION",
        target_id=str(new_job.id),
        metadata={"name": new_job.name, "priority": new_job.priority},
    )
    await db.commit()
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
    if current_user.role != ROLE_ADMIN and job.user_id is not None and job.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Forbidden: You do not own this job")

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
    
    record_execution_transition("queued", category=new_execution.category)

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


# ---------------------------------------------------------------------------
# Workers Endpoints
# ---------------------------------------------------------------------------
@app.get("/workers/", response_model=list[WorkerResponse])
async def list_workers_route(
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:read")),
):
    now = datetime.now(timezone.utc)
    result = await db.execute(
        select(models.Worker).order_by(models.Worker.last_heartbeat_at.desc())
    )
    workers = result.scalars().all()
    alive_threshold = now - timedelta(seconds=60)
    return [
        WorkerResponse(
            worker_id=w.worker_id,
            status=w.status,
            created_at=w.created_at,
            last_heartbeat_at=w.last_heartbeat_at,
            is_alive=(
                w.status == "ACTIVE"
                and w.last_heartbeat_at is not None
                and w.last_heartbeat_at >= alive_threshold
            ),
        )
        for w in workers
    ]


@app.post("/workers/{worker_id}/disable", response_model=WorkerResponse)
async def disable_worker_route(
    worker_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:manage")),
):
    try:
        r_url = os.getenv("REDIS_URL")
        redis_client = redis.from_url(r_url, decode_responses=True) if r_url else None
        w = await disable_worker(db, worker_id, redis_client=redis_client)
        await record_audit_event(
            db,
            actor_id=str(current_user.id),
            action="WORKER_DISABLED",
            target_type="WORKER",
            target_id=worker_id,
        )
        await db.commit()
        return WorkerResponse(
            worker_id=w.worker_id,
            status=w.status,
            created_at=w.created_at,
            last_heartbeat_at=w.last_heartbeat_at,
            is_alive=False,
        )
    except UnknownWorkerError as err:
        raise HTTPException(status_code=404, detail=str(err))


@app.post("/workers/{worker_id}/enable", response_model=WorkerResponse)
async def enable_worker_route(
    worker_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("policies:manage")),
):
    try:
        w = await enable_worker(db, worker_id)
        await record_audit_event(
            db,
            actor_id=str(current_user.id),
            action="WORKER_ENABLED",
            target_type="WORKER",
            target_id=worker_id,
        )
        await db.commit()
        now = datetime.now(timezone.utc)
        alive_threshold = now - timedelta(seconds=60)
        return WorkerResponse(
            worker_id=w.worker_id,
            status=w.status,
            created_at=w.created_at,
            last_heartbeat_at=w.last_heartbeat_at,
            is_alive=(
                w.status == "ACTIVE"
                and w.last_heartbeat_at is not None
                and w.last_heartbeat_at >= alive_threshold
            ),
        )
    except UnknownWorkerError as err:
        raise HTTPException(status_code=404, detail=str(err))

# Workflow Definitions & Runs Endpoints
# ---------------------------------------------------------------------------
@app.get("/workflows/", response_model=list[WorkflowDefinitionResponse])
async def list_workflows_route(
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("workflows:read")),
):
    stmt = (
        select(models.WorkflowDefinition)
        .options(
            selectinload(models.WorkflowDefinition.tasks),
            selectinload(models.WorkflowDefinition.edges),
        )
    )
    if current_user.role not in (ROLE_ADMIN, ROLE_OBSERVER):
        stmt = stmt.where(
            (models.WorkflowDefinition.user_id == current_user.id)
            | (models.WorkflowDefinition.user_id.is_(None))
        )
    stmt = stmt.order_by(models.WorkflowDefinition.id.desc())
    result = await db.execute(stmt)
    return result.scalars().all()


@app.get("/workflows/{workflow_id}", response_model=WorkflowDefinitionResponse)
async def get_workflow_route(
    workflow_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("workflows:read")),
):
    result = await db.execute(
        select(models.WorkflowDefinition)
        .options(
            selectinload(models.WorkflowDefinition.tasks),
            selectinload(models.WorkflowDefinition.edges),
        )
        .where(models.WorkflowDefinition.id == workflow_id)
    )
    wf = result.scalar_one_or_none()
    if not wf:
        raise HTTPException(status_code=404, detail="Workflow definition not found")
    if current_user.role not in (ROLE_ADMIN, ROLE_OBSERVER) and wf.user_id is not None and wf.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Forbidden: You do not own this workflow")
    return wf


@app.post("/workflows/", response_model=WorkflowDefinitionResponse, status_code=status.HTTP_201_CREATED)
async def create_workflow_route(
    payload: WorkflowCreateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("workflows:manage")),
):
    try:
        wf = await create_workflow(
            session=db,
            name=payload.name,
            tasks=payload.tasks,
            edges=payload.edges,
            description=payload.description,
            validate=True,
            user_id=current_user.id,
        )
        await db.commit()
        await record_audit_event(
            db,
            actor_id=str(current_user.id),
            action="WORKFLOW_CREATED",
            target_type="WORKFLOW_DEFINITION",
            target_id=str(wf.id),
            metadata={"name": wf.name, "task_count": len(payload.tasks), "edge_count": len(payload.edges)},
        )
        await db.commit()
    except WorkflowValidationError as err:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(err))

    result = await db.execute(
        select(models.WorkflowDefinition)
        .options(
            selectinload(models.WorkflowDefinition.tasks),
            selectinload(models.WorkflowDefinition.edges),
        )
        .where(models.WorkflowDefinition.id == wf.id)
    )
    return result.scalar_one()


@app.get("/workflows/runs/", response_model=list[WorkflowRunResponse])
async def list_workflow_runs_route(
    workflow_id: Optional[int] = None,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("workflows:read")),
):
    query = (
        select(models.WorkflowRun)
        .options(
            selectinload(models.WorkflowRun.task_executions).selectinload(
                models.WorkflowTaskExecution.workflow_task
            )
        )
        .order_by(models.WorkflowRun.id.desc())
    )
    if workflow_id is not None:
        query = query.where(models.WorkflowRun.workflow_id == workflow_id)

    result = await db.execute(query)
    runs = result.scalars().all()
    out = []
    for r in runs:
        task_list = [
            WorkflowTaskExecutionResponse(
                id=te.id,
                workflow_task_id=te.workflow_task_id,
                task_name=te.workflow_task.name if te.workflow_task else None,
                task_type=te.workflow_task.task_type if te.workflow_task else None,
                execution_id=te.execution_id,
                status=te.status,
                attempt=te.attempt,
                started_at=te.started_at,
                finished_at=te.finished_at,
                error_summary=te.error_summary,
            )
            for te in r.task_executions
        ]
        out.append(
            WorkflowRunResponse(
                id=r.id,
                workflow_id=r.workflow_id,
                status=r.status,
                triggered_by=r.triggered_by,
                started_at=r.started_at,
                finished_at=r.finished_at,
                error_summary=r.error_summary,
                created_at=r.created_at,
                task_executions=task_list,
            )
        )
    return out


@app.get("/workflows/runs/{run_id}", response_model=WorkflowRunResponse)
async def get_workflow_run_route(
    run_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("workflows:read")),
):
    result = await db.execute(
        select(models.WorkflowRun)
        .options(
            selectinload(models.WorkflowRun.task_executions).selectinload(
                models.WorkflowTaskExecution.workflow_task
            )
        )
        .where(models.WorkflowRun.id == run_id)
    )
    run = result.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="Workflow run not found")

    task_list = [
        WorkflowTaskExecutionResponse(
            id=te.id,
            workflow_task_id=te.workflow_task_id,
            task_name=te.workflow_task.name if te.workflow_task else None,
            task_type=te.workflow_task.task_type if te.workflow_task else None,
            execution_id=te.execution_id,
            status=te.status,
            attempt=te.attempt,
            started_at=te.started_at,
            finished_at=te.finished_at,
            error_summary=te.error_summary,
        )
        for te in run.task_executions
    ]
    return WorkflowRunResponse(
        id=run.id,
        workflow_id=run.workflow_id,
        status=run.status,
        triggered_by=run.triggered_by,
        started_at=run.started_at,
        finished_at=run.finished_at,
        error_summary=run.error_summary,
        created_at=run.created_at,
        task_executions=task_list,
    )


@app.post("/workflows/{workflow_id}/runs", response_model=WorkflowRunResponse, status_code=status.HTTP_201_CREATED)
async def trigger_workflow_run_route(
    workflow_id: int,
    payload: Optional[WorkflowRunTriggerRequest] = None,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("workflows:manage")),
):
    wf = await db.get(models.WorkflowDefinition, workflow_id)
    if not wf:
        raise HTTPException(status_code=404, detail="Workflow definition not found")

    triggered_by = payload.triggered_by if payload else "MANUAL"
    try:
        run = await create_workflow_run(
            session=db,
            workflow_id=workflow_id,
            triggered_by=triggered_by,
        )
        r_url = os.getenv("REDIS_URL")
        redis_client = redis.from_url(r_url, decode_responses=True) if r_url else None
        await resolve_and_dispatch(db, run.id, redis_client=redis_client)
        await record_audit_event(
            db,
            actor_id=str(current_user.id),
            action="WORKFLOW_RUN_TRIGGERED",
            target_type="WORKFLOW_RUN",
            target_id=str(run.id),
            metadata={"workflow_id": workflow_id, "triggered_by": triggered_by},
        )
        await db.commit()
    except WorkflowValidationError as err:
        await db.rollback()
        raise HTTPException(status_code=400, detail=str(err))

    result = await db.execute(
        select(models.WorkflowRun)
        .options(
            selectinload(models.WorkflowRun.task_executions).selectinload(
                models.WorkflowTaskExecution.workflow_task
            )
        )
        .where(models.WorkflowRun.id == run.id)
    )
    fresh_run = result.scalar_one()
    task_list = [
        WorkflowTaskExecutionResponse(
            id=te.id,
            workflow_task_id=te.workflow_task_id,
            task_name=te.workflow_task.name if te.workflow_task else None,
            task_type=te.workflow_task.task_type if te.workflow_task else None,
            execution_id=te.execution_id,
            status=te.status,
            attempt=te.attempt,
            started_at=te.started_at,
            finished_at=te.finished_at,
            error_summary=te.error_summary,
        )
        for te in fresh_run.task_executions
    ]
    return WorkflowRunResponse(
        id=fresh_run.id,
        workflow_id=fresh_run.workflow_id,
        status=fresh_run.status,
        triggered_by=fresh_run.triggered_by,
        started_at=fresh_run.started_at,
        finished_at=fresh_run.finished_at,
        error_summary=fresh_run.error_summary,
        created_at=fresh_run.created_at,
        task_executions=task_list,
    )


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
        await record_audit_event(
            db,
            actor_id=str(current_user.id),
            action="WORKFLOW_RUN_CANCEL_REQUESTED",
            target_type="WORKFLOW_RUN",
            target_id=str(run.id),
            metadata={"status": run.status},
        )
        await db.commit()
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
# Schedules API
# ---------------------------------------------------------------------------
@app.get("/schedules/", response_model=list[ScheduleResponse])
async def list_schedules_route(
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:read")),
):
    stmt = select(models.ScheduleDefinition)
    if current_user.role not in (ROLE_ADMIN, ROLE_OBSERVER):
        stmt = stmt.where(
            (models.ScheduleDefinition.user_id == current_user.id)
            | (models.ScheduleDefinition.user_id.is_(None))
        )
    stmt = stmt.order_by(models.ScheduleDefinition.id.desc())
    result = await db.execute(stmt)
    return result.scalars().all()


@app.get("/schedules/{schedule_id}", response_model=ScheduleResponse)
async def get_schedule_route(
    schedule_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:read")),
):
    schedule = await db.get(models.ScheduleDefinition, schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")
    if current_user.role not in (ROLE_ADMIN, ROLE_OBSERVER) and schedule.user_id is not None and schedule.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Forbidden: You do not own this schedule")
    return schedule


@app.post("/schedules/", response_model=ScheduleResponse, status_code=status.HTTP_201_CREATED)
async def create_schedule_route(
    payload: ScheduleCreateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:trigger")),
):
    schedule = models.ScheduleDefinition(
        name=payload.name,
        schedule_type=payload.schedule_type,
        enabled=payload.enabled,
        configuration=payload.configuration,
        user_id=current_user.id,
    )
    db.add(schedule)
    await db.commit()
    await db.refresh(schedule)

    await record_audit_event(
        db,
        actor_id=str(current_user.id),
        action="SCHEDULE_CREATED",
        target_type="SCHEDULE",
        target_id=str(schedule.id),
        metadata={"name": schedule.name, "type": schedule.schedule_type},
    )
    await db.commit()
    return schedule


@app.patch("/schedules/{schedule_id}", response_model=ScheduleResponse)
async def update_schedule_route(
    schedule_id: int,
    payload: ScheduleUpdateRequest,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:trigger")),
):
    schedule = await db.get(models.ScheduleDefinition, schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")

    changes = payload.model_dump(exclude_unset=True)
    if "name" in changes:
        schedule.name = changes["name"]
    if "enabled" in changes:
        schedule.enabled = changes["enabled"]
    if "configuration" in changes:
        schedule.configuration = changes["configuration"]

    await record_audit_event(
        db,
        actor_id=str(current_user.id),
        action="SCHEDULE_UPDATED",
        target_type="SCHEDULE",
        target_id=str(schedule.id),
        metadata=changes,
    )
    await db.commit()
    await db.refresh(schedule)
    return schedule


@app.delete("/schedules/{schedule_id}")
async def delete_schedule_route(
    schedule_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("executions:trigger")),
):
    schedule = await db.get(models.ScheduleDefinition, schedule_id)
    if not schedule:
        raise HTTPException(status_code=404, detail="Schedule not found")
    await db.delete(schedule)
    await record_audit_event(
        db,
        actor_id=str(current_user.id),
        action="SCHEDULE_DELETED",
        target_type="SCHEDULE",
        target_id=str(schedule_id),
    )
    await db.commit()
    return {"message": "Schedule deleted", "id": schedule_id}


# ---------------------------------------------------------------------------
# Audit API
# ---------------------------------------------------------------------------
@app.get("/audit/", response_model=list[AuditEventResponse])
async def list_audit_events_route(
    actor_id: Optional[str] = None,
    action: Optional[str] = None,
    target_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
    current_user: models.User = Depends(require_permission("users:manage")),
):
    events = await list_audit_events(
        db,
        actor_id=actor_id,
        action=action,
        target_type=target_type,
        limit=limit,
        offset=offset,
    )
    return events


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



