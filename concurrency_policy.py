"""FlowForge Concurrency and Rate Limit Policy Engine.

Durable, PostgreSQL-backed policy models and atomic admission evaluation for:
- Concurrency limits on workflows and task types
- Rate limits on job/task categories
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from models import (
    ConcurrencyLimitPolicy,
    Execution,
    JobDefinition,
    RateLimitPolicy,
    RateLimitRecord,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowTask,
    WorkflowTaskExecution,
)


class PolicyValidationError(ValueError):
    """Raised when concurrency or rate limit policy parameters violate validation rules."""
    pass


VALID_CONCURRENCY_TARGET_TYPES = {"WORKFLOW", "TASK_TYPE"}
VALID_RATE_LIMIT_TARGET_TYPES = {"CATEGORY", "JOB_CATEGORY", "TASK_TYPE", "WORKFLOW"}
DEFAULT_MAX_CONCURRENCY = 1
DEFAULT_MAX_REQUESTS = 10
DEFAULT_WINDOW_SECONDS = 60


@dataclass(frozen=True)
class AdmissionResult:
    admitted: bool
    reason: str | None = None
    policy_type: str | None = None
    policy_target: str | None = None
    current_count: int = 0
    limit: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "admitted": self.admitted,
            "reason": self.reason,
            "policy_type": self.policy_type,
            "policy_target": self.policy_target,
            "current_count": self.current_count,
            "limit": self.limit,
        }


def validate_concurrency_policy(
    target_type: str,
    target_id: str | int,
    max_concurrency: int,
) -> tuple[str, str, int]:
    """Validate concurrency policy parameters."""
    if not isinstance(target_type, str):
        raise PolicyValidationError(f"target_type must be a string, got {type(target_type).__name__}")
    normalized_type = target_type.strip().upper()
    if normalized_type not in VALID_CONCURRENCY_TARGET_TYPES:
        raise PolicyValidationError(
            f"Invalid target_type '{target_type}'. Must be one of {sorted(VALID_CONCURRENCY_TARGET_TYPES)}"
        )

    t_id_str = str(target_id).strip()
    if not t_id_str:
        raise PolicyValidationError("target_id cannot be empty or whitespace")

    if not isinstance(max_concurrency, int) or max_concurrency < 1:
        raise PolicyValidationError(
            f"max_concurrency must be an integer >= 1, got {max_concurrency}"
        )

    return normalized_type, t_id_str, max_concurrency


def validate_rate_limit_policy(
    target_type: str,
    target_id: str | int,
    max_requests: int,
    window_seconds: int,
) -> tuple[str, str, int, int]:
    """Validate rate limit policy parameters."""
    if not isinstance(target_type, str):
        raise PolicyValidationError(f"target_type must be a string, got {type(target_type).__name__}")
    normalized_type = target_type.strip().upper()
    if normalized_type not in VALID_RATE_LIMIT_TARGET_TYPES:
        raise PolicyValidationError(
            f"Invalid target_type '{target_type}'. Must be one of {sorted(VALID_RATE_LIMIT_TARGET_TYPES)}"
        )

    t_id_str = str(target_id).strip()
    if not t_id_str:
        raise PolicyValidationError("target_id cannot be empty or whitespace")

    if not isinstance(max_requests, int) or max_requests < 1:
        raise PolicyValidationError(
            f"max_requests must be an integer >= 1, got {max_requests}"
        )

    if not isinstance(window_seconds, int) or window_seconds < 1:
        raise PolicyValidationError(
            f"window_seconds must be an integer >= 1, got {window_seconds}"
        )

    return normalized_type, t_id_str, max_requests, window_seconds


async def set_concurrency_limit_policy(
    session: AsyncSession,
    target_type: str,
    target_id: str | int,
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
    is_enabled: bool = True,
) -> ConcurrencyLimitPolicy:
    """Create or update a durable concurrency limit policy."""
    normalized_type, t_id_str, max_c = validate_concurrency_policy(
        target_type, target_id, max_concurrency
    )

    stmt = select(ConcurrencyLimitPolicy).where(
        ConcurrencyLimitPolicy.target_type == normalized_type,
        ConcurrencyLimitPolicy.target_id == t_id_str,
    )
    policy = (await session.execute(stmt)).scalar_one_or_none()
    if policy:
        policy.max_concurrency = max_c
        policy.is_enabled = is_enabled
    else:
        policy = ConcurrencyLimitPolicy(
            target_type=normalized_type,
            target_id=t_id_str,
            max_concurrency=max_c,
            is_enabled=is_enabled,
        )
        session.add(policy)

    await session.flush()
    return policy


async def get_concurrency_limit_policy(
    session: AsyncSession,
    target_type: str,
    target_id: str | int,
) -> ConcurrencyLimitPolicy | None:
    """Retrieve an existing concurrency limit policy by target type and target id."""
    normalized_type = str(target_type).strip().upper()
    t_id_str = str(target_id).strip()
    stmt = select(ConcurrencyLimitPolicy).where(
        ConcurrencyLimitPolicy.target_type == normalized_type,
        ConcurrencyLimitPolicy.target_id == t_id_str,
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def get_concurrency_limit_policy_by_id(
    session: AsyncSession,
    policy_id: int,
) -> ConcurrencyLimitPolicy | None:
    """Retrieve an existing concurrency limit policy by primary key id."""
    return await session.get(ConcurrencyLimitPolicy, policy_id)


async def list_concurrency_limit_policies(
    session: AsyncSession,
    target_type: str | None = None,
    target_id: str | int | None = None,
) -> list[ConcurrencyLimitPolicy]:
    """List concurrency limit policies, optionally filtered by target type and target id."""
    stmt = select(ConcurrencyLimitPolicy)
    if target_type is not None:
        stmt = stmt.where(
            ConcurrencyLimitPolicy.target_type == target_type.strip().upper()
        )
    if target_id is not None:
        stmt = stmt.where(
            ConcurrencyLimitPolicy.target_id == str(target_id).strip()
        )
    stmt = stmt.order_by(ConcurrencyLimitPolicy.id.asc())
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def update_concurrency_limit_policy(
    session: AsyncSession,
    policy_id: int,
    max_concurrency: int | None = None,
    is_enabled: bool | None = None,
) -> ConcurrencyLimitPolicy | None:
    """Update max_concurrency and/or is_enabled on an existing concurrency limit policy."""
    policy = await session.get(ConcurrencyLimitPolicy, policy_id)
    if not policy:
        return None

    if max_concurrency is not None:
        if not isinstance(max_concurrency, int) or max_concurrency < 1:
            raise PolicyValidationError(
                f"max_concurrency must be an integer >= 1, got {max_concurrency}"
            )
        policy.max_concurrency = max_concurrency

    if is_enabled is not None:
        policy.is_enabled = bool(is_enabled)

    await session.flush()
    return policy


async def set_concurrency_limit_policy_enabled(
    session: AsyncSession,
    policy_id: int,
    is_enabled: bool,
) -> ConcurrencyLimitPolicy | None:
    """Enable or disable an existing concurrency limit policy."""
    return await update_concurrency_limit_policy(
        session, policy_id, is_enabled=is_enabled
    )


async def delete_concurrency_limit_policy(
    session: AsyncSession,
    policy_id: int,
) -> bool:
    """Delete a concurrency limit policy by id. Returns True if deleted, False if not found."""
    policy = await session.get(ConcurrencyLimitPolicy, policy_id)
    if not policy:
        return False
    await session.delete(policy)
    await session.flush()
    return True


async def set_rate_limit_policy(
    session: AsyncSession,
    target_type: str,
    target_id: str | int,
    max_requests: int = DEFAULT_MAX_REQUESTS,
    window_seconds: int = DEFAULT_WINDOW_SECONDS,
    is_enabled: bool = True,
) -> RateLimitPolicy:
    """Create or update a durable rate limit policy."""
    normalized_type, t_id_str, max_r, win_s = validate_rate_limit_policy(
        target_type, target_id, max_requests, window_seconds
    )

    stmt = select(RateLimitPolicy).where(
        RateLimitPolicy.target_type == normalized_type,
        RateLimitPolicy.target_id == t_id_str,
    )
    policy = (await session.execute(stmt)).scalar_one_or_none()
    if policy:
        policy.max_requests = max_r
        policy.window_seconds = win_s
        policy.is_enabled = is_enabled
    else:
        policy = RateLimitPolicy(
            target_type=normalized_type,
            target_id=t_id_str,
            max_requests=max_r,
            window_seconds=win_s,
            is_enabled=is_enabled,
        )
        session.add(policy)

    await session.flush()
    return policy


async def get_rate_limit_policy(
    session: AsyncSession,
    target_type: str,
    target_id: str | int,
) -> RateLimitPolicy | None:
    """Retrieve an existing rate limit policy by target type and target id."""
    normalized_type = str(target_type).strip().upper()
    t_id_str = str(target_id).strip()
    stmt = select(RateLimitPolicy).where(
        RateLimitPolicy.target_type == normalized_type,
        RateLimitPolicy.target_id == t_id_str,
    )
    return (await session.execute(stmt)).scalar_one_or_none()


async def get_rate_limit_policy_by_id(
    session: AsyncSession,
    policy_id: int,
) -> RateLimitPolicy | None:
    """Retrieve an existing rate limit policy by primary key id."""
    return await session.get(RateLimitPolicy, policy_id)


async def list_rate_limit_policies(
    session: AsyncSession,
    target_type: str | None = None,
    target_id: str | int | None = None,
) -> list[RateLimitPolicy]:
    """List rate limit policies, optionally filtered by target type and target id."""
    stmt = select(RateLimitPolicy)
    if target_type is not None:
        stmt = stmt.where(
            RateLimitPolicy.target_type == target_type.strip().upper()
        )
    if target_id is not None:
        stmt = stmt.where(RateLimitPolicy.target_id == str(target_id).strip())
    stmt = stmt.order_by(RateLimitPolicy.id.asc())
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def update_rate_limit_policy(
    session: AsyncSession,
    policy_id: int,
    max_requests: int | None = None,
    window_seconds: int | None = None,
    is_enabled: bool | None = None,
) -> RateLimitPolicy | None:
    """Update max_requests, window_seconds, and/or is_enabled on an existing rate limit policy."""
    policy = await session.get(RateLimitPolicy, policy_id)
    if not policy:
        return None

    if max_requests is not None:
        if not isinstance(max_requests, int) or max_requests < 1:
            raise PolicyValidationError(
                f"max_requests must be an integer >= 1, got {max_requests}"
            )
        policy.max_requests = max_requests

    if window_seconds is not None:
        if not isinstance(window_seconds, int) or window_seconds < 1:
            raise PolicyValidationError(
                f"window_seconds must be an integer >= 1, got {window_seconds}"
            )
        policy.window_seconds = window_seconds

    if is_enabled is not None:
        policy.is_enabled = bool(is_enabled)

    await session.flush()
    return policy


async def set_rate_limit_policy_enabled(
    session: AsyncSession,
    policy_id: int,
    is_enabled: bool,
) -> RateLimitPolicy | None:
    """Enable or disable an existing rate limit policy."""
    return await update_rate_limit_policy(
        session, policy_id, is_enabled=is_enabled
    )


async def delete_rate_limit_policy(
    session: AsyncSession,
    policy_id: int,
) -> bool:
    """Delete a rate limit policy by id. Returns True if deleted, False if not found."""
    policy = await session.get(RateLimitPolicy, policy_id)
    if not policy:
        return False
    await session.delete(policy)
    await session.flush()
    return True


async def resolve_execution_context(
    session: AsyncSession,
    execution: Execution,
) -> dict[str, Any]:
    """Resolve workflow, task type, and category identifiers for an execution."""
    context: dict[str, Any] = {
        "workflow_id": None,
        "workflow_name": None,
        "task_type": None,
        "category": execution.category,
    }

    # 1. Check if linked to WorkflowTaskExecution
    stmt = (
        select(WorkflowTaskExecution)
        .options(
            selectinload(WorkflowTaskExecution.workflow_task),
            selectinload(WorkflowTaskExecution.workflow_run),
        )
        .where(WorkflowTaskExecution.execution_id == execution.id)
    )
    task_exec = (await session.execute(stmt)).scalar_one_or_none()

    if task_exec:
        wf_run = task_exec.workflow_run
        if wf_run:
            context["workflow_id"] = wf_run.workflow_id
            wf_def = await session.get(WorkflowDefinition, wf_run.workflow_id)
            if wf_def:
                context["workflow_name"] = wf_def.name

        wf_task = task_exec.workflow_task
        if wf_task:
            context["task_type"] = wf_task.task_type
            if not context["category"] and wf_task.config and isinstance(wf_task.config, dict):
                context["category"] = (
                    wf_task.config.get("category")
                    or wf_task.config.get("job_category")
                )

    # 2. Check JobDefinition if category not resolved
    if not context["category"] and execution.job_definition_id:
        job_def = await session.get(JobDefinition, execution.job_definition_id)
        if job_def:
            if getattr(job_def, "category", None):
                context["category"] = job_def.category
            elif job_def.payload and isinstance(job_def.payload, dict):
                context["category"] = (
                    job_def.payload.get("category")
                    or job_def.payload.get("job_category")
                )
            if not context["category"]:
                context["category"] = job_def.name

    return context


async def check_and_record_admission(
    session: AsyncSession,
    execution: Execution,
    now: datetime | None = None,
) -> tuple[bool, str | None]:
    """
    Atomically evaluate concurrency and rate limits for an execution during claiming.

    - PostgreSQL is the authority.
    - Locks policy rows FOR UPDATE in deterministic order to eliminate race conditions.
    - Derives active counts directly from PostgreSQL status in ('CLAIMED', 'RUNNING') with non-expired leases.
    - If rate limit applies and execution is admitted, persists a RateLimitRecord in the same transaction.
    - Returns (True, None) on admission, or (False, reason) on limit rejection.
    """
    current_time = now or datetime.now(timezone.utc)
    context = await resolve_execution_context(session, execution)

    workflow_id = context.get("workflow_id")
    workflow_name = context.get("workflow_name")
    task_type = context.get("task_type")
    category = context.get("category")

    # -----------------------------------------------------------------------
    # 1. Fetch & lock applicable Concurrency Policies
    # -----------------------------------------------------------------------
    concurrency_targets: list[tuple[str, str]] = []
    if workflow_id is not None:
        concurrency_targets.append(("WORKFLOW", str(workflow_id)))
    if workflow_name is not None and str(workflow_name) != str(workflow_id):
        concurrency_targets.append(("WORKFLOW", workflow_name))
    if task_type:
        concurrency_targets.append(("TASK_TYPE", task_type))

    applicable_conc_policies: list[ConcurrencyLimitPolicy] = []
    if concurrency_targets:
        clauses = [
            (ConcurrencyLimitPolicy.target_type == t_type) & (ConcurrencyLimitPolicy.target_id == t_id)
            for t_type, t_id in concurrency_targets
        ]
        stmt = (
            select(ConcurrencyLimitPolicy)
            .where(or_(*clauses), ConcurrencyLimitPolicy.is_enabled.is_(True))
            .order_by(ConcurrencyLimitPolicy.id)
            .with_for_update()
        )
        applicable_conc_policies = list((await session.execute(stmt)).scalars().all())

    # -----------------------------------------------------------------------
    # 2. Fetch & lock applicable Rate Limit Policies
    # -----------------------------------------------------------------------
    rate_targets: list[tuple[str, str]] = []
    if category:
        rate_targets.append(("CATEGORY", category))
        rate_targets.append(("JOB_CATEGORY", category))
    if task_type:
        rate_targets.append(("TASK_TYPE", task_type))
    if workflow_id is not None:
        rate_targets.append(("WORKFLOW", str(workflow_id)))

    applicable_rate_policies: list[RateLimitPolicy] = []
    if rate_targets:
        clauses = [
            (RateLimitPolicy.target_type == t_type) & (RateLimitPolicy.target_id == t_id)
            for t_type, t_id in rate_targets
        ]
        stmt = (
            select(RateLimitPolicy)
            .where(or_(*clauses), RateLimitPolicy.is_enabled.is_(True))
            .order_by(RateLimitPolicy.id)
            .with_for_update()
        )
        applicable_rate_policies = list((await session.execute(stmt)).scalars().all())

    # -----------------------------------------------------------------------
    # 3. Evaluate Concurrency Limits
    # -----------------------------------------------------------------------
    active_statuses = ("CLAIMED", "RUNNING")

    for policy in applicable_conc_policies:
        if policy.target_type == "WORKFLOW":
            wf_stmt = (
                select(func.count(Execution.id))
                .join(WorkflowTaskExecution, WorkflowTaskExecution.execution_id == Execution.id)
                .join(WorkflowRun, WorkflowRun.id == WorkflowTaskExecution.workflow_run_id)
                .where(
                    WorkflowRun.workflow_id == workflow_id,
                    Execution.status.in_(active_statuses),
                    Execution.id != execution.id,
                    or_(
                        Execution.lease_until.is_(None),
                        Execution.lease_until >= current_time,
                    ),
                )
            )
            active_count = (await session.execute(wf_stmt)).scalar() or 0
            if active_count >= policy.max_concurrency:
                return False, (
                    f"Workflow concurrency limit reached for target '{policy.target_id}': "
                    f"{active_count} active >= limit {policy.max_concurrency}"
                )

        elif policy.target_type == "TASK_TYPE":
            tt_stmt = (
                select(func.count(Execution.id))
                .join(WorkflowTaskExecution, WorkflowTaskExecution.execution_id == Execution.id)
                .join(WorkflowTask, WorkflowTask.id == WorkflowTaskExecution.workflow_task_id)
                .where(
                    WorkflowTask.task_type == task_type,
                    Execution.status.in_(active_statuses),
                    Execution.id != execution.id,
                    or_(
                        Execution.lease_until.is_(None),
                        Execution.lease_until >= current_time,
                    ),
                )
            )
            active_count = (await session.execute(tt_stmt)).scalar() or 0
            if active_count >= policy.max_concurrency:
                return False, (
                    f"Task type concurrency limit reached for target '{policy.target_id}': "
                    f"{active_count} active >= limit {policy.max_concurrency}"
                )

    # -----------------------------------------------------------------------
    # 4. Evaluate Rate Limits
    # -----------------------------------------------------------------------
    for policy in applicable_rate_policies:
        window_start = current_time - timedelta(seconds=policy.window_seconds)
        rl_stmt = select(func.count(RateLimitRecord.id)).where(
            RateLimitRecord.policy_id == policy.id,
            RateLimitRecord.recorded_at >= window_start,
        )
        usage_count = (await session.execute(rl_stmt)).scalar() or 0
        if usage_count >= policy.max_requests:
            return False, (
                f"Rate limit reached for policy '{policy.target_type}:{policy.target_id}': "
                f"{usage_count} requests in {policy.window_seconds}s >= limit {policy.max_requests}"
            )

    # -----------------------------------------------------------------------
    # 5. Admitted: Record Rate Limit Usage
    # -----------------------------------------------------------------------
    for policy in applicable_rate_policies:
        record = RateLimitRecord(
            policy_id=policy.id,
            execution_id=execution.id,
            recorded_at=current_time,
        )
        session.add(record)

    return True, None


async def evaluate_execution_admission(
    session: AsyncSession,
    execution_id: int,
    now: datetime | None = None,
) -> AdmissionResult:
    """Non-mutating evaluation of execution admission against all applicable policies."""
    current_time = now or datetime.now(timezone.utc)
    execution = await session.get(Execution, int(execution_id))
    if not execution:
        return AdmissionResult(admitted=False, reason=f"Execution {execution_id} not found")

    context = await resolve_execution_context(session, execution)
    workflow_id = context.get("workflow_id")
    workflow_name = context.get("workflow_name")
    task_type = context.get("task_type")
    category = context.get("category")

    active_statuses = ("CLAIMED", "RUNNING")

    # Check workflow concurrency
    if workflow_id is not None:
        policy = await get_concurrency_limit_policy(session, "WORKFLOW", str(workflow_id))
        if not policy and workflow_name:
            policy = await get_concurrency_limit_policy(session, "WORKFLOW", workflow_name)
        if policy and policy.is_enabled:
            wf_stmt = (
                select(func.count(Execution.id))
                .join(WorkflowTaskExecution, WorkflowTaskExecution.execution_id == Execution.id)
                .join(WorkflowRun, WorkflowRun.id == WorkflowTaskExecution.workflow_run_id)
                .where(
                    WorkflowRun.workflow_id == workflow_id,
                    Execution.status.in_(active_statuses),
                    Execution.id != execution.id,
                    or_(
                        Execution.lease_until.is_(None),
                        Execution.lease_until >= current_time,
                    ),
                )
            )
            count = (await session.execute(wf_stmt)).scalar() or 0
            if count >= policy.max_concurrency:
                return AdmissionResult(
                    admitted=False,
                    reason=f"Workflow concurrency limit reached ({count}/{policy.max_concurrency})",
                    policy_type="CONCURRENCY",
                    policy_target=f"WORKFLOW:{policy.target_id}",
                    current_count=count,
                    limit=policy.max_concurrency,
                )

    # Check task type concurrency
    if task_type:
        policy = await get_concurrency_limit_policy(session, "TASK_TYPE", task_type)
        if policy and policy.is_enabled:
            tt_stmt = (
                select(func.count(Execution.id))
                .join(WorkflowTaskExecution, WorkflowTaskExecution.execution_id == Execution.id)
                .join(WorkflowTask, WorkflowTask.id == WorkflowTaskExecution.workflow_task_id)
                .where(
                    WorkflowTask.task_type == task_type,
                    Execution.status.in_(active_statuses),
                    Execution.id != execution.id,
                    or_(
                        Execution.lease_until.is_(None),
                        Execution.lease_until >= current_time,
                    ),
                )
            )
            count = (await session.execute(tt_stmt)).scalar() or 0
            if count >= policy.max_concurrency:
                return AdmissionResult(
                    admitted=False,
                    reason=f"Task type concurrency limit reached ({count}/{policy.max_concurrency})",
                    policy_type="CONCURRENCY",
                    policy_target=f"TASK_TYPE:{task_type}",
                    current_count=count,
                    limit=policy.max_concurrency,
                )

    # Check category rate limit
    if category:
        policy = await get_rate_limit_policy(session, "CATEGORY", category)
        if not policy:
            policy = await get_rate_limit_policy(session, "JOB_CATEGORY", category)
        if policy and policy.is_enabled:
            window_start = current_time - timedelta(seconds=policy.window_seconds)
            rl_stmt = select(func.count(RateLimitRecord.id)).where(
                RateLimitRecord.policy_id == policy.id,
                RateLimitRecord.recorded_at >= window_start,
            )
            count = (await session.execute(rl_stmt)).scalar() or 0
            if count >= policy.max_requests:
                return AdmissionResult(
                    admitted=False,
                    reason=f"Rate limit reached for category '{category}' ({count}/{policy.max_requests})",
                    policy_type="RATE_LIMIT",
                    policy_target=f"CATEGORY:{category}",
                    current_count=count,
                    limit=policy.max_requests,
                )

    return AdmissionResult(admitted=True)


async def cleanup_expired_rate_limit_records(
    session: AsyncSession,
    now: datetime | None = None,
    buffer_seconds: int = 3600,
) -> int:
    """Prune historical rate limit records older than the cutoff threshold."""
    current_time = now or datetime.now(timezone.utc)
    cutoff = current_time - timedelta(seconds=buffer_seconds)
    from sqlalchemy import delete

    stmt = delete(RateLimitRecord).where(RateLimitRecord.recorded_at < cutoff)
    result = await session.execute(stmt)
    return result.rowcount or 0

