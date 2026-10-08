"""Durable execution retry and dead-letter handling.

PostgreSQL is the source of truth for execution states:
FAILED -> RETRY_WAIT when retryable (attempt < max_retries)
FAILED -> DEAD_LETTERED when retries are exhausted (attempt >= max_retries)
RETRY_WAIT -> QUEUED when eligible (available_at is None or available_at <= now)
"""

import os
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from execution_claim import ExecutionClaim, fail_execution
from models import Execution


DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY = timedelta(
    seconds=float(os.getenv("EXECUTION_RETRY_BASE_DELAY_SECONDS", "10"))
)
DEFAULT_BACKOFF_FACTOR = 2.0


def compute_exponential_backoff(
    attempt: int,
    base_delay: timedelta = DEFAULT_BASE_DELAY,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    max_delay: timedelta | None = None,
) -> timedelta:
    """Calculate deterministic exponential backoff delay based on retry attempt.

    Formula: base_delay * (backoff_factor ** attempt)
    Deterministic without jitter.
    """
    exponent = max(0, int(attempt))
    multiplier = backoff_factor ** exponent
    delay_seconds = base_delay.total_seconds() * multiplier
    delay = timedelta(seconds=delay_seconds)
    if max_delay is not None and delay > max_delay:
        return max_delay
    return delay


async def transition_failed_execution(
    session: AsyncSession,
    execution_id: int,
    base_delay: timedelta | None = None,
    max_retries: int | None = None,
    now: datetime | None = None,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    max_delay: timedelta | None = None,
    retry_delay: timedelta | None = None,
    redis_client=None,
) -> str | None:
    """Evaluate a FAILED execution: move to RETRY_WAIT if retryable, or DEAD_LETTERED if exhausted.

    Calculates deterministic exponential backoff based on retry attempt:
    available_at = now + (base_delay * (backoff_factor ** attempt))
    """
    current_time = now or datetime.now(timezone.utc)
    effective_base_delay = (
        base_delay
        if base_delay is not None
        else (retry_delay if retry_delay is not None else DEFAULT_BASE_DELAY)
    )

    async with session.begin():
        row = await session.get(Execution, int(execution_id), with_for_update=True)
        if row is None or row.status != "FAILED":
            return None

        limit = (
            max_retries
            if max_retries is not None
            else (row.max_retries if row.max_retries is not None else DEFAULT_MAX_RETRIES)
        )
        current_attempt = row.attempt or 0

        if current_attempt < limit:
            delay = compute_exponential_backoff(
                attempt=current_attempt,
                base_delay=effective_base_delay,
                backoff_factor=backoff_factor,
                max_delay=max_delay,
            )
            row.status = "RETRY_WAIT"
            row.available_at = current_time + delay
            return "RETRY_WAIT"
        else:
            row.status = "DEAD_LETTERED"
            row.available_at = None
            row.finished_at = current_time
            outcome = "DEAD_LETTERED"

    if outcome == "DEAD_LETTERED":
        from workflow_engine import advance_workflow_on_execution_terminal
        await advance_workflow_on_execution_terminal(
            session, int(execution_id), redis_client=redis_client
        )

    return outcome


async def process_failed_executions(
    session: AsyncSession,
    base_delay: timedelta | None = None,
    now: datetime | None = None,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    max_delay: timedelta | None = None,
    retry_delay: timedelta | None = None,
    redis_client=None,
) -> dict[str, list[int]]:
    """Partition all FAILED executions into RETRY_WAIT or DEAD_LETTERED atomically."""
    current_time = now or datetime.now(timezone.utc)
    effective_base_delay = (
        base_delay
        if base_delay is not None
        else (retry_delay if retry_delay is not None else DEFAULT_BASE_DELAY)
    )

    async with session.begin():
        result = await session.execute(
            select(Execution)
            .where(Execution.status == "FAILED")
            .with_for_update()
        )
        failed_executions = result.scalars().all()

        retry_wait_ids = []
        dead_lettered_ids = []

        for row in failed_executions:
            limit = (
                row.max_retries
                if row.max_retries is not None
                else DEFAULT_MAX_RETRIES
            )
            current_attempt = row.attempt or 0

            if current_attempt < limit:
                delay = compute_exponential_backoff(
                    attempt=current_attempt,
                    base_delay=effective_base_delay,
                    backoff_factor=backoff_factor,
                    max_delay=max_delay,
                )
                row.status = "RETRY_WAIT"
                row.available_at = current_time + delay
                retry_wait_ids.append(row.id)
            else:
                row.status = "DEAD_LETTERED"
                row.available_at = None
                row.finished_at = current_time
                dead_lettered_ids.append(row.id)

    if dead_lettered_ids:
        from workflow_engine import advance_workflow_on_execution_terminal
        for dead_id in dead_lettered_ids:
            await advance_workflow_on_execution_terminal(
                session, dead_id, redis_client=redis_client
            )

    return {
        "retry_wait": retry_wait_ids,
        "dead_lettered": dead_lettered_ids,
    }


async def requeue_retry_execution(
    session: AsyncSession,
    execution_id: int,
    now: datetime | None = None,
) -> bool:
    """Transition a single eligible RETRY_WAIT execution to QUEUED.

    Preserves attempt semantics consistently with lease recovery by incrementing attempt.
    """
    requeue_time = now or datetime.now(timezone.utc)

    async with session.begin():
        result = await session.execute(
            update(Execution)
            .where(
                Execution.id == int(execution_id),
                Execution.status == "RETRY_WAIT",
                or_(
                    Execution.available_at.is_(None),
                    Execution.available_at <= requeue_time,
                ),
            )
            .values(
                status="QUEUED",
                worker_id=None,
                lease_until=None,
                available_at=None,
                attempt=func.coalesce(Execution.attempt, 0) + 1,
            )
            .returning(Execution.id)
        )
        return result.scalar_one_or_none() is not None


async def requeue_eligible_retries(
    session: AsyncSession,
    now: datetime | None = None,
) -> list[int]:
    """Transition all eligible RETRY_WAIT executions back to QUEUED."""
    requeue_time = now or datetime.now(timezone.utc)

    async with session.begin():
        result = await session.execute(
            update(Execution)
            .where(
                Execution.status == "RETRY_WAIT",
                or_(
                    Execution.available_at.is_(None),
                    Execution.available_at <= requeue_time,
                ),
            )
            .values(
                status="QUEUED",
                worker_id=None,
                lease_until=None,
                available_at=None,
                attempt=func.coalesce(Execution.attempt, 0) + 1,
            )
            .returning(Execution.id)
        )
        requeued_ids = list(result.scalars())

    return requeued_ids


async def fail_and_evaluate_retry(
    session: AsyncSession,
    claim: ExecutionClaim,
    error_summary: str | None = None,
    base_delay: timedelta | None = None,
    max_retries: int | None = None,
    now: datetime | None = None,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
    max_delay: timedelta | None = None,
    retry_delay: timedelta | None = None,
    redis_client=None,
) -> str | None:
    """Transition RUNNING -> FAILED -> RETRY_WAIT or DEAD_LETTERED for an owned claim."""
    failed = await fail_execution(session, claim, error_summary=error_summary, redis_client=redis_client)
    if not failed:
        return None
    return await transition_failed_execution(
        session,
        claim.execution_id,
        base_delay=base_delay,
        max_retries=max_retries,
        now=now,
        backoff_factor=backoff_factor,
        max_delay=max_delay,
        retry_delay=retry_delay,
        redis_client=redis_client,
    )
