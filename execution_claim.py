"""Database-backed execution claiming used by queue workers.

Redis delivers work notifications, but PostgreSQL is the authority for whether a
worker owns an execution.  The conditional UPDATE below is deliberately a
single statement so PostgreSQL can serialize competing claim attempts.
"""

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from models import Execution


@asynccontextmanager
async def _atomic_session(session: AsyncSession):
    """Ensure atomic transaction execution whether session is already in a transaction or not."""
    if session.in_transaction():
        yield session
        await session.flush()
    else:
        async with session.begin():
            yield session


@dataclass(frozen=True)
class ExecutionClaim:
    execution_id: int
    worker_id: str
    lease_until: datetime


@dataclass(frozen=True)
class CancellationResult:
    execution_id: int
    cancelled: bool
    status: str
    message: str | None = None


async def claim_execution(
    session: AsyncSession,
    execution_id: int,
    worker_id: str,
    lease_duration: timedelta,
    now: datetime | None = None,
    redis_client=None,
) -> ExecutionClaim | None:
    """Atomically claim a queued execution, enforcing concurrency and rate limits.

    PostgreSQL row locking serializes competing claim attempts on the Execution
    row and any applicable policy rows.
    """
    current_time = now or datetime.now(timezone.utc)
    lease_until = current_time + lease_duration

    async with _atomic_session(session):
        row = await session.get(Execution, int(execution_id), with_for_update=True)
        if row is None or row.status != "QUEUED":
            return None
        if row.available_at is not None and row.available_at > current_time:
            return None

        from concurrency_policy import check_and_record_admission
        admitted, reason = await check_and_record_admission(
            session, row, now=current_time
        )
        if not admitted:
            return None

        row.status = "CLAIMED"
        row.worker_id = worker_id
        row.lease_until = lease_until
        claimed_id = row.id
        claimed_worker_id = row.worker_id
        claimed_lease_until = row.lease_until
        claimed_cat = row.category
        claimed_created = row.created_at

    claim = ExecutionClaim(
        execution_id=claimed_id,
        worker_id=claimed_worker_id,
        lease_until=claimed_lease_until,
    )

    from metrics import record_claim_outcome
    record_claim_outcome("claimed", category=claimed_cat, created_at=claimed_created)

    from events import EVENT_EXECUTION_CLAIMED, create_event, publish_event
    publish_event(
        redis_client,
        create_event(
            EVENT_EXECUTION_CLAIMED,
            execution_id=claimed_id,
            status="CLAIMED",
            worker_id=claimed_worker_id,
            metadata={"lease_until": claimed_lease_until.isoformat()},
        ),
    )
    return claim


async def claim_next_execution(
    session: AsyncSession,
    worker_id: str,
    lease_duration: timedelta,
    now: datetime | None = None,
    redis_client=None,
) -> ExecutionClaim | None:
    """Atomically claim the highest-priority runnable QUEUED execution.

    Ordering rule:
    1. Execution.priority DESC (highest priority first)
    2. Execution.id ASC (deterministic FIFO tie-breaker)

    Enforces:
    - Status must be QUEUED
    - available_at is None or <= now
    - Concurrency and rate limit policies (via check_and_record_admission)
    - Concurrency-safe via row locking (with_for_update, skip_locked=True)
    """
    current_time = now or datetime.now(timezone.utc)
    lease_until = current_time + lease_duration
    claimed_claim = None

    async with _atomic_session(session):
        stmt = (
            select(Execution)
            .where(
                Execution.status == "QUEUED",
                or_(
                    Execution.available_at.is_(None),
                    Execution.available_at <= current_time,
                ),
            )
            .order_by(
                Execution.priority.desc(),
                Execution.id.asc(),
            )
            .with_for_update(skip_locked=True)
        )
        result = await session.execute(stmt)
        candidates = result.scalars().all()

        from concurrency_policy import check_and_record_admission

        for row in candidates:
            admitted, reason = await check_and_record_admission(
                session, row, now=current_time
            )
            if not admitted:
                continue

            row.status = "CLAIMED"
            row.worker_id = worker_id
            row.lease_until = lease_until
            claimed_claim = ExecutionClaim(
                execution_id=row.id,
                worker_id=row.worker_id,
                lease_until=row.lease_until,
            )
            claimed_cat = row.category
            claimed_created = row.created_at
            break

    from metrics import record_claim_outcome
    if claimed_claim is not None:
        record_claim_outcome("claimed", category=claimed_cat, created_at=claimed_created)
        from events import EVENT_EXECUTION_CLAIMED, create_event, publish_event
        publish_event(
            redis_client,
            create_event(
                EVENT_EXECUTION_CLAIMED,
                execution_id=claimed_claim.execution_id,
                status="CLAIMED",
                worker_id=claimed_claim.worker_id,
                metadata={"lease_until": claimed_claim.lease_until.isoformat()},
            ),
        )
    else:
        record_claim_outcome("empty")
    return claimed_claim


async def start_execution(
    session: AsyncSession,
    claim: ExecutionClaim,
    redis_client=None,
) -> bool:
    """Transition an execution to RUNNING only for its exact claim owner."""
    async with _atomic_session(session):
        result = await session.execute(
            update(Execution)
            .where(
                Execution.id == claim.execution_id,
                Execution.status == "CLAIMED",
                Execution.worker_id == claim.worker_id,
                Execution.lease_until == claim.lease_until,
            )
            .values(status="RUNNING", started_at=datetime.now(timezone.utc))
            .returning(Execution.id, Execution.category)
        )
        row = result.first()
        started = row is not None

    if started:
        from metrics import record_execution_transition
        record_execution_transition("running", category=row[1])
        from events import EVENT_EXECUTION_RUNNING, create_event, publish_event
        publish_event(
            redis_client,
            create_event(
                EVENT_EXECUTION_RUNNING,
                execution_id=claim.execution_id,
                status="RUNNING",
                worker_id=claim.worker_id,
            ),
        )
    return started


async def complete_execution(
    session: AsyncSession,
    claim: ExecutionClaim,
    redis_client=None,
) -> bool:
    """Mark an execution successful only when the original claim still owns it."""
    async with _atomic_session(session):
        result = await session.execute(
            update(Execution)
            .where(
                Execution.id == claim.execution_id,
                Execution.status == "RUNNING",
                Execution.worker_id == claim.worker_id,
                Execution.lease_until == claim.lease_until,
            )
            .values(
                status="SUCCEEDED",
                lease_until=None,
                finished_at=datetime.now(timezone.utc),
            )
            .returning(Execution.id, Execution.started_at, Execution.finished_at, Execution.category)
        )
        row = result.first()
        succeeded = row is not None

    if not succeeded:
        return False

    from metrics import record_execution_transition
    record_execution_transition("succeeded", category=row[3], started_at=row[1], finished_at=row[2])

    from events import EVENT_EXECUTION_SUCCEEDED, create_event, publish_event
    publish_event(
        redis_client,
        create_event(
            EVENT_EXECUTION_SUCCEEDED,
            execution_id=claim.execution_id,
            status="SUCCEEDED",
            worker_id=claim.worker_id,
        ),
    )

    from workflow_engine import advance_workflow_on_execution_terminal
    await advance_workflow_on_execution_terminal(
        session, claim.execution_id, redis_client=redis_client
    )
    return True


async def fail_execution(
    session: AsyncSession,
    claim: ExecutionClaim,
    error_summary: str | None = None,
    redis_client=None,
) -> bool:
    """Mark an execution failed only when the original claim still owns it."""
    async with _atomic_session(session):
        result = await session.execute(
            update(Execution)
            .where(
                Execution.id == claim.execution_id,
                Execution.status == "RUNNING",
                Execution.worker_id == claim.worker_id,
                Execution.lease_until == claim.lease_until,
            )
            .values(
                status="FAILED",
                lease_until=None,
                finished_at=datetime.now(timezone.utc),
                error_summary=error_summary,
            )
            .returning(Execution.id, Execution.started_at, Execution.finished_at, Execution.category)
        )
        row = result.first()
        failed = row is not None

    if failed:
        from metrics import record_execution_transition
        record_execution_transition("failed", category=row[3], started_at=row[1], finished_at=row[2])
        from events import EVENT_EXECUTION_FAILED, create_event, publish_event
        publish_event(
            redis_client,
            create_event(
                EVENT_EXECUTION_FAILED,
                execution_id=claim.execution_id,
                status="FAILED",
                worker_id=claim.worker_id,
                metadata={"error_summary": error_summary},
            ),
        )
    return failed


async def cancel_execution(
    session: AsyncSession,
    execution_id: int,
    reason: str | None = None,
    redis_client=None,
    now: datetime | None = None,
    advance_workflow: bool = True,
) -> CancellationResult:
    """Atomically cancel an execution.

    Semantics:
    - If already CANCELLED: returns cancelled=True (idempotent).
    - If terminal (SUCCEEDED, DEAD_LETTERED): returns cancelled=False (terminality preserved).
    - If QUEUED, RETRY_WAIT, CLAIMED, RUNNING, FAILED: transitions to CANCELLED.
      - Sets status="CANCELLED"
      - Sets finished_at=now
      - Clears lease_until=None and available_at=None
      - Sets error_summary=reason
    - If linked to a WorkflowTaskExecution and advance_workflow is True, synchronizes task execution and triggers DAG resolution.
    - Concurrency-safe via row locking (with_for_update=True).
    """
    current_time = now or datetime.now(timezone.utc)
    cancelled_now = False

    async with _atomic_session(session):
        row = await session.get(Execution, int(execution_id), with_for_update=True)
        if row is None:
            return CancellationResult(
                execution_id=int(execution_id),
                cancelled=False,
                status="NOT_FOUND",
                message="Execution not found",
            )

        if row.status == "CANCELLED":
            return CancellationResult(
                execution_id=row.id,
                cancelled=True,
                status="CANCELLED",
                message="Execution already cancelled",
            )

        if row.status in ("SUCCEEDED", "DEAD_LETTERED"):
            return CancellationResult(
                execution_id=row.id,
                cancelled=False,
                status=row.status,
                message=f"Cannot cancel execution in terminal state '{row.status}'",
            )

        row.status = "CANCELLED"
        row.lease_until = None
        row.available_at = None
        row.finished_at = current_time
        row.error_summary = reason or "Execution cancelled"
        cancelled_now = True

    if cancelled_now:
        from metrics import EXECUTION_CANCELLED_TOTAL, record_execution_transition
        EXECUTION_CANCELLED_TOTAL.inc()
        record_execution_transition("cancelled", category=row.category)

        from events import EVENT_EXECUTION_CANCELLED, create_event, publish_event
        publish_event(
            redis_client,
            create_event(
                EVENT_EXECUTION_CANCELLED,
                execution_id=int(execution_id),
                status="CANCELLED",
                metadata={"reason": reason or "Execution cancelled"},
            ),
        )

        if advance_workflow:
            from workflow_engine import advance_workflow_on_execution_terminal

            await advance_workflow_on_execution_terminal(
                session, int(execution_id), redis_client=redis_client
            )

    return CancellationResult(
        execution_id=int(execution_id),
        cancelled=True,
        status="CANCELLED",
        message="Execution cancelled successfully",
    )


async def update_execution_priority(
    session: AsyncSession,
    execution_id: int,
    priority: int,
    redis_client=None,
) -> Execution | None:
    """Update priority of a QUEUED execution. Returns updated Execution or None if not QUEUED/not found."""
    updated_row = None
    async with _atomic_session(session):
        row = await session.get(Execution, int(execution_id), with_for_update=True)
        if row is None or row.status != "QUEUED":
            return None
        row.priority = int(priority)
        updated_row = row

    if updated_row is not None:
        from events import EVENT_EXECUTION_PRIORITY_CHANGED, create_event, publish_event
        publish_event(
            redis_client,
            create_event(
                EVENT_EXECUTION_PRIORITY_CHANGED,
                execution_id=int(execution_id),
                status=updated_row.status,
                metadata={"priority": int(priority)},
            ),
        )
    return updated_row

