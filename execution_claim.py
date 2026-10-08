"""Database-backed execution claiming used by queue workers.

Redis delivers work notifications, but PostgreSQL is the authority for whether a
worker owns an execution.  The conditional UPDATE below is deliberately a
single statement so PostgreSQL can serialize competing claim attempts.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from models import Execution


@dataclass(frozen=True)
class ExecutionClaim:
    execution_id: int
    worker_id: str
    lease_until: datetime


async def claim_execution(
    session: AsyncSession,
    execution_id: int,
    worker_id: str,
    lease_duration: timedelta,
    now: datetime | None = None,
) -> ExecutionClaim | None:
    """Atomically claim a queued execution, enforcing concurrency and rate limits.

    PostgreSQL row locking serializes competing claim attempts on the Execution
    row and any applicable policy rows.
    """
    current_time = now or datetime.now(timezone.utc)
    lease_until = current_time + lease_duration

    async with session.begin():
        row = await session.get(Execution, int(execution_id), with_for_update=True)
        if row is None or row.status != "QUEUED":
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

    return ExecutionClaim(
        execution_id=claimed_id,
        worker_id=claimed_worker_id,
        lease_until=claimed_lease_until,
    )


async def start_execution(
    session: AsyncSession,
    claim: ExecutionClaim,
) -> bool:
    """Transition an execution to RUNNING only for its exact claim owner."""
    async with session.begin():
        result = await session.execute(
            update(Execution)
            .where(
                Execution.id == claim.execution_id,
                Execution.status == "CLAIMED",
                Execution.worker_id == claim.worker_id,
                Execution.lease_until == claim.lease_until,
            )
            .values(status="RUNNING", started_at=datetime.now(timezone.utc))
            .returning(Execution.id)
        )
        return result.scalar_one_or_none() is not None


async def complete_execution(
    session: AsyncSession,
    claim: ExecutionClaim,
    redis_client=None,
) -> bool:
    """Mark an execution successful only when the original claim still owns it."""
    async with session.begin():
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
            .returning(Execution.id)
        )
        succeeded = result.scalar_one_or_none() is not None

    if not succeeded:
        return False

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
    async with session.begin():
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
            .returning(Execution.id)
        )
        return result.scalar_one_or_none() is not None
