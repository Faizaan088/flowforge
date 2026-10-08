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
) -> ExecutionClaim | None:
    """Atomically claim a queued execution, or return ``None`` if unavailable.

    The status predicate is part of the UPDATE rather than a preceding read.
    With PostgreSQL's row locking and READ COMMITTED semantics, concurrent
    claimers of the same row wait as necessary and only one can update QUEUED
    to CLAIMED.
    """
    lease_until = datetime.now(timezone.utc) + lease_duration

    async with session.begin():
        result = await session.execute(
            update(Execution)
            .where(
                Execution.id == int(execution_id),
                Execution.status == "QUEUED",
            )
            .values(
                status="CLAIMED",
                worker_id=worker_id,
                lease_until=lease_until,
            )
            .returning(Execution.id, Execution.worker_id, Execution.lease_until)
        )
        claimed = result.one_or_none()

    if claimed is None:
        return None

    return ExecutionClaim(
        execution_id=claimed.id,
        worker_id=claimed.worker_id,
        lease_until=claimed.lease_until,
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
