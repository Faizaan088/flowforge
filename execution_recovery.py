"""Atomic recovery of executions whose ownership lease has expired."""

from datetime import datetime, timezone

from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession

from models import Execution


RECOVERABLE_STATUSES = ("CLAIMED", "RUNNING")


async def recover_expired_executions(
    session: AsyncSession,
    now: datetime | None = None,
    redis_client=None,
) -> list[int]:
    """Return expired claimed/running work to QUEUED in one transaction."""
    recovery_time = now or datetime.now(timezone.utc)

    async with session.begin():
        result = await session.execute(
            update(Execution)
            .where(
                Execution.status.in_(RECOVERABLE_STATUSES),
                Execution.lease_until.is_not(None),
                Execution.lease_until < recovery_time,
            )
            .values(
                status="QUEUED",
                worker_id=None,
                lease_until=None,
                attempt=func.coalesce(Execution.attempt, 0) + 1,
            )
            .returning(Execution.id)
        )
        recovered_ids = list(result.scalars())

    if recovered_ids:
        from metrics import EXECUTION_RECOVERED_TOTAL
        EXECUTION_RECOVERED_TOTAL.inc(len(recovered_ids))

    from events import EVENT_EXECUTION_RECOVERED, create_event, publish_event
    for eid in recovered_ids:
        publish_event(
            redis_client,
            create_event(
                EVENT_EXECUTION_RECOVERED,
                execution_id=eid,
                status="QUEUED",
                metadata={"recovered_lease_expired": True},
            ),
        )

    return recovered_ids
