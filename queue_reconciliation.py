"""Recovery-safe replay of durable queued executions to Redis."""

from dataclasses import dataclass

from redis.exceptions import RedisError
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from models import Execution


QUEUE_NAME = "flowforge:queue"
DEFAULT_RECONCILIATION_BATCH_SIZE = 100


@dataclass(frozen=True)
class ReconciliationResult:
    found: int
    enqueued: int
    redis_error: str | None = None

    def as_dict(self) -> dict[str, int | str | None]:
        return {
            "found": self.found,
            "enqueued": self.enqueued,
            "redis_error": self.redis_error,
        }


def enqueue_execution(redis_client, execution_id: int) -> None:
    redis_client.lpush(QUEUE_NAME, execution_id)


def deliver_executions_to_redis(
    redis_client, execution_ids: list[int]
) -> ReconciliationResult:
    """Deliver a specific list of execution IDs to Redis."""
    enqueued = 0
    for execution_id in execution_ids:
        try:
            enqueue_execution(redis_client, execution_id)
            enqueued += 1
        except RedisError as error:
            return ReconciliationResult(
                found=len(execution_ids),
                enqueued=enqueued,
                redis_error=str(error),
            )
    return ReconciliationResult(found=len(execution_ids), enqueued=enqueued)


async def reconcile_queued_executions(
    session: AsyncSession,
    redis_client,
    batch_size: int = DEFAULT_RECONCILIATION_BATCH_SIZE,
) -> ReconciliationResult:
    """Replay durable QUEUED execution IDs to Redis without changing PostgreSQL.

    Redis can accept duplicate IDs; the worker's PostgreSQL claim is the
    execution correctness boundary.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    summary = await session.execute(
        select(func.count(Execution.id), func.max(Execution.id)).where(
            Execution.status == "QUEUED"
        )
    )
    found, upper_bound_id = summary.one()
    if upper_bound_id is None:
        return ReconciliationResult(found=0, enqueued=0)

    enqueued = 0
    last_seen_id = 0

    while last_seen_id < upper_bound_id:
        result = await session.execute(
            select(Execution.id)
            .where(
                Execution.status == "QUEUED",
                Execution.id > last_seen_id,
                Execution.id <= upper_bound_id,
            )
            .order_by(Execution.id)
            .limit(batch_size)
        )
        execution_ids = result.scalars().all()
        if not execution_ids:
            break

        for execution_id in execution_ids:
            try:
                enqueue_execution(redis_client, execution_id)
                enqueued += 1
            except RedisError as error:
                return ReconciliationResult(
                    found=found,
                    enqueued=enqueued,
                    redis_error=str(error),
                )
        last_seen_id = execution_ids[-1]

    return ReconciliationResult(found=found, enqueued=enqueued)
