"""Durable worker registration and liveness timestamps."""

from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from models import Worker


class UnknownWorkerError(LookupError):
    pass


@dataclass(frozen=True)
class WorkerRegistration:
    worker_id: str
    created_at: datetime
    last_heartbeat_at: datetime
    status: str


async def register_worker(
    session: AsyncSession,
    worker_id: str,
) -> WorkerRegistration:
    """Create or refresh a worker record without creating duplicate IDs."""
    now = datetime.now(timezone.utc)

    async with session.begin():
        result = await session.execute(
            insert(Worker)
            .values(
                worker_id=worker_id,
                status="ACTIVE",
                last_heartbeat_at=now,
            )
            .on_conflict_do_update(
                index_elements=[Worker.worker_id],
                set_={"status": "ACTIVE", "last_heartbeat_at": now},
            )
            .returning(
                Worker.worker_id,
                Worker.created_at,
                Worker.last_heartbeat_at,
                Worker.status,
            )
        )
        registration = result.one()

    return WorkerRegistration(
        worker_id=registration.worker_id,
        created_at=registration.created_at,
        last_heartbeat_at=registration.last_heartbeat_at,
        status=registration.status,
    )


async def heartbeat_worker(session: AsyncSession, worker_id: str) -> datetime:
    """Persist a heartbeat for an existing worker."""
    now = datetime.now(timezone.utc)

    async with session.begin():
        result = await session.execute(
            update(Worker)
            .where(Worker.worker_id == worker_id)
            .values(last_heartbeat_at=now)
            .returning(Worker.last_heartbeat_at)
        )
        heartbeat_at = result.scalar_one_or_none()

    if heartbeat_at is None:
        raise UnknownWorkerError(f"Worker {worker_id!r} is not registered")
    return heartbeat_at
