"""Durable worker registration, liveness timestamps, and lifecycle management."""

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from models import Execution, Worker


@asynccontextmanager
async def _atomic_session(session: AsyncSession):
    """Ensure atomic transaction execution whether session is already in a transaction or not."""
    if session.in_transaction():
        yield session
        await session.flush()
    else:
        async with session.begin():
            yield session


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
    """Create or refresh a worker record. Preserves existing status (e.g. DISABLED)."""
    now = datetime.now(timezone.utc)

    async with _atomic_session(session):
        result = await session.execute(
            insert(Worker)
            .values(
                worker_id=worker_id,
                status="ACTIVE",
                last_heartbeat_at=now,
            )
            .on_conflict_do_update(
                index_elements=[Worker.worker_id],
                set_={"last_heartbeat_at": now},
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

    async with _atomic_session(session):
        result = await session.execute(
            update(Worker)
            .where(Worker.worker_id == worker_id)
            .values(last_heartbeat_at=now)
            .returning(Worker.last_heartbeat_at)
        )
        heartbeat_at = result.scalar_one_or_none()

    if heartbeat_at is None:
        from metrics import WORKER_HEARTBEAT_FAILURES_TOTAL
        WORKER_HEARTBEAT_FAILURES_TOTAL.labels(reason="not_found").inc()
        raise UnknownWorkerError(f"Worker {worker_id!r} is not registered")

    from metrics import WORKER_HEARTBEATS_TOTAL
    WORKER_HEARTBEATS_TOTAL.labels(status="active").inc()
    return heartbeat_at


async def is_worker_disabled(session: AsyncSession, worker_id: str) -> bool:
    """Check if worker exists and is marked DISABLED."""
    worker = await session.get(Worker, worker_id)
    return worker is not None and worker.status == "DISABLED"


async def disable_worker(
    session: AsyncSession,
    worker_id: str,
    redis_client=None,
) -> Worker:
    """Set worker status to DISABLED and handle in-flight work safely.

    Safe handling of in-flight work:
    - Executions in CLAIMED state are immediately returned to QUEUED so other healthy workers can pick them up.
    - Executions in RUNNING state are marked FAILED with an explicit reason and evaluated for retry/dead-lettering.
    No tasks are silently lost or allowed to complete successfully under a disabled worker.
    """
    now = datetime.now(timezone.utc)

    async with _atomic_session(session):
        worker = await session.get(Worker, worker_id)
        if worker is None:
            raise UnknownWorkerError(f"Worker {worker_id!r} is not registered")

        worker.status = "DISABLED"

        # 1. Safely re-queue CLAIMED tasks
        claimed_result = await session.execute(
            select(Execution).where(
                Execution.worker_id == worker_id,
                Execution.status == "CLAIMED",
            ).with_for_update()
        )
        claimed_executions = claimed_result.scalars().all()
        claimed_ids = []
        for exc in claimed_executions:
            exc.status = "QUEUED"
            exc.worker_id = None
            exc.lease_until = None
            claimed_ids.append(exc.id)

        # 2. Safely transition RUNNING tasks to FAILED
        running_result = await session.execute(
            select(Execution).where(
                Execution.worker_id == worker_id,
                Execution.status == "RUNNING",
            ).with_for_update()
        )
        running_executions = running_result.scalars().all()
        running_ids = []
        for exc in running_executions:
            exc.status = "FAILED"
            exc.lease_until = None
            exc.finished_at = now
            exc.error_summary = f"Worker {worker_id} was disabled while execution was in flight"
            running_ids.append(exc.id)

        # 3. Evaluate retry or dead-letter for all failed in-flight tasks
        from execution_retry import transition_failed_execution
        for eid in running_ids:
            await transition_failed_execution(session, eid, redis_client=redis_client)

    # Publish events for reclaimed tasks
    from events import EVENT_EXECUTION_RECOVERED, create_event, publish_event
    for eid in claimed_ids:
        publish_event(
            redis_client,
            create_event(
                EVENT_EXECUTION_RECOVERED,
                execution_id=eid,
                status="QUEUED",
                metadata={"reason": f"Worker {worker_id} disabled before execution started"},
            ),
        )

    return worker


async def enable_worker(session: AsyncSession, worker_id: str) -> Worker:
    """Set worker status to ACTIVE, enabling it to claim and execute new tasks."""
    async with _atomic_session(session):
        worker = await session.get(Worker, worker_id)
        if worker is None:
            raise UnknownWorkerError(f"Worker {worker_id!r} is not registered")
        worker.status = "ACTIVE"
    return worker
