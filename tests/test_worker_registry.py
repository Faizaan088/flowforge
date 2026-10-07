"""PostgreSQL integration tests for durable worker registration."""

import asyncio
import os
from datetime import datetime, timezone

import pytest
import pytest_asyncio


TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL must point to an isolated PostgreSQL database",
)

if TEST_DATABASE_URL:
    from sqlalchemy import func, select, update
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

    from database import Base  # noqa: E402
    from models import Worker  # noqa: E402
    from worker_registry import (  # noqa: E402
        UnknownWorkerError,
        heartbeat_worker,
        register_worker,
    )


@pytest_asyncio.fixture
async def session_factory():
    engine = create_async_engine(TEST_DATABASE_URL, echo=False, poolclass=NullPool)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
        await engine.dispose()


@pytest_asyncio.fixture(autouse=True)
async def clear_workers(session_factory):
    async with session_factory() as session:
        await session.execute(Worker.__table__.delete())
        await session.commit()


@pytest.mark.asyncio
async def test_registration_creates_worker_with_utc_heartbeat(session_factory):
    before = datetime.now(timezone.utc)

    async with session_factory() as session:
        registration = await register_worker(session, "worker-a")

    assert registration.worker_id == "worker-a"
    assert registration.status == "ACTIVE"
    assert registration.created_at.tzinfo is not None
    assert registration.last_heartbeat_at.tzinfo is not None
    assert registration.last_heartbeat_at >= before


@pytest.mark.asyncio
async def test_duplicate_registration_updates_existing_worker(session_factory):
    async with session_factory() as session:
        first = await register_worker(session, "worker-a")
    async with session_factory() as session:
        second = await register_worker(session, "worker-a")

    assert second.created_at == first.created_at
    assert second.last_heartbeat_at >= first.last_heartbeat_at

    async with session_factory() as session:
        count = await session.scalar(
            select(func.count(Worker.worker_id)).where(Worker.worker_id == "worker-a")
        )
    assert count == 1


@pytest.mark.asyncio
async def test_concurrent_registration_creates_one_worker(session_factory):
    async def register_once():
        async with session_factory() as session:
            return await register_worker(session, "worker-a")

    first, second = await asyncio.gather(register_once(), register_once())

    assert {first.worker_id, second.worker_id} == {"worker-a"}
    async with session_factory() as session:
        count = await session.scalar(
            select(func.count(Worker.worker_id)).where(Worker.worker_id == "worker-a")
        )
    assert count == 1


@pytest.mark.asyncio
async def test_heartbeat_updates_existing_worker(session_factory):
    async with session_factory() as session:
        await register_worker(session, "worker-a")
        await session.execute(
            update(Worker)
            .where(Worker.worker_id == "worker-a")
            .values(last_heartbeat_at=datetime(2000, 1, 1, tzinfo=timezone.utc))
        )
        await session.commit()

    async with session_factory() as session:
        heartbeat_at = await heartbeat_worker(session, "worker-a")

    assert heartbeat_at > datetime(2000, 1, 1, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_unknown_worker_heartbeat_fails_explicitly(session_factory):
    async with session_factory() as session:
        with pytest.raises(UnknownWorkerError, match="not registered"):
            await heartbeat_worker(session, "missing-worker")
