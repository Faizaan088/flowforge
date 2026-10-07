"""PostgreSQL-backed tests for Redis queue reconciliation."""

import asyncio
import os
from datetime import timedelta

import pytest
import pytest_asyncio


TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL must point to an isolated PostgreSQL database",
)

if TEST_DATABASE_URL:
    from redis.exceptions import RedisError
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

    from database import Base  # noqa: E402
    from execution_claim import claim_execution  # noqa: E402
    from models import Execution  # noqa: E402
    from queue_reconciliation import (  # noqa: E402
        enqueue_execution,
        reconcile_queued_executions,
    )


class RecordingRedis:
    def __init__(self, unavailable=False, fail_after=None):
        self.entries = []
        self.unavailable = unavailable
        self.fail_after = fail_after

    def lpush(self, _queue_name, execution_id):
        if self.unavailable or (
            self.fail_after is not None and len(self.entries) >= self.fail_after
        ):
            raise RedisError("Redis is unavailable")
        self.entries.insert(0, execution_id)


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
async def clear_executions(session_factory):
    async with session_factory() as session:
        await session.execute(Execution.__table__.delete())
        await session.commit()


async def create_queued_execution(session_factory):
    async with session_factory() as session:
        execution = Execution(status="QUEUED")
        session.add(execution)
        await session.commit()
        await session.refresh(execution)
        return execution.id


async def execution_state(session_factory, execution_id):
    async with session_factory() as session:
        execution = await session.get(Execution, execution_id)
        return execution.status, execution.worker_id, execution.lease_until


@pytest.mark.asyncio
async def test_queued_execution_survives_producer_enqueue_failure(session_factory):
    execution_id = await create_queued_execution(session_factory)

    with pytest.raises(RedisError):
        enqueue_execution(RecordingRedis(unavailable=True), execution_id)

    assert await execution_state(session_factory, execution_id) == ("QUEUED", None, None)


@pytest.mark.asyncio
async def test_reconciliation_recovers_queued_execution_after_redis_returns(session_factory):
    execution_id = await create_queued_execution(session_factory)

    async with session_factory() as session:
        unavailable = await reconcile_queued_executions(
            session, RecordingRedis(unavailable=True)
        )
    assert unavailable.found == 1
    assert unavailable.enqueued == 0
    assert unavailable.redis_error == "Redis is unavailable"
    assert await execution_state(session_factory, execution_id) == ("QUEUED", None, None)

    redis_client = RecordingRedis()
    async with session_factory() as session:
        recovered = await reconcile_queued_executions(session, redis_client)
    assert recovered.found == 1
    assert recovered.enqueued == 1
    assert redis_client.entries == [execution_id]


@pytest.mark.asyncio
async def test_reconciliation_is_repeatable_and_safe_when_concurrent(session_factory):
    execution_id = await create_queued_execution(session_factory)
    redis_client = RecordingRedis()

    async def reconcile_once():
        async with session_factory() as session:
            return await reconcile_queued_executions(session, redis_client)

    first, second = await asyncio.gather(reconcile_once(), reconcile_once())

    assert (first.enqueued, second.enqueued) == (1, 1)
    assert redis_client.entries.count(execution_id) == 2
    assert await execution_state(session_factory, execution_id) == ("QUEUED", None, None)


@pytest.mark.asyncio
async def test_reconciliation_processes_queued_executions_in_multiple_batches(session_factory):
    execution_ids = [
        await create_queued_execution(session_factory)
        for _ in range(5)
    ]
    redis_client = RecordingRedis()

    async with session_factory() as session:
        result = await reconcile_queued_executions(
            session, redis_client, batch_size=2
        )

    assert result.found == 5
    assert result.enqueued == 5
    assert sorted(redis_client.entries) == execution_ids


@pytest.mark.asyncio
async def test_batch_reconciliation_stops_on_redis_failure(session_factory):
    for _ in range(5):
        await create_queued_execution(session_factory)
    redis_client = RecordingRedis(fail_after=3)

    async with session_factory() as session:
        result = await reconcile_queued_executions(
            session, redis_client, batch_size=2
        )

    assert result.found == 5
    assert result.enqueued == 3
    assert result.redis_error == "Redis is unavailable"


@pytest.mark.asyncio
async def test_restart_reconciliation_does_not_strand_queued_execution(session_factory):
    execution_id = await create_queued_execution(session_factory)

    async with session_factory() as session:
        await reconcile_queued_executions(session, RecordingRedis(unavailable=True))

    restarted_redis = RecordingRedis()
    async with session_factory() as session:
        result = await reconcile_queued_executions(session, restarted_redis)

    assert result.enqueued == 1
    assert restarted_redis.entries == [execution_id]


@pytest.mark.asyncio
async def test_duplicate_redis_entries_allow_only_one_claim(session_factory):
    execution_id = await create_queued_execution(session_factory)
    redis_client = RecordingRedis()

    async with session_factory() as session:
        await reconcile_queued_executions(session, redis_client)
    async with session_factory() as session:
        await reconcile_queued_executions(session, redis_client)
    assert redis_client.entries.count(execution_id) == 2

    async def claim(worker_id):
        async with session_factory() as session:
            return await claim_execution(
                session,
                execution_id,
                worker_id,
                lease_duration=timedelta(seconds=30),
            )

    first, second = await asyncio.gather(claim("worker-a"), claim("worker-b"))

    assert len([result for result in (first, second) if result is not None]) == 1
