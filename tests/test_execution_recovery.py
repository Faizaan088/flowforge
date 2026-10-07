"""PostgreSQL integration tests for expired execution lease recovery."""

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
import pytest_asyncio


TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL must point to an isolated PostgreSQL database",
)

if TEST_DATABASE_URL:
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

    from database import Base  # noqa: E402
    from execution_claim import (  # noqa: E402
        claim_execution,
        complete_execution,
        start_execution,
    )
    from execution_recovery import recover_expired_executions  # noqa: E402
    from models import Execution  # noqa: E402
    from queue_reconciliation import reconcile_queued_executions  # noqa: E402


class RecordingRedis:
    def __init__(self):
        self.entries = []

    def lpush(self, _queue_name, execution_id):
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


async def create_execution(
    session_factory,
    status,
    lease_until=None,
    worker_id=None,
):
    async with session_factory() as session:
        execution = Execution(
            status=status,
            lease_until=lease_until,
            worker_id=worker_id,
        )
        session.add(execution)
        await session.commit()
        await session.refresh(execution)
        return execution.id


async def get_execution(session_factory, execution_id):
    async with session_factory() as session:
        return await session.get(Execution, execution_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["CLAIMED", "RUNNING"])
async def test_recovers_expired_claimed_or_running_execution(session_factory, status):
    execution_id = await create_execution(
        session_factory,
        status,
        lease_until=datetime.now(timezone.utc) - timedelta(seconds=1),
        worker_id="worker-old",
    )

    async with session_factory() as session:
        recovered = await recover_expired_executions(session)

    assert recovered == [execution_id]
    execution = await get_execution(session_factory, execution_id)
    assert execution.status == "QUEUED"
    assert execution.worker_id is None
    assert execution.lease_until is None
    assert execution.attempt == 1


@pytest.mark.asyncio
async def test_does_not_recover_non_expired_lease(session_factory):
    execution_id = await create_execution(
        session_factory,
        "RUNNING",
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
        worker_id="worker-a",
    )

    async with session_factory() as session:
        assert await recover_expired_executions(session) == []

    execution = await get_execution(session_factory, execution_id)
    assert execution.status == "RUNNING"
    assert execution.worker_id == "worker-a"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["SUCCEEDED", "FAILED", "DEAD_LETTERED"])
async def test_does_not_recover_terminal_execution(session_factory, status):
    execution_id = await create_execution(
        session_factory,
        status,
        lease_until=datetime.now(timezone.utc) - timedelta(seconds=1),
    )

    async with session_factory() as session:
        assert await recover_expired_executions(session) == []

    execution = await get_execution(session_factory, execution_id)
    assert execution.status == status


@pytest.mark.asyncio
async def test_only_one_concurrent_recovery_succeeds(session_factory):
    execution_id = await create_execution(
        session_factory,
        "RUNNING",
        lease_until=datetime.now(timezone.utc) - timedelta(seconds=1),
        worker_id="worker-old",
    )

    async def recover_once():
        async with session_factory() as session:
            return await recover_expired_executions(session)

    first, second = await asyncio.gather(recover_once(), recover_once())
    assert sorted(first + second) == [execution_id]


@pytest.mark.asyncio
async def test_old_worker_cannot_complete_after_recovery(session_factory):
    execution_id = await create_execution(session_factory, "QUEUED")

    async with session_factory() as session:
        old_claim = await claim_execution(
            session,
            execution_id,
            "worker-old",
            timedelta(seconds=-1),
        )
        assert old_claim is not None
        assert await start_execution(session, old_claim)

    async with session_factory() as session:
        assert await recover_expired_executions(session) == [execution_id]

    async with session_factory() as session:
        assert not await complete_execution(session, old_claim)
        new_claim = await claim_execution(
            session,
            execution_id,
            "worker-new",
            timedelta(seconds=30),
        )
    assert new_claim is not None


@pytest.mark.asyncio
async def test_recovered_execution_returns_to_queue_and_can_be_claimed(session_factory):
    execution_id = await create_execution(
        session_factory,
        "CLAIMED",
        lease_until=datetime.now(timezone.utc) - timedelta(seconds=1),
        worker_id="worker-old",
    )

    async with session_factory() as session:
        assert await recover_expired_executions(session) == [execution_id]

    redis_client = RecordingRedis()
    async with session_factory() as session:
        reconciliation = await reconcile_queued_executions(session, redis_client)
    assert reconciliation.enqueued == 1
    assert redis_client.entries == [execution_id]

    async with session_factory() as session:
        claim = await claim_execution(
            session,
            execution_id,
            "worker-new",
            timedelta(seconds=30),
        )
    assert claim is not None
