"""Focused tests for the application-managed execution recovery loop."""

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
    from redis.exceptions import RedisError
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

    import main  # noqa: E402
    from database import Base  # noqa: E402
    from execution_claim import (  # noqa: E402
        claim_execution,
        complete_execution,
        start_execution,
    )
    from models import Execution, Worker  # noqa: E402
    from queue_reconciliation import ReconciliationResult  # noqa: E402
    from worker_registry import (  # noqa: E402
        heartbeat_worker,
        register_worker,
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
async def clear_database(session_factory):
    async with session_factory() as session:
        await session.execute(Execution.__table__.delete())
        await session.execute(Worker.__table__.delete())
        await session.commit()


@pytest.mark.asyncio
async def test_recovery_cycle_recovers_then_reconciles(session_factory, monkeypatch):
    async with session_factory() as session:
        execution = Execution(
            status="RUNNING",
            worker_id="worker-old",
            lease_until=datetime.now(timezone.utc) - timedelta(seconds=1),
        )
        session.add(execution)
        await session.commit()
        await session.refresh(execution)
        execution_id = execution.id

    reconciled_ids = []
    fake_reconciliation = ReconciliationResult(found=1, enqueued=1)

    async def reconcile_queue(session):
        result = await session.execute(
            select(Execution.id).where(Execution.status == "QUEUED")
        )
        reconciled_ids.extend(result.scalars())
        return fake_reconciliation

    monkeypatch.setattr(main, "AsyncSessionLocal", session_factory)
    monkeypatch.setattr(main, "reconcile_queue", reconcile_queue)

    recovered_ids, reconciliation = await main.run_recovery_cycle()

    assert recovered_ids == [execution_id]
    assert reconciliation == fake_reconciliation
    assert reconciled_ids == [execution_id]


@pytest.mark.asyncio
async def test_recovery_cycle_skips_reconciliation_when_nothing_recovered(
    session_factory, monkeypatch
):
    async with session_factory() as session:
        active_execution = Execution(
            status="RUNNING",
            worker_id="worker-active",
            lease_until=datetime.now(timezone.utc) + timedelta(minutes=5),
        )
        queued_execution = Execution(
            status="QUEUED",
        )
        session.add_all([active_execution, queued_execution])
        await session.commit()

    reconcile_called = False

    async def reconcile_queue(_session):
        nonlocal reconcile_called
        reconcile_called = True
        return ReconciliationResult(found=1, enqueued=1)

    monkeypatch.setattr(main, "AsyncSessionLocal", session_factory)
    monkeypatch.setattr(main, "reconcile_queue", reconcile_queue)

    recovered_ids, reconciliation = await main.run_recovery_cycle()

    assert recovered_ids == []
    assert reconciliation is None
    assert not reconcile_called


@pytest.mark.asyncio
async def test_lifespan_starts_and_cancels_recovery_loop(monkeypatch):
    started = asyncio.Event()
    stopped = asyncio.Event()

    class FakeConnection:
        async def run_sync(self, _operation):
            return None

    class FakeBegin:
        async def __aenter__(self):
            return FakeConnection()

        async def __aexit__(self, _type, _value, _traceback):
            return False

    class FakeEngine:
        def begin(self):
            return FakeBegin()

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, _type, _value, _traceback):
            return False

    async def recovery_loop():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    async def reconcile_queue(_session):
        return None

    monkeypatch.setattr(main, "engine", FakeEngine())
    monkeypatch.setattr(main, "AsyncSessionLocal", FakeSession)
    monkeypatch.setattr(main, "recovery_loop", recovery_loop)
    monkeypatch.setattr(main, "reconcile_queue", reconcile_queue)

    async with main.lifespan(main.app):
        await asyncio.wait_for(started.wait(), timeout=1)

    await asyncio.wait_for(stopped.wait(), timeout=1)


@pytest.mark.asyncio
async def test_worker_crash_recovery_cycle_allows_reclaim(session_factory, monkeypatch):
    worker_crashed = "worker-crashed"
    worker_recovering = "worker-recovering"

    async with session_factory() as session:
        execution = Execution(status="QUEUED")
        session.add(execution)
        await session.commit()
        await session.refresh(execution)
        execution_id = execution.id

    async with session_factory() as session:
        reg_1 = await register_worker(session, worker_crashed)
        assert reg_1.status == "ACTIVE"
        last_hb = await heartbeat_worker(session, worker_crashed)
        assert last_hb is not None

        claim_1 = await claim_execution(
            session,
            execution_id,
            worker_crashed,
            lease_duration=timedelta(seconds=-1),
        )
        assert claim_1 is not None
        assert claim_1.worker_id == worker_crashed

        started = await start_execution(session, claim_1)
        assert started is True

    async with session_factory() as session:
        row = await session.get(Execution, execution_id)
        assert row.status == "RUNNING"
        assert row.worker_id == worker_crashed
        assert row.lease_until < datetime.now(timezone.utc)

    recording_redis = RecordingRedis()
    monkeypatch.setattr(main, "AsyncSessionLocal", session_factory)
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(main.redis, "from_url", lambda *args, **kwargs: recording_redis)

    recovered_ids, reconciliation = await main.run_recovery_cycle()

    assert recovered_ids == [execution_id]
    assert reconciliation is not None
    assert reconciliation.enqueued == 1
    assert recording_redis.entries == [execution_id]

    async with session_factory() as session:
        # The expired worker cannot complete the job after ownership was recovered
        assert await complete_execution(session, claim_1) is False

        row = await session.get(Execution, execution_id)
        assert row.status == "QUEUED"
        assert row.worker_id is None
        assert row.lease_until is None
        assert row.attempt == 1

    queued_id = recording_redis.entries.pop()
    assert queued_id == execution_id

    async with session_factory() as session:
        reg_2 = await register_worker(session, worker_recovering)
        assert reg_2.status == "ACTIVE"
        await heartbeat_worker(session, worker_recovering)

        claim_2 = await claim_execution(
            session,
            queued_id,
            worker_recovering,
            lease_duration=timedelta(seconds=30),
        )
        assert claim_2 is not None
        assert claim_2.worker_id == worker_recovering
        assert claim_2.execution_id == execution_id

        assert await start_execution(session, claim_2) is True
        assert await complete_execution(session, claim_2) is True

        completed_row = await session.get(Execution, execution_id)
        assert completed_row.status == "SUCCEEDED"
        assert completed_row.worker_id == worker_recovering
        assert completed_row.finished_at is not None


@pytest.mark.asyncio
async def test_recovery_cycle_requeues_and_delivers_eligible_retry(
    session_factory, monkeypatch
):
    base_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    async with session_factory() as session:
        eligible_retry = Execution(
            status="RETRY_WAIT",
            available_at=base_time - timedelta(seconds=10),
            attempt=1,
        )
        existing_queued = Execution(
            status="QUEUED",
            attempt=1,
        )
        session.add_all([eligible_retry, existing_queued])
        await session.commit()
        await session.refresh(eligible_retry)
        await session.refresh(existing_queued)
        retry_id = eligible_retry.id
        existing_id = existing_queued.id

    recording_redis = RecordingRedis()
    monkeypatch.setattr(main, "AsyncSessionLocal", session_factory)
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(main.redis, "from_url", lambda *args, **kwargs: recording_redis)

    requeued_ids, reconciliation = await main.run_recovery_cycle(now=base_time)

    assert requeued_ids == [retry_id]
    assert reconciliation is not None
    assert reconciliation.enqueued == 1
    assert reconciliation.redis_error is None
    assert recording_redis.entries == [retry_id]

    async with session_factory() as session:
        retry_row = await session.get(Execution, retry_id)
        assert retry_row.status == "QUEUED"
        assert retry_row.available_at is None
        assert retry_row.attempt == 2

        existing_row = await session.get(Execution, existing_id)
        assert existing_row.status == "QUEUED"
        assert existing_row.attempt == 1


@pytest.mark.asyncio
async def test_recovery_cycle_ignores_future_retry(session_factory, monkeypatch):
    base_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    async with session_factory() as session:
        future_retry = Execution(
            status="RETRY_WAIT",
            available_at=base_time + timedelta(seconds=30),
            attempt=1,
        )
        session.add(future_retry)
        await session.commit()
        await session.refresh(future_retry)
        retry_id = future_retry.id

    recording_redis = RecordingRedis()
    monkeypatch.setattr(main, "AsyncSessionLocal", session_factory)
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(main.redis, "from_url", lambda *args, **kwargs: recording_redis)

    requeued_ids, reconciliation = await main.run_recovery_cycle(now=base_time)

    assert requeued_ids == []
    assert reconciliation is None
    assert recording_redis.entries == []

    async with session_factory() as session:
        row = await session.get(Execution, retry_id)
        assert row.status == "RETRY_WAIT"
        assert row.attempt == 1
        assert row.available_at == base_time + timedelta(seconds=30)


@pytest.mark.asyncio
async def test_recovery_cycle_handles_multiple_eligible_retries(
    session_factory, monkeypatch
):
    base_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    async with session_factory() as session:
        eligible_1 = Execution(
            status="RETRY_WAIT",
            available_at=base_time - timedelta(seconds=20),
            attempt=1,
        )
        eligible_2 = Execution(
            status="RETRY_WAIT",
            available_at=base_time - timedelta(seconds=5),
            attempt=2,
        )
        future_retry = Execution(
            status="RETRY_WAIT",
            available_at=base_time + timedelta(seconds=60),
            attempt=1,
        )
        session.add_all([eligible_1, eligible_2, future_retry])
        await session.commit()
        await session.refresh(eligible_1)
        await session.refresh(eligible_2)
        await session.refresh(future_retry)
        id_1 = eligible_1.id
        id_2 = eligible_2.id
        id_future = future_retry.id

    recording_redis = RecordingRedis()
    monkeypatch.setattr(main, "AsyncSessionLocal", session_factory)
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(main.redis, "from_url", lambda *args, **kwargs: recording_redis)

    requeued_ids, reconciliation = await main.run_recovery_cycle(now=base_time)

    assert set(requeued_ids) == {id_1, id_2}
    assert reconciliation is not None
    assert reconciliation.enqueued == 2
    assert set(recording_redis.entries) == {id_1, id_2}

    async with session_factory() as session:
        row_1 = await session.get(Execution, id_1)
        assert row_1.status == "QUEUED"
        assert row_1.attempt == 2
        assert row_1.available_at is None

        row_2 = await session.get(Execution, id_2)
        assert row_2.status == "QUEUED"
        assert row_2.attempt == 3
        assert row_2.available_at is None

        row_future = await session.get(Execution, id_future)
        assert row_future.status == "RETRY_WAIT"
        assert row_future.attempt == 1


@pytest.mark.asyncio
async def test_recovery_cycle_redis_failure_preserves_durable_db_state(
    session_factory, monkeypatch
):
    base_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    async with session_factory() as session:
        retry_execution = Execution(
            status="RETRY_WAIT",
            available_at=base_time - timedelta(seconds=5),
            attempt=1,
        )
        session.add(retry_execution)
        await session.commit()
        await session.refresh(retry_execution)
        execution_id = retry_execution.id

    failing_redis = RecordingRedis(unavailable=True)
    monkeypatch.setattr(main, "AsyncSessionLocal", session_factory)
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(main.redis, "from_url", lambda *args, **kwargs: failing_redis)

    requeued_ids, reconciliation = await main.run_recovery_cycle(now=base_time)

    assert requeued_ids == [execution_id]
    assert reconciliation is not None
    assert reconciliation.enqueued == 0
    assert reconciliation.redis_error is not None
    assert "Redis is unavailable" in reconciliation.redis_error

    async with session_factory() as session:
        row = await session.get(Execution, execution_id)
        assert row.status == "QUEUED"
        assert row.attempt == 2
        assert row.available_at is None


@pytest.mark.asyncio
async def test_recovery_cycle_handles_both_expired_leases_and_eligible_retries(
    session_factory, monkeypatch
):
    base_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    async with session_factory() as session:
        expired_lease = Execution(
            status="RUNNING",
            worker_id="worker-crashed",
            lease_until=base_time - timedelta(seconds=15),
            attempt=1,
        )
        eligible_retry = Execution(
            status="RETRY_WAIT",
            available_at=base_time - timedelta(seconds=10),
            attempt=1,
        )
        session.add_all([expired_lease, eligible_retry])
        await session.commit()
        await session.refresh(expired_lease)
        await session.refresh(eligible_retry)
        lease_id = expired_lease.id
        retry_id = eligible_retry.id

    recording_redis = RecordingRedis()
    monkeypatch.setattr(main, "AsyncSessionLocal", session_factory)
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/0")
    monkeypatch.setattr(main.redis, "from_url", lambda *args, **kwargs: recording_redis)

    requeued_ids, reconciliation = await main.run_recovery_cycle(now=base_time)

    assert set(requeued_ids) == {lease_id, retry_id}
    assert reconciliation is not None
    assert reconciliation.enqueued == 2
    assert set(recording_redis.entries) == {lease_id, retry_id}

    async with session_factory() as session:
        recovered_row = await session.get(Execution, lease_id)
        assert recovered_row.status == "QUEUED"
        assert recovered_row.worker_id is None
        assert recovered_row.lease_until is None
        assert recovered_row.attempt == 2

        retry_row = await session.get(Execution, retry_id)
        assert retry_row.status == "QUEUED"
        assert retry_row.worker_id is None
        assert retry_row.available_at is None
        assert retry_row.attempt == 2

