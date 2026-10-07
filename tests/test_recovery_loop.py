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
    from sqlalchemy import select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

    import main  # noqa: E402
    from database import Base  # noqa: E402
    from models import Execution  # noqa: E402
    from queue_reconciliation import ReconciliationResult  # noqa: E402


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
