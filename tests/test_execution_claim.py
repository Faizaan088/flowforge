"""PostgreSQL integration tests for durable execution claiming.

Run with TEST_DATABASE_URL set to an isolated PostgreSQL database.  SQLite is
not used here because the concurrency contract is specifically PostgreSQL's.
"""

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
    from models import Execution  # noqa: E402


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


async def create_execution(session_factory, status="QUEUED"):
    async with session_factory() as session:
        execution = Execution(status=status)
        session.add(execution)
        await session.commit()
        await session.refresh(execution)
        return execution.id


@pytest.mark.asyncio
async def test_claims_a_queued_execution(session_factory):
    execution_id = await create_execution(session_factory)

    async with session_factory() as session:
        claim = await claim_execution(
            session, execution_id, "worker-a", timedelta(seconds=30)
        )

    assert claim is not None
    assert claim.execution_id == execution_id

    async with session_factory() as session:
        execution = await session.get(Execution, execution_id)
    assert execution.status == "CLAIMED"


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["CLAIMED", "RUNNING", "SUCCEEDED", "FAILED"])
async def test_rejects_non_queued_execution(session_factory, status):
    execution_id = await create_execution(session_factory, status)

    async with session_factory() as session:
        claim = await claim_execution(
            session, execution_id, "worker-a", timedelta(seconds=30)
        )

    assert claim is None


@pytest.mark.asyncio
async def test_only_one_concurrent_worker_can_claim(session_factory):
    execution_id = await create_execution(session_factory)

    async def attempt(worker_id):
        async with session_factory() as session:
            return await claim_execution(
                session, execution_id, worker_id, timedelta(seconds=30)
            )

    first, second = await asyncio.gather(attempt("worker-a"), attempt("worker-b"))
    successful_claims = [claim for claim in (first, second) if claim is not None]

    assert len(successful_claims) == 1
    assert successful_claims[0].worker_id in {"worker-a", "worker-b"}


@pytest.mark.asyncio
async def test_claim_records_winning_worker_id(session_factory):
    execution_id = await create_execution(session_factory)

    async with session_factory() as session:
        await claim_execution(session, execution_id, "worker-42", timedelta(seconds=30))

    async with session_factory() as session:
        execution = await session.get(Execution, execution_id)
    assert execution.worker_id == "worker-42"


@pytest.mark.asyncio
async def test_claim_records_lease(session_factory):
    execution_id = await create_execution(session_factory)
    before_claim = datetime.now(timezone.utc)

    async with session_factory() as session:
        claim = await claim_execution(
            session, execution_id, "worker-a", timedelta(seconds=30)
        )

    assert claim is not None
    assert claim.lease_until >= before_claim + timedelta(seconds=29)
    assert claim.lease_until <= datetime.now(timezone.utc) + timedelta(seconds=31)

    async with session_factory() as session:
        execution = await session.get(Execution, execution_id)
    assert execution.lease_until == claim.lease_until


@pytest.mark.asyncio
async def test_only_claim_owner_can_transition_to_running(session_factory):
    execution_id = await create_execution(session_factory)

    async with session_factory() as session:
        claim = await claim_execution(session, execution_id, "worker-a", timedelta(seconds=30))
        assert claim is not None
        assert not await start_execution(
            session,
            claim.__class__(claim.execution_id, "worker-b", claim.lease_until),
        )
        assert await start_execution(session, claim)

    async with session_factory() as session:
        execution = await session.get(Execution, execution_id)
    assert execution.status == "RUNNING"


@pytest.mark.asyncio
async def test_non_owner_or_stale_claim_cannot_complete_execution(session_factory):
    execution_id = await create_execution(session_factory)

    async with session_factory() as session:
        claim = await claim_execution(session, execution_id, "worker-a", timedelta(seconds=30))
        assert claim is not None
        assert await start_execution(session, claim)

        non_owner_claim = claim.__class__(
            claim.execution_id, "worker-b", claim.lease_until
        )
        stale_claim = claim.__class__(
            claim.execution_id,
            claim.worker_id,
            claim.lease_until - timedelta(microseconds=1),
        )
        assert not await complete_execution(session, non_owner_claim)
        assert not await complete_execution(session, stale_claim)
        assert await complete_execution(session, claim)

    async with session_factory() as session:
        execution = await session.get(Execution, execution_id)
    assert execution.status == "SUCCEEDED"
