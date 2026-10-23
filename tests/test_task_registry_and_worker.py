"""Integration and unit tests for Task Registry and Worker execution."""

import asyncio
from datetime import timedelta
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

import os
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL must point to an isolated PostgreSQL database",
)

if TEST_DATABASE_URL:
    from database import Base
    from models import Execution, JobDefinition, Worker
    from task_registry import TaskRegistry, TaskResult, default_task_registry
    from execution_claim import claim_execution, complete_execution, fail_execution, renew_lease, start_execution
    from worker_registry import register_worker, disable_worker, enable_worker, UnknownWorkerError
    from worker import execute_task_payload


@pytest.fixture
def custom_registry():
    return TaskRegistry()


@pytest.mark.asyncio
async def test_task_registry_echo_handler(custom_registry):
    res = await custom_registry.execute("echo", {"message": "hello world"})
    assert res.status == "SUCCEEDED"
    assert res.output["echo"] == "hello world"


@pytest.mark.asyncio
async def test_task_registry_compute_hash(custom_registry):
    res = await custom_registry.execute("compute_hash", {"data": "test_flowforge", "algorithm": "sha256"})
    assert res.status == "SUCCEEDED"
    assert "hash" in res.output
    assert len(res.output["hash"]) == 64


@pytest.mark.asyncio
async def test_task_registry_fail_handler(custom_registry):
    res = await custom_registry.execute("fail", {"error": "forced error"})
    assert res.status == "FAILED"
    assert "forced error" in res.error


@pytest.mark.asyncio
async def test_task_registry_timeout(custom_registry):
    # sleep for 2 seconds with a 0.1s timeout
    res = await custom_registry.execute("sleep", {"seconds": 2.0}, timeout_seconds=0.1)
    assert res.status == "FAILED"
    assert "timed out" in res.error.lower()


@pytest.mark.asyncio
async def test_task_registry_unknown_handler(custom_registry):
    res = await custom_registry.execute("non_existent_handler")
    assert res.status == "FAILED"
    assert "unknown task handler" in res.error.lower()


import pytest_asyncio

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


@pytest.mark.asyncio
async def test_worker_disable_and_lease_renewal(session_factory):
    async with session_factory() as session:
        reg = await register_worker(session, "worker-test-1")
        assert reg.status == "ACTIVE"

        disabled = await disable_worker(session, "worker-test-1")
        assert disabled.status == "DISABLED"

        with pytest.raises(UnknownWorkerError):
            await disable_worker(session, "non-existent-worker")

        # Test lease renewal
        job = JobDefinition(name="lease-job", priority=1)
        session.add(job)
        await session.flush()

        execution = Execution(job_definition_id=job.id, status="QUEUED")
        session.add(execution)
        await session.commit()

        # Disabled worker cannot claim
        blocked_claim = await claim_execution(session, execution.id, "worker-test-1", timedelta(seconds=10))
        assert blocked_claim is None

        # Re-enabling worker allows claiming and lease renewal
        await enable_worker(session, "worker-test-1")
        claim = await claim_execution(session, execution.id, "worker-test-1", timedelta(seconds=10))
        assert claim is not None

        started = await start_execution(session, claim)
        assert started is True

        renewed = await renew_lease(session, claim, timedelta(seconds=30))
        assert renewed is not None
        assert renewed.lease_until > claim.lease_until

        completed = await complete_execution(session, renewed)
        assert completed is True
