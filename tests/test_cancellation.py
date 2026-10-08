"""Comprehensive tests for execution and workflow cancellation."""

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

    from database import Base, get_db
    from execution_claim import (
        cancel_execution,
        claim_execution,
        claim_next_execution,
        complete_execution,
        fail_execution,
        start_execution,
    )
    from execution_recovery import recover_expired_executions
    from execution_retry import (
        process_failed_executions,
        requeue_eligible_retries,
        transition_failed_execution,
    )
    from main import app
    from models import (
        Execution,
        RateLimitPolicy,
        RateLimitRecord,
        Worker,
        WorkflowDefinition,
        WorkflowEdge,
        WorkflowRun,
        WorkflowTask,
        WorkflowTaskExecution,
        User,
    )
    from auth import get_current_user
    from tests.test_policy_api import AsyncApiClient
    from workflow_engine import (
        cancel_workflow_run,
        create_workflow,
        create_workflow_run,
        dispatch_ready_tasks,
        resolve_and_dispatch,
        resolve_dependencies,
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
async def clear_database(session_factory):
    async with session_factory() as session:
        await session.execute(RateLimitRecord.__table__.delete())
        await session.execute(RateLimitPolicy.__table__.delete())
        await session.execute(WorkflowTaskExecution.__table__.delete())
        await session.execute(WorkflowEdge.__table__.delete())
        await session.execute(WorkflowTask.__table__.delete())
        await session.execute(WorkflowRun.__table__.delete())
        await session.execute(WorkflowDefinition.__table__.delete())
        await session.execute(Execution.__table__.delete())
        await session.execute(Worker.__table__.delete())
        await session.commit()


@pytest_asyncio.fixture
async def client(session_factory):
    async def get_test_db():
        async with session_factory() as session:
            yield session

    admin_user = User(id=1, username="test-admin", role="admin", is_active=True)

    async def get_test_user():
        return admin_user

    app.dependency_overrides[get_db] = get_test_db
    app.dependency_overrides[get_current_user] = get_test_user
    try:
        yield AsyncApiClient(app)
    finally:
        app.dependency_overrides.pop(get_db, None)
        app.dependency_overrides.pop(get_current_user, None)


# ===========================================================================
# 1. Basic Execution Cancellation
# ===========================================================================

@pytest.mark.asyncio
async def test_cancel_queued_execution(session_factory):
    """A QUEUED execution can be cancelled directly."""
    async with session_factory() as session:
        exec_row = Execution(status="QUEUED")
        session.add(exec_row)
        await session.commit()
        await session.refresh(exec_row)
        exec_id = exec_row.id

    async with session_factory() as session:
        result = await cancel_execution(session, exec_id, reason="No longer needed")
        assert result.cancelled is True
        assert result.status == "CANCELLED"

    async with session_factory() as session:
        row = await session.get(Execution, exec_id)
        assert row.status == "CANCELLED"
        assert row.lease_until is None
        assert row.available_at is None
        assert row.finished_at is not None
        assert row.error_summary == "No longer needed"


@pytest.mark.asyncio
async def test_cancel_claimed_execution(session_factory):
    """A CLAIMED execution can be cancelled, clearing its lease."""
    async with session_factory() as session:
        exec_row = Execution(status="QUEUED")
        session.add(exec_row)
        await session.commit()
        exec_id = exec_row.id

    async with session_factory() as session:
        claim = await claim_execution(session, exec_id, "worker-1", timedelta(seconds=30))
        assert claim is not None

    async with session_factory() as session:
        result = await cancel_execution(session, exec_id, reason="Worker cancelled")
        assert result.cancelled is True

    async with session_factory() as session:
        row = await session.get(Execution, exec_id)
        assert row.status == "CANCELLED"
        assert row.lease_until is None


@pytest.mark.asyncio
async def test_cancel_running_execution(session_factory):
    """A RUNNING execution can be cancelled safely."""
    async with session_factory() as session:
        exec_row = Execution(status="QUEUED")
        session.add(exec_row)
        await session.commit()
        exec_id = exec_row.id

    async with session_factory() as session:
        claim = await claim_execution(session, exec_id, "worker-1", timedelta(seconds=30))
        started = await start_execution(session, claim)
        assert started is True

    async with session_factory() as session:
        result = await cancel_execution(session, exec_id)
        assert result.cancelled is True

    async with session_factory() as session:
        row = await session.get(Execution, exec_id)
        assert row.status == "CANCELLED"
        assert row.lease_until is None


@pytest.mark.asyncio
async def test_repeated_cancellation_is_idempotent(session_factory):
    """Calling cancel multiple times on the same execution is idempotent."""
    async with session_factory() as session:
        exec_row = Execution(status="QUEUED")
        session.add(exec_row)
        await session.commit()
        exec_id = exec_row.id

    async with session_factory() as session:
        r1 = await cancel_execution(session, exec_id)
        assert r1.cancelled is True
        assert r1.status == "CANCELLED"

    async with session_factory() as session:
        r2 = await cancel_execution(session, exec_id)
        assert r2.cancelled is True
        assert r2.status == "CANCELLED"
        assert "already cancelled" in r2.message


@pytest.mark.asyncio
async def test_cancel_missing_execution(session_factory):
    """Attempting to cancel a non-existent execution returns NOT_FOUND."""
    async with session_factory() as session:
        result = await cancel_execution(session, 999999)
        assert result.cancelled is False
        assert result.status == "NOT_FOUND"


# ===========================================================================
# 2. Worker Fencing and Claim Invariants
# ===========================================================================

@pytest.mark.asyncio
async def test_cancelled_execution_cannot_be_claimed(session_factory):
    """A cancelled execution must never be claimed by any worker."""
    async with session_factory() as session:
        exec_row = Execution(status="CANCELLED")
        session.add(exec_row)
        await session.commit()
        exec_id = exec_row.id

    async with session_factory() as session:
        claim = await claim_execution(session, exec_id, "worker-1", timedelta(seconds=30))
        assert claim is None

        next_claim = await claim_next_execution(session, "worker-1", timedelta(seconds=30))
        assert next_claim is None


@pytest.mark.asyncio
async def test_cancelled_execution_cannot_be_completed_by_stale_worker(session_factory):
    """A worker that owns a lease cannot mark an execution complete after cancellation."""
    async with session_factory() as session:
        exec_row = Execution(status="QUEUED")
        session.add(exec_row)
        await session.commit()
        exec_id = exec_row.id

    async with session_factory() as session:
        claim = await claim_execution(session, exec_id, "worker-stale", timedelta(seconds=30))
        await start_execution(session, claim)

    # Execution is cancelled out from under the worker
    async with session_factory() as session:
        cancel_res = await cancel_execution(session, exec_id, reason="Admin abort")
        assert cancel_res.cancelled is True

    # Stale worker attempts to complete execution
    async with session_factory() as session:
        completed = await complete_execution(session, claim)
        assert completed is False

    # Stale worker attempts to fail execution
    async with session_factory() as session:
        failed = await fail_execution(session, claim)
        assert failed is False

    # Status remains CANCELLED
    async with session_factory() as session:
        row = await session.get(Execution, exec_id)
        assert row.status == "CANCELLED"


@pytest.mark.asyncio
async def test_cancelled_execution_cannot_retry_or_recover(session_factory):
    """Cancelled executions cannot enter RETRY_WAIT, recover from lease, or be requeued."""
    now = datetime(2026, 10, 8, 12, 0, 0, tzinfo=timezone.utc)
    async with session_factory() as session:
        exec_row = Execution(
            status="CANCELLED",
            lease_until=now - timedelta(seconds=10),
            available_at=now - timedelta(seconds=10),
        )
        session.add(exec_row)
        await session.commit()
        exec_id = exec_row.id

    # Recovery must ignore it
    async with session_factory() as session:
        recovered = await recover_expired_executions(session, now=now)
        assert exec_id not in recovered

    # Retry requeue must ignore it
    async with session_factory() as session:
        requeued = await requeue_eligible_retries(session, now=now)
        assert exec_id not in requeued

    # Transition failed execution must ignore it
    async with session_factory() as session:
        outcome = await transition_failed_execution(session, exec_id, now=now)
        assert outcome is None

    async with session_factory() as session:
        row = await session.get(Execution, exec_id)
        assert row.status == "CANCELLED"


@pytest.mark.asyncio
async def test_terminal_execution_cannot_be_cancelled(session_factory):
    """SUCCEEDED and DEAD_LETTERED executions cannot be cancelled."""
    async with session_factory() as session:
        succeeded = Execution(status="SUCCEEDED")
        dead_lettered = Execution(status="DEAD_LETTERED")
        session.add_all([succeeded, dead_lettered])
        await session.commit()
        s_id = succeeded.id
        d_id = dead_lettered.id

    async with session_factory() as session:
        r_succ = await cancel_execution(session, s_id)
        assert r_succ.cancelled is False
        assert r_succ.status == "SUCCEEDED"

        r_dead = await cancel_execution(session, d_id)
        assert r_dead.cancelled is False
        assert r_dead.status == "DEAD_LETTERED"


# ===========================================================================
# 3. Race Conditions and Concurrency
# ===========================================================================

@pytest.mark.asyncio
async def test_cancellation_race_with_worker_claim(session_factory):
    """Concurrent claim and cancel operations resolve safely without corrupted state."""
    async with session_factory() as session:
        exec_row = Execution(status="QUEUED")
        session.add(exec_row)
        await session.commit()
        exec_id = exec_row.id

    async def do_claim():
        async with session_factory() as session:
            return await claim_execution(session, exec_id, "worker-race", timedelta(seconds=30))

    async def do_cancel():
        async with session_factory() as session:
            return await cancel_execution(session, exec_id, reason="Raced cancel")

    claim_result, cancel_result = await asyncio.gather(do_claim(), do_cancel())

    # Cancel must always succeed
    assert cancel_result.cancelled is True

    # Final database status must always be CANCELLED and have no active lease
    async with session_factory() as session:
        row = await session.get(Execution, exec_id)
        assert row.status == "CANCELLED"
        assert row.lease_until is None


# ===========================================================================
# 4. Workflow Integration
# ===========================================================================

@pytest.mark.asyncio
async def test_workflow_task_execution_cancellation_skips_downstream(session_factory):
    """Cancelling a workflow task execution causes downstream tasks to be SKIPPED."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="cancel-test-wf",
            tasks=[{"name": "root_task"}, {"name": "downstream_task"}],
            edges=[(1, 2)],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        # Dispatch root task
        d_res = await dispatch_ready_tasks(session, run.id)
        assert len(d_res.created_executions) == 1
        root_exec_id = d_res.created_executions[0]
        await session.commit()
        run_id = run.id

    # Cancel root task execution
    async with session_factory() as session:
        c_res = await cancel_execution(session, root_exec_id, reason="Cancel root task")
        assert c_res.cancelled is True

    # Check workflow state: downstream task must be SKIPPED and run CANCELLED
    async with session_factory() as session:
        run = await session.get(WorkflowRun, run_id)
        assert run.status == "CANCELLED"

        task_execs = {te.workflow_task_id: te.status for te in run.task_executions}
        assert task_execs[1] == "CANCELLED"
        assert task_execs[2] == "SKIPPED"


@pytest.mark.asyncio
async def test_cancel_workflow_run(session_factory):
    """Cancelling an active WorkflowRun cancels active tasks and marks run CANCELLED."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="run-cancel-wf",
            tasks=[{"name": "task_1"}, {"name": "task_2"}],
            edges=[],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        d_res = await dispatch_ready_tasks(session, run.id)
        await session.commit()
        run_id = run.id
        exec_ids = d_res.created_executions

    async with session_factory() as session:
        cancelled_run = await cancel_workflow_run(session, run_id, reason="User cancelled workflow")
        assert cancelled_run.status == "CANCELLED"

    # Both underlying executions must be CANCELLED
    async with session_factory() as session:
        for eid in exec_ids:
            row = await session.get(Execution, eid)
            assert row.status == "CANCELLED"


# ===========================================================================
# 5. API Endpoints
# ===========================================================================

@pytest.mark.asyncio
async def test_cancellation_api_endpoints(client, session_factory):
    """Test REST API cancellation endpoints."""
    async with session_factory() as session:
        e1 = Execution(status="QUEUED")
        e2 = Execution(status="SUCCEEDED")
        session.add_all([e1, e2])
        await session.commit()
        e1_id = e1.id
        e2_id = e2.id

    # 1. Cancel queued execution -> 200
    r1 = await client.post(f"/executions/{e1_id}/cancel", json={"reason": "API cancel"})
    assert r1.status_code == 200
    assert r1.json()["status"] == "CANCELLED"

    # 2. Idempotent repeat cancel -> 200
    r1_repeat = await client.post(f"/executions/{e1_id}/cancel")
    assert r1_repeat.status_code == 200
    assert r1_repeat.json()["status"] == "CANCELLED"

    # 3. Missing execution -> 404
    r_missing = await client.post("/executions/999999/cancel")
    assert r_missing.status_code == 404

    # 4. Terminal execution -> 409
    r_terminal = await client.post(f"/executions/{e2_id}/cancel")
    assert r_terminal.status_code == 409
