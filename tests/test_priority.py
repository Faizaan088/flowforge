"""Comprehensive tests for execution priority ordering, persistence, limits, and integration."""

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

    from concurrency_policy import (
        set_concurrency_limit_policy,
        set_rate_limit_policy,
    )
    from database import Base, get_db
    from execution_claim import (
        claim_execution,
        claim_next_execution,
        update_execution_priority,
    )
    from execution_recovery import recover_expired_executions
    from execution_retry import requeue_eligible_retries
    from main import app
    from models import (
        ConcurrencyLimitPolicy,
        Execution,
        JobDefinition,
        RateLimitPolicy,
        RateLimitRecord,
        ScheduleDefinition,
        ScheduleOccurrence,
        Worker,
        WorkflowDefinition,
        WorkflowEdge,
        WorkflowRun,
        WorkflowTask,
        WorkflowTaskExecution,
    )
    from scheduler import dispatch_due_occurrences
    from tests.test_policy_api import AsyncApiClient
    from workflow_engine import (
        create_workflow,
        create_workflow_run,
        dispatch_ready_tasks,
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
        await session.execute(ConcurrencyLimitPolicy.__table__.delete())
        await session.execute(WorkflowTaskExecution.__table__.delete())
        await session.execute(WorkflowEdge.__table__.delete())
        await session.execute(WorkflowTask.__table__.delete())
        await session.execute(WorkflowRun.__table__.delete())
        await session.execute(WorkflowDefinition.__table__.delete())
        await session.execute(Execution.__table__.delete())
        await session.execute(ScheduleOccurrence.__table__.delete())
        await session.execute(ScheduleDefinition.__table__.delete())
        await session.execute(JobDefinition.__table__.delete())
        await session.execute(Worker.__table__.delete())
        await session.commit()


@pytest_asyncio.fixture
async def client(session_factory):
    async def get_test_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = get_test_db
    try:
        yield AsyncApiClient(app)
    finally:
        app.dependency_overrides.pop(get_db, None)


# ===========================================================================
# 1. Priority Defaults and Persistence
# ===========================================================================

@pytest.mark.asyncio
async def test_default_priority(session_factory):
    """Execution priority defaults to 0."""
    async with session_factory() as session:
        execution = Execution(status="QUEUED")
        session.add(execution)
        await session.commit()
        await session.refresh(execution)
        assert execution.priority == 0


@pytest.mark.asyncio
async def test_priority_persistence(session_factory):
    """Explicit priority value is persisted durably in PostgreSQL."""
    async with session_factory() as session:
        execution = Execution(status="QUEUED", priority=100)
        session.add(execution)
        await session.commit()
        exec_id = execution.id

    async with session_factory() as session:
        row = await session.get(Execution, exec_id)
        assert row.priority == 100


# ===========================================================================
# 2. Priority Ordering and Deterministic Tie-breaking
# ===========================================================================

@pytest.mark.asyncio
async def test_high_priority_selected_first(session_factory):
    """Executions with higher priority are claimed before lower priority executions."""
    async with session_factory() as session:
        e_low = Execution(status="QUEUED", priority=1)
        e_high = Execution(status="QUEUED", priority=10)
        e_med = Execution(status="QUEUED", priority=5)
        session.add_all([e_low, e_high, e_med])
        await session.commit()
        low_id, high_id, med_id = e_low.id, e_high.id, e_med.id

    # Claim 1: must be high priority (10)
    async with session_factory() as session:
        claim1 = await claim_next_execution(session, "worker-1", timedelta(seconds=30))
        assert claim1 is not None
        assert claim1.execution_id == high_id

    # Claim 2: must be medium priority (5)
    async with session_factory() as session:
        claim2 = await claim_next_execution(session, "worker-1", timedelta(seconds=30))
        assert claim2 is not None
        assert claim2.execution_id == med_id

    # Claim 3: must be low priority (1)
    async with session_factory() as session:
        claim3 = await claim_next_execution(session, "worker-1", timedelta(seconds=30))
        assert claim3 is not None
        assert claim3.execution_id == low_id

    # Claim 4: no more QUEUED executions
    async with session_factory() as session:
        claim4 = await claim_next_execution(session, "worker-1", timedelta(seconds=30))
        assert claim4 is None


@pytest.mark.asyncio
async def test_deterministic_fifo_tie_breaking(session_factory):
    """When priority is identical, tie-breaking is strictly FIFO by Execution.id ASC."""
    async with session_factory() as session:
        e1 = Execution(status="QUEUED", priority=5)
        e2 = Execution(status="QUEUED", priority=5)
        e3 = Execution(status="QUEUED", priority=5)
        session.add_all([e1, e2, e3])
        await session.commit()
        id1, id2, id3 = e1.id, e2.id, e3.id

    claimed_order = []
    for _ in range(3):
        async with session_factory() as session:
            c = await claim_next_execution(session, "worker-1", timedelta(seconds=30))
            assert c is not None
            claimed_order.append(c.execution_id)

    assert claimed_order == [id1, id2, id3]


# ===========================================================================
# 3. Priority Preserved in Lifecycle (Retry and Recovery)
# ===========================================================================

@pytest.mark.asyncio
async def test_priority_preserved_with_retry(session_factory):
    """Priority is preserved when an execution transitions through retry back to QUEUED."""
    now = datetime(2026, 10, 8, 12, 0, 0, tzinfo=timezone.utc)
    async with session_factory() as session:
        exec_row = Execution(
            status="RETRY_WAIT",
            priority=42,
            available_at=now - timedelta(seconds=5),
            attempt=1,
        )
        session.add(exec_row)
        await session.commit()
        exec_id = exec_row.id

    async with session_factory() as session:
        requeued = await requeue_eligible_retries(session, now=now)
        assert requeued == [exec_id]

    async with session_factory() as session:
        row = await session.get(Execution, exec_id)
        assert row.status == "QUEUED"
        assert row.priority == 42


@pytest.mark.asyncio
async def test_priority_preserved_with_recovery(session_factory):
    """Priority is preserved when an expired lease is recovered back to QUEUED."""
    now = datetime(2026, 10, 8, 12, 0, 0, tzinfo=timezone.utc)
    async with session_factory() as session:
        exec_row = Execution(
            status="RUNNING",
            worker_id="crashed-worker",
            priority=88,
            lease_until=now - timedelta(seconds=10),
            attempt=1,
        )
        session.add(exec_row)
        await session.commit()
        exec_id = exec_row.id

    async with session_factory() as session:
        recovered = await recover_expired_executions(session, now=now)
        assert recovered == [exec_id]

    async with session_factory() as session:
        row = await session.get(Execution, exec_id)
        assert row.status == "QUEUED"
        assert row.priority == 88


# ===========================================================================
# 4. Priority with Policies (Concurrency & Rate Limits)
# ===========================================================================

@pytest.mark.asyncio
async def test_priority_with_concurrency_limits(session_factory):
    """High priority execution does not bypass concurrency limits."""
    async with session_factory() as session:
        wf1 = await create_workflow(session=session, name="wf-1", tasks=[{"name": "t1"}], edges=[])
        wf2 = await create_workflow(session=session, name="wf-2", tasks=[{"name": "t2"}], edges=[])
        # Workflow 1 has max_concurrency = 1
        await set_concurrency_limit_policy(session, "WORKFLOW", str(wf1.id), max_concurrency=1)
        await session.commit()

        run1 = await create_workflow_run(session=session, workflow_id=wf1.id, auto_ready_roots=True)
        run2 = await create_workflow_run(session=session, workflow_id=wf2.id, auto_ready_roots=True)
        d1 = await dispatch_ready_tasks(session, run1.id)
        d2 = await dispatch_ready_tasks(session, run2.id)
        e_wf1_1 = d1.created_executions[0]
        e_wf2 = d2.created_executions[0]
        await session.commit()

    # Set e_wf1_1 priority=100, e_wf2 priority=10
    async with session_factory() as session:
        r1 = await session.get(Execution, e_wf1_1)
        r1.priority = 100
        r2 = await session.get(Execution, e_wf2)
        r2.priority = 10
        await session.commit()

    # Claim e_wf1_1 -> occupies wf-1 capacity
    async with session_factory() as session:
        c1 = await claim_next_execution(session, "w1", timedelta(seconds=30))
        assert c1.execution_id == e_wf1_1

    # Now create another run of wf1 with very high priority (200)
    async with session_factory() as session:
        run1_second = await create_workflow_run(session=session, workflow_id=wf1.id, auto_ready_roots=True)
        d1_second = await dispatch_ready_tasks(session, run1_second.id)
        e_wf1_2 = d1_second.created_executions[0]
        r1_second = await session.get(Execution, e_wf1_2)
        r1_second.priority = 200
        await session.commit()

    # Next claim: e_wf1_2 has priority 200, but wf-1 concurrency limit is exhausted!
    # Therefore, e_wf2 (priority 10) must be claimed instead!
    async with session_factory() as session:
        c2 = await claim_next_execution(session, "w2", timedelta(seconds=30))
        assert c2 is not None
        assert c2.execution_id == e_wf2


@pytest.mark.asyncio
async def test_priority_with_rate_limits(session_factory):
    """High priority execution does not bypass category rate limits."""
    async with session_factory() as session:
        # Rate limit "bulk" category to 1 request per 60 seconds
        await set_rate_limit_policy(session, "CATEGORY", "bulk", max_requests=1, window_seconds=60)
        e_bulk1 = Execution(status="QUEUED", category="bulk", priority=100)
        e_bulk2 = Execution(status="QUEUED", category="bulk", priority=90)
        e_other = Execution(status="QUEUED", category="other", priority=10)
        session.add_all([e_bulk1, e_bulk2, e_other])
        await session.commit()
        bulk1_id, bulk2_id, other_id = e_bulk1.id, e_bulk2.id, e_other.id

    # Claim 1: e_bulk1 is claimed (priority 100)
    async with session_factory() as session:
        c1 = await claim_next_execution(session, "w1", timedelta(seconds=30))
        assert c1.execution_id == bulk1_id

    # Claim 2: e_bulk2 has priority 90, but "bulk" rate limit is exhausted.
    # Therefore, e_other (priority 10) must be claimed!
    async with session_factory() as session:
        c2 = await claim_next_execution(session, "w2", timedelta(seconds=30))
        assert c2.execution_id == other_id


# ===========================================================================
# 5. Workflow and Scheduler Integration
# ===========================================================================

@pytest.mark.asyncio
async def test_priority_with_workflow_execution(session_factory):
    """Workflow task configuration priority propagates to created Execution."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="prio-wf",
            tasks=[{"name": "high_prio_task", "config": {"priority": 77}}],
            edges=[],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        d_res = await dispatch_ready_tasks(session, run.id)
        exec_id = d_res.created_executions[0]
        await session.commit()

    async with session_factory() as session:
        exec_row = await session.get(Execution, exec_id)
        assert exec_row.priority == 77


@pytest.mark.asyncio
async def test_priority_with_scheduler_dispatch(session_factory):
    """Scheduler dispatches occurrences with priority from configuration."""
    now = datetime(2026, 10, 8, 12, 0, 0, tzinfo=timezone.utc)
    async with session_factory() as session:
        sched = ScheduleDefinition(
            name="prio-sched",
            schedule_type="once",
            configuration={"priority": 55},
            enabled=True,
        )
        session.add(sched)
        await session.commit()
        sched_id = sched.id

        occ = ScheduleOccurrence(
            schedule_definition_id=sched_id,
            scheduled_for=now - timedelta(seconds=10),
            status="SCHEDULED",
        )
        session.add(occ)
        await session.commit()

    async with session_factory() as session:
        _, created_exec_ids, _ = await dispatch_due_occurrences(session, now=now)
        assert len(created_exec_ids) == 1
        exec_id = created_exec_ids[0]

    async with session_factory() as session:
        exec_row = await session.get(Execution, exec_id)
        assert exec_row.priority == 55


# ===========================================================================
# 6. Priority API Endpoints
# ===========================================================================

@pytest.mark.asyncio
async def test_priority_api_behavior(client, session_factory):
    """Test priority API behavior for triggering and updating priority."""
    async with session_factory() as session:
        job = JobDefinition(name="job-prio-test", priority=5)
        session.add(job)
        await session.commit()
        job_id = job.id

    # 1. Trigger job with default priority (from job_definition = 5)
    r1 = await client.post(f"/jobs/{job_id}/execute")
    assert r1.status_code == 200
    e1_id = r1.json()["id"]
    assert r1.json()["priority"] == 5

    # 2. Trigger job with explicit custom priority (42)
    r2 = await client.post(f"/jobs/{job_id}/execute", json={"priority": 42})
    assert r2.status_code == 200
    e2_id = r2.json()["id"]
    assert r2.json()["priority"] == 42

    # 3. Update priority on QUEUED execution e1 to 99
    r_patch = await client.patch(f"/executions/{e1_id}/priority", json={"priority": 99})
    assert r_patch.status_code == 200
    assert r_patch.json()["priority"] == 99

    # 4. Claim e1 -> status is now CLAIMED
    async with session_factory() as session:
        await claim_execution(session, e1_id, "worker-test", timedelta(seconds=30))

    # 5. Updating priority on non-QUEUED execution returns 409 Conflict
    r_conflict = await client.patch(f"/executions/{e1_id}/priority", json={"priority": 100})
    assert r_conflict.status_code == 409

    # 6. Updating priority on missing execution returns 404
    r_missing = await client.patch("/executions/999999/priority", json={"priority": 10})
    assert r_missing.status_code == 404
