"""Tests for FlowForge Concurrency and Rate Limit Policy Engine.

Covers:
- validation rules and defaults
- concurrency limit reached on workflow and task type
- concurrent claim race
- execution finishing and freeing capacity
- rate-limit acceptance and rejection
- policy disabled / unlimited behavior
- recovery not leaking capacity
- duplicate queue delivery not bypassing limits
"""

import asyncio
from datetime import datetime, timedelta, timezone
import os
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
    from sqlalchemy.orm import selectinload
    from sqlalchemy.pool import NullPool

    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

    from database import Base
    from concurrency_policy import (
        AdmissionResult,
        PolicyValidationError,
        cleanup_expired_rate_limit_records,
        evaluate_execution_admission,
        get_concurrency_limit_policy,
        get_rate_limit_policy,
        set_concurrency_limit_policy,
        set_rate_limit_policy,
        validate_concurrency_policy,
        validate_rate_limit_policy,
    )
    from execution_claim import (
        claim_execution,
        complete_execution,
        start_execution,
    )
    from execution_recovery import recover_expired_executions
    from models import (
        ConcurrencyLimitPolicy,
        Execution,
        JobDefinition,
        RateLimitPolicy,
        RateLimitRecord,
        WorkflowDefinition,
        WorkflowEdge,
        WorkflowRun,
        WorkflowTask,
        WorkflowTaskExecution,
    )
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
async def clear_tables(session_factory):
    async with session_factory() as session:
        await session.execute(RateLimitRecord.__table__.delete())
        await session.execute(RateLimitPolicy.__table__.delete())
        await session.execute(ConcurrencyLimitPolicy.__table__.delete())
        await session.execute(WorkflowTaskExecution.__table__.delete())
        await session.execute(WorkflowRun.__table__.delete())
        await session.execute(WorkflowEdge.__table__.delete())
        await session.execute(WorkflowTask.__table__.delete())
        await session.execute(WorkflowDefinition.__table__.delete())
        await session.execute(Execution.__table__.delete())
        await session.execute(JobDefinition.__table__.delete())
        await session.commit()


# ---------------------------------------------------------------------------
# 1. Validation rules and defaults
# ---------------------------------------------------------------------------
def test_validation_rules_and_defaults():
    """Verify validation rules and parameter constraints for policies."""
    # Valid concurrency params
    target_type, target_id, max_c = validate_concurrency_policy("WORKFLOW", "wf-1", 3)
    assert target_type == "WORKFLOW"
    assert target_id == "wf-1"
    assert max_c == 3

    # Invalid target_type
    with pytest.raises(PolicyValidationError, match="Invalid target_type"):
        validate_concurrency_policy("INVALID_TYPE", "wf-1", 1)

    # Empty target_id
    with pytest.raises(PolicyValidationError, match="target_id cannot be empty"):
        validate_concurrency_policy("WORKFLOW", "   ", 1)

    # max_concurrency < 1
    with pytest.raises(PolicyValidationError, match="max_concurrency must be an integer >= 1"):
        validate_concurrency_policy("WORKFLOW", "wf-1", 0)

    # Valid rate limit params
    rtype, rid, max_r, wins = validate_rate_limit_policy("CATEGORY", "api-calls", 10, 60)
    assert rtype == "CATEGORY"
    assert rid == "api-calls"
    assert max_r == 10
    assert wins == 60

    # max_requests < 1
    with pytest.raises(PolicyValidationError, match="max_requests must be an integer >= 1"):
        validate_rate_limit_policy("CATEGORY", "api-calls", 0, 60)

    # window_seconds < 1
    with pytest.raises(PolicyValidationError, match="window_seconds must be an integer >= 1"):
        validate_rate_limit_policy("CATEGORY", "api-calls", 10, 0)


@pytest.mark.asyncio
async def test_policy_crud_operations(session_factory):
    """Verify durable policy persistence, retrieval, and updates."""
    async with session_factory() as session:
        # Create concurrency policy
        pol = await set_concurrency_limit_policy(session, "WORKFLOW", "wf-100", max_concurrency=5)
        assert pol.max_concurrency == 5
        assert pol.is_enabled is True
        await session.commit()

    async with session_factory() as session:
        # Retrieve policy
        pol = await get_concurrency_limit_policy(session, "WORKFLOW", "wf-100")
        assert pol is not None
        assert pol.max_concurrency == 5

        # Update policy
        updated = await set_concurrency_limit_policy(session, "WORKFLOW", "wf-100", max_concurrency=2, is_enabled=False)
        assert updated.max_concurrency == 2
        assert updated.is_enabled is False
        await session.commit()

    async with session_factory() as session:
        pol = await get_concurrency_limit_policy(session, "WORKFLOW", "wf-100")
        assert pol.max_concurrency == 2
        assert pol.is_enabled is False


# ---------------------------------------------------------------------------
# 2. Concurrency limit reached on workflow
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concurrency_limit_reached_on_workflow(session_factory):
    """When active executions reach the workflow concurrency limit, further claims are rejected."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="wf-limit-test",
            tasks=[{"name": "t1"}, {"name": "t2"}, {"name": "t3"}],
            edges=[],  # all 3 are root tasks
        )
        # Set max_concurrency = 2 on this workflow
        await set_concurrency_limit_policy(session, "WORKFLOW", str(wf.id), max_concurrency=2)

        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        d_res = await dispatch_ready_tasks(session, run.id)
        exec_ids = d_res.created_executions
        assert len(exec_ids) == 3
        await session.commit()

    exec_1, exec_2, exec_3 = exec_ids

    # Worker 1 claims task 1 (1/2)
    async with session_factory() as session:
        claim_1 = await claim_execution(session, exec_1, "w1", timedelta(seconds=30))
        assert claim_1 is not None

    # Worker 2 claims task 2 (2/2)
    async with session_factory() as session:
        claim_2 = await claim_execution(session, exec_2, "w2", timedelta(seconds=30))
        assert claim_2 is not None

    # Worker 3 attempts to claim task 3 -> Limit 2/2 reached, claim must be rejected!
    async with session_factory() as session:
        claim_3 = await claim_execution(session, exec_3, "w3", timedelta(seconds=30))
        assert claim_3 is None  # Rejected!

    # Verify task 3 remains in QUEUED status
    async with session_factory() as session:
        row_3 = await session.get(Execution, exec_3)
        assert row_3.status == "QUEUED"


# ---------------------------------------------------------------------------
# 3. Concurrency limit reached on task type
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concurrency_limit_reached_on_task_type(session_factory):
    """A concurrency limit on a task_type limits concurrent execution across different workflows."""
    async with session_factory() as session:
        await set_concurrency_limit_policy(session, "TASK_TYPE", "HTTP_REQUEST", max_concurrency=1)

        wf1 = await create_workflow(
            session=session,
            name="wf-http-1",
            tasks=[{"name": "req_1", "task_type": "HTTP_REQUEST"}],
            edges=[],
        )
        wf2 = await create_workflow(
            session=session,
            name="wf-http-2",
            tasks=[{"name": "req_2", "task_type": "HTTP_REQUEST"}],
            edges=[],
        )

        run1 = await create_workflow_run(session=session, workflow_id=wf1.id, auto_ready_roots=True)
        run2 = await create_workflow_run(session=session, workflow_id=wf2.id, auto_ready_roots=True)

        d1 = await dispatch_ready_tasks(session, run1.id)
        d2 = await dispatch_ready_tasks(session, run2.id)
        exec_1 = d1.created_executions[0]
        exec_2 = d2.created_executions[0]
        await session.commit()

    # Claim task from wf1 (1/1 active HTTP_REQUEST tasks)
    async with session_factory() as session:
        claim_1 = await claim_execution(session, exec_1, "w1", timedelta(seconds=30))
        assert claim_1 is not None

    # Claim task from wf2 -> Rejected because HTTP_REQUEST limit (1) is reached!
    async with session_factory() as session:
        claim_2 = await claim_execution(session, exec_2, "w2", timedelta(seconds=30))
        assert claim_2 is None

    # Verify exec_2 remains QUEUED
    async with session_factory() as session:
        e2 = await session.get(Execution, exec_2)
        assert e2.status == "QUEUED"


# ---------------------------------------------------------------------------
# 4. Concurrent claim race
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concurrent_claim_race(session_factory):
    """Under a concurrency limit of 1, competing workers racing to claim 2 queued executions allow only 1 claim."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="race-wf",
            tasks=[{"name": "task_a"}, {"name": "task_b"}],
            edges=[],
        )
        await set_concurrency_limit_policy(session, "WORKFLOW", str(wf.id), max_concurrency=1)
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        d_res = await dispatch_ready_tasks(session, run.id)
        exec_a, exec_b = d_res.created_executions
        await session.commit()

    # Two workers race to claim simultaneously
    async def try_claim_a():
        async with session_factory() as s:
            return await claim_execution(s, exec_a, "worker-a", timedelta(seconds=30))

    async def try_claim_b():
        async with session_factory() as s:
            return await claim_execution(s, exec_b, "worker-b", timedelta(seconds=30))

    results = await asyncio.gather(try_claim_a(), try_claim_b())
    successful_claims = [r for r in results if r is not None]
    rejected_claims = [r for r in results if r is None]

    assert len(successful_claims) == 1
    assert len(rejected_claims) == 1

    # In database: exactly 1 is CLAIMED, exactly 1 is QUEUED
    async with session_factory() as session:
        ea = await session.get(Execution, exec_a)
        eb = await session.get(Execution, exec_b)
        statuses = {ea.status, eb.status}
        assert statuses == {"CLAIMED", "QUEUED"}


# ---------------------------------------------------------------------------
# 5. Execution finishing and freeing capacity
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_execution_finishing_frees_capacity(session_factory):
    """Completing an active execution frees capacity, allowing a previously rejected execution to be claimed."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="capacity-free-wf",
            tasks=[{"name": "first"}, {"name": "second"}],
            edges=[],
        )
        await set_concurrency_limit_policy(session, "WORKFLOW", str(wf.id), max_concurrency=1)
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        d_res = await dispatch_ready_tasks(session, run.id)
        exec_1, exec_2 = d_res.created_executions
        await session.commit()

    # Claim first
    async with session_factory() as session:
        claim_1 = await claim_execution(session, exec_1, "w1", timedelta(seconds=30))
        assert claim_1 is not None

    # Claim second -> Rejected
    async with session_factory() as session:
        claim_2 = await claim_execution(session, exec_2, "w2", timedelta(seconds=30))
        assert claim_2 is None

    # Worker completes first execution
    async with session_factory() as session:
        await start_execution(session, claim_1)
        completed = await complete_execution(session, claim_1)
        assert completed is True

    # Capacity is now freed! Claim second execution -> Now succeeds!
    async with session_factory() as session:
        claim_2_retry = await claim_execution(session, exec_2, "w2", timedelta(seconds=30))
        assert claim_2_retry is not None
        assert claim_2_retry.execution_id == exec_2


# ---------------------------------------------------------------------------
# 6. Rate-limit acceptance and rejection
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_rate_limit_acceptance_and_rejection(session_factory):
    """Rate limit admits up to max_requests in window, rejects excess, and admits after window expires."""
    async with session_factory() as session:
        # Category "llm-calls" rate limit: 2 requests per 10 seconds
        await set_rate_limit_policy(
            session, "CATEGORY", "llm-calls", max_requests=2, window_seconds=10
        )

        # Create 3 executions with category "llm-calls"
        e1 = Execution(status="QUEUED", category="llm-calls")
        e2 = Execution(status="QUEUED", category="llm-calls")
        e3 = Execution(status="QUEUED", category="llm-calls")
        # And 1 execution with category "db-query" (unaffected)
        e_db = Execution(status="QUEUED", category="db-query")
        session.add_all([e1, e2, e3, e_db])
        await session.commit()
        e1_id, e2_id, e3_id, edb_id = e1.id, e2.id, e3.id, e_db.id

    t0 = datetime(2026, 10, 8, 12, 0, 0, tzinfo=timezone.utc)

    # 1st claim in window: admitted
    async with session_factory() as session:
        c1 = await claim_execution(session, e1_id, "w", timedelta(seconds=30), now=t0)
        assert c1 is not None

    # 2nd claim in window: admitted (2/2)
    async with session_factory() as session:
        c2 = await claim_execution(session, e2_id, "w", timedelta(seconds=30), now=t0 + timedelta(seconds=2))
        assert c2 is not None

    # 3rd claim in window: REJECTED (exceeds 2/2)
    async with session_factory() as session:
        c3 = await claim_execution(session, e3_id, "w", timedelta(seconds=30), now=t0 + timedelta(seconds=4))
        assert c3 is None

    # Execution with different category ("db-query") is admitted without rate limit restriction
    async with session_factory() as session:
        c_db = await claim_execution(session, edb_id, "w", timedelta(seconds=30), now=t0 + timedelta(seconds=4))
        assert c_db is not None

    # Time advances past 10-second window (t0 + 12s)
    # The first claim at t0 is now outside the [t0+2s, t0+12s] window!
    async with session_factory() as session:
        c3_after_window = await claim_execution(
            session, e3_id, "w", timedelta(seconds=30), now=t0 + timedelta(seconds=12)
        )
        assert c3_after_window is not None
        assert c3_after_window.execution_id == e3_id


# ---------------------------------------------------------------------------
# 7. Policy disabled / unlimited behavior
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_policy_disabled_and_unlimited_behavior(session_factory):
    """When a policy is disabled (is_enabled=False) or absent, claims proceed without restriction."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="disabled-wf",
            tasks=[{"name": "t1"}, {"name": "t2"}],
            edges=[],
        )
        # Policy is explicitly DISABLED
        await set_concurrency_limit_policy(
            session, "WORKFLOW", str(wf.id), max_concurrency=1, is_enabled=False
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        d_res = await dispatch_ready_tasks(session, run.id)
        exec_1, exec_2 = d_res.created_executions
        await session.commit()

    # Both executions can be claimed concurrently because policy is disabled
    async with session_factory() as session:
        c1 = await claim_execution(session, exec_1, "w1", timedelta(seconds=30))
        assert c1 is not None

    async with session_factory() as session:
        c2 = await claim_execution(session, exec_2, "w2", timedelta(seconds=30))
        assert c2 is not None


# ---------------------------------------------------------------------------
# 8. Recovery not leaking capacity
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_recovery_not_leaking_capacity(session_factory):
    """When an execution with an expired lease is recovered to QUEUED, it does not leak capacity."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="recovery-capacity-wf",
            tasks=[{"name": "task_1"}, {"name": "task_2"}],
            edges=[],
        )
        await set_concurrency_limit_policy(session, "WORKFLOW", str(wf.id), max_concurrency=1)
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        d_res = await dispatch_ready_tasks(session, run.id)
        exec_1, exec_2 = d_res.created_executions
        await session.commit()

    # Worker 1 claims task_1 with short lease
    async with session_factory() as session:
        claim_1 = await claim_execution(session, exec_1, "worker-crash", timedelta(seconds=10))
        assert claim_1 is not None
        await start_execution(session, claim_1)

    # Task 2 cannot be claimed while task_1 is running
    async with session_factory() as session:
        assert await claim_execution(session, exec_2, "w2", timedelta(seconds=30)) is None

    # Worker 1 crashes. Time advances past lease expiration.
    recovery_time = datetime.now(timezone.utc) + timedelta(seconds=20)
    async with session_factory() as session:
        recovered = await recover_expired_executions(session, now=recovery_time)
        assert exec_1 in recovered

    # In database: task_1 is now QUEUED (no longer active).
    # Task 2 can now be claimed immediately!
    async with session_factory() as session:
        claim_2 = await claim_execution(session, exec_2, "w2", timedelta(seconds=30))
        assert claim_2 is not None
        assert claim_2.execution_id == exec_2


# ---------------------------------------------------------------------------
# 9. Duplicate queue delivery not bypassing limits
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_duplicate_queue_delivery_not_bypassing_limits(session_factory):
    """Redelivered queue messages cannot bypass concurrency limits while capacity is full."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="duplicate-delivery-wf",
            tasks=[{"name": "running_task"}, {"name": "waiting_task"}],
            edges=[],
        )
        await set_concurrency_limit_policy(session, "WORKFLOW", str(wf.id), max_concurrency=1)
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        d_res = await dispatch_ready_tasks(session, run.id)
        exec_running, exec_waiting = d_res.created_executions
        await session.commit()

    # Claim running_task
    async with session_factory() as session:
        c1 = await claim_execution(session, exec_running, "w1", timedelta(seconds=30))
        assert c1 is not None

    # Simulate 5 duplicate queue deliveries for waiting_task
    for i in range(5):
        async with session_factory() as session:
            duplicate_claim = await claim_execution(
                session, exec_waiting, f"worker-{i}", timedelta(seconds=30)
            )
            assert duplicate_claim is None  # Every duplicate attempt is rejected!

    # Verify waiting_task is still QUEUED and untouched
    async with session_factory() as session:
        row = await session.get(Execution, exec_waiting)
        assert row.status == "QUEUED"
        assert row.worker_id is None


# ---------------------------------------------------------------------------
# 10. Rate limit record growth and cleanup
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_rate_limit_record_cleanup(session_factory):
    """Verify that historical rate limit records older than retention buffer are pruned."""
    async with session_factory() as session:
        policy = await set_rate_limit_policy(
            session, "CATEGORY", "cleanup-test", max_requests=10, window_seconds=60
        )
        await session.commit()
        pol_id = policy.id

    now = datetime(2026, 10, 8, 12, 0, 0, tzinfo=timezone.utc)
    old_time = now - timedelta(hours=2)
    recent_time = now - timedelta(minutes=5)

    async with session_factory() as session:
        # Create 3 old records (2 hours ago) and 2 recent records (5 minutes ago)
        old_records = [
            RateLimitRecord(policy_id=pol_id, recorded_at=old_time) for _ in range(3)
        ]
        recent_records = [
            RateLimitRecord(policy_id=pol_id, recorded_at=recent_time) for _ in range(2)
        ]
        session.add_all(old_records + recent_records)
        await session.commit()

    # Clean up records older than 1 hour (buffer_seconds=3600)
    async with session_factory() as session:
        deleted = await cleanup_expired_rate_limit_records(
            session, now=now, buffer_seconds=3600
        )
        await session.commit()
        assert deleted == 3

    # Verify only the 2 recent records remain
    async with session_factory() as session:
        stmt = select(RateLimitRecord).where(RateLimitRecord.policy_id == pol_id)
        remaining = (await session.execute(stmt)).scalars().all()
        assert len(remaining) == 2

