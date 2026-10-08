"""Hardening integration tests for the FlowForge DAG/workflow engine.

Focuses on:
- workflow restart/recovery after process or worker interruption
- workflow-linked executions stuck in CLAIMED/RUNNING and lease recovery
- retry/retry-wait/dead-letter interactions
- concurrent dependency resolution and duplicate dispatch
- terminal workflow state correctness and dispatch fencing
- diamond/parallel workflow completion with partial branch failure
- partial Redis failure during multi-task dispatch and queue reconciliation
- stale worker completion fencing
- idempotent repeated resolution calls
"""

import asyncio
from datetime import datetime, timedelta, timezone
import os
import pytest
import pytest_asyncio
from redis.exceptions import RedisError

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
    from execution_claim import (
        claim_execution,
        complete_execution,
        fail_execution,
        start_execution,
    )
    from execution_recovery import recover_expired_executions
    from execution_retry import (
        fail_and_evaluate_retry,
        requeue_eligible_retries,
        transition_failed_execution,
    )
    from models import (
        Execution,
        JobDefinition,
        WorkflowDefinition,
        WorkflowTask,
        WorkflowEdge,
        WorkflowRun,
        WorkflowTaskExecution,
    )
    from queue_reconciliation import QUEUE_NAME, reconcile_queued_executions
    from workflow_engine import (
        TASK_STATUS_CANCELLED,
        TASK_STATUS_FAILED,
        TASK_STATUS_PENDING,
        TASK_STATUS_READY,
        TASK_STATUS_RUNNING,
        TASK_STATUS_SKIPPED,
        TASK_STATUS_SUCCEEDED,
        RUN_STATUS_CANCELLED,
        RUN_STATUS_FAILED,
        RUN_STATUS_PENDING,
        RUN_STATUS_RUNNING,
        RUN_STATUS_SUCCEEDED,
        advance_workflow_on_execution_terminal,
        create_workflow,
        create_workflow_run,
        dispatch_ready_tasks,
        resolve_dependencies,
        resolve_and_dispatch,
        transition_workflow_run,
    )


class RecordingRedis:
    """Mock Redis client capturing pushed execution IDs and optionally simulating failures."""

    def __init__(self, fail_after: int | None = None, unavailable: bool = False):
        self.entries: list[int] = []
        self.fail_after = fail_after
        self.unavailable = unavailable
        self._push_count = 0

    def lpush(self, queue_name: str, execution_id: int) -> None:
        if self.unavailable:
            raise RedisError("Redis connection refused")
        if self.fail_after is not None and self._push_count >= self.fail_after:
            raise RedisError("Simulated Redis failure after threshold")
        self._push_count += 1
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
async def clear_tables(session_factory):
    async with session_factory() as session:
        await session.execute(WorkflowTaskExecution.__table__.delete())
        await session.execute(WorkflowRun.__table__.delete())
        await session.execute(WorkflowEdge.__table__.delete())
        await session.execute(WorkflowTask.__table__.delete())
        await session.execute(WorkflowDefinition.__table__.delete())
        await session.execute(Execution.__table__.delete())
        await session.execute(JobDefinition.__table__.delete())
        await session.commit()


# ---------------------------------------------------------------------------
# 1. Workflow restart and recovery after worker crash
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_workflow_restart_and_recovery_after_worker_crash(session_factory):
    """Workflow task stuck in RUNNING has its expired lease recovered, resets to READY, and completes under worker 2."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="crash-recovery-wf",
            tasks=[{"name": "step_a"}, {"name": "step_b"}],
            edges=[("step_a", "step_b")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        d_res = await dispatch_ready_tasks(session, run.id, redis_client=redis)
        exec_a_id = d_res.created_executions[0]
        run_id = run.id

    # Worker 1 claims step_a with 10s lease and starts it
    async with session_factory() as session:
        claim_1 = await claim_execution(session, exec_a_id, "worker-1", timedelta(seconds=10))
        assert claim_1 is not None
        await start_execution(session, claim_1)

    # Simulate worker crash and restart after lease expiration (20s later)
    recovery_time = datetime.now(timezone.utc) + timedelta(seconds=20)
    async with session_factory() as session:
        recovered = await recover_expired_executions(session, now=recovery_time)
        assert exec_a_id in recovered

    # Workflow resolution runs upon system restart
    async with session_factory() as session:
        res = await resolve_and_dispatch(session, run_id, redis_client=redis)
        # step_a execution was recovered to QUEUED, so its task execution status was reset to READY safely.
        # No duplicate execution is created because step_a already has execution_id linked.
        assert len(res.created_executions) == 0

    # Worker 2 claims the recovered execution and completes it
    async with session_factory() as session:
        claim_2 = await claim_execution(session, exec_a_id, "worker-2", timedelta(seconds=30))
        assert claim_2 is not None
        await start_execution(session, claim_2)
        assert await complete_execution(session, claim_2, redis_client=redis) is True

    # step_b is automatically READY and dispatched
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}
        assert tasks["step_a"].status == TASK_STATUS_SUCCEEDED
        assert tasks["step_b"].status == TASK_STATUS_READY
        assert tasks["step_b"].execution_id is not None
        exec_b_id = tasks["step_b"].execution_id

    # Worker completes step_b
    async with session_factory() as session:
        claim_b = await claim_execution(session, exec_b_id, "worker-2", timedelta(seconds=30))
        await start_execution(session, claim_b)
        assert await complete_execution(session, claim_b, redis_client=redis) is True

    # WorkflowRun successfully reaches SUCCEEDED
    async with session_factory() as session:
        wf_run = await session.get(WorkflowRun, run_id)
        assert wf_run.status == RUN_STATUS_SUCCEEDED


# ---------------------------------------------------------------------------
# 2. Retry / retry-wait / dead-letter interaction
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_retry_wait_recovery_cycle_and_exhaustion_dead_letter(session_factory):
    """Workflow task with max_retries=2 traverses RETRY_WAIT twice before terminal DEAD_LETTERED skips downstreams."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="retry-exhaustion-wf",
            tasks=[
                {"name": "root_task", "config": {"max_retries": 2}},
                {"name": "downstream_task"},
            ],
            edges=[("root_task", "downstream_task")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        d_res = await dispatch_ready_tasks(session, run.id, redis_client=redis)
        exec_id = d_res.created_executions[0]
        run_id = run.id

    # Attempt 0: fail -> moves to RETRY_WAIT
    async with session_factory() as session:
        claim_0 = await claim_execution(session, exec_id, "w1", timedelta(seconds=30))
        await start_execution(session, claim_0)
        outcome_0 = await fail_and_evaluate_retry(
            session, claim_0, error_summary="attempt 0 fail", base_delay=timedelta(seconds=1)
        )
        assert outcome_0 == "RETRY_WAIT"

    # Dependency resolution during RETRY_WAIT does NOT fail workflow task or skip downstreams
    async with session_factory() as session:
        cycle_res = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(cycle_res.ready_task_ids) == 0
        assert len(cycle_res.skipped_task_ids) == 0

        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}
        assert tasks["root_task"].status in (TASK_STATUS_READY, TASK_STATUS_RUNNING)
        assert tasks["downstream_task"].status == TASK_STATUS_PENDING

    # Requeue attempt 1
    async with session_factory() as session:
        await requeue_eligible_retries(session, now=datetime.now(timezone.utc) + timedelta(seconds=5))

    # Attempt 1: fail -> moves to RETRY_WAIT
    async with session_factory() as session:
        claim_1 = await claim_execution(session, exec_id, "w2", timedelta(seconds=30))
        await start_execution(session, claim_1)
        outcome_1 = await fail_and_evaluate_retry(
            session, claim_1, error_summary="attempt 1 fail", base_delay=timedelta(seconds=1)
        )
        assert outcome_1 == "RETRY_WAIT"

    # Requeue attempt 2
    async with session_factory() as session:
        await requeue_eligible_retries(session, now=datetime.now(timezone.utc) + timedelta(seconds=5))

    # Attempt 2 (retries exhausted): fail -> DEAD_LETTERED
    async with session_factory() as session:
        claim_2 = await claim_execution(session, exec_id, "w3", timedelta(seconds=30))
        await start_execution(session, claim_2)
        outcome_2 = await fail_and_evaluate_retry(
            session, claim_2, error_summary="fatal failure", redis_client=redis
        )
        assert outcome_2 == "DEAD_LETTERED"

    # Check terminal states: root is FAILED, downstream is SKIPPED, run is FAILED
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}
        assert tasks["root_task"].status == TASK_STATUS_FAILED
        assert tasks["downstream_task"].status == TASK_STATUS_SKIPPED
        assert tasks["downstream_task"].execution_id is None

        wf_run = await session.get(WorkflowRun, run_id)
        assert wf_run.status == RUN_STATUS_FAILED


# ---------------------------------------------------------------------------
# 3. Concurrent dependency resolution and idempotent repeated calls
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concurrent_resolution_and_idempotent_repeated_calls(session_factory):
    """Concurrent resolve_and_dispatch calls on diamond DAG produce 0 duplicate executions and stable results."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="diamond-concurrency",
            tasks=[{"name": "root"}, {"name": "b1"}, {"name": "b2"}, {"name": "join"}],
            edges=[("root", "b1"), ("root", "b2"), ("b1", "join"), ("b2", "join")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        run_id = run.id

    # Concurrently call resolve_and_dispatch 5 times for root task
    async def run_cycle():
        async with session_factory() as s:
            return await resolve_and_dispatch(s, run_id, redis_client=redis)

    results = await asyncio.gather(*(run_cycle() for _ in range(5)))
    total_created = sum(len(r.created_executions) for r in results)
    assert total_created == 1  # Exactly one execution created across all 5 concurrent calls

    # Complete root task
    async with session_factory() as session:
        stmt = select(WorkflowTaskExecution).where(
            WorkflowTaskExecution.workflow_run_id == run_id,
            WorkflowTaskExecution.execution_id.is_not(None),
        )
        root_te = (await session.execute(stmt)).scalar_one()
        root_exec_id = root_te.execution_id

    async with session_factory() as session:
        claim_root = await claim_execution(session, root_exec_id, "w", timedelta(seconds=30))
        await start_execution(session, claim_root)
        await complete_execution(session, claim_root, redis_client=redis)

    # complete_execution on root already automatically dispatched b1 and b2.
    # Concurrently calling resolve_and_dispatch 5 times must create 0 duplicate executions.
    branch_results = await asyncio.gather(*(run_cycle() for _ in range(5)))
    branch_created = sum(len(r.created_executions) for r in branch_results)
    assert branch_created == 0  # Duplicate protection guarantees no extra executions created

    # Fetch b1 and b2 execution IDs
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(
                WorkflowTaskExecution.workflow_run_id == run_id,
                WorkflowTaskExecution.execution_id.is_not(None),
            )
        )
        branch_tes = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}
        b1_exec_id = branch_tes["b1"].execution_id
        b2_exec_id = branch_tes["b2"].execution_id

    # Concurrently complete b1 and b2
    async def finish_b1():
        async with session_factory() as s:
            claim_b1 = await claim_execution(s, b1_exec_id, "wb1", timedelta(seconds=30))
            await start_execution(s, claim_b1)
            return await complete_execution(s, claim_b1, redis_client=redis)

    async def finish_b2():
        async with session_factory() as s:
            claim_b2 = await claim_execution(s, b2_exec_id, "wb2", timedelta(seconds=30))
            await start_execution(s, claim_b2)
            return await complete_execution(s, claim_b2, redis_client=redis)

    b_results = await asyncio.gather(finish_b1(), finish_b2())
    assert b_results == [True, True]

    # Complete join
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}
        join_exec_id = tasks["join"].execution_id
        assert join_exec_id is not None

    async with session_factory() as session:
        claim_join = await claim_execution(session, join_exec_id, "wj", timedelta(seconds=30))
        await start_execution(session, claim_join)
        await complete_execution(session, claim_join, redis_client=redis)

    # Repeated calls on the completed workflow run are fully idempotent no-ops
    for _ in range(3):
        async with session_factory() as session:
            final_cycle = await resolve_and_dispatch(session, run_id, redis_client=redis)
            assert final_cycle.ready_task_ids == []
            assert final_cycle.skipped_task_ids == []
            assert final_cycle.created_executions == []

    async with session_factory() as session:
        wf_run = await session.get(WorkflowRun, run_id)
        assert wf_run.status == RUN_STATUS_SUCCEEDED


# ---------------------------------------------------------------------------
# 4. Terminal workflow state dispatch fencing
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_terminal_workflow_state_dispatch_fencing(session_factory):
    """If a workflow run is CANCELLED, dispatch_ready_tasks creates 0 executions and fences dispatch."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="cancel-fence-wf",
            tasks=[{"name": "task_1"}, {"name": "task_2"}],
            edges=[("task_1", "task_2")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        run_id = run.id

        # Cancel the workflow run
        transition_workflow_run(run, RUN_STATUS_CANCELLED)
        await session.commit()

    # Attempt to dispatch ready tasks on the cancelled run
    async with session_factory() as session:
        res = await dispatch_ready_tasks(session, run_id, redis_client=redis)
        assert len(res.dispatched_task_execution_ids) == 0
        assert len(res.created_execution_ids) == 0

        cycle = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(cycle.created_executions) == 0

    # Verify no Executions were created in DB
    async with session_factory() as session:
        stmt = select(WorkflowTaskExecution).where(WorkflowTaskExecution.workflow_run_id == run_id)
        tasks = (await session.execute(stmt)).scalars().all()
        for t in tasks:
            assert t.execution_id is None

        wf_run = await session.get(WorkflowRun, run_id)
        assert wf_run.status == RUN_STATUS_CANCELLED


# ---------------------------------------------------------------------------
# 5. Diamond workflow partial branch failure and skip propagation
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_diamond_workflow_partial_branch_failure(session_factory):
    """In a diamond DAG, if one branch succeeds and the other fails terminally, join is SKIPPED and run FAILED."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="diamond-failure-propagation",
            tasks=[{"name": "root"}, {"name": "left"}, {"name": "right"}, {"name": "join"}],
            edges=[("root", "left"), ("root", "right"), ("left", "join"), ("right", "join")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        run_id = run.id

        # Complete root
        d_root = await dispatch_ready_tasks(session, run_id, redis_client=redis)
        root_exec_id = d_root.created_executions[0]

    async with session_factory() as session:
        c_root = await claim_execution(session, root_exec_id, "w", timedelta(seconds=30))
        await start_execution(session, c_root)
        await complete_execution(session, c_root, redis_client=redis)

    async with session_factory() as session:
        # Get left and right executions
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(
                WorkflowTaskExecution.workflow_run_id == run_id,
                WorkflowTaskExecution.status == TASK_STATUS_READY,
            )
        )
        ready_tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}
        left_exec_id = ready_tasks["left"].execution_id
        right_exec_id = ready_tasks["right"].execution_id

    # Left branch succeeds
    async with session_factory() as session:
        c_left = await claim_execution(session, left_exec_id, "w_left", timedelta(seconds=30))
        await start_execution(session, c_left)
        await complete_execution(session, c_left, redis_client=redis)

    # Right branch fails terminally (max_retries=0 -> DEAD_LETTERED)
    async with session_factory() as session:
        c_right = await claim_execution(session, right_exec_id, "w_right", timedelta(seconds=30))
        await start_execution(session, c_right)
        outcome = await fail_and_evaluate_retry(
            session, c_right, error_summary="right branch fatal error", max_retries=0, redis_client=redis
        )
        assert outcome == "DEAD_LETTERED"

    # Verify final states
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}

        assert tasks["root"].status == TASK_STATUS_SUCCEEDED
        assert tasks["left"].status == TASK_STATUS_SUCCEEDED
        assert tasks["right"].status == TASK_STATUS_FAILED
        assert tasks["join"].status == TASK_STATUS_SKIPPED
        assert tasks["join"].execution_id is None

        wf_run = await session.get(WorkflowRun, run_id)
        assert wf_run.status == RUN_STATUS_FAILED


# ---------------------------------------------------------------------------
# 6. Partial Redis failure during multi-task dispatch and reconciliation
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_partial_redis_failure_during_multi_task_dispatch_and_reconciliation(session_factory):
    """When dispatching 3 parallel tasks and Redis fails after the 1st push, PostgreSQL remains intact and reconciliation completes delivery."""
    partial_redis = RecordingRedis(fail_after=1)
    healthy_redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="multi-dispatch-redis-partial-fail",
            tasks=[{"name": "root"}, {"name": "p1"}, {"name": "p2"}, {"name": "p3"}],
            edges=[("root", "p1"), ("root", "p2"), ("root", "p3")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        run_id = run.id

        # Dispatch and complete root using healthy Redis
        d_root = await dispatch_ready_tasks(session, run_id, redis_client=healthy_redis)
        root_exec_id = d_root.created_executions[0]

    async with session_factory() as session:
        c_root = await claim_execution(session, root_exec_id, "w", timedelta(seconds=30))
        await start_execution(session, c_root)
        # Complete root using partial_redis when dispatching downstreams
        await complete_execution(session, c_root, redis_client=partial_redis)

    # In partial_redis, only 1 task made it in
    assert len(partial_redis.entries) == 1

    # In PostgreSQL, ALL 3 tasks are committed with QUEUED executions
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}
        assert tasks["root"].status == TASK_STATUS_SUCCEEDED
        p_exec_ids = {}
        for p in ["p1", "p2", "p3"]:
            assert tasks[p].status == TASK_STATUS_READY
            assert tasks[p].execution_id is not None
            p_exec = await session.get(Execution, tasks[p].execution_id)
            assert p_exec is not None
            assert p_exec.status == "QUEUED"
            p_exec_ids[p] = tasks[p].execution_id

    # Queue reconciliation replays all QUEUED executions to healthy Redis
    async with session_factory() as session:
        recon = await reconcile_queued_executions(session, healthy_redis)
        assert recon.found >= 3
        assert recon.enqueued >= 3

    # All 3 parallel tasks are executed and completed by workers
    for p in ["p1", "p2", "p3"]:
        exec_id = p_exec_ids[p]
        async with session_factory() as session:
            claim = await claim_execution(session, exec_id, f"worker-{p}", timedelta(seconds=30))
            assert claim is not None
            await start_execution(session, claim)
            assert await complete_execution(session, claim, redis_client=healthy_redis) is True

    # Entire workflow completes successfully
    async with session_factory() as session:
        wf_run = await session.get(WorkflowRun, run_id)
        assert wf_run.status == RUN_STATUS_SUCCEEDED


# ---------------------------------------------------------------------------
# 7. Stale worker completion fencing
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_stale_worker_completion_fencing(session_factory):
    """A zombie worker attempting complete_execution on an already recovered and completed execution is rejected."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="zombie-fence-wf",
            tasks=[{"name": "t1"}, {"name": "t2"}],
            edges=[("t1", "t2")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        d_res = await dispatch_ready_tasks(session, run.id, redis_client=redis)
        exec_id = d_res.created_executions[0]
        run_id = run.id

    # Worker 1 claims with short lease
    async with session_factory() as session:
        claim_w1 = await claim_execution(session, exec_id, "zombie-w1", timedelta(seconds=10))
        await start_execution(session, claim_w1)

    # Lease expires, recovery resets to QUEUED
    recovery_time = datetime.now(timezone.utc) + timedelta(seconds=20)
    async with session_factory() as session:
        recovered = await recover_expired_executions(session, now=recovery_time)
        assert exec_id in recovered

    # Worker 2 claims and successfully completes
    async with session_factory() as session:
        claim_w2 = await claim_execution(session, exec_id, "active-w2", timedelta(seconds=30))
        assert claim_w2 is not None
        await start_execution(session, claim_w2)
        assert await complete_execution(session, claim_w2, redis_client=redis) is True

    # Zombie worker 1 wakes up and attempts complete_execution
    async with session_factory() as session:
        assert await complete_execution(session, claim_w1, redis_client=redis) is False

    # Zombie worker 1 attempts fail_execution
    async with session_factory() as session:
        assert await fail_execution(session, claim_w1, error_summary="zombie fail", redis_client=redis) is False

    # Workflow task t1 remains SUCCEEDED and t2 is READY with 1 execution
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}
        assert tasks["t1"].status == TASK_STATUS_SUCCEEDED
        assert tasks["t2"].status == TASK_STATUS_READY
        assert tasks["t2"].execution_id is not None
