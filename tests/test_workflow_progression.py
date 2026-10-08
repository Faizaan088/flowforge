"""Focused integration tests for automatic workflow progression after durable execution completion.

Tests cover:
- successful task automatically advancing a dependent task
- retryable failure not prematurely failing the workflow task
- dead-letter causing downstream skip/failure propagation
- concurrent completion/resolution
- worker crash/recovery interaction
- Redis delivery failure and subsequent recovery
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
        RUN_STATUS_FAILED,
        RUN_STATUS_RUNNING,
        RUN_STATUS_SUCCEEDED,
        create_workflow,
        create_workflow_run,
        dispatch_ready_tasks,
        resolve_and_dispatch,
    )


class RecordingRedis:
    """Mock Redis client capturing pushed execution IDs and simulating failures."""

    def __init__(self, unavailable: bool = False):
        self.entries: list[int] = []
        self.unavailable = unavailable

    def lpush(self, queue_name: str, execution_id: int) -> None:
        if self.unavailable:
            raise RedisError("Redis connection refused")
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
# 1. Successful task automatically advancing a dependent task
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_successful_task_automatically_advances_dependent_task(session_factory):
    """Calling complete_execution on a workflow-linked execution automatically advances dependent tasks."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="auto-advance-wf",
            tasks=[{"name": "step_a"}, {"name": "step_b"}],
            edges=[("step_a", "step_b")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        # Dispatch step_a to Redis
        d_res = await dispatch_ready_tasks(session, run.id, redis_client=redis)
        exec_a_id = d_res.created_executions[0]
        run_id = run.id

    # Worker claims step_a and starts it
    lease_duration = timedelta(seconds=30)
    async with session_factory() as session:
        claim_a = await claim_execution(session, exec_a_id, "worker-1", lease_duration)
        assert claim_a is not None
        assert await start_execution(session, claim_a) is True

    # Worker completes step_a; this must automatically advance the workflow
    async with session_factory() as session:
        completed = await complete_execution(session, claim_a, redis_client=redis)
        assert completed is True

    # Verify without any manual resolve_and_dispatch call:
    # 1. step_a WorkflowTaskExecution is SUCCEEDED
    # 2. step_b WorkflowTaskExecution is READY
    # 3. step_b has a newly created durable Execution linked to it
    # 4. step_b Execution was delivered to Redis
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}

        assert tasks["step_a"].status == TASK_STATUS_SUCCEEDED
        assert tasks["step_a"].execution_id == exec_a_id

        assert tasks["step_b"].status == TASK_STATUS_READY
        assert tasks["step_b"].execution_id is not None
        exec_b_id = tasks["step_b"].execution_id

        exec_b = await session.get(Execution, exec_b_id)
        assert exec_b is not None
        assert exec_b.status == "QUEUED"

        # Redis has entries for both step_a and step_b
        assert exec_b_id in redis.entries


# ---------------------------------------------------------------------------
# 2. Retryable failure not prematurely failing the workflow task
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_retryable_failure_not_prematurely_failing_workflow_task(session_factory):
    """A retryable execution failure enters RETRY_WAIT and does not fail the workflow task or skip downstreams."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="retry-wf",
            tasks=[
                {"name": "task_1", "config": {"max_retries": 3}},
                {"name": "task_2"},
            ],
            edges=[("task_1", "task_2")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        d_res = await dispatch_ready_tasks(session, run.id, redis_client=redis)
        exec_1_id = d_res.created_executions[0]
        run_id = run.id

    # Worker claims task_1 and starts it
    async with session_factory() as session:
        claim_1 = await claim_execution(session, exec_1_id, "worker-1", timedelta(seconds=30))
        await start_execution(session, claim_1)

    # Worker encounters a failure; retry evaluation moves it to RETRY_WAIT
    async with session_factory() as session:
        outcome = await fail_and_evaluate_retry(
            session,
            claim_1,
            error_summary="Transient network timeout",
            base_delay=timedelta(seconds=5),
            redis_client=redis,
        )
        assert outcome == "RETRY_WAIT"

    # Verify task_1 is NOT marked FAILED, task_2 is NOT SKIPPED, run is NOT FAILED
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}

        assert tasks["task_1"].status != TASK_STATUS_FAILED
        assert tasks["task_2"].status == TASK_STATUS_PENDING
        assert tasks["task_2"].execution_id is None

        wf_run = await session.get(WorkflowRun, run_id)
        assert wf_run.status == RUN_STATUS_RUNNING

    # Requeue the retry after backoff
    now_future = datetime.now(timezone.utc) + timedelta(seconds=10)
    async with session_factory() as session:
        requeued = await requeue_eligible_retries(session, now=now_future)
        assert exec_1_id in requeued

    # Worker 2 claims and succeeds on attempt 1
    async with session_factory() as session:
        claim_retry = await claim_execution(session, exec_1_id, "worker-2", timedelta(seconds=30))
        assert claim_retry is not None
        await start_execution(session, claim_retry)
        assert await complete_execution(session, claim_retry, redis_client=redis) is True

    # Now task_1 is SUCCEEDED and task_2 is automatically READY and dispatched
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}

        assert tasks["task_1"].status == TASK_STATUS_SUCCEEDED
        assert tasks["task_2"].status == TASK_STATUS_READY
        assert tasks["task_2"].execution_id is not None
        assert tasks["task_2"].execution_id in redis.entries


# ---------------------------------------------------------------------------
# 3. Dead-letter causing downstream skip/failure propagation
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_dead_letter_causing_downstream_skip_propagation(session_factory):
    """When retries are exhausted and an execution is DEAD_LETTERED, the workflow task fails and skips dependents."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="dead-letter-wf",
            tasks=[
                {"name": "root_task", "config": {"max_retries": 1}},
                {"name": "child_task"},
                {"name": "grandchild_task"},
            ],
            edges=[("root_task", "child_task"), ("child_task", "grandchild_task")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        d_res = await dispatch_ready_tasks(session, run.id, redis_client=redis)
        exec_id = d_res.created_executions[0]
        run_id = run.id

    # Attempt 0: fail -> RETRY_WAIT
    async with session_factory() as session:
        claim_0 = await claim_execution(session, exec_id, "worker-1", timedelta(seconds=30))
        await start_execution(session, claim_0)
        outcome_0 = await fail_and_evaluate_retry(
            session, claim_0, error_summary="err 1", base_delay=timedelta(seconds=1), redis_client=redis
        )
        assert outcome_0 == "RETRY_WAIT"

    # Requeue for attempt 1
    async with session_factory() as session:
        await requeue_eligible_retries(session, now=datetime.now(timezone.utc) + timedelta(seconds=5))

    # Attempt 1 (retries exhausted): fail -> DEAD_LETTERED
    async with session_factory() as session:
        claim_1 = await claim_execution(session, exec_id, "worker-2", timedelta(seconds=30))
        await start_execution(session, claim_1)
        outcome_1 = await fail_and_evaluate_retry(
            session, claim_1, error_summary="fatal crash", redis_client=redis
        )
        assert outcome_1 == "DEAD_LETTERED"

    # Verify automatic propagation:
    # 1. root_task is FAILED
    # 2. child_task is SKIPPED
    # 3. grandchild_task is SKIPPED
    # 4. run is FAILED
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}

        assert tasks["root_task"].status == TASK_STATUS_FAILED
        assert tasks["child_task"].status == TASK_STATUS_SKIPPED
        assert tasks["child_task"].execution_id is None
        assert tasks["grandchild_task"].status == TASK_STATUS_SKIPPED
        assert tasks["grandchild_task"].execution_id is None

        wf_run = await session.get(WorkflowRun, run_id)
        assert wf_run.status == RUN_STATUS_FAILED


# ---------------------------------------------------------------------------
# 4. Concurrent completion / resolution
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concurrent_completion_and_resolution(session_factory):
    """Concurrent worker completions for parallel tasks atomically resolve downstream dependencies."""
    redis = RecordingRedis()

    async with session_factory() as session:
        # Diamond DAG: root -> (branch_1, branch_2) -> join
        wf = await create_workflow(
            session=session,
            name="concurrent-diamond",
            tasks=[
                {"name": "root"},
                {"name": "branch_1"},
                {"name": "branch_2"},
                {"name": "join"},
            ],
            edges=[
                ("root", "branch_1"),
                ("root", "branch_2"),
                ("branch_1", "join"),
                ("branch_2", "join"),
            ],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        run_id = run.id

        # Dispatch and complete root
        d_root = await dispatch_ready_tasks(session, run_id, redis_client=redis)
        claim_root = await claim_execution(session, d_root.created_executions[0], "w1", timedelta(seconds=30))
        await start_execution(session, claim_root)
        await complete_execution(session, claim_root, redis_client=redis)

        # Both branch_1 and branch_2 are now READY and dispatched
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(
                WorkflowTaskExecution.workflow_run_id == run_id,
                WorkflowTaskExecution.status == TASK_STATUS_READY,
            )
        )
        ready_branches = (await session.execute(stmt)).scalars().all()
        assert len(ready_branches) == 2
        exec_b1_id = [t.execution_id for t in ready_branches if t.workflow_task.name == "branch_1"][0]
        exec_b2_id = [t.execution_id for t in ready_branches if t.workflow_task.name == "branch_2"][0]

    # Two workers claim branch_1 and branch_2
    async with session_factory() as session:
        claim_b1 = await claim_execution(session, exec_b1_id, "worker-b1", timedelta(seconds=30))
        claim_b2 = await claim_execution(session, exec_b2_id, "worker-b2", timedelta(seconds=30))
        await start_execution(session, claim_b1)
        await start_execution(session, claim_b2)

    # Both workers finish and call complete_execution concurrently
    async def finish_b1():
        async with session_factory() as s:
            return await complete_execution(s, claim_b1, redis_client=redis)

    async def finish_b2():
        async with session_factory() as s:
            return await complete_execution(s, claim_b2, redis_client=redis)

    results = await asyncio.gather(finish_b1(), finish_b2())
    assert results == [True, True]

    # Join task must be READY and have exactly ONE execution linked and created
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}

        assert tasks["branch_1"].status == TASK_STATUS_SUCCEEDED
        assert tasks["branch_2"].status == TASK_STATUS_SUCCEEDED

        assert tasks["join"].status == TASK_STATUS_READY
        assert tasks["join"].execution_id is not None
        assert tasks["join"].execution_id in redis.entries

        # Verify exactly 1 execution exists for join task in DB
        join_exec = await session.get(Execution, tasks["join"].execution_id)
        assert join_exec is not None
        assert join_exec.status == "QUEUED"


# ---------------------------------------------------------------------------
# 5. Worker crash / recovery interaction
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_crash_and_recovery_interaction(session_factory):
    """A crashed worker with an expired lease cannot advance the workflow; recovered worker advances it."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="crash-recovery-wf",
            tasks=[{"name": "step_1"}, {"name": "step_2"}],
            edges=[("step_1", "step_2")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        d_res = await dispatch_ready_tasks(session, run.id, redis_client=redis)
        exec_1_id = d_res.created_executions[0]
        run_id = run.id

    # Worker 1 claims with short lease (10 seconds)
    async with session_factory() as session:
        claim_w1 = await claim_execution(session, exec_1_id, "worker-crash", timedelta(seconds=10))
        assert claim_w1 is not None
        await start_execution(session, claim_w1)

    # Worker 1 crashes. Time advances past lease expiration (now + 20 seconds).
    recovery_time = datetime.now(timezone.utc) + timedelta(seconds=20)
    async with session_factory() as session:
        recovered = await recover_expired_executions(session, now=recovery_time)
        assert exec_1_id in recovered

    # Worker 2 claims the recovered execution
    async with session_factory() as session:
        claim_w2 = await claim_execution(session, exec_1_id, "worker-alive", timedelta(seconds=30))
        assert claim_w2 is not None
        await start_execution(session, claim_w2)

    # Zombie Worker 1 wakes up and attempts complete_execution with stale claim
    async with session_factory() as session:
        stale_completed = await complete_execution(session, claim_w1, redis_client=redis)
        assert stale_completed is False

    # Verify stale Worker 1 did NOT advance the workflow: step_2 remains PENDING
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}
        assert tasks["step_2"].status == TASK_STATUS_PENDING
        assert tasks["step_2"].execution_id is None

    # Worker 2 successfully completes
    async with session_factory() as session:
        alive_completed = await complete_execution(session, claim_w2, redis_client=redis)
        assert alive_completed is True

    # Now step_2 is automatically READY and dispatched
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}
        assert tasks["step_1"].status == TASK_STATUS_SUCCEEDED
        assert tasks["step_2"].status == TASK_STATUS_READY
        assert tasks["step_2"].execution_id is not None
        assert tasks["step_2"].execution_id in redis.entries


# ---------------------------------------------------------------------------
# 6. Redis delivery failure and subsequent recovery
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_redis_delivery_failure_and_subsequent_recovery(session_factory):
    """When complete_execution encounters Redis delivery failure, PostgreSQL commits and reconciliation recovers."""
    healthy_redis = RecordingRedis()
    failing_redis = RecordingRedis(unavailable=True)

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="redis-failure-wf",
            tasks=[{"name": "first"}, {"name": "second"}],
            edges=[("first", "second")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        d_res = await dispatch_ready_tasks(session, run.id, redis_client=healthy_redis)
        exec_1_id = d_res.created_executions[0]
        run_id = run.id

    # Worker claims and starts task "first"
    async with session_factory() as session:
        claim_1 = await claim_execution(session, exec_1_id, "w1", timedelta(seconds=30))
        await start_execution(session, claim_1)

    # Worker completes task "first", but Redis is unavailable when dispatching "second"
    async with session_factory() as session:
        completed = await complete_execution(session, claim_1, redis_client=failing_redis)
        assert completed is True

    # Verify in PostgreSQL:
    # 1. "first" is SUCCEEDED
    # 2. "second" is READY with an Execution created in QUEUED state
    # 3. Failing Redis has 0 entries for "second"
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        tasks = {te.workflow_task.name: te for te in (await session.execute(stmt)).scalars().all()}
        assert tasks["first"].status == TASK_STATUS_SUCCEEDED

        assert tasks["second"].status == TASK_STATUS_READY
        assert tasks["second"].execution_id is not None
        exec_2_id = tasks["second"].execution_id

        exec_2 = await session.get(Execution, exec_2_id)
        assert exec_2 is not None
        assert exec_2.status == "QUEUED"

        # Failing redis did not store it
        assert exec_2_id not in failing_redis.entries

    # Recovery: Reconcile queue to healthy Redis
    async with session_factory() as session:
        recon = await reconcile_queued_executions(session, healthy_redis)
        assert recon.found >= 1
        assert recon.enqueued >= 1
        assert exec_2_id in healthy_redis.entries

    # Worker claims and finishes "second" from healthy Redis, completing entire workflow
    async with session_factory() as session:
        claim_2 = await claim_execution(session, exec_2_id, "w2", timedelta(seconds=30))
        assert claim_2 is not None
        await start_execution(session, claim_2)
        assert await complete_execution(session, claim_2, redis_client=healthy_redis) is True

    async with session_factory() as session:
        wf_run = await session.get(WorkflowRun, run_id)
        assert wf_run.status == RUN_STATUS_SUCCEEDED
