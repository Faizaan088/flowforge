"""Focused integration tests for DAG dependency resolution and READY-task dispatch.

Tests cover:
- single dependency
- multiple dependencies
- diamond DAG
- parallel READY branches
- failed upstream propagation (linear and diamond)
- duplicate / concurrent resolution
- duplicate dispatch protection
- Redis delivery failure preserving PostgreSQL state
"""

import asyncio
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
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.orm import selectinload
    from sqlalchemy.pool import NullPool

    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

    from database import Base
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
        resolve_dependencies,
        resolve_and_dispatch,
        transition_task_execution,
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
# 1. Single dependency (A -> B)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_single_dependency_resolution_and_dispatch(session_factory):
    """Test linear single dependency A -> B: A becomes READY, then B after A succeeds."""
    redis = RecordingRedis()

    async with session_factory() as session:
        job_a = JobDefinition(name="job-a", payload={"step": "A"})
        session.add(job_a)
        await session.flush()

        wf = await create_workflow(
            session=session,
            name="linear-a-b",
            tasks=[
                {"name": "task_a", "config": {"job_definition_id": job_a.id}},
                {"name": "task_b"},
            ],
            edges=[("task_a", "task_b")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id)
        run_id = run.id

    # First cycle: task_a should become READY and be dispatched; task_b remains PENDING
    async with session_factory() as session:
        cycle1 = await resolve_and_dispatch(session, run_id, redis_client=redis)

        assert len(cycle1.ready_task_ids) == 1
        assert len(cycle1.dispatched_task_ids) == 1
        assert len(cycle1.created_executions) == 1
        assert len(redis.entries) == 1
        exec_a_id = cycle1.created_executions[0]

        # Verify task_a state and execution linkage
        te_a_id = cycle1.dispatched_task_ids[0]
        te_a = await session.get(WorkflowTaskExecution, te_a_id)
        assert te_a.status == TASK_STATUS_READY
        assert te_a.execution_id == exec_a_id

        # Verify task_b remains PENDING
        stmt = select(WorkflowTaskExecution).where(
            WorkflowTaskExecution.workflow_run_id == run_id,
            WorkflowTaskExecution.id != te_a_id,
        )
        te_b = (await session.execute(stmt)).scalar_one()
        assert te_b.status == TASK_STATUS_PENDING
        assert te_b.execution_id is None

        # Verify workflow run transitioned to RUNNING
        wf_run = await session.get(WorkflowRun, run_id)
        assert wf_run.status == RUN_STATUS_RUNNING

    # Simulate worker executing task_a and succeeding
    async with session_factory() as session:
        exec_a = await session.get(Execution, exec_a_id)
        exec_a.status = "SUCCEEDED"
        await session.commit()

    # Second cycle: task_b upstream is now SUCCEEDED, so task_b becomes READY and dispatched
    async with session_factory() as session:
        cycle2 = await resolve_and_dispatch(session, run_id, redis_client=redis)

        assert len(cycle2.ready_task_ids) == 1
        assert len(cycle2.dispatched_task_ids) == 1
        assert len(cycle2.created_executions) == 1
        assert len(redis.entries) == 2
        exec_b_id = cycle2.created_executions[0]

        te_b_updated = await session.get(WorkflowTaskExecution, cycle2.dispatched_task_ids[0])
        assert te_b_updated.status == TASK_STATUS_READY
        assert te_b_updated.execution_id == exec_b_id

    # Simulate task_b succeeding
    async with session_factory() as session:
        exec_b = await session.get(Execution, exec_b_id)
        exec_b.status = "SUCCEEDED"
        await session.commit()

    # Final cycle: workflow run completes as SUCCEEDED
    async with session_factory() as session:
        cycle3 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(cycle3.ready_task_ids) == 0
        assert len(cycle3.dispatched_task_ids) == 0

        wf_run = await session.get(WorkflowRun, run_id)
        assert wf_run.status == RUN_STATUS_SUCCEEDED


# ---------------------------------------------------------------------------
# 2. Multiple upstream dependencies (A -> C, B -> C)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_multiple_upstream_dependencies(session_factory):
    """Test task C depending on multiple upstreams (A and B): only READY when ALL succeed."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="join-pattern",
            tasks=[{"name": "task_a"}, {"name": "task_b"}, {"name": "task_c"}],
            edges=[("task_a", "task_c"), ("task_b", "task_c")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id)
        run_id = run.id

    # Cycle 1: tasks A and B both become READY and are dispatched; C remains PENDING
    async with session_factory() as session:
        cycle1 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(cycle1.ready_task_ids) == 2
        assert len(cycle1.dispatched_task_ids) == 2
        assert len(cycle1.created_executions) == 2
        exec_ids = cycle1.created_executions

    # Only task A succeeds; task B is still running
    async with session_factory() as session:
        exec_a = await session.get(Execution, exec_ids[0])
        exec_a.status = "SUCCEEDED"
        await session.commit()

    # Cycle 2: C must still be PENDING because B has not succeeded
    async with session_factory() as session:
        cycle2 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(cycle2.ready_task_ids) == 0
        assert len(cycle2.dispatched_task_ids) == 0

    # Now task B succeeds
    async with session_factory() as session:
        exec_b = await session.get(Execution, exec_ids[1])
        exec_b.status = "SUCCEEDED"
        await session.commit()

    # Cycle 3: Now all upstreams of C are SUCCEEDED -> C becomes READY and dispatched
    async with session_factory() as session:
        cycle3 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(cycle3.ready_task_ids) == 1
        assert len(cycle3.dispatched_task_ids) == 1
        assert len(cycle3.created_executions) == 1


# ---------------------------------------------------------------------------
# 3. Diamond DAG (A -> B, A -> C, B -> D, C -> D)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_diamond_dag_resolution_lifecycle(session_factory):
    """Test full diamond DAG: A -> (B, C) -> D."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="diamond-dag",
            tasks=[
                {"name": "A"},
                {"name": "B"},
                {"name": "C"},
                {"name": "D"},
            ],
            edges=[
                ("A", "B"),
                ("A", "C"),
                ("B", "D"),
                ("C", "D"),
            ],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id)
        run_id = run.id

    # 1. Initially only root A is READY and dispatched
    async with session_factory() as session:
        c1 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(c1.ready_task_ids) == 1
        assert len(c1.created_executions) == 1
        exec_a_id = c1.created_executions[0]

    # 2. Complete A
    async with session_factory() as session:
        exec_a = await session.get(Execution, exec_a_id)
        exec_a.status = "SUCCEEDED"
        await session.commit()

    # 3. Both B and C become READY and are dispatched
    async with session_factory() as session:
        c2 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(c2.ready_task_ids) == 2
        assert len(c2.created_executions) == 2
        exec_b_id, exec_c_id = c2.created_executions

    # 4. B completes, C still running -> D must remain PENDING
    async with session_factory() as session:
        exec_b = await session.get(Execution, exec_b_id)
        exec_b.status = "SUCCEEDED"
        await session.commit()

    async with session_factory() as session:
        c3 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(c3.ready_task_ids) == 0
        assert len(c3.created_executions) == 0

    # 5. C completes -> D becomes READY and is dispatched
    async with session_factory() as session:
        exec_c = await session.get(Execution, exec_c_id)
        exec_c.status = "SUCCEEDED"
        await session.commit()

    async with session_factory() as session:
        c4 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(c4.ready_task_ids) == 1
        assert len(c4.created_executions) == 1
        exec_d_id = c4.created_executions[0]

    # 6. D completes -> entire workflow SUCCEEDED
    async with session_factory() as session:
        exec_d = await session.get(Execution, exec_d_id)
        exec_d.status = "SUCCEEDED"
        await session.commit()

    async with session_factory() as session:
        c5 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        wf_run = await session.get(WorkflowRun, run_id)
        assert wf_run.status == RUN_STATUS_SUCCEEDED


# ---------------------------------------------------------------------------
# 4. Parallel READY branches
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_parallel_ready_branches(session_factory):
    """Test root task completion concurrently enables multiple independent parallel branches."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="wide-branches",
            tasks=[
                {"name": "root"},
                {"name": "branch_1"},
                {"name": "branch_2"},
                {"name": "branch_3"},
                {"name": "branch_4"},
            ],
            edges=[
                ("root", "branch_1"),
                ("root", "branch_2"),
                ("root", "branch_3"),
                ("root", "branch_4"),
            ],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id)
        run_id = run.id

    # Root becomes ready and dispatched
    async with session_factory() as session:
        c1 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(c1.dispatched_task_ids) == 1
        root_exec_id = c1.created_executions[0]

    # Root completes
    async with session_factory() as session:
        root_exec = await session.get(Execution, root_exec_id)
        root_exec.status = "SUCCEEDED"
        await session.commit()

    # All 4 parallel branches must become READY and dispatched in the same pass
    async with session_factory() as session:
        c2 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(c2.ready_task_ids) == 4
        assert len(c2.dispatched_task_ids) == 4
        assert len(c2.created_executions) == 4
        # All 4 executions must be distinct
        assert len(set(c2.created_executions)) == 4
        # Redis received all 4 branches + 1 root = 5 items
        assert len(redis.entries) == 5


# ---------------------------------------------------------------------------
# 5. Failed upstream propagation
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_failed_upstream_propagation_linear(session_factory):
    """Test failure of upstream task cascades SKIPPED to all dependent tasks transitively."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="cascade-failure",
            tasks=[
                {"name": "step_1"},
                {"name": "step_2"},
                {"name": "step_3"},
                {"name": "step_4"},
            ],
            edges=[
                ("step_1", "step_2"),
                ("step_2", "step_3"),
                ("step_3", "step_4"),
            ],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id)
        run_id = run.id

    # Dispatch step_1
    async with session_factory() as session:
        c1 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(c1.dispatched_task_ids) == 1
        exec_1_id = c1.created_executions[0]

    # Fail step_1
    async with session_factory() as session:
        exec_1 = await session.get(Execution, exec_1_id)
        exec_1.status = "FAILED"
        exec_1.error_summary = "Out of memory"
        await session.commit()

    # Resolution cycle: step_2, step_3, step_4 must all cascade to SKIPPED
    async with session_factory() as session:
        c2 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(c2.ready_task_ids) == 0
        assert len(c2.created_executions) == 0
        assert len(c2.skipped_task_ids) == 3

        # Verify in DB that step_2, step_3, step_4 are all SKIPPED
        stmt = (
            select(WorkflowTaskExecution)
            .options(selectinload(WorkflowTaskExecution.workflow_task))
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        all_te = (await session.execute(stmt)).scalars().all()
        for te in all_te:
            if te.workflow_task.name == "step_1":
                assert te.status == TASK_STATUS_FAILED
            else:
                assert te.status == TASK_STATUS_SKIPPED
                assert te.execution_id is None
                assert "Upstream dependency failed" in (te.error_summary or "")

        # WorkflowRun must transition to FAILED
        wf_run = await session.get(WorkflowRun, run_id)
        assert wf_run.status == RUN_STATUS_FAILED


@pytest.mark.asyncio
async def test_failed_upstream_propagation_diamond_join(session_factory):
    """Test diamond DAG with one branch failing: join node D must become SKIPPED."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="diamond-failure",
            tasks=[{"name": "A"}, {"name": "B"}, {"name": "C"}, {"name": "D"}],
            edges=[("A", "B"), ("A", "C"), ("B", "D"), ("C", "D")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id)
        run_id = run.id

    # Root A dispatched and succeeds
    async with session_factory() as session:
        c1 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        exec_a = await session.get(Execution, c1.created_executions[0])
        exec_a.status = "SUCCEEDED"
        await session.commit()

    # B and C dispatched
    async with session_factory() as session:
        c2 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(c2.dispatched_task_ids) == 2
        exec_b_id, exec_c_id = c2.created_executions

    # B succeeds, but C FAILS
    async with session_factory() as session:
        exec_b = await session.get(Execution, exec_b_id)
        exec_b.status = "SUCCEEDED"
        exec_c = await session.get(Execution, exec_c_id)
        exec_c.status = "FAILED"
        await session.commit()

    # D has upstream C (FAILED) -> D must be SKIPPED, NOT READY
    async with session_factory() as session:
        c3 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(c3.ready_task_ids) == 0
        assert len(c3.created_executions) == 0
        assert len(c3.skipped_task_ids) == 1

        stmt = select(WorkflowTaskExecution).where(
            WorkflowTaskExecution.workflow_run_id == run_id
        )
        tasks = {t.workflow_task_id: t for t in (await session.execute(stmt)).scalars().all()}
        # Join node D is skipped
        d_exec = [t for t in tasks.values() if t.status == TASK_STATUS_SKIPPED]
        assert len(d_exec) == 1
        assert d_exec[0].execution_id is None


# ---------------------------------------------------------------------------
# 6. Duplicate and concurrent resolution
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_duplicate_and_concurrent_resolution(session_factory):
    """Test concurrent workers attempting resolve_and_dispatch do not race or duplicate state."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="concurrent-wf",
            tasks=[{"name": "root_1"}, {"name": "child_1"}],
            edges=[("root_1", "child_1")],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id)
        run_id = run.id

    async def run_worker_cycle():
        async with session_factory() as worker_session:
            return await resolve_and_dispatch(worker_session, run_id, redis_client=redis)

    # Launch two concurrent workers at the exact same time
    results = await asyncio.gather(run_worker_cycle(), run_worker_cycle())

    # Combined dispatched count must be exactly 1 (one winner, one no-op)
    total_dispatched = sum(len(r.dispatched_task_ids) for r in results)
    total_created_executions = sum(len(r.created_executions) for r in results)
    assert total_dispatched == 1
    assert total_created_executions == 1

    # Exactly 1 execution linked to root_1 in the DB
    async with session_factory() as session:
        stmt = select(WorkflowTaskExecution).where(
            WorkflowTaskExecution.workflow_run_id == run_id,
            WorkflowTaskExecution.status == TASK_STATUS_READY,
        )
        ready_tasks = (await session.execute(stmt)).scalars().all()
        assert len(ready_tasks) == 1
        assert ready_tasks[0].execution_id is not None

        # Verify only 1 Execution in DB
        exec_count = len((await session.execute(select(Execution))).scalars().all())
        assert exec_count == 1


# ---------------------------------------------------------------------------
# 7. Duplicate dispatch protection
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_duplicate_dispatch_protection(session_factory):
    """Test calling dispatch_ready_tasks multiple times preserves idempotency and creates 1 execution."""
    redis = RecordingRedis()

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="dup-dispatch-wf",
            tasks=[{"name": "only_task"}],
        )
        # auto_ready_roots=True sets status to READY initially
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        run_id = run.id

    # First dispatch creates and links Execution
    async with session_factory() as session:
        d1 = await dispatch_ready_tasks(session, run_id, redis_client=redis)
        assert len(d1.dispatched_task_ids) == 1
        assert len(d1.created_executions) == 1
        first_exec_id = d1.created_executions[0]

    # Second dispatch must be a no-op: 0 dispatched, 0 created
    async with session_factory() as session:
        d2 = await dispatch_ready_tasks(session, run_id, redis_client=redis)
        assert len(d2.dispatched_task_ids) == 0
        assert len(d2.created_executions) == 0

    # Third dispatch via resolve_and_dispatch must also be a no-op
    async with session_factory() as session:
        d3 = await resolve_and_dispatch(session, run_id, redis_client=redis)
        assert len(d3.dispatched_task_ids) == 0
        assert len(d3.created_executions) == 0

    # Verify task retains original execution_id
    async with session_factory() as session:
        te = (
            await session.execute(
                select(WorkflowTaskExecution).where(
                    WorkflowTaskExecution.workflow_run_id == run_id
                )
            )
        ).scalar_one()
        assert te.execution_id == first_exec_id

    # Verify unique constraint prevents two task executions sharing an execution_id
    async with session_factory() as session:
        duplicate_te = WorkflowTaskExecution(
            workflow_run_id=run_id,
            workflow_task_id=te.workflow_task_id + 999,  # different task id
            execution_id=first_exec_id,
        )
        session.add(duplicate_te)
        with pytest.raises(IntegrityError) as exc_info:
            await session.commit()
        assert "uq_workflow_task_executions_execution_id" in str(exc_info.value).lower()


# ---------------------------------------------------------------------------
# 8. Redis delivery failure preserving PostgreSQL state
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_redis_delivery_failure_preserving_postgresql_state(session_factory):
    """Test Redis delivery failure leaves durable Execution and READY task in PostgreSQL intact."""
    failing_redis = RecordingRedis(unavailable=True)

    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="redis-fail-wf",
            tasks=[{"name": "durable_task"}],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id, auto_ready_roots=True)
        run_id = run.id

    # Dispatch to failing Redis
    async with session_factory() as session:
        dispatch_result = await dispatch_ready_tasks(
            session, run_id, redis_client=failing_redis
        )

    # Redis error captured gracefully
    assert len(dispatch_result.dispatched_task_ids) == 1
    assert len(dispatch_result.created_executions) == 1
    assert dispatch_result.reconciliation is not None
    assert dispatch_result.reconciliation.redis_error is not None
    assert "Redis connection refused" in dispatch_result.reconciliation.redis_error
    created_exec_id = dispatch_result.created_executions[0]

    # Verify PostgreSQL state was durably preserved
    async with session_factory() as session:
        execution = await session.get(Execution, created_exec_id)
        assert execution is not None
        assert execution.status == "QUEUED"

        task_exec = (
            await session.execute(
                select(WorkflowTaskExecution).where(
                    WorkflowTaskExecution.workflow_run_id == run_id
                )
            )
        ).scalar_one()
        assert task_exec.status == TASK_STATUS_READY
        assert task_exec.execution_id == created_exec_id

    # Later: healthy Redis recovers and queue reconciliation replays durable executions
    healthy_redis = RecordingRedis()
    async with session_factory() as session:
        recon_result = await reconcile_queued_executions(session, healthy_redis)
        assert recon_result.found >= 1
        assert recon_result.enqueued >= 1
        assert created_exec_id in healthy_redis.entries
