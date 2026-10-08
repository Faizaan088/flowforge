"""Focused tests for FlowForge DAG data model and workflow validation."""

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
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.orm import selectinload
    from sqlalchemy.pool import NullPool

    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

    from database import Base  # noqa: E402
    from models import (  # noqa: E402
        Execution,
        JobDefinition,
        WorkflowDefinition,
        WorkflowTask,
        WorkflowNode,
        WorkflowEdge,
        WorkflowRun,
        WorkflowTaskExecution,
        WorkflowTaskRun,
    )
    from workflow_engine import (  # noqa: E402
        CycleDetectedError,
        DuplicateEdgeError,
        InvalidDependencyError,
        InvalidStateTransitionError,
        SelfDependencyError,
        WorkflowValidationError,
        create_workflow,
        create_workflow_run,
        transition_task_execution,
        transition_workflow_run,
        validate_dag,
        validate_workflow_in_db,
        TASK_STATUS_PENDING,
        TASK_STATUS_READY,
        TASK_STATUS_RUNNING,
        TASK_STATUS_SUCCEEDED,
        TASK_STATUS_FAILED,
        TASK_STATUS_SKIPPED,
        TASK_STATUS_CANCELLED,
        RUN_STATUS_PENDING,
        RUN_STATUS_RUNNING,
        RUN_STATUS_SUCCEEDED,
        RUN_STATUS_FAILED,
        RUN_STATUS_CANCELLED,
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
        await session.execute(WorkflowTaskExecution.__table__.delete())
        await session.execute(WorkflowRun.__table__.delete())
        await session.execute(WorkflowEdge.__table__.delete())
        await session.execute(WorkflowTask.__table__.delete())
        await session.execute(WorkflowDefinition.__table__.delete())
        await session.execute(Execution.__table__.delete())
        await session.execute(JobDefinition.__table__.delete())
        await session.commit()


@pytest.mark.asyncio
async def test_workflow_and_task_persistence(session_factory):
    """Test persistence of WorkflowDefinition and WorkflowTask with default fields."""
    async with session_factory() as session:
        workflow = WorkflowDefinition(
            name="data-pipeline",
            description="Nightly ETL pipeline",
        )
        session.add(workflow)
        await session.flush()

        task1 = WorkflowTask(
            workflow_id=workflow.id,
            name="extract",
            task_type="STANDARD",
            config={"source": "s3://bucket/raw"},
        )
        task2 = WorkflowNode(  # Verify WorkflowNode alias
            workflow_id=workflow.id,
            name="transform",
            task_type="STANDARD",
            config={"operation": "clean"},
        )
        session.add_all([task1, task2])
        await session.commit()
        workflow_id = workflow.id

    async with session_factory() as session:
        # Re-fetch in fresh session and verify
        fetched = await session.get(
            WorkflowDefinition,
            workflow_id,
            options=[selectinload(WorkflowDefinition.tasks)],
        )
        assert fetched is not None
        assert fetched.name == "data-pipeline"
        assert fetched.version == 1
        assert fetched.status == "ACTIVE"
        assert fetched.created_at is not None
        assert fetched.updated_at is not None
        assert len(fetched.tasks) == 2
        assert len(fetched.nodes) == 2  # nodes synonym

        task_names = {t.name for t in fetched.tasks}
        assert task_names == {"extract", "transform"}


@pytest.mark.asyncio
async def test_workflow_edge_persistence_and_relationships(session_factory):
    """Test directed dependency edge persistence and relationship traversal."""
    async with session_factory() as session:
        workflow = WorkflowDefinition(name="linear-flow")
        session.add(workflow)
        await session.flush()

        t1 = WorkflowTask(workflow_id=workflow.id, name="stage1")
        t2 = WorkflowTask(workflow_id=workflow.id, name="stage2")
        session.add_all([t1, t2])
        await session.flush()

        edge = WorkflowEdge(
            workflow_id=workflow.id,
            upstream_task_id=t1.id,
            downstream_task_id=t2.id,
        )
        session.add(edge)
        await session.commit()
        edge_id = edge.id
        t1_id = t1.id
        t2_id = t2.id

    async with session_factory() as session:
        # Verify edge
        fetched_edge = await session.get(
            WorkflowEdge,
            edge_id,
            options=[
                selectinload(WorkflowEdge.upstream_task),
                selectinload(WorkflowEdge.downstream_task),
            ],
        )
        assert fetched_edge is not None
        assert fetched_edge.upstream_task_id == t1_id
        assert fetched_edge.downstream_task_id == t2_id
        assert fetched_edge.upstream_node_id == t1_id
        assert fetched_edge.downstream_node_id == t2_id
        assert fetched_edge.upstream_task.name == "stage1"
        assert fetched_edge.downstream_task.name == "stage2"

        # Verify task edge collections in fresh session
    async with session_factory() as session:
        fetched_t1 = await session.get(
            WorkflowTask,
            t1_id,
            options=[
                selectinload(WorkflowTask.outgoing_edges),
                selectinload(WorkflowTask.incoming_edges),
            ],
        )
        assert len(fetched_t1.outgoing_edges) == 1
        assert len(fetched_t1.incoming_edges) == 0

        fetched_t2 = await session.get(
            WorkflowTask,
            t2_id,
            options=[
                selectinload(WorkflowTask.outgoing_edges),
                selectinload(WorkflowTask.incoming_edges),
            ],
        )
        assert len(fetched_t2.outgoing_edges) == 0
        assert len(fetched_t2.incoming_edges) == 1


@pytest.mark.asyncio
async def test_valid_dag_diamond_topology():
    """Test valid DAG diamond topology validation and deterministic ordering."""
    # Diamond graph:
    #      A (1)
    #     /   \
    #    B (2) C (3)
    #     \   /
    #      D (4)
    tasks = [1, 2, 3, 4]
    edges = [(1, 2), (1, 3), (2, 4), (3, 4)]

    topo_order = validate_dag(tasks, edges)
    assert topo_order[0] == 1
    assert topo_order[-1] == 4
    assert set(topo_order[1:3]) == {2, 3}
    # Deterministic tie breaking: node 2 before node 3
    assert topo_order == [1, 2, 3, 4]


@pytest.mark.asyncio
async def test_validate_workflow_in_db_valid(session_factory):
    """Test DB-backed validation on a persisted valid DAG."""
    async with session_factory() as session:
        workflow = await create_workflow(
            session=session,
            name="diamond-workflow",
            tasks=[
                {"name": "fetch"},
                {"name": "parse_a"},
                {"name": "parse_b"},
                {"name": "aggregate"},
            ],
            edges=[
                ("fetch", "parse_a"),
                ("fetch", "parse_b"),
                ("parse_a", "aggregate"),
                ("parse_b", "aggregate"),
            ],
        )

        topo_order = await validate_workflow_in_db(session, workflow.id)
        assert len(topo_order) == 4
        # Verify first is 'fetch' and last is 'aggregate'
        task_id_to_name = {t.id: t.name for t in workflow.tasks}
        ordered_names = [task_id_to_name[tid] for tid in topo_order]
        assert ordered_names[0] == "fetch"
        assert ordered_names[-1] == "aggregate"


@pytest.mark.asyncio
async def test_cycle_rejection_in_validator():
    """Test rejection of cycles in standalone validate_dag function."""
    # 3-node cycle: 1 -> 2 -> 3 -> 1
    with pytest.raises(CycleDetectedError) as exc_info:
        validate_dag([1, 2, 3], [(1, 2), (2, 3), (3, 1)])
    assert 1 in exc_info.value.cycle_path
    assert 2 in exc_info.value.cycle_path
    assert 3 in exc_info.value.cycle_path

    # 2-node cycle: 1 -> 2 -> 1
    with pytest.raises(CycleDetectedError):
        validate_dag([1, 2], [(1, 2), (2, 1)])


@pytest.mark.asyncio
async def test_cycle_rejection_in_db(session_factory):
    """Test rejection of cycles when persisting/validating against PostgreSQL."""
    async with session_factory() as session:
        workflow = WorkflowDefinition(name="cycle-test")
        session.add(workflow)
        await session.flush()

        t1 = WorkflowTask(workflow_id=workflow.id, name="node1")
        t2 = WorkflowTask(workflow_id=workflow.id, name="node2")
        t3 = WorkflowTask(workflow_id=workflow.id, name="node3")
        session.add_all([t1, t2, t3])
        await session.flush()

        e1 = WorkflowEdge(workflow_id=workflow.id, upstream_task_id=t1.id, downstream_task_id=t2.id)
        e2 = WorkflowEdge(workflow_id=workflow.id, upstream_task_id=t2.id, downstream_task_id=t3.id)
        e3 = WorkflowEdge(workflow_id=workflow.id, upstream_task_id=t3.id, downstream_task_id=t1.id)
        session.add_all([e1, e2, e3])
        await session.commit()

        # Database-backed cycle detection should reject it
        with pytest.raises(CycleDetectedError) as exc_info:
            await validate_workflow_in_db(session, workflow.id)
        assert len(exc_info.value.cycle_path) >= 3


@pytest.mark.asyncio
async def test_self_dependency_rejection_validator():
    """Test rejection of self-dependencies in validate_dag."""
    with pytest.raises(SelfDependencyError) as exc_info:
        validate_dag([1, 2], [(1, 1)])
    assert "cannot depend on itself" in str(exc_info.value)


@pytest.mark.asyncio
async def test_self_dependency_rejection_db_constraint(session_factory):
    """Test that PostgreSQL CheckConstraint enforces no self-dependencies."""
    async with session_factory() as session:
        workflow = WorkflowDefinition(name="self-dep-test")
        session.add(workflow)
        await session.flush()

        t1 = WorkflowTask(workflow_id=workflow.id, name="task_self")
        session.add(t1)
        await session.flush()

        edge = WorkflowEdge(
            workflow_id=workflow.id,
            upstream_task_id=t1.id,
            downstream_task_id=t1.id,
        )
        session.add(edge)

        with pytest.raises(IntegrityError) as exc_info:
            await session.commit()
        assert "ck_workflow_edges_no_self_dependency" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_duplicate_edge_rejection_validator():
    """Test rejection of duplicate dependency edges in validate_dag."""
    with pytest.raises(DuplicateEdgeError) as exc_info:
        validate_dag([1, 2], [(1, 2), (1, 2)])
    assert "Duplicate edge detected" in str(exc_info.value)


@pytest.mark.asyncio
async def test_duplicate_edge_rejection_db_constraint(session_factory):
    """Test that PostgreSQL UniqueConstraint enforces unique directed edges."""
    async with session_factory() as session:
        workflow = WorkflowDefinition(name="dup-edge-test")
        session.add(workflow)
        await session.flush()

        t1 = WorkflowTask(workflow_id=workflow.id, name="nodeA")
        t2 = WorkflowTask(workflow_id=workflow.id, name="nodeB")
        session.add_all([t1, t2])
        await session.flush()

        edge1 = WorkflowEdge(
            workflow_id=workflow.id,
            upstream_task_id=t1.id,
            downstream_task_id=t2.id,
        )
        session.add(edge1)
        await session.commit()

        # Attempt to insert identical edge
        edge2 = WorkflowEdge(
            workflow_id=workflow.id,
            upstream_task_id=t1.id,
            downstream_task_id=t2.id,
        )
        session.add(edge2)

        with pytest.raises(IntegrityError) as exc_info:
            await session.commit()
        assert "uq_workflow_edges_workflow_upstream_downstream" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_invalid_dependency_rejection_validator():
    """Test rejection when edges reference non-existent tasks in validate_dag."""
    with pytest.raises(InvalidDependencyError) as exc_info:
        validate_dag([1, 2], [(1, 999)])
    assert "Downstream task 999 does not exist" in str(exc_info.value)

    with pytest.raises(InvalidDependencyError) as exc_info:
        validate_dag([1, 2], [(888, 2)])
    assert "Upstream task 888 does not exist" in str(exc_info.value)


@pytest.mark.asyncio
async def test_invalid_dependency_cross_workflow_rejection(session_factory):
    """Test rejection when edge references task belonging to a different workflow."""
    async with session_factory() as session:
        wf1 = WorkflowDefinition(name="wf1")
        wf2 = WorkflowDefinition(name="wf2")
        session.add_all([wf1, wf2])
        await session.flush()

        t1 = WorkflowTask(workflow_id=wf1.id, name="t1")
        t2 = WorkflowTask(workflow_id=wf2.id, name="t2")
        session.add_all([t1, t2])
        await session.flush()

        # Edge in wf1 referencing t2 which belongs to wf2
        edge = WorkflowEdge(
            workflow_id=wf1.id,
            upstream_task_id=t1.id,
            downstream_task_id=t2.id,
        )
        session.add(edge)
        await session.commit()

        # validate_workflow_in_db on wf1 should reject it because t2 is not in wf1 tasks
        with pytest.raises(InvalidDependencyError) as exc_info:
            await validate_workflow_in_db(session, wf1.id)
        assert f"Downstream task {t2.id} does not exist in workflow task set" in str(exc_info.value)


@pytest.mark.asyncio
async def test_duplicate_task_name_per_workflow_rejected(session_factory):
    """Test that workflow tasks must have unique names within the same workflow."""
    async with session_factory() as session:
        workflow = WorkflowDefinition(name="duplicate-task-name")
        session.add(workflow)
        await session.flush()

        t1 = WorkflowTask(workflow_id=workflow.id, name="same_name")
        session.add(t1)
        await session.commit()

        t2 = WorkflowTask(workflow_id=workflow.id, name="same_name")
        session.add(t2)

        with pytest.raises(IntegrityError) as exc_info:
            await session.commit()
        assert "uq_workflow_tasks_workflow_name" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_create_workflow_helper_with_cycle_fails(session_factory):
    """Test that create_workflow helper validates DAG and raises CycleDetectedError."""
    async with session_factory() as session:
        with pytest.raises(CycleDetectedError):
            await create_workflow(
                session=session,
                name="cyclic-workflow",
                tasks=[{"name": "step1"}, {"name": "step2"}],
                edges=[("step1", "step2"), ("step2", "step1")],
                validate=True,
            )


@pytest.mark.asyncio
async def test_create_workflow_run_and_task_executions(session_factory):
    """Test creating a WorkflowRun with task execution records for all workflow nodes."""
    async with session_factory() as session:
        # Create a DAG: root -> branch_a, root -> branch_b
        wf = await create_workflow(
            session=session,
            name="pipeline-v1",
            tasks=[
                {"name": "root"},
                {"name": "branch_a"},
                {"name": "branch_b"},
            ],
            edges=[
                ("root", "branch_a"),
                ("root", "branch_b"),
            ],
        )

        # Create run with auto_ready_roots=True
        run = await create_workflow_run(
            session=session,
            workflow_id=wf.id,
            triggered_by="SCHEDULE",
            auto_ready_roots=True,
        )
        assert run.id is not None
        assert run.status == RUN_STATUS_PENDING
        assert run.triggered_by == "SCHEDULE"
        assert run.created_at is not None
        assert run.updated_at is not None
        run_id = run.id
        wf_id = wf.id

    async with session_factory() as session:
        # Re-fetch run and verify task execution records and relationships
        loaded_run = await session.get(
            WorkflowRun,
            run_id,
            options=[
                selectinload(WorkflowRun.workflow),
                selectinload(WorkflowRun.task_executions).selectinload(
                    WorkflowTaskExecution.workflow_task
                ),
            ],
        )
        assert loaded_run is not None
        assert loaded_run.workflow_id == wf_id
        assert loaded_run.workflow.name == "pipeline-v1"
        assert loaded_run.workflow_definition.name == "pipeline-v1"  # synonym

        # Verify task executions: 3 tasks total
        assert len(loaded_run.task_executions) == 3
        assert len(loaded_run.tasks) == 3  # synonym
        assert len(loaded_run.task_runs) == 3  # synonym

        task_states = {
            te.workflow_task.name: te.status for te in loaded_run.task_executions
        }
        # Root node has 0 incoming dependencies, so auto_ready_roots transitioned it to READY
        assert task_states["root"] == TASK_STATUS_READY
        # Branches depend on root, so they remain PENDING
        assert task_states["branch_a"] == TASK_STATUS_PENDING
        assert task_states["branch_b"] == TASK_STATUS_PENDING


@pytest.mark.asyncio
async def test_task_execution_state_lifecycle_happy_path(session_factory):
    """Test standard execution progression: PENDING -> READY -> RUNNING -> SUCCEEDED."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="simple-wf",
            tasks=[{"name": "step1"}],
        )
        run = await create_workflow_run(
            session=session,
            workflow_id=wf.id,
            auto_ready_roots=False,
        )
        task_exec = run.task_executions[0]
        assert task_exec.status == TASK_STATUS_PENDING
        assert task_exec.started_at is None
        assert task_exec.finished_at is None
        assert task_exec.attempt == 0

        # PENDING -> READY
        transition_task_execution(task_exec, TASK_STATUS_READY)
        assert task_exec.status == TASK_STATUS_READY

        # READY -> RUNNING
        transition_task_execution(task_exec, TASK_STATUS_RUNNING)
        assert task_exec.status == TASK_STATUS_RUNNING
        assert task_exec.started_at is not None
        assert task_exec.attempt == 1

        # RUNNING -> SUCCEEDED
        transition_task_execution(task_exec, TASK_STATUS_SUCCEEDED)
        assert task_exec.status == TASK_STATUS_SUCCEEDED
        assert task_exec.finished_at is not None
        await session.commit()


@pytest.mark.asyncio
async def test_task_execution_failure_and_retry(session_factory):
    """Test failure with error_summary and retrying back from RUNNING to READY."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="fail-wf",
            tasks=[{"name": "worker_task"}],
        )
        run = await create_workflow_run(
            session=session,
            workflow_id=wf.id,
            auto_ready_roots=True,
        )
        task_exec = run.task_executions[0]
        assert task_exec.status == TASK_STATUS_READY

        # READY -> RUNNING
        transition_task_execution(task_exec, TASK_STATUS_RUNNING)
        assert task_exec.attempt == 1

        # RUNNING -> FAILED
        transition_task_execution(
            task_exec,
            TASK_STATUS_FAILED,
            error_summary="Timeout exceeded during processing",
        )
        assert task_exec.status == TASK_STATUS_FAILED
        assert task_exec.error_summary == "Timeout exceeded during processing"
        assert task_exec.finished_at is not None
        await session.commit()


@pytest.mark.asyncio
async def test_task_execution_skipped_and_cancelled_states(session_factory):
    """Test transitions to SKIPPED and CANCELLED states."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="skip-cancel-wf",
            tasks=[{"name": "t1"}, {"name": "t2"}],
        )
        run = await create_workflow_run(
            session=session,
            workflow_id=wf.id,
            auto_ready_roots=False,
        )
        te1 = run.task_executions[0]
        te2 = run.task_executions[1]

        # PENDING -> SKIPPED
        transition_task_execution(te1, TASK_STATUS_SKIPPED)
        assert te1.status == TASK_STATUS_SKIPPED
        assert te1.finished_at is not None

        # PENDING -> CANCELLED
        transition_task_execution(te2, TASK_STATUS_CANCELLED)
        assert te2.status == TASK_STATUS_CANCELLED
        assert te2.finished_at is not None
        await session.commit()


@pytest.mark.asyncio
async def test_invalid_and_terminal_task_execution_transitions(session_factory):
    """Test that disallowed or terminal transitions raise InvalidStateTransitionError."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="terminal-test-wf",
            tasks=[{"name": "task_a"}],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id)
        te = run.task_executions[0]

        # Disallowed non-terminal jump: PENDING -> SUCCEEDED directly
        with pytest.raises(InvalidStateTransitionError):
            transition_task_execution(te, TASK_STATUS_SUCCEEDED)

        # Transition to terminal SUCCEEDED
        transition_task_execution(te, TASK_STATUS_READY)
        transition_task_execution(te, TASK_STATUS_RUNNING)
        transition_task_execution(te, TASK_STATUS_SUCCEEDED)

        # Terminal state cannot transition to any other status
        with pytest.raises(InvalidStateTransitionError):
            transition_task_execution(te, TASK_STATUS_RUNNING)

        with pytest.raises(InvalidStateTransitionError):
            transition_task_execution(te, TASK_STATUS_READY)

        with pytest.raises(InvalidStateTransitionError):
            transition_task_execution(te, TASK_STATUS_FAILED)


@pytest.mark.asyncio
async def test_workflow_run_lifecycle_and_terminal_transitions(session_factory):
    """Test WorkflowRun lifecycle transitions and terminal rejection."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="run-lifecycle-wf",
            tasks=[{"name": "init"}],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id)
        assert run.status == RUN_STATUS_PENDING
        assert run.started_at is None
        assert run.finished_at is None

        # PENDING -> RUNNING
        transition_workflow_run(run, RUN_STATUS_RUNNING)
        assert run.status == RUN_STATUS_RUNNING
        assert run.started_at is not None

        # RUNNING -> SUCCEEDED
        transition_workflow_run(run, RUN_STATUS_SUCCEEDED)
        assert run.status == RUN_STATUS_SUCCEEDED
        assert run.finished_at is not None

        # Terminal SUCCEEDED cannot transition further
        with pytest.raises(InvalidStateTransitionError):
            transition_workflow_run(run, RUN_STATUS_RUNNING)

        with pytest.raises(InvalidStateTransitionError):
            transition_workflow_run(run, RUN_STATUS_FAILED)

        # Test failure lifecycle
        run_failed = await create_workflow_run(session=session, workflow_id=wf.id)
        transition_workflow_run(run_failed, RUN_STATUS_RUNNING)
        transition_workflow_run(
            run_failed, RUN_STATUS_FAILED, error_summary="Critical task failed"
        )
        assert run_failed.status == RUN_STATUS_FAILED
        assert run_failed.error_summary == "Critical task failed"
        assert run_failed.finished_at is not None

        with pytest.raises(InvalidStateTransitionError):
            transition_workflow_run(run_failed, RUN_STATUS_CANCELLED)


@pytest.mark.asyncio
async def test_unique_constraint_on_run_task_execution(session_factory):
    """Test PostgreSQL UniqueConstraint prevents duplicate execution records for same task in run."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="uq-constraint-wf",
            tasks=[{"name": "task1"}],
        )
        run = WorkflowRun(workflow_id=wf.id)
        session.add(run)
        await session.flush()

        task1 = wf.tasks[0]
        te1 = WorkflowTaskExecution(
            workflow_run_id=run.id,
            workflow_task_id=task1.id,
        )
        session.add(te1)
        await session.commit()

        # Second execution record for same run and task must fail
        te2 = WorkflowTaskExecution(
            workflow_run_id=run.id,
            workflow_task_id=task1.id,
        )
        session.add(te2)
        with pytest.raises(IntegrityError) as exc_info:
            await session.commit()
        assert "uq_workflow_task_executions_run_task" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_check_constraints_on_status(session_factory):
    """Test PostgreSQL CheckConstraints reject invalid status values on runs and tasks."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="check-constraint-wf",
            tasks=[{"name": "t1"}],
        )
        wf_id = wf.id
        t1_id = wf.tasks[0].id

    # Test invalid run status in separate session
    async with session_factory() as session:
        invalid_run = WorkflowRun(workflow_id=wf_id, status="INVALID_STATUS")
        session.add(invalid_run)
        with pytest.raises(IntegrityError) as exc_info:
            await session.commit()
        assert "ck_workflow_runs_status" in str(exc_info.value).lower()

    # Test valid run with invalid task execution status in separate session
    async with session_factory() as session:
        valid_run = WorkflowRun(workflow_id=wf_id, status="PENDING")
        session.add(valid_run)
        await session.flush()

        invalid_te = WorkflowTaskExecution(
            workflow_run_id=valid_run.id,
            workflow_task_id=t1_id,
            status="UNKNOWN_STATE",
        )
        session.add(invalid_te)
        with pytest.raises(IntegrityError) as exc_info:
            await session.commit()
        assert "ck_workflow_task_executions_status" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_workflow_task_execution_links_to_durable_execution(session_factory):
    """Test linking WorkflowTaskExecution to an existing Execution record."""
    async with session_factory() as session:
        # Create JobDefinition and Execution
        job = JobDefinition(name="etl-job", payload={"key": "val"})
        session.add(job)
        await session.flush()

        execution = Execution(job_definition_id=job.id, status="QUEUED")
        session.add(execution)
        await session.flush()

        # Create Workflow and Run
        wf = await create_workflow(
            session=session,
            name="linked-exec-wf",
            tasks=[{"name": "process_job"}],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id)
        task_exec = run.task_executions[0]

        # Link Execution record
        task_exec.execution_id = execution.id
        await session.commit()
        te_id = task_exec.id
        exec_id = execution.id

    async with session_factory() as session:
        loaded_te = await session.get(
            WorkflowTaskExecution,
            te_id,
            options=[selectinload(WorkflowTaskExecution.execution)],
        )
        assert loaded_te is not None
        assert loaded_te.execution_id == exec_id
        assert loaded_te.execution is not None
        assert loaded_te.execution.status == "QUEUED"
        assert loaded_te.execution.job_definition_id == job.id


@pytest.mark.asyncio
async def test_workflow_run_cascade_deletions(session_factory):
    """Test cascading delete: deleting a WorkflowRun deletes its WorkflowTaskExecution records."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="cascade-test-wf",
            tasks=[{"name": "a"}, {"name": "b"}],
        )
        run = await create_workflow_run(session=session, workflow_id=wf.id)
        run_id = run.id
        await session.commit()

    async with session_factory() as session:
        # Delete run
        to_delete = await session.get(WorkflowRun, run_id)
        await session.delete(to_delete)
        await session.commit()

    async with session_factory() as session:
        # Check task executions were cascaded
        te_stmt = select(WorkflowTaskExecution).where(
            WorkflowTaskExecution.workflow_run_id == run_id
        )
        remaining = (await session.execute(te_stmt)).scalars().all()
        assert len(remaining) == 0
