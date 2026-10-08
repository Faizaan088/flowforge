"""FlowForge Workflow Engine - DAG validation, run tracking, and state management."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from models import (
    Execution,
    WorkflowDefinition,
    WorkflowTask,
    WorkflowEdge,
    WorkflowRun,
    WorkflowTaskExecution,
)
from queue_reconciliation import deliver_executions_to_redis, ReconciliationResult



class WorkflowValidationError(Exception):
    """Base exception for workflow validation errors."""
    pass


class SelfDependencyError(WorkflowValidationError):
    """Raised when a task depends directly on itself."""
    pass


class DuplicateEdgeError(WorkflowValidationError):
    """Raised when duplicate dependency edges are detected."""
    pass


class InvalidDependencyError(WorkflowValidationError):
    """Raised when an edge references nonexistent or mismatched tasks."""
    pass


class CycleDetectedError(WorkflowValidationError):
    """Raised when a cycle is detected in the workflow DAG."""

    def __init__(self, message: str, cycle_path: list[int] | None = None):
        super().__init__(message)
        self.cycle_path = cycle_path or []


class InvalidStateTransitionError(WorkflowValidationError):
    """Raised when an invalid state transition is attempted on a run or task execution."""
    pass


# Task execution state constants
TASK_STATUS_PENDING = "PENDING"
TASK_STATUS_READY = "READY"
TASK_STATUS_RUNNING = "RUNNING"
TASK_STATUS_SUCCEEDED = "SUCCEEDED"
TASK_STATUS_FAILED = "FAILED"
TASK_STATUS_SKIPPED = "SKIPPED"
TASK_STATUS_CANCELLED = "CANCELLED"

TERMINAL_TASK_STATUSES = {
    TASK_STATUS_SUCCEEDED,
    TASK_STATUS_FAILED,
    TASK_STATUS_SKIPPED,
    TASK_STATUS_CANCELLED,
}

VALID_TASK_TRANSITIONS: dict[str, set[str]] = {
    TASK_STATUS_PENDING: {
        TASK_STATUS_READY,
        TASK_STATUS_RUNNING,
        TASK_STATUS_SKIPPED,
        TASK_STATUS_CANCELLED,
    },
    TASK_STATUS_READY: {
        TASK_STATUS_RUNNING,
        TASK_STATUS_SKIPPED,
        TASK_STATUS_CANCELLED,
    },
    TASK_STATUS_RUNNING: {
        TASK_STATUS_SUCCEEDED,
        TASK_STATUS_FAILED,
        TASK_STATUS_SKIPPED,
        TASK_STATUS_CANCELLED,
        TASK_STATUS_READY,
    },
    TASK_STATUS_SUCCEEDED: set(),
    TASK_STATUS_FAILED: set(),
    TASK_STATUS_SKIPPED: set(),
    TASK_STATUS_CANCELLED: set(),
}

# Workflow run state constants
RUN_STATUS_PENDING = "PENDING"
RUN_STATUS_RUNNING = "RUNNING"
RUN_STATUS_SUCCEEDED = "SUCCEEDED"
RUN_STATUS_FAILED = "FAILED"
RUN_STATUS_CANCELLED = "CANCELLED"

TERMINAL_RUN_STATUSES = {
    RUN_STATUS_SUCCEEDED,
    RUN_STATUS_FAILED,
    RUN_STATUS_CANCELLED,
}

VALID_RUN_TRANSITIONS: dict[str, set[str]] = {
    RUN_STATUS_PENDING: {
        RUN_STATUS_RUNNING,
        RUN_STATUS_CANCELLED,
        RUN_STATUS_FAILED,
    },
    RUN_STATUS_RUNNING: {
        RUN_STATUS_SUCCEEDED,
        RUN_STATUS_FAILED,
        RUN_STATUS_CANCELLED,
    },
    RUN_STATUS_SUCCEEDED: set(),
    RUN_STATUS_FAILED: set(),
    RUN_STATUS_CANCELLED: set(),
}


def transition_task_execution(
    task_exec: WorkflowTaskExecution,
    new_status: str,
    *,
    error_summary: str | None = None,
    now: datetime | None = None,
) -> WorkflowTaskExecution:
    """
    Safely transition a WorkflowTaskExecution to a new status.
    Raises InvalidStateTransitionError on disallowed or terminal transitions.
    """
    current_status = task_exec.status
    allowed = VALID_TASK_TRANSITIONS.get(current_status, set())
    if new_status not in allowed:
        raise InvalidStateTransitionError(
            f"Cannot transition task execution from '{current_status}' to '{new_status}'"
        )

    ts = now or datetime.now(timezone.utc)
    if new_status == TASK_STATUS_RUNNING:
        if task_exec.started_at is None:
            task_exec.started_at = ts
        task_exec.attempt += 1
    elif new_status in TERMINAL_TASK_STATUSES:
        task_exec.finished_at = ts

    if error_summary is not None:
        task_exec.error_summary = error_summary

    task_exec.status = new_status
    return task_exec


def transition_workflow_run(
    run: WorkflowRun,
    new_status: str,
    *,
    error_summary: str | None = None,
    now: datetime | None = None,
) -> WorkflowRun:
    """
    Safely transition a WorkflowRun to a new status.
    Raises InvalidStateTransitionError on disallowed or terminal transitions.
    """
    current_status = run.status
    allowed = VALID_RUN_TRANSITIONS.get(current_status, set())
    if new_status not in allowed:
        raise InvalidStateTransitionError(
            f"Cannot transition workflow run from '{current_status}' to '{new_status}'"
        )

    ts = now or datetime.now(timezone.utc)
    if new_status == RUN_STATUS_RUNNING:
        if run.started_at is None:
            run.started_at = ts
    elif new_status in TERMINAL_RUN_STATUSES:
        run.finished_at = ts

    if error_summary is not None:
        run.error_summary = error_summary

    run.status = new_status
    return run


def validate_dag(
    task_ids: Iterable[int],
    edges: Iterable[tuple[int, int] | WorkflowEdge],
) -> list[int]:
    """
    Validate that the given tasks and directed edges form a valid directed acyclic graph (DAG).

    - Rejects self-dependencies (u -> u).
    - Rejects edges referencing tasks not in task_ids.
    - Rejects duplicate edges.
    - Rejects cycles (returns topological sort if acyclic).

    Returns:
        List of task IDs in a deterministic topological order (Kahn's algorithm with tie-breaking).
    """
    node_set = set(task_ids)
    normalized_edges: list[tuple[int, int]] = []
    seen_edges: set[tuple[int, int]] = set()

    for edge in edges:
        if isinstance(edge, WorkflowEdge):
            u, v = edge.upstream_task_id, edge.downstream_task_id
        elif isinstance(edge, (tuple, list)) and len(edge) == 2:
            u, v = edge[0], edge[1]
        else:
            raise WorkflowValidationError(f"Invalid edge format: {edge}")

        if u == v:
            raise SelfDependencyError(
                f"Self-dependency detected: task {u} cannot depend on itself"
            )

        if u not in node_set:
            raise InvalidDependencyError(
                f"Upstream task {u} does not exist in workflow task set"
            )
        if v not in node_set:
            raise InvalidDependencyError(
                f"Downstream task {v} does not exist in workflow task set"
            )

        edge_tuple = (u, v)
        if edge_tuple in seen_edges:
            raise DuplicateEdgeError(
                f"Duplicate edge detected from task {u} to task {v}"
            )
        seen_edges.add(edge_tuple)
        normalized_edges.append(edge_tuple)

    # In-degree and adjacency list
    in_degree: dict[int, int] = {t: 0 for t in node_set}
    adjacency: dict[int, list[int]] = {t: [] for t in node_set}

    for u, v in normalized_edges:
        adjacency[u].append(v)
        in_degree[v] += 1

    # Kahn's algorithm for deterministic topological sort
    ready: list[int] = sorted([t for t, deg in in_degree.items() if deg == 0])
    topo_order: list[int] = []

    while ready:
        curr = ready.pop(0)
        topo_order.append(curr)

        for neighbor in sorted(adjacency[curr]):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                ready.append(neighbor)
                ready.sort()

    if len(topo_order) != len(node_set):
        cycle_path = _find_cycle(node_set, adjacency, in_degree)
        cycle_str = " -> ".join(map(str, cycle_path)) if cycle_path else "unknown"
        raise CycleDetectedError(
            f"Cycle detected in workflow DAG: {cycle_str}",
            cycle_path=cycle_path,
        )

    return topo_order


def _find_cycle(
    nodes: set[int],
    adjacency: dict[int, list[int]],
    remaining_in_degree: dict[int, int],
) -> list[int]:
    """Find a cycle deterministically using DFS on nodes with remaining in-degree > 0."""
    candidates = sorted([t for t in nodes if remaining_in_degree[t] > 0])
    visited: set[int] = set()
    rec_stack: list[int] = []
    in_stack: set[int] = set()

    def dfs(u: int) -> list[int] | None:
        visited.add(u)
        rec_stack.append(u)
        in_stack.add(u)

        for v in sorted(adjacency[u]):
            if remaining_in_degree[v] > 0:
                if v in in_stack:
                    cycle_start_idx = rec_stack.index(v)
                    return rec_stack[cycle_start_idx:] + [v]
                if v not in visited:
                    cycle = dfs(v)
                    if cycle:
                        return cycle

        in_stack.remove(u)
        rec_stack.pop()
        return None

    for start_node in candidates:
        if start_node not in visited:
            cycle = dfs(start_node)
            if cycle:
                return cycle

    return []


async def validate_workflow_in_db(
    session: AsyncSession,
    workflow_id: int,
) -> list[int]:
    """
    Validate a persisted workflow definition directly from the database.

    Loads the workflow tasks and directed edges directly from the database.
    Validates self-dependencies, duplicate edges, cross-workflow references, and cycles.

    Returns:
        Deterministic topological ordering of task IDs.
    """
    wf = await session.get(WorkflowDefinition, workflow_id)
    if not wf:
        raise WorkflowValidationError(f"Workflow definition {workflow_id} not found")

    task_stmt = select(WorkflowTask).where(WorkflowTask.workflow_id == workflow_id)
    task_res = await session.execute(task_stmt)
    tasks = task_res.scalars().all()
    if not tasks:
        return []

    task_ids = {t.id for t in tasks}

    edge_stmt = select(WorkflowEdge).where(WorkflowEdge.workflow_id == workflow_id)
    edge_res = await session.execute(edge_stmt)
    edges = edge_res.scalars().all()

    return validate_dag(task_ids, edges)


async def create_workflow(
    session: AsyncSession,
    name: str,
    tasks: Sequence[dict | WorkflowTask],
    edges: Sequence[tuple[str, str] | tuple[int, int] | WorkflowEdge] | None = None,
    description: str | None = None,
    validate: bool = True,
) -> WorkflowDefinition:
    """
    Convenience function to create, persist, and optionally validate a workflow definition.
    """
    workflow = WorkflowDefinition(name=name, description=description)
    session.add(workflow)
    await session.flush()

    task_name_to_id: dict[str, int] = {}
    task_objs: list[WorkflowTask] = []
    for t in tasks:
        if isinstance(t, WorkflowTask):
            t.workflow_id = workflow.id
            task_objs.append(t)
        elif isinstance(t, dict):
            task_obj = WorkflowTask(
                workflow_id=workflow.id,
                name=t["name"],
                task_type=t.get("task_type", "STANDARD"),
                config=t.get("config"),
            )
            task_objs.append(task_obj)
        else:
            raise WorkflowValidationError(f"Invalid task specification: {t}")

    session.add_all(task_objs)
    await session.flush()

    for t in task_objs:
        task_name_to_id[t.name] = t.id

    edge_objs: list[WorkflowEdge] = []
    if edges:
        for e in edges:
            if isinstance(e, WorkflowEdge):
                e.workflow_id = workflow.id
                edge_objs.append(e)
            elif isinstance(e, (tuple, list)) and len(e) == 2:
                u, v = e[0], e[1]
                u_id = task_name_to_id.get(u, u) if isinstance(u, str) else u
                v_id = task_name_to_id.get(v, v) if isinstance(v, str) else v
                edge_objs.append(
                    WorkflowEdge(
                        workflow_id=workflow.id,
                        upstream_task_id=u_id,
                        downstream_task_id=v_id,
                    )
                )
            else:
                raise WorkflowValidationError(f"Invalid edge specification: {e}")

    session.add_all(edge_objs)
    await session.flush()

    if validate:
        await validate_workflow_in_db(session, workflow.id)

    await session.commit()
    await session.refresh(workflow)
    return workflow


async def create_workflow_run(
    session: AsyncSession,
    workflow_id: int,
    *,
    triggered_by: str = "MANUAL",
    initial_task_status: str = TASK_STATUS_PENDING,
    auto_ready_roots: bool = False,
) -> WorkflowRun:
    """
    Create and persist a new WorkflowRun for the specified WorkflowDefinition.
    Instantiates WorkflowTaskExecution tracking records for every task in the workflow.
    Validates DAG integrity before creating the run.
    """
    # 1. Validate DAG integrity
    await validate_workflow_in_db(session, workflow_id)

    # 2. Query tasks
    task_stmt = select(WorkflowTask).where(WorkflowTask.workflow_id == workflow_id)
    tasks = (await session.execute(task_stmt)).scalars().all()

    # 3. Create WorkflowRun
    run = WorkflowRun(
        workflow_id=workflow_id,
        status=RUN_STATUS_PENDING,
        triggered_by=triggered_by,
    )
    session.add(run)
    await session.flush()

    # 4. Identify root tasks if auto_ready_roots is True
    root_task_ids: set[int] = set()
    if auto_ready_roots:
        edge_stmt = select(WorkflowEdge.downstream_task_id).where(
            WorkflowEdge.workflow_id == workflow_id
        )
        downstream_ids = set((await session.execute(edge_stmt)).scalars().all())
        root_task_ids = {t.id for t in tasks if t.id not in downstream_ids}

    # 5. Create task executions
    task_execs = []
    for t in tasks:
        status = TASK_STATUS_READY if (auto_ready_roots and t.id in root_task_ids) else initial_task_status
        task_exec = WorkflowTaskExecution(
            workflow_run_id=run.id,
            workflow_task_id=t.id,
            status=status,
        )
        task_execs.append(task_exec)

    session.add_all(task_execs)
    await session.commit()
    await session.refresh(run)
    return run


@dataclass(frozen=True)
class DependencyResolutionResult:
    ready_task_execution_ids: list[int]
    skipped_task_execution_ids: list[int]
    ready_workflow_task_ids: list[int] = field(default_factory=list)
    skipped_workflow_task_ids: list[int] = field(default_factory=list)

    @property
    def ready_task_ids(self) -> list[int]:
        return self.ready_task_execution_ids

    @property
    def skipped_task_ids(self) -> list[int]:
        return self.skipped_task_execution_ids

    def as_dict(self) -> dict[str, Any]:
        return {
            "ready_task_execution_ids": self.ready_task_execution_ids,
            "skipped_task_execution_ids": self.skipped_task_execution_ids,
            "ready_workflow_task_ids": self.ready_workflow_task_ids,
            "skipped_workflow_task_ids": self.skipped_workflow_task_ids,
        }


@dataclass(frozen=True)
class TaskDispatchResult:
    dispatched_task_execution_ids: list[int]
    created_execution_ids: list[int]
    reconciliation: ReconciliationResult | None = None

    @property
    def dispatched_task_ids(self) -> list[int]:
        return self.dispatched_task_execution_ids

    @property
    def created_executions(self) -> list[int]:
        return self.created_execution_ids

    def as_dict(self) -> dict[str, Any]:
        return {
            "dispatched_task_execution_ids": self.dispatched_task_execution_ids,
            "created_execution_ids": self.created_execution_ids,
            "reconciliation": (
                self.reconciliation.as_dict() if self.reconciliation else None
            ),
        }


@dataclass(frozen=True)
class WorkflowCycleResult:
    ready_task_ids: list[int]
    skipped_task_ids: list[int]
    dispatched_task_ids: list[int]
    created_executions: list[int]
    reconciliation: ReconciliationResult | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "ready_task_ids": self.ready_task_ids,
            "skipped_task_ids": self.skipped_task_ids,
            "dispatched_task_ids": self.dispatched_task_ids,
            "created_executions": self.created_executions,
            "reconciliation": (
                self.reconciliation.as_dict() if self.reconciliation else None
            ),
        }


async def sync_task_execution_status(
    session: AsyncSession,
    task_exec: WorkflowTaskExecution,
) -> bool:
    """
    Synchronize the WorkflowTaskExecution status from its linked durable Execution.
    Returns True if the task execution status changed.
    """
    if task_exec.execution_id is None or task_exec.status in TERMINAL_TASK_STATUSES:
        return False

    exec_row = await session.get(Execution, task_exec.execution_id)
    if not exec_row:
        return False

    changed = False
    if exec_row.status == "SUCCEEDED":
        if task_exec.status == TASK_STATUS_READY:
            transition_task_execution(task_exec, TASK_STATUS_RUNNING)
        if task_exec.status == TASK_STATUS_RUNNING:
            transition_task_execution(task_exec, TASK_STATUS_SUCCEEDED)
            changed = True
    elif exec_row.status in ("FAILED", "DEAD_LETTERED"):
        if task_exec.status == TASK_STATUS_READY:
            transition_task_execution(task_exec, TASK_STATUS_RUNNING)
        if task_exec.status == TASK_STATUS_RUNNING:
            transition_task_execution(
                task_exec, TASK_STATUS_FAILED, error_summary=exec_row.error_summary
            )
            changed = True
    elif exec_row.status in ("CLAIMED", "RUNNING"):
        if task_exec.status == TASK_STATUS_READY:
            transition_task_execution(task_exec, TASK_STATUS_RUNNING)
            changed = True
    elif exec_row.status in ("QUEUED", "RETRY_WAIT"):
        if task_exec.status == TASK_STATUS_RUNNING:
            transition_task_execution(task_exec, TASK_STATUS_READY)
            changed = True

    return changed


async def resolve_dependencies(
    session: AsyncSession,
    workflow_run_id: int,
) -> DependencyResolutionResult:
    """
    Resolve upstream dependencies for a WorkflowRun.

    - PENDING task becomes READY when ALL upstream dependencies are SUCCEEDED (or 0 upstreams).
    - If ANY upstream dependency fails or becomes terminally unsuccessful (FAILED, SKIPPED, CANCELLED),
      propagate SKIPPED state to downstream tasks.
    - Cascades transitive SKIPPED states deterministically.
    - Concurrent-safe via PostgreSQL row-level locks on WorkflowRun and WorkflowTaskExecution rows.
    """
    run_stmt = select(WorkflowRun).where(WorkflowRun.id == workflow_run_id).with_for_update()
    run = (await session.execute(run_stmt)).scalar_one_or_none()
    if not run:
        raise WorkflowValidationError(f"WorkflowRun {workflow_run_id} not found")

    if run.status in TERMINAL_RUN_STATUSES:
        return DependencyResolutionResult(
            ready_task_execution_ids=[],
            skipped_task_execution_ids=[],
            ready_workflow_task_ids=[],
            skipped_workflow_task_ids=[],
        )

    stmt = (
        select(WorkflowTaskExecution)
        .options(selectinload(WorkflowTaskExecution.workflow_task))
        .where(WorkflowTaskExecution.workflow_run_id == workflow_run_id)
        .order_by(WorkflowTaskExecution.id)
        .with_for_update()
    )
    task_execs = list((await session.execute(stmt)).scalars().all())
    if not task_execs:
        return DependencyResolutionResult(
            ready_task_execution_ids=[],
            skipped_task_execution_ids=[],
            ready_workflow_task_ids=[],
            skipped_workflow_task_ids=[],
        )

    # Sync any linked Execution statuses
    for te in task_execs:
        await sync_task_execution_status(session, te)

    edge_stmt = (
        select(WorkflowEdge)
        .where(WorkflowEdge.workflow_id == run.workflow_id)
        .order_by(WorkflowEdge.upstream_task_id, WorkflowEdge.downstream_task_id)
    )
    edges = (await session.execute(edge_stmt)).scalars().all()

    te_by_task_id: dict[int, WorkflowTaskExecution] = {
        te.workflow_task_id: te for te in task_execs
    }
    upstreams_by_task_id: dict[int, list[int]] = {
        te.workflow_task_id: [] for te in task_execs
    }
    for edge in edges:
        if edge.downstream_task_id in upstreams_by_task_id:
            upstreams_by_task_id[edge.downstream_task_id].append(edge.upstream_task_id)

    for tid in upstreams_by_task_id:
        upstreams_by_task_id[tid].sort()

    task_ids = set(te_by_task_id.keys())
    try:
        topo_order = validate_dag(
            task_ids, [(e.upstream_task_id, e.downstream_task_id) for e in edges]
        )
    except Exception:
        topo_order = sorted(task_ids)

    TERMINAL_UNSUCCESSFUL = {
        TASK_STATUS_FAILED,
        TASK_STATUS_SKIPPED,
        TASK_STATUS_CANCELLED,
    }

    newly_ready_te_ids: list[int] = []
    newly_ready_wf_task_ids: list[int] = []
    newly_skipped_te_ids: list[int] = []
    newly_skipped_wf_task_ids: list[int] = []

    while True:
        changed = False
        for tid in topo_order:
            te = te_by_task_id[tid]
            if te.status != TASK_STATUS_PENDING:
                continue

            up_ids = upstreams_by_task_id.get(tid, [])
            up_execs = [te_by_task_id[u] for u in up_ids if u in te_by_task_id]

            # 1. Any upstream is terminally unsuccessful -> SKIPPED
            failed_ups = [u for u in up_execs if u.status in TERMINAL_UNSUCCESSFUL]
            if failed_ups:
                failed_names = [
                    u.workflow_task.name if u.workflow_task else f"task-{u.workflow_task_id}"
                    for u in failed_ups
                ]
                reason = f"Upstream dependency failed or skipped ({', '.join(failed_names)})"
                transition_task_execution(te, TASK_STATUS_SKIPPED, error_summary=reason)
                newly_skipped_te_ids.append(te.id)
                newly_skipped_wf_task_ids.append(tid)
                changed = True
                continue

            # 2. All upstreams are SUCCEEDED (or 0 upstreams for root tasks) -> READY
            if all(u.status == TASK_STATUS_SUCCEEDED for u in up_execs):
                transition_task_execution(te, TASK_STATUS_READY)
                newly_ready_te_ids.append(te.id)
                newly_ready_wf_task_ids.append(tid)
                changed = True
                continue

        if not changed:
            break

    # WorkflowRun status update
    all_statuses = [te.status for te in task_execs]

    if run.status == RUN_STATUS_PENDING:
        if any(s in (TASK_STATUS_READY, TASK_STATUS_RUNNING) for s in all_statuses):
            transition_workflow_run(run, RUN_STATUS_RUNNING)

    if all(s in TERMINAL_TASK_STATUSES for s in all_statuses):
        if any(s == TASK_STATUS_FAILED for s in all_statuses):
            if run.status == RUN_STATUS_PENDING:
                transition_workflow_run(run, RUN_STATUS_RUNNING)
            if run.status == RUN_STATUS_RUNNING:
                transition_workflow_run(
                    run, RUN_STATUS_FAILED, error_summary="Workflow task execution failed"
                )
        elif any(s == TASK_STATUS_CANCELLED for s in all_statuses):
            if run.status == RUN_STATUS_PENDING:
                transition_workflow_run(run, RUN_STATUS_RUNNING)
            if run.status == RUN_STATUS_RUNNING:
                transition_workflow_run(
                    run, RUN_STATUS_CANCELLED, error_summary="Workflow task execution cancelled"
                )
        else:
            if run.status == RUN_STATUS_PENDING:
                transition_workflow_run(run, RUN_STATUS_RUNNING)
            if run.status == RUN_STATUS_RUNNING:
                transition_workflow_run(run, RUN_STATUS_SUCCEEDED)

    await session.commit()

    return DependencyResolutionResult(
        ready_task_execution_ids=newly_ready_te_ids,
        skipped_task_execution_ids=newly_skipped_te_ids,
        ready_workflow_task_ids=newly_ready_wf_task_ids,
        skipped_workflow_task_ids=newly_skipped_wf_task_ids,
    )


async def dispatch_ready_tasks(
    session: AsyncSession,
    workflow_run_id: int,
    redis_client=None,
) -> TaskDispatchResult:
    """
    Create and link durable Execution records for READY workflow tasks and enqueue to Redis.

    - Protects against duplicate Execution creation for the same WorkflowTaskExecution.
    - PostgreSQL state is committed before best-effort delivery to Redis.
    - If Redis fails, PostgreSQL state remains committed and durable.
    """
    run = await session.get(WorkflowRun, workflow_run_id, with_for_update=True)
    if not run:
        raise WorkflowValidationError(f"WorkflowRun {workflow_run_id} not found")

    if run.status in TERMINAL_RUN_STATUSES:
        return TaskDispatchResult(
            dispatched_task_execution_ids=[],
            created_execution_ids=[],
            reconciliation=None,
        )

    stmt = (
        select(WorkflowTaskExecution)
        .options(selectinload(WorkflowTaskExecution.workflow_task))
        .where(
            WorkflowTaskExecution.workflow_run_id == workflow_run_id,
            WorkflowTaskExecution.status == TASK_STATUS_READY,
            WorkflowTaskExecution.execution_id.is_(None),
        )
        .order_by(WorkflowTaskExecution.id)
        .with_for_update()
    )
    ready_execs = list((await session.execute(stmt)).scalars().all())

    if not ready_execs:
        return TaskDispatchResult(
            dispatched_task_execution_ids=[],
            created_execution_ids=[],
            reconciliation=None,
        )

    dispatched_te_ids: list[int] = []
    created_exec_ids: list[int] = []

    for te in ready_execs:
        if te.execution_id is not None:
            continue

        wf_task = te.workflow_task
        if wf_task is None:
            wf_task = await session.get(WorkflowTask, te.workflow_task_id)

        job_def_id = None
        max_retries = 3
        if wf_task and wf_task.config and isinstance(wf_task.config, dict):
            job_def_id = wf_task.config.get("job_definition_id") or wf_task.config.get("job_id")
            max_retries = wf_task.config.get("max_retries", 3)

        execution = Execution(
            job_definition_id=job_def_id,
            status="QUEUED",
            max_retries=max_retries,
        )
        session.add(execution)
        await session.flush()

        te.execution_id = execution.id
        dispatched_te_ids.append(te.id)
        created_exec_ids.append(execution.id)

    if created_exec_ids and run.status == RUN_STATUS_PENDING:
        transition_workflow_run(run, RUN_STATUS_RUNNING)

    # Durable PostgreSQL state is committed first
    await session.commit()

    # Best-effort Redis delivery
    reconciliation = None
    if redis_client is not None and created_exec_ids:
        reconciliation = deliver_executions_to_redis(redis_client, created_exec_ids)

    return TaskDispatchResult(
        dispatched_task_execution_ids=dispatched_te_ids,
        created_execution_ids=created_exec_ids,
        reconciliation=reconciliation,
    )


async def resolve_and_dispatch(
    session: AsyncSession,
    workflow_run_id: int,
    redis_client=None,
) -> WorkflowCycleResult:
    """
    Execute a full cycle: dependency resolution followed by READY task dispatch.
    """
    resolution = await resolve_dependencies(session, workflow_run_id)
    dispatch = await dispatch_ready_tasks(
        session, workflow_run_id, redis_client=redis_client
    )
    return WorkflowCycleResult(
        ready_task_ids=resolution.ready_task_ids,
        skipped_task_ids=resolution.skipped_task_ids,
        dispatched_task_ids=dispatch.dispatched_task_ids,
        created_executions=dispatch.created_executions,
        reconciliation=dispatch.reconciliation,
    )


async def advance_workflow_on_execution_terminal(
    session: AsyncSession,
    execution_id: int,
    redis_client=None,
) -> WorkflowCycleResult | None:
    """
    Automatically synchronize WorkflowTaskExecution state and advance the workflow
    when a workflow-linked durable Execution reaches a terminal state (SUCCEEDED, DEAD_LETTERED, or exhausted FAILED).

    - If execution is not workflow-linked, returns None.
    - If execution is non-terminal (RETRY_WAIT, QUEUED, CLAIMED, RUNNING, or retryable FAILED), returns None.
    - If execution is SUCCEEDED, marks task SUCCEEDED and triggers resolve_and_dispatch.
    - If execution is DEAD_LETTERED (or terminal FAILED), marks task FAILED and triggers resolve_and_dispatch (propagating SKIPPED).
    - Thread/transaction-safe: handles active session states and commits transitions cleanly.
    """
    stmt = (
        select(WorkflowTaskExecution)
        .where(WorkflowTaskExecution.execution_id == execution_id)
        .with_for_update()
    )
    task_exec = (await session.execute(stmt)).scalar_one_or_none()
    if task_exec is None:
        if session.in_transaction():
            await session.commit()
        return None

    execution = await session.get(Execution, execution_id)
    if execution is None:
        if session.in_transaction():
            await session.commit()
        return None

    if execution.status == "SUCCEEDED":
        if task_exec.status not in TERMINAL_TASK_STATUSES:
            if task_exec.status == TASK_STATUS_READY:
                transition_task_execution(task_exec, TASK_STATUS_RUNNING)
            if task_exec.status == TASK_STATUS_RUNNING:
                transition_task_execution(task_exec, TASK_STATUS_SUCCEEDED)
        await session.commit()
        return await resolve_and_dispatch(
            session, task_exec.workflow_run_id, redis_client=redis_client
        )

    elif execution.status in ("FAILED", "DEAD_LETTERED"):
        if task_exec.status not in TERMINAL_TASK_STATUSES:
            if task_exec.status == TASK_STATUS_READY:
                transition_task_execution(task_exec, TASK_STATUS_RUNNING)
            if task_exec.status == TASK_STATUS_RUNNING:
                transition_task_execution(
                    task_exec,
                    TASK_STATUS_FAILED,
                    error_summary=execution.error_summary or "Execution failed or dead-lettered",
                )
        await session.commit()
        return await resolve_and_dispatch(
            session, task_exec.workflow_run_id, redis_client=redis_client
        )

    if session.in_transaction():
        await session.commit()
    return None


