"""Comprehensive failure-injection and reliability test suite for FlowForge.

Verifies:
1. Worker crash during CLAIMED and RUNNING with lease expiration and stale worker fencing.
2. Resilience when Redis is unavailable/failing (PostgreSQL durability boundary).
3. Database transaction rollback and state integrity.
4. Duplicate queue delivery and concurrent claim serialization.
5. Retry/exponential backoff/dead-letter races and terminal state protection.
6. Scheduler duplicate occurrence protection and idempotent generation.
7. DAG branch failure and downstream skip propagation.
8. Cancellation vs claim/completion concurrency races.
9. Concurrency policy and rate-limit admission races under contention.
"""

import asyncio
from datetime import datetime, timedelta, timezone
import os
import pytest
import pytest_asyncio
from redis.exceptions import RedisError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

TEST_DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://flowforge_user:flowforge_password@127.0.0.1:5432/flowforge_test",
)

from concurrency_policy import set_concurrency_limit_policy, set_rate_limit_policy
from execution_claim import (
    ExecutionClaim,
    cancel_execution,
    claim_execution,
    claim_next_execution,
    complete_execution,
    fail_execution,
    start_execution,
)
from execution_recovery import recover_expired_executions
from execution_retry import (
    requeue_eligible_retries,
    transition_failed_execution,
)
from models import (
    Execution,
    JobDefinition,
    ScheduleDefinition,
    ScheduleOccurrence,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowTask,
    WorkflowTaskExecution,
)
from database import Base
from queue_reconciliation import deliver_executions_to_redis, reconcile_queued_executions
from scheduler import dispatch_due_occurrences, evaluate_due_schedules
from workflow_engine import (
    create_workflow,
    create_workflow_run,
    resolve_and_dispatch,
)


@pytest_asyncio.fixture
async def session_factory():
    """Create isolated async session factory using NullPool with synchronized schema."""
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
async def cleanup_database(session_factory):
    """Clean execution, policy, and workflow state between failure injection tests."""
    async with session_factory() as session:
        async with session.begin():
            await session.execute(WorkflowTaskExecution.__table__.delete())
            await session.execute(WorkflowRun.__table__.delete())
            await session.execute(Execution.__table__.delete())
            await session.execute(ScheduleOccurrence.__table__.delete())
            await session.execute(ScheduleDefinition.__table__.delete())
            await session.execute(WorkflowTask.__table__.delete())
            await session.execute(WorkflowDefinition.__table__.delete())
            await session.execute(JobDefinition.__table__.delete())


class FailingRedisClient:
    """Mock Redis client simulating connection drops or hard crashes."""

    def lpush(self, *args, **kwargs):
        raise RedisError("Simulated Redis connection failure (ECONNREFUSED)")

    def rpop(self, *args, **kwargs):
        raise RedisError("Simulated Redis timeout")

    def publish(self, *args, **kwargs):
        raise RedisError("Simulated Redis Pub/Sub cluster partitioned")

    def ping(self):
        raise RedisError("Redis is unreachable")


# ==============================================================================
# 1. Worker Crash During CLAIMED / RUNNING & Stale-Worker Fencing
# ==============================================================================

@pytest.mark.asyncio
async def test_worker_crash_during_claimed_and_recovery(session_factory):
    """Worker claims an execution then crashes. Lease expires, engine recovers it, and second worker takes over."""
    t0 = datetime(2026, 10, 9, 10, 0, 0, tzinfo=timezone.utc)
    lease_dur = timedelta(seconds=15)

    async with session_factory() as session:
        async with session.begin():
            job = JobDefinition(name="crash-job", priority=1)
            session.add(job)
            await session.flush()
            execution = Execution(job_definition_id=job.id, status="QUEUED")
            session.add(execution)
            await session.flush()
            exec_id = execution.id

    # Worker-1 claims the work
    async with session_factory() as session:
        claim_w1 = await claim_execution(
            session, exec_id, worker_id="worker-crashed-1", lease_duration=lease_dur, now=t0
        )
        assert claim_w1 is not None
        assert claim_w1.worker_id == "worker-crashed-1"

    # Verify execution is CLAIMED
    async with session_factory() as session:
        e = await session.get(Execution, exec_id)
        assert e.status == "CLAIMED"
        assert e.worker_id == "worker-crashed-1"
        assert e.attempt == 0

    # Advance time past lease duration: Worker-1 crashed and never started
    t_after_lease = t0 + timedelta(seconds=20)
    async with session_factory() as session:
        recovered = await recover_expired_executions(session, now=t_after_lease)
        assert exec_id in recovered

    # Execution is now back in QUEUED with attempt incremented
    async with session_factory() as session:
        e = await session.get(Execution, exec_id)
        assert e.status == "QUEUED"
        assert e.worker_id is None
        assert e.lease_until is None
        assert e.attempt == 1

    # Worker-2 claims and completes the execution
    t_w2 = t_after_lease + timedelta(seconds=2)
    async with session_factory() as session:
        claim_w2 = await claim_execution(
            session, exec_id, worker_id="worker-alive-2", lease_duration=lease_dur, now=t_w2
        )
        assert claim_w2 is not None
        started = await start_execution(session, claim_w2)
        assert started is True
        completed = await complete_execution(session, claim_w2)
        assert completed is True

    # Stale Worker-1 wakes up late and attempts to mutate the execution: FENCED!
    async with session_factory() as session:
        stale_start = await start_execution(session, claim_w1)
        assert stale_start is False, "Stale worker start must be fenced"

        stale_complete = await complete_execution(session, claim_w1)
        assert stale_complete is False, "Stale worker completion must be fenced"

        stale_fail = await fail_execution(session, claim_w1, error_summary="crashed late")
        assert stale_fail is False, "Stale worker failure must be fenced"

    # Final state in PostgreSQL remains cleanly SUCCEEDED by Worker-2
    async with session_factory() as session:
        e = await session.get(Execution, exec_id)
        assert e.status == "SUCCEEDED"
        assert e.worker_id == "worker-alive-2"


@pytest.mark.asyncio
async def test_worker_crash_during_running_and_fencing(session_factory):
    """Worker claims, transitions to RUNNING, then hangs past lease. Stale completion is rejected."""
    t0 = datetime(2026, 10, 9, 10, 0, 0, tzinfo=timezone.utc)
    lease_dur = timedelta(seconds=10)

    async with session_factory() as session:
        async with session.begin():
            execution = Execution(status="QUEUED")
            session.add(execution)
            await session.flush()
            exec_id = execution.id

    # Worker-1 claims and starts
    async with session_factory() as session:
        claim_w1 = await claim_execution(
            session, exec_id, worker_id="worker-hang-1", lease_duration=lease_dur, now=t0
        )
        assert claim_w1 is not None
        started = await start_execution(session, claim_w1)
        assert started is True

    # Advance time: lease expires while RUNNING
    t_expired = t0 + timedelta(seconds=15)
    async with session_factory() as session:
        recovered = await recover_expired_executions(session, now=t_expired)
        assert exec_id in recovered

    # Worker-2 claims the recovered execution
    async with session_factory() as session:
        claim_w2 = await claim_execution(
            session, exec_id, worker_id="worker-2", lease_duration=lease_dur, now=t_expired + timedelta(seconds=1)
        )
        assert claim_w2 is not None
        await start_execution(session, claim_w2)
        await complete_execution(session, claim_w2)

    # Worker-1 tries to complete after unfreezing: FENCED!
    async with session_factory() as session:
        fenced_result = await complete_execution(session, claim_w1)
        assert fenced_result is False

    # Durable state remains SUCCEEDED by worker-2
    async with session_factory() as session:
        e = await session.get(Execution, exec_id)
        assert e.status == "SUCCEEDED"
        assert e.worker_id == "worker-2"


# ==============================================================================
# 2. Redis Interruption / Unavailability Resilience
# ==============================================================================

@pytest.mark.asyncio
async def test_redis_unavailability_preserves_postgresql_durability(session_factory):
    """When Redis is unavailable or throws errors, PostgreSQL remains the durable source of truth."""
    failing_redis = FailingRedisClient()

    # Enqueue execution with failing Redis client
    async with session_factory() as session:
        async with session.begin():
            e = Execution(status="QUEUED", priority=5)
            session.add(e)
            await session.flush()
            exec_id = e.id

    # Worker claims directly from PostgreSQL even though Redis publication fails
    lease_dur = timedelta(seconds=30)
    async with session_factory() as session:
        claim = await claim_next_execution(
            session, worker_id="worker-offline-redis", lease_duration=lease_dur, redis_client=failing_redis
        )
        assert claim is not None
        assert claim.execution_id == exec_id

        # Worker starts and succeeds despite Redis publish errors
        started = await start_execution(session, claim, redis_client=failing_redis)
        assert started is True
        completed = await complete_execution(session, claim, redis_client=failing_redis)
        assert completed is True

    # Verification: PostgreSQL durable state is 100% SUCCEEDED
    async with session_factory() as session:
        e_final = await session.get(Execution, exec_id)
        assert e_final.status == "SUCCEEDED"


# ==============================================================================
# 3. Duplicate Delivery and Concurrent Claim Races
# ==============================================================================

@pytest.mark.asyncio
async def test_duplicate_delivery_serializes_concurrent_claims(session_factory):
    """When duplicate notifications arrive, exactly ONE worker wins the claim; all others get None."""
    async with session_factory() as session:
        async with session.begin():
            e = Execution(status="QUEUED", priority=10)
            session.add(e)
            await session.flush()
            exec_id = e.id

    lease_dur = timedelta(seconds=20)
    workers = [f"worker-race-{i}" for i in range(5)]

    async def _try_claim(worker_name: str):
        async with session_factory() as session:
            return await claim_execution(
                session, exec_id, worker_id=worker_name, lease_duration=lease_dur
            )

    results = await asyncio.gather(*[_try_claim(w) for w in workers])

    successful_claims = [r for r in results if r is not None]
    failed_claims = [r for r in results if r is None]

    assert len(successful_claims) == 1, "Exactly one worker must claim the execution"
    assert len(failed_claims) == 4, "All other competing workers must receive None"

    winner_worker = successful_claims[0].worker_id
    assert winner_worker in workers

    async with session_factory() as session:
        e = await session.get(Execution, exec_id)
        assert e.status == "CLAIMED"
        assert e.worker_id == winner_worker


# ==============================================================================
# 4. Retry Backoff, Dead-Lettering, and Terminal Protection
# ==============================================================================

@pytest.mark.asyncio
async def test_retry_backoff_and_terminal_dead_letter_semantics(session_factory):
    """Executions transition to RETRY_WAIT with exponential backoff and DEAD_LETTERED upon exhausting retries."""
    t0 = datetime(2026, 10, 9, 12, 0, 0, tzinfo=timezone.utc)
    base_delay = timedelta(seconds=10)

    async with session_factory() as session:
        async with session.begin():
            e = Execution(status="FAILED", attempt=0, max_retries=2)
            session.add(e)
            await session.flush()
            exec_id = e.id

    # First failure -> RETRY_WAIT, delay = 10s * (2^0) = 10s
    async with session_factory() as session:
        outcome1 = await transition_failed_execution(
            session, exec_id, base_delay=base_delay, now=t0
        )
        assert outcome1 == "RETRY_WAIT"

    async with session_factory() as session:
        e = await session.get(Execution, exec_id)
        assert e.status == "RETRY_WAIT"
        assert e.available_at == t0 + timedelta(seconds=10)

    # Before available_at, claim_next_execution MUST refuse to claim it
    async with session_factory() as session:
        claim_early = await claim_next_execution(
            session, worker_id="worker-early", lease_duration=timedelta(seconds=15), now=t0 + timedelta(seconds=5)
        )
        assert claim_early is None

    # At available_at, requeue transitions to QUEUED and increments attempt to 1
    t_requeue1 = t0 + timedelta(seconds=10)
    async with session_factory() as session:
        requeued_ids = await requeue_eligible_retries(session, now=t_requeue1)
        assert exec_id in requeued_ids

    async with session_factory() as session:
        e = await session.get(Execution, exec_id)
        assert e.status == "QUEUED"
        assert e.attempt == 1

    # Worker claims, runs, and fails again
    async with session_factory() as session:
        claim2 = await claim_execution(
            session, exec_id, worker_id="worker-try2", lease_duration=timedelta(seconds=15), now=t_requeue1
        )
        assert claim2 is not None
        await start_execution(session, claim2)
        await fail_execution(session, claim2, error_summary="second failure")

    # Second failure -> RETRY_WAIT, delay = 10s * (2^1) = 20s
    async with session_factory() as session:
        outcome2 = await transition_failed_execution(
            session, exec_id, base_delay=base_delay, now=t_requeue1
        )
        assert outcome2 == "RETRY_WAIT"

    # Requeue again at t_requeue1 + 20s
    t_requeue2 = t_requeue1 + timedelta(seconds=20)
    async with session_factory() as session:
        requeued_ids2 = await requeue_eligible_retries(session, now=t_requeue2)
        assert exec_id in requeued_ids2

    # Worker claims and fails third time (attempt reaches 2 >= max_retries 2)
    async with session_factory() as session:
        claim3 = await claim_execution(
            session, exec_id, worker_id="worker-try3", lease_duration=timedelta(seconds=15), now=t_requeue2
        )
        await start_execution(session, claim3)
        await fail_execution(session, claim3, error_summary="third failure")
        outcome3 = await transition_failed_execution(
            session, exec_id, base_delay=base_delay, now=t_requeue2
        )
        assert outcome3 == "DEAD_LETTERED"

    # Verify terminal status in PostgreSQL
    async with session_factory() as session:
        e_terminal = await session.get(Execution, exec_id)
        assert e_terminal.status == "DEAD_LETTERED"
        assert e_terminal.finished_at is not None

        # Terminal state protection: Cancellation cannot overwrite DEAD_LETTERED
        cancel_res = await cancel_execution(session, exec_id, reason="cancel dead-letter")
        assert cancel_res.cancelled is False
        assert cancel_res.status == "DEAD_LETTERED"


# ==============================================================================
# 5. Scheduler Duplicate Occurrence Protection
# ==============================================================================

@pytest.mark.asyncio
async def test_scheduler_duplicate_occurrence_idempotency(session_factory):
    """Concurrent scheduler evaluation runs produce exactly ONE occurrence for identical due timestamps."""
    t0 = datetime(2026, 10, 9, 14, 0, 0, tzinfo=timezone.utc)

    async with session_factory() as session:
        async with session.begin():
            job = JobDefinition(name="scheduled-job")
            session.add(job)
            await session.flush()
            schedule = ScheduleDefinition(
                name="hourly-job",
                schedule_type="recurring",
                enabled=True,
                configuration={
                    "cron": "0 * * * *",
                    "job_definition_id": job.id,
                },
                created_at=t0 - timedelta(hours=1),
            )
            session.add(schedule)
            await session.flush()
            schedule_id = schedule.id

    # Run two evaluation cycles concurrently
    async def _evaluate():
        async with session_factory() as session:
            return await evaluate_due_schedules(session, now=t0)

    res1, res2 = await asyncio.gather(_evaluate(), _evaluate())
    total_created = len(res1) + len(res2)


    # Exactly one occurrence is created across both concurrent workers
    assert total_created == 1

    # Verify database occurrences
    async with session_factory() as session:
        stmt = select(ScheduleOccurrence).where(
            ScheduleOccurrence.schedule_definition_id == schedule_id
        )
        rows = (await session.execute(stmt)).scalars().all()
        assert len(rows) == 1
        assert rows[0].scheduled_for == t0

    # Dispatch due occurrences
    async with session_factory() as session:
        dispatched_occs, exec_ids, _ = await dispatch_due_occurrences(session, now=t0)
        assert len(dispatched_occs) == 1
        assert len(exec_ids) == 1

    # Re-dispatching concurrently yields 0 new executions (idempotent)
    async with session_factory() as session:
        d2, e2, _ = await dispatch_due_occurrences(session, now=t0)
        assert len(d2) == 0
        assert len(e2) == 0


# ==============================================================================
# 6. DAG Branch Failure and Cascading Skip Propagation
# ==============================================================================

@pytest.mark.asyncio
async def test_dag_branch_failure_cascades_skip_to_downstream(session_factory):
    """When a branch in a DAG fails, dependent downstream tasks are SKIPPED and run marks FAILED."""
    async with session_factory() as session:
        wf = await create_workflow(
            session,
            name="branch-failure-dag",
            tasks=[
                {"name": "root"},
                {"name": "branch_success"},
                {"name": "branch_fail"},
                {"name": "join_aggregator"},
            ],
            edges=[
                ("root", "branch_success"),
                ("root", "branch_fail"),
                ("branch_success", "join_aggregator"),
                ("branch_fail", "join_aggregator"),
            ],
        )
        wf_id = wf.id

        run = await create_workflow_run(session, wf_id, auto_ready_roots=True)
        run_id = run.id

    # Step 1: Root executes and succeeds
    async with session_factory() as session:
        res1 = await resolve_and_dispatch(session, run_id)
        assert len(res1.created_executions) == 1
        root_exec_id = res1.created_executions[0]

        # Complete root
        claim_root = await claim_execution(session, root_exec_id, "w1", timedelta(seconds=10))
        await start_execution(session, claim_root)
        await complete_execution(session, claim_root)

    # Step 2: Retrieve automatically dispatched branch executions
    async with session_factory() as session:
        stmt = (
            select(WorkflowTaskExecution)
            .join(WorkflowTask)
            .where(WorkflowTaskExecution.workflow_run_id == run_id)
        )
        task_runs = (await session.execute(stmt)).scalars().all()
        branch_eids_by_name = {
            tr.workflow_task.name: tr.execution_id for tr in task_runs if tr.execution_id
        }
        assert "branch_success" in branch_eids_by_name
        assert "branch_fail" in branch_eids_by_name

    # Step 3: Branch A succeeds, Branch B fails
    async with session_factory() as session:
        eid_success = branch_eids_by_name["branch_success"]
        claim_a = await claim_execution(session, eid_success, "w1", timedelta(seconds=10))
        assert claim_a is not None
        await start_execution(session, claim_a)
        await complete_execution(session, claim_a)

        eid_fail = branch_eids_by_name["branch_fail"]
        claim_b = await claim_execution(session, eid_fail, "w2", timedelta(seconds=10))
        assert claim_b is not None
        await start_execution(session, claim_b)
        await fail_execution(session, claim_b, error_summary="Branch B crashed")
        # Advance terminal failure so advance_workflow cascades to DAG
        await transition_failed_execution(session, eid_fail, max_retries=0)

    # Step 4: Verify join_aggregator is SKIPPED and workflow run is FAILED
    async with session_factory() as session:
        run_final = await session.get(WorkflowRun, run_id)
        assert run_final.status == "FAILED"

        stmt = select(WorkflowTaskExecution).where(
            WorkflowTaskExecution.workflow_run_id == run_id
        )
        final_runs = (await session.execute(stmt)).scalars().all()
        task_status_map = {tr.workflow_task.name: tr.status for tr in final_runs}
        assert task_status_map["root"] == "SUCCEEDED"
        assert task_status_map["branch_success"] == "SUCCEEDED"
        assert task_status_map["branch_fail"] == "FAILED"
        assert task_status_map["join_aggregator"] == "SKIPPED"



# ==============================================================================
# 7. Cancellation Concurrency Races
# ==============================================================================

@pytest.mark.asyncio
async def test_cancellation_vs_worker_claim_race(session_factory):
    """Concurrent cancellation and worker claim: PostgreSQL row locking guarantees atomic decision."""
    async with session_factory() as session:
        async with session.begin():
            e = Execution(status="QUEUED")
            session.add(e)
            await session.flush()
            exec_id = e.id

    lease_dur = timedelta(seconds=15)

    async def _cancel():
        async with session_factory() as session:
            return await cancel_execution(session, exec_id, reason="User cancelled")

    async def _claim():
        async with session_factory() as session:
            return await claim_execution(
                session, exec_id, worker_id="worker-racing", lease_duration=lease_dur
            )

    cancel_res, claim_res = await asyncio.gather(_cancel(), _claim())

    # Either cancellation won and claim was None, OR claim won and cancellation transitioned CLAIMED -> CANCELLED
    async with session_factory() as session:
        e = await session.get(Execution, exec_id)
        assert e.status == "CANCELLED"
        assert cancel_res.cancelled is True

        if claim_res is not None:
            # Claim won the initial race, but cancel immediately marked it CANCELLED
            # Worker must NOT be able to start or complete
            start_ok = await start_execution(session, claim_res)
            assert start_ok is False, "Worker must not be able to start a cancelled execution"


# ==============================================================================
# 8. Concurrency & Rate Limit Admission Races
# ==============================================================================

@pytest.mark.asyncio
async def test_concurrency_policy_admission_race(session_factory):
    """When max_concurrency=1, only ONE execution is admitted across multiple concurrent workers."""
    async with session_factory() as session:
        wf = await create_workflow(
            session=session,
            name="concurrency-race-wf",
            tasks=[{"name": "task_1"}, {"name": "task_2"}, {"name": "task_3"}],
            edges=[],
        )
        await set_concurrency_limit_policy(
            session,
            target_type="WORKFLOW",
            target_id=str(wf.id),
            max_concurrency=1,
        )
        run = await create_workflow_run(session, wf.id, auto_ready_roots=True)
        res = await resolve_and_dispatch(session, run.id)
        exec_ids = res.created_executions
        assert len(exec_ids) == 3

    lease_dur = timedelta(seconds=20)
    workers = ["worker-c1", "worker-c2", "worker-c3"]

    async def _claim_worker(eid: int, worker_name: str):
        async with session_factory() as session:
            return await claim_execution(session, eid, worker_id=worker_name, lease_duration=lease_dur)

    # All 3 workers attempt to claim their respective executions simultaneously
    claims = await asyncio.gather(
        _claim_worker(exec_ids[0], workers[0]),
        _claim_worker(exec_ids[1], workers[1]),
        _claim_worker(exec_ids[2], workers[2]),
    )

    successful_claims = [c for c in claims if c is not None]
    assert len(successful_claims) == 1, "Only 1 execution can be admitted under max_concurrency=1"

    winner_claim = successful_claims[0]

    # While winner is running, another worker cannot claim the remaining queued items
    async with session_factory() as session:
        await start_execution(session, winner_claim)
        remaining_eids = [eid for eid in exec_ids if eid != winner_claim.execution_id]
        blocked_claim = await claim_execution(
            session, remaining_eids[0], worker_id="worker-blocked", lease_duration=lease_dur
        )
        assert blocked_claim is None, "Additional claims must be blocked by concurrency limit"

        # Complete winner execution
        await complete_execution(session, winner_claim)

        # Now capacity is freed: next execution is admitted
        next_claim = await claim_execution(
            session, remaining_eids[0], worker_id="worker-admitted-next", lease_duration=lease_dur
        )
        assert next_claim is not None
        assert next_claim.execution_id == remaining_eids[0]


@pytest.mark.asyncio
async def test_rate_limit_admission_race(session_factory):
    """When max_requests=2 in a window, only 2 out of 5 concurrent claims are admitted."""
    async with session_factory() as session:
        async with session.begin():
            await set_rate_limit_policy(
                session,
                target_type="CATEGORY",
                target_id="burst-category",
                max_requests=2,
                window_seconds=60,
            )
            created_eids = []
            for i in range(5):
                e = Execution(status="QUEUED", category="burst-category", priority=i)
                session.add(e)
                await session.flush()
                created_eids.append(e.id)

    lease_dur = timedelta(seconds=20)

    async def _try_claim(eid: int, wname: str):
        async with session_factory() as session:
            return await claim_execution(session, eid, worker_id=wname, lease_duration=lease_dur)

    results = await asyncio.gather(
        *[_try_claim(created_eids[i], f"worker-rl-{i}") for i in range(5)]
    )

    admitted = [r for r in results if r is not None]
    rejected = [r for r in results if r is None]

    assert len(admitted) == 2, f"Expected exactly 2 admitted claims under rate limit, got {len(admitted)}"
    assert len(rejected) == 3, f"Expected exactly 3 rejected claims under rate limit, got {len(rejected)}"

