"""Focused integration tests for the FlowForge Scheduler subsystem."""

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
    from redis.exceptions import RedisError
    from sqlalchemy import func, select
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

    from database import Base  # noqa: E402
    from execution_claim import claim_execution  # noqa: E402
    from models import (  # noqa: E402
        Execution,
        JobDefinition,
        ScheduleDefinition,
        ScheduleOccurrence,
    )
    from scheduler import (  # noqa: E402
        compute_next_occurrence,
        dispatch_due_occurrences,
        evaluate_due_schedules,
        next_cron_occurrence,
        resolve_timezone,
        run_scheduler_cycle,
    )


class RecordingRedis:
    """Mock Redis client capturing pushed execution IDs and simulating failures."""

    def __init__(self, unavailable: bool = False):
        self.entries: list[int] = []
        self.unavailable = unavailable

    def lpush(self, _queue_name: str, execution_id: int) -> None:
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
async def clear_database(session_factory):
    async with session_factory() as session:
        await session.execute(Execution.__table__.delete())
        await session.execute(ScheduleOccurrence.__table__.delete())
        await session.execute(ScheduleDefinition.__table__.delete())
        await session.execute(JobDefinition.__table__.delete())
        await session.commit()


# ---------------------------------------------------------------------------
# 1. One-time schedule: future / due / idempotency
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_one_time_schedule_future_due_and_idempotency(session_factory):
    """Verify one-time schedule ignores future, triggers when due, and remains idempotent."""
    target_time = datetime(2026, 10, 15, 10, 0, tzinfo=timezone.utc)
    future_time = target_time - timedelta(minutes=5)
    due_time = target_time + timedelta(minutes=1)

    async with session_factory() as session:
        schedule = ScheduleDefinition(
            name="one-time-test",
            schedule_type="one_time",
            configuration={"run_at": target_time.isoformat()},
        )
        session.add(schedule)
        await session.commit()
        await session.refresh(schedule)
        schedule_id = schedule.id

    # 1. Evaluated in the future: no occurrence created
    async with session_factory() as session:
        created = await evaluate_due_schedules(session, now=future_time)
        assert created == []

    # 2. Evaluated when due: creates 1 occurrence
    async with session_factory() as session:
        created = await evaluate_due_schedules(session, now=due_time)
        assert len(created) == 1
        occ_id = created[0]

    async with session_factory() as session:
        occ = await session.get(ScheduleOccurrence, occ_id)
        assert occ is not None
        assert occ.schedule_definition_id == schedule_id
        assert occ.scheduled_for == target_time
        assert occ.status == "SCHEDULED"

    # 3. Repeated evaluation (idempotency): creates 0 new occurrences
    async with session_factory() as session:
        repeated = await evaluate_due_schedules(session, now=due_time + timedelta(days=1))
        assert repeated == []

        all_occs = (
            await session.scalars(
                select(ScheduleOccurrence).where(
                    ScheduleOccurrence.schedule_definition_id == schedule_id
                )
            )
        ).all()
        assert len(all_occs) == 1


# ---------------------------------------------------------------------------
# 2. Delayed schedule: future / due / idempotency
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_delayed_schedule_future_due_and_idempotency(session_factory):
    """Verify delayed schedule ignores future, fires after delay, and remains idempotent."""
    base_time = datetime(2026, 10, 10, 12, 0, tzinfo=timezone.utc)
    delay_seconds = 120
    due_time = base_time + timedelta(seconds=delay_seconds)

    async with session_factory() as session:
        schedule = ScheduleDefinition(
            name="delayed-test",
            schedule_type="delayed",
            configuration={"delay_seconds": delay_seconds},
        )
        # Explicit created_at for deterministic calculation
        schedule.created_at = base_time
        session.add(schedule)
        await session.commit()
        await session.refresh(schedule)
        schedule_id = schedule.id

    # 1. Before delay expires: no occurrence
    async with session_factory() as session:
        created = await evaluate_due_schedules(
            session, now=base_time + timedelta(seconds=60)
        )
        assert created == []

    # 2. When delay has elapsed: occurrence created
    async with session_factory() as session:
        created = await evaluate_due_schedules(
            session, now=due_time + timedelta(seconds=5)
        )
        assert len(created) == 1
        occ_id = created[0]

    async with session_factory() as session:
        occ = await session.get(ScheduleOccurrence, occ_id)
        assert occ.scheduled_for == due_time

    # 3. Repeated evaluation: idempotent
    async with session_factory() as session:
        repeated = await evaluate_due_schedules(
            session, now=due_time + timedelta(hours=2)
        )
        assert repeated == []


# ---------------------------------------------------------------------------
# 3. Recurring next-occurrence generation
# ---------------------------------------------------------------------------
def test_recurring_next_occurrence_generation():
    """Verify deterministic next occurrence generation for cron and interval configurations."""
    start = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)

    # 1. Interval configuration
    interval_config = {"interval_seconds": 300}
    next_interval = compute_next_occurrence(interval_config, after_time=start)
    assert next_interval == start + timedelta(seconds=300)

    # 2. Cron: daily at 2am
    daily_config = {"cron": "0 2 * * *"}
    next_daily = compute_next_occurrence(daily_config, after_time=start)
    assert next_daily == datetime(2026, 10, 9, 2, 0, tzinfo=timezone.utc)

    # 3. Cron: every 15 minutes
    every_15 = compute_next_occurrence({"cron": "*/15 * * * *"}, after_time=start)
    assert every_15 == datetime(2026, 10, 8, 12, 15, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# 4. Timezone behavior where supported
# ---------------------------------------------------------------------------
def test_timezone_behavior():
    """Verify cron evaluation respects specified timezone offsets."""
    # 9:00 AM daily in UTC-5 (EST)
    tz_config = {"cron": "0 9 * * *", "timezone": "-05:00"}
    start = datetime(2026, 10, 8, 0, 0, tzinfo=timezone.utc)

    next_occ = compute_next_occurrence(tz_config, after_time=start)
    # 09:00 at UTC-5 equals 14:00 UTC
    assert next_occ == datetime(2026, 10, 8, 14, 0, tzinfo=timezone.utc)

    # Verify resolve_timezone handles offsets and UTC
    assert resolve_timezone("UTC") == timezone.utc
    assert resolve_timezone("+05:30") == timezone(timedelta(hours=5, minutes=30))
    assert resolve_timezone("-04:00") == timezone(timedelta(hours=-4))


# ---------------------------------------------------------------------------
# 5. Duplicate scheduler evaluation
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_duplicate_scheduler_evaluation(session_factory):
    """Verify back-to-back evaluations of the same due state do not duplicate occurrences."""
    t0 = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)

    async with session_factory() as session:
        schedule = ScheduleDefinition(
            name="dup-eval-test",
            schedule_type="recurring",
            configuration={"interval_seconds": 60},
        )
        schedule.created_at = t0
        session.add(schedule)
        await session.commit()
        await session.refresh(schedule)

    eval_time = t0 + timedelta(seconds=70)

    # Cycle 1
    async with session_factory() as session:
        first = await evaluate_due_schedules(session, now=eval_time)
        assert len(first) == 1

    # Cycle 2 (same evaluation time)
    async with session_factory() as session:
        second = await evaluate_due_schedules(session, now=eval_time)
        assert second == []

    # Confirm only 1 occurrence in database
    async with session_factory() as session:
        count = await session.scalar(select(func.count(ScheduleOccurrence.id)))
        assert count == 1


# ---------------------------------------------------------------------------
# 6. Concurrent scheduler runs
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concurrent_scheduler_runs(session_factory):
    """Verify concurrent scheduler dispatch calls do not produce duplicate executions."""
    now = datetime.now(timezone.utc)

    async with session_factory() as session:
        schedule = ScheduleDefinition(name="concurrent-test", schedule_type="recurring")
        session.add(schedule)
        await session.commit()
        await session.refresh(schedule)

        occ = ScheduleOccurrence(
            schedule_definition_id=schedule.id,
            scheduled_for=now - timedelta(seconds=10),
            status="SCHEDULED",
        )
        session.add(occ)
        await session.commit()

    async def attempt_dispatch():
        async with session_factory() as session:
            dispatched, created, _ = await dispatch_due_occurrences(session, now=now)
            return dispatched, created

    res1, res2 = await asyncio.gather(attempt_dispatch(), attempt_dispatch())

    total_dispatched = res1[0] + res2[0]
    total_created = res1[1] + res2[1]

    # Exactly one worker should claim and create the execution
    assert len(total_dispatched) == 1
    assert len(total_created) == 1

    async with session_factory() as session:
        exec_count = await session.scalar(select(func.count(Execution.id)))
        assert exec_count == 1


# ---------------------------------------------------------------------------
# 7. Scheduler restart / recovery
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_scheduler_restart_and_recovery(session_factory):
    """Verify restarted scheduler cleanly resumes and dispatches pending occurrences from PostgreSQL."""
    now = datetime.now(timezone.utc)

    # Simulate occurrences left in SCHEDULED state prior to scheduler restart
    async with session_factory() as session:
        sched = ScheduleDefinition(name="restart-test", schedule_type="recurring")
        session.add(sched)
        await session.commit()
        await session.refresh(sched)

        occ = ScheduleOccurrence(
            schedule_definition_id=sched.id,
            scheduled_for=now - timedelta(seconds=30),
            status="SCHEDULED",
        )
        session.add(occ)
        await session.commit()
        await session.refresh(occ)
        occ_id = occ.id

    # Restarted scheduler runs a new cycle
    redis = RecordingRedis()
    result = await run_scheduler_cycle(session_factory, redis_client=redis, now=now)

    assert occ_id in result.occurrences_dispatched
    assert len(result.executions_created) == 1

    # Verify occurrence transitioned to QUEUED
    async with session_factory() as session:
        reloaded_occ = await session.get(ScheduleOccurrence, occ_id)
        assert reloaded_occ.status == "QUEUED"


# ---------------------------------------------------------------------------
# 8. Execution creation & links
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_execution_creation_from_occurrence(session_factory):
    """Verify Execution record inherits job definition and links to occurrence."""
    now = datetime.now(timezone.utc)

    async with session_factory() as session:
        job_def = JobDefinition(name="job-to-run", payload={"cmd": "run"})
        session.add(job_def)
        await session.commit()
        await session.refresh(job_def)

        sched = ScheduleDefinition(
            name="job-sched",
            schedule_type="recurring",
            configuration={"job_definition_id": job_def.id},
        )
        session.add(sched)
        await session.commit()
        await session.refresh(sched)

        occ = ScheduleOccurrence(
            schedule_definition_id=sched.id,
            scheduled_for=now - timedelta(seconds=5),
            status="SCHEDULED",
        )
        session.add(occ)
        await session.commit()
        await session.refresh(occ)
        occ_id = occ.id

    async with session_factory() as session:
        dispatched, exec_ids, _ = await dispatch_due_occurrences(session, now=now)
        assert dispatched == [occ_id]
        assert len(exec_ids) == 1
        exec_id = exec_ids[0]

    async with session_factory() as session:
        execution = await session.get(Execution, exec_id)
        assert execution is not None
        assert execution.status == "QUEUED"
        assert execution.job_definition_id == job_def.id
        assert execution.schedule_occurrence_id == occ_id


# ---------------------------------------------------------------------------
# 9. Redis delivery failure preserves durable state
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_redis_delivery_failure_preserves_postgresql_state(session_factory):
    """Verify failure in Redis enqueue does not drop or roll back durable PostgreSQL state."""
    now = datetime.now(timezone.utc)

    async with session_factory() as session:
        sched = ScheduleDefinition(name="redis-fail-test", schedule_type="recurring")
        session.add(sched)
        await session.commit()
        await session.refresh(sched)

        occ = ScheduleOccurrence(
            schedule_definition_id=sched.id,
            scheduled_for=now - timedelta(seconds=10),
            status="SCHEDULED",
        )
        session.add(occ)
        await session.commit()
        await session.refresh(occ)
        occ_id = occ.id

    failing_redis = RecordingRedis(unavailable=True)

    async with session_factory() as session:
        dispatched, created, reconciliation = await dispatch_due_occurrences(
            session, redis_client=failing_redis, now=now
        )

    # PostgreSQL commit succeeded
    assert dispatched == [occ_id]
    assert len(created) == 1
    # Redis failure is surfaced cleanly without dropping work
    assert reconciliation is not None
    assert reconciliation.redis_error is not None
    assert "Redis connection refused" in reconciliation.redis_error

    # Execution remains durable in Postgres
    async with session_factory() as session:
        execution = await session.get(Execution, created[0])
        assert execution is not None
        assert execution.status == "QUEUED"


# ---------------------------------------------------------------------------
# 10. Full scheduler end-to-end path
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_full_scheduler_end_to_end_path(session_factory):
    """End-to-end: schedule defined -> evaluated -> occurrence -> execution -> worker claims."""
    t0 = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)
    due_time = t0 + timedelta(seconds=60)

    # 1. Define schedule
    async with session_factory() as session:
        job = JobDefinition(name="e2e-job")
        session.add(job)
        await session.commit()
        await session.refresh(job)

        schedule = ScheduleDefinition(
            name="e2e-schedule",
            schedule_type="delayed",
            configuration={"delay_seconds": 60, "job_definition_id": job.id},
        )
        schedule.created_at = t0
        session.add(schedule)
        await session.commit()
        await session.refresh(schedule)

    redis = RecordingRedis()

    # 2. Run scheduler cycle at due time
    cycle_result = await run_scheduler_cycle(
        session_factory, redis_client=redis, now=due_time
    )

    assert len(cycle_result.occurrences_created) == 1
    assert len(cycle_result.occurrences_dispatched) == 1
    assert len(cycle_result.executions_created) == 1
    assert redis.entries == cycle_result.executions_created

    created_exec_id = cycle_result.executions_created[0]

    # 3. Worker claims the newly queued execution
    async with session_factory() as session:
        claim = await claim_execution(
            session,
            execution_id=created_exec_id,
            worker_id="worker-e2e",
            lease_duration=timedelta(seconds=30),
        )
        assert claim is not None
        assert claim.execution_id == created_exec_id

    # 4. Verify claimed status in PostgreSQL
    async with session_factory() as session:
        claimed_execution = await session.get(Execution, created_exec_id)
        assert claimed_execution.status == "CLAIMED"
        assert claimed_execution.worker_id == "worker-e2e"


# ---------------------------------------------------------------------------
# 11. Review: Cron syntax scope & unsupported syntax rejection
# ---------------------------------------------------------------------------
def test_cron_syntax_scope_and_unsupported_rejection():
    """Verify supported cron features work and unsupported expressions are explicitly rejected."""
    dt = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)

    # Supported syntax
    # 1. Standard wildcard
    assert next_cron_occurrence("* * * * *", dt) == dt + timedelta(minutes=1)
    # 2. Stepped range
    res = next_cron_occurrence("1-5/2 * * * *", dt)
    assert res.minute in {1, 3, 5}
    # 3. Comma list and step
    res_list = next_cron_occurrence("0,30 * * * *", dt)
    assert res_list.minute in {0, 30}

    # Unsupported syntax explicitly rejected with ValueError
    with pytest.raises(ValueError, match="Unsupported cron macro"):
        next_cron_occurrence("@daily", dt)

    with pytest.raises(ValueError, match="expected exactly 5 fields"):
        next_cron_occurrence("* * * *", dt)  # only 4 fields

    with pytest.raises(ValueError, match="expected exactly 5 fields"):
        next_cron_occurrence("* * * * * *", dt)  # 6 fields

    with pytest.raises(ValueError, match="Unsupported cron modifier"):
        next_cron_occurrence("0 0 ? * *", dt)  # '?' modifier

    with pytest.raises(ValueError, match="Unsupported cron modifier"):
        next_cron_occurrence("0 0 L * *", dt)  # 'L' modifier

    with pytest.raises(ValueError, match="Unsupported or non-numeric token"):
        next_cron_occurrence("0 0 * * MON", dt)  # named weekday abbreviation


# ---------------------------------------------------------------------------
# 12. Review: Missed-occurrence policy when scheduler down for multiple periods
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_missed_occurrences_when_scheduler_down_catch_up(session_factory):
    """Verify default catch_up policy sequentially schedules all missed periods."""
    t0 = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)

    async with session_factory() as session:
        sched = ScheduleDefinition(
            name="catchup-test",
            schedule_type="recurring",
            configuration={"interval_seconds": 60, "misfire_policy": "catch_up"},
        )
        sched.created_at = t0
        session.add(sched)
        await session.commit()
        await session.refresh(sched)
        sched_id = sched.id

    # Scheduler was offline for 3 periods (180s)
    eval_time = t0 + timedelta(seconds=185)

    async with session_factory() as session:
        created_ids = await evaluate_due_schedules(session, now=eval_time)
        assert len(created_ids) == 3

    async with session_factory() as session:
        occs = (
            await session.scalars(
                select(ScheduleOccurrence)
                .where(ScheduleOccurrence.schedule_definition_id == sched_id)
                .order_by(ScheduleOccurrence.scheduled_for)
            )
        ).all()
        assert len(occs) == 3
        assert occs[0].scheduled_for == t0 + timedelta(seconds=60)
        assert occs[1].scheduled_for == t0 + timedelta(seconds=120)
        assert occs[2].scheduled_for == t0 + timedelta(seconds=180)


@pytest.mark.asyncio
async def test_missed_occurrences_when_scheduler_down_skip_policy(session_factory):
    """Verify skip/coalesce policy creates only the single latest due occurrence."""
    t0 = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)

    async with session_factory() as session:
        sched = ScheduleDefinition(
            name="skip-test",
            schedule_type="recurring",
            configuration={"interval_seconds": 60, "misfire_policy": "skip"},
        )
        sched.created_at = t0
        session.add(sched)
        await session.commit()
        await session.refresh(sched)
        sched_id = sched.id

    # Scheduler was offline for 3 periods (180s)
    eval_time = t0 + timedelta(seconds=185)

    async with session_factory() as session:
        created_ids = await evaluate_due_schedules(session, now=eval_time)
        assert len(created_ids) == 1

    async with session_factory() as session:
        occs = (
            await session.scalars(
                select(ScheduleOccurrence).where(
                    ScheduleOccurrence.schedule_definition_id == sched_id
                )
            )
        ).all()
        assert len(occs) == 1
        # Only the latest due period (180s) is scheduled
        assert occs[0].scheduled_for == t0 + timedelta(seconds=180)


# ---------------------------------------------------------------------------
# 13. Review: Invariant that one ScheduleOccurrence produces at most one Execution
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_occurrence_can_produce_at_most_one_execution_constraint(session_factory):
    """Verify PostgreSQL uniqueness constraint strictly enforces at most one Execution per occurrence."""
    now = datetime.now(timezone.utc)

    async with session_factory() as session:
        sched = ScheduleDefinition(name="one-exec-test", schedule_type="recurring")
        session.add(sched)
        await session.commit()
        await session.refresh(sched)

        occ = ScheduleOccurrence(
            schedule_definition_id=sched.id,
            scheduled_for=now,
            status="QUEUED",
        )
        session.add(occ)
        await session.commit()
        await session.refresh(occ)
        occ_id = occ.id

        # First execution linked to occurrence succeeds
        exec1 = Execution(schedule_occurrence_id=occ_id, status="QUEUED")
        session.add(exec1)
        await session.commit()
        await session.refresh(exec1)
        assert exec1.id is not None
        exec1_id = exec1.id

        # Attempting a second execution linked to the SAME occurrence must fail with IntegrityError
        exec2 = Execution(schedule_occurrence_id=occ_id, status="QUEUED")
        session.add(exec2)
        with pytest.raises(IntegrityError):
            await session.commit()
        await session.rollback()

    # Verify multiple ad-hoc executions with schedule_occurrence_id=None are permitted
    async with session_factory() as session:
        adhoc1 = Execution(schedule_occurrence_id=None, status="QUEUED")
        adhoc2 = Execution(schedule_occurrence_id=None, status="QUEUED")
        session.add_all([adhoc1, adhoc2])
        await session.commit()
        assert adhoc1.id is not None
        assert adhoc2.id is not None

    # Verify 1-to-1 relationship on occurrence
    async with session_factory() as session:
        reloaded_occ = await session.get(ScheduleOccurrence, occ_id)
        assert reloaded_occ is not None
        assert reloaded_occ.execution is not None
        assert reloaded_occ.execution.id == exec1_id
        assert len(reloaded_occ.executions) == 1


