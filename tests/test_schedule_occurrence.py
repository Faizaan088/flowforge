"""PostgreSQL integration tests for durable ScheduleOccurrence and idempotency."""

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
    from sqlalchemy import func, select
    from sqlalchemy.dialects.postgresql import insert
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

    from sqlalchemy.orm import selectinload
    from database import Base  # noqa: E402
    from models import ScheduleDefinition, ScheduleOccurrence  # noqa: E402


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
        await session.execute(ScheduleOccurrence.__table__.delete())
        await session.execute(ScheduleDefinition.__table__.delete())
        await session.commit()


async def create_test_schedule(session, name="test-schedule", schedule_type="recurring"):
    schedule = ScheduleDefinition(name=name, schedule_type=schedule_type)
    session.add(schedule)
    await session.commit()
    await session.refresh(schedule)
    return schedule


@pytest.mark.asyncio
async def test_schedule_occurrence_creation_and_defaults(session_factory):
    """Verify in-memory creation and default status of ScheduleOccurrence."""
    scheduled_time = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)
    occurrence = ScheduleOccurrence(
        schedule_definition_id=1,
        scheduled_for=scheduled_time,
    )

    assert occurrence.schedule_definition_id == 1
    assert occurrence.scheduled_for == scheduled_time
    assert occurrence.status == "SCHEDULED"


@pytest.mark.asyncio
async def test_schedule_occurrence_persistence(session_factory):
    """Verify persisting a ScheduleOccurrence to PostgreSQL and loading it back."""
    before_persist = datetime.now(timezone.utc)
    scheduled_time = datetime(2026, 10, 20, 8, 30, tzinfo=timezone.utc)

    async with session_factory() as session:
        schedule = await create_test_schedule(session, name="persist-test")
        occurrence = ScheduleOccurrence(
            schedule_definition_id=schedule.id,
            scheduled_for=scheduled_time,
            status="SCHEDULED",
        )
        session.add(occurrence)
        await session.commit()
        await session.refresh(occurrence)
        occurrence_id = occurrence.id

        assert occurrence_id is not None
        assert occurrence.created_at is not None
        assert occurrence.created_at.tzinfo is not None
        assert occurrence.created_at >= before_persist

    async with session_factory() as session:
        loaded = await session.get(ScheduleOccurrence, occurrence_id)
        assert loaded is not None
        assert loaded.id == occurrence_id
        assert loaded.schedule_definition_id == schedule.id
        assert loaded.scheduled_for == scheduled_time
        assert loaded.status == "SCHEDULED"
        assert loaded.created_at is not None


@pytest.mark.asyncio
async def test_relationship_to_schedule_definition(session_factory):
    """Verify bidirectional relationship between ScheduleDefinition and ScheduleOccurrence."""
    scheduled_time = datetime(2026, 10, 22, 14, 0, tzinfo=timezone.utc)

    async with session_factory() as session:
        schedule = await create_test_schedule(session, name="rel-test")
        occurrence = ScheduleOccurrence(
            schedule_definition=schedule,
            scheduled_for=scheduled_time,
        )
        session.add(occurrence)
        await session.commit()
        await session.refresh(occurrence)
        occurrence_id = occurrence.id

    async with session_factory() as session:
        loaded_occ = await session.get(ScheduleOccurrence, occurrence_id)
        assert loaded_occ is not None
        # Access relationship from occurrence to definition
        assert loaded_occ.schedule_definition is not None
        assert loaded_occ.schedule_definition.id == schedule.id
        assert loaded_occ.schedule_definition.name == "rel-test"

    async with session_factory() as session:
        # Access relationship from definition to occurrences
        loaded_sched = await session.get(
            ScheduleDefinition,
            schedule.id,
            options=[selectinload(ScheduleDefinition.occurrences)],
        )
        assert loaded_sched is not None
        assert len(loaded_sched.occurrences) == 1
        assert loaded_sched.occurrences[0].id == occurrence_id
        assert loaded_sched.occurrences[0].scheduled_for == scheduled_time


@pytest.mark.asyncio
async def test_duplicate_occurrence_rejection_unique_constraint(session_factory):
    """Verify uniqueness constraint prevents duplicate occurrences for the same schedule and timestamp."""
    scheduled_time = datetime(2026, 10, 25, 6, 0, tzinfo=timezone.utc)

    async with session_factory() as session:
        schedule = await create_test_schedule(session, name="idempotent-test")

        first_occurrence = ScheduleOccurrence(
            schedule_definition_id=schedule.id,
            scheduled_for=scheduled_time,
        )
        session.add(first_occurrence)
        await session.commit()

        # Second occurrence with same definition and same scheduled_for must fail
        duplicate_occurrence = ScheduleOccurrence(
            schedule_definition_id=schedule.id,
            scheduled_for=scheduled_time,
        )
        session.add(duplicate_occurrence)
        with pytest.raises(IntegrityError):
            await session.commit()


@pytest.mark.asyncio
async def test_idempotent_occurrence_upsert(session_factory):
    """Verify ON CONFLICT DO NOTHING achieves durable idempotent creation."""
    scheduled_time = datetime(2026, 10, 25, 9, 0, tzinfo=timezone.utc)

    async with session_factory() as session:
        schedule = await create_test_schedule(session, name="upsert-test")

        # Initial insert
        stmt1 = (
            insert(ScheduleOccurrence)
            .values(
                schedule_definition_id=schedule.id,
                scheduled_for=scheduled_time,
                status="SCHEDULED",
            )
            .on_conflict_do_nothing(
                index_elements=["schedule_definition_id", "scheduled_for"]
            )
            .returning(ScheduleOccurrence.id)
        )
        res1 = await session.execute(stmt1)
        first_id = res1.scalar_one_or_none()
        await session.commit()
        assert first_id is not None

        # Repeat insert for the exact same occurrence
        stmt2 = (
            insert(ScheduleOccurrence)
            .values(
                schedule_definition_id=schedule.id,
                scheduled_for=scheduled_time,
                status="SCHEDULED",
            )
            .on_conflict_do_nothing(
                index_elements=["schedule_definition_id", "scheduled_for"]
            )
            .returning(ScheduleOccurrence.id)
        )
        res2 = await session.execute(stmt2)
        duplicate_id = res2.scalar_one_or_none()
        await session.commit()
        # DO NOTHING produces no row returned, confirming idempotent skip
        assert duplicate_id is None

        # Confirm exactly one occurrence persists in database
        count = await session.scalar(
            select(func.count(ScheduleOccurrence.id)).where(
                ScheduleOccurrence.schedule_definition_id == schedule.id
            )
        )
        assert count == 1


@pytest.mark.asyncio
async def test_multiple_distinct_occurrences_for_same_schedule(session_factory):
    """Verify multiple occurrences at distinct timestamps succeed for the same schedule."""
    t0 = datetime(2026, 10, 26, 0, 0, tzinfo=timezone.utc)
    t1 = t0 + timedelta(hours=1)
    t2 = t0 + timedelta(hours=2)

    async with session_factory() as session:
        schedule = await create_test_schedule(session, name="multi-occ-test")

        occ0 = ScheduleOccurrence(schedule_definition_id=schedule.id, scheduled_for=t0)
        occ1 = ScheduleOccurrence(schedule_definition_id=schedule.id, scheduled_for=t1)
        occ2 = ScheduleOccurrence(schedule_definition_id=schedule.id, scheduled_for=t2)

        session.add_all([occ0, occ1, occ2])
        await session.commit()

        await session.refresh(schedule)
        assert len(schedule.occurrences) == 3
        timestamps = {occ.scheduled_for for occ in schedule.occurrences}
        assert timestamps == {t0, t1, t2}


@pytest.mark.asyncio
async def test_same_scheduled_for_allowed_across_different_schedules(session_factory):
    """Verify distinct schedules can have occurrences scheduled for the same timestamp without conflict."""
    same_time = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)

    async with session_factory() as session:
        sched_a = await create_test_schedule(session, name="schedule-a")
        sched_b = await create_test_schedule(session, name="schedule-b")

        occ_a = ScheduleOccurrence(schedule_definition_id=sched_a.id, scheduled_for=same_time)
        occ_b = ScheduleOccurrence(schedule_definition_id=sched_b.id, scheduled_for=same_time)

        session.add_all([occ_a, occ_b])
        await session.commit()

        await session.refresh(occ_a)
        await session.refresh(occ_b)

        assert occ_a.id is not None
        assert occ_b.id is not None
        assert occ_a.schedule_definition_id != occ_b.schedule_definition_id
        assert occ_a.scheduled_for == occ_b.scheduled_for
