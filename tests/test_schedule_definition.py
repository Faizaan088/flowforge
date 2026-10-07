"""PostgreSQL integration tests for durable Schedule Definition foundation."""

import os
from datetime import datetime, timezone

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
    from sqlalchemy.pool import NullPool

    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

    from database import Base  # noqa: E402
    from models import ScheduleDefinition  # noqa: E402


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
async def clear_schedule_definitions(session_factory):
    async with session_factory() as session:
        await session.execute(ScheduleDefinition.__table__.delete())
        await session.commit()


@pytest.mark.asyncio
async def test_schedule_definition_creation_and_defaults(session_factory):
    """Verify default values and PostgreSQL persistence of a basic ScheduleDefinition."""
    schedule = ScheduleDefinition(
        name="daily-report",
        schedule_type="recurring",
    )

    # In-memory defaults
    assert schedule.enabled is True
    assert schedule.is_enabled is True
    assert schedule.configuration is None
    assert schedule.payload is None

    before_persist = datetime.now(timezone.utc)

    async with session_factory() as session:
        session.add(schedule)
        await session.commit()
        await session.refresh(schedule)

        schedule_id = schedule.id
        assert schedule_id is not None
        assert schedule.name == "daily-report"
        assert schedule.schedule_type == "recurring"
        assert schedule.enabled is True
        assert schedule.is_enabled is True
        assert schedule.created_at is not None
        assert schedule.updated_at is not None
        assert schedule.created_at.tzinfo is not None
        assert schedule.updated_at.tzinfo is not None
        assert schedule.created_at >= before_persist
        assert schedule.updated_at >= before_persist

    # Verify querying back from a clean session
    async with session_factory() as session:
        persisted = await session.get(ScheduleDefinition, schedule_id)
        assert persisted is not None
        assert persisted.name == "daily-report"
        assert persisted.schedule_type == "recurring"
        assert persisted.enabled is True


@pytest.mark.asyncio
async def test_persist_recurring_schedule_with_config(session_factory):
    """Verify recurring schedule definition with cron configuration and payload."""
    config = {"cron": "0 2 * * *", "timezone": "UTC"}

    async with session_factory() as session:
        schedule = ScheduleDefinition(
            name="nightly-cleanup",
            schedule_type="recurring",
            configuration=config,
        )
        session.add(schedule)
        await session.commit()
        await session.refresh(schedule)
        schedule_id = schedule.id

    async with session_factory() as session:
        persisted = await session.get(ScheduleDefinition, schedule_id)
        assert persisted is not None
        assert persisted.name == "nightly-cleanup"
        assert persisted.schedule_type == "recurring"
        assert persisted.configuration == config
        assert persisted.payload == config


@pytest.mark.asyncio
async def test_persist_one_time_and_delayed_schedules(session_factory):
    """Verify compatibility with one-time and delayed schedule definitions."""
    async with session_factory() as session:
        one_time = ScheduleDefinition(
            name="scheduled-export",
            schedule_type="one_time",
            configuration={"run_at": "2026-10-15T09:00:00Z"},
        )
        delayed = ScheduleDefinition(
            name="retry-notification",
            schedule_type="delayed",
            configuration={"delay_seconds": 3600},
        )
        session.add_all([one_time, delayed])
        await session.commit()

        await session.refresh(one_time)
        await session.refresh(delayed)
        one_time_id = one_time.id
        delayed_id = delayed.id

    async with session_factory() as session:
        ot = await session.get(ScheduleDefinition, one_time_id)
        assert ot.schedule_type == "one_time"
        assert ot.configuration == {"run_at": "2026-10-15T09:00:00Z"}

        dl = await session.get(ScheduleDefinition, delayed_id)
        assert dl.schedule_type == "delayed"
        assert dl.configuration == {"delay_seconds": 3600}


@pytest.mark.asyncio
async def test_enabled_disabled_state_transitions(session_factory):
    """Verify explicit disabled creation and toggling enabled state."""
    async with session_factory() as session:
        disabled_schedule = ScheduleDefinition(
            name="paused-job",
            schedule_type="recurring",
            enabled=False,
        )
        session.add(disabled_schedule)
        await session.commit()
        await session.refresh(disabled_schedule)
        schedule_id = disabled_schedule.id
        assert disabled_schedule.enabled is False
        assert disabled_schedule.is_enabled is False

    # Toggle to enabled
    async with session_factory() as session:
        loaded = await session.get(ScheduleDefinition, schedule_id)
        loaded.enabled = True
        await session.commit()

    async with session_factory() as session:
        reloaded = await session.get(ScheduleDefinition, schedule_id)
        assert reloaded.enabled is True
        assert reloaded.is_enabled is True


@pytest.mark.asyncio
async def test_configuration_and_payload_interoperability(session_factory):
    """Verify payload and configuration field access interoperability."""
    async with session_factory() as session:
        # Creating via payload
        s1 = ScheduleDefinition(
            name="via-payload",
            schedule_type="recurring",
            payload={"interval_seconds": 60},
        )
        # Creating with distinct configuration and payload
        s2 = ScheduleDefinition(
            name="dual-payload-config",
            schedule_type="recurring",
            configuration={"cron": "*/5 * * * *"},
            payload={"action": "sync_users"},
        )
        session.add_all([s1, s2])
        await session.commit()
        await session.refresh(s1)
        await session.refresh(s2)
        s1_id, s2_id = s1.id, s2.id

    async with session_factory() as session:
        loaded_s1 = await session.get(ScheduleDefinition, s1_id)
        assert loaded_s1.payload == {"interval_seconds": 60}
        assert loaded_s1.configuration == {"interval_seconds": 60}
        assert loaded_s1.config == {"interval_seconds": 60}

        loaded_s2 = await session.get(ScheduleDefinition, s2_id)
        assert loaded_s2.configuration == {"cron": "*/5 * * * *"}
        assert loaded_s2.payload == {"action": "sync_users"}


@pytest.mark.asyncio
async def test_update_modifies_updated_at(session_factory):
    """Verify updated_at reflects modifications after creation."""
    async with session_factory() as session:
        schedule = ScheduleDefinition(
            name="to-update",
            schedule_type="recurring",
            configuration={"cron": "0 0 * * *"},
        )
        session.add(schedule)
        await session.commit()
        await session.refresh(schedule)
        schedule_id = schedule.id
        initial_created_at = schedule.created_at
        initial_updated_at = schedule.updated_at

    async with session_factory() as session:
        schedule = await session.get(ScheduleDefinition, schedule_id)
        schedule.name = "updated-name"
        schedule.configuration = {"cron": "0 12 * * *"}
        await session.commit()
        await session.refresh(schedule)

        assert schedule.name == "updated-name"
        assert schedule.configuration == {"cron": "0 12 * * *"}
        assert schedule.created_at == initial_created_at
        assert schedule.updated_at >= initial_updated_at


@pytest.mark.asyncio
async def test_name_and_schedule_type_required_constraints(session_factory):
    """Verify that non-nullable columns reject null values."""
    async with session_factory() as session:
        missing_name = ScheduleDefinition(schedule_type="recurring")
        session.add(missing_name)
        with pytest.raises(IntegrityError):
            await session.commit()

    async with session_factory() as session:
        missing_type = ScheduleDefinition(name="missing-type")
        session.add(missing_type)
        with pytest.raises(IntegrityError):
            await session.commit()


@pytest.mark.asyncio
async def test_query_filter_by_type_and_enabled_status(session_factory):
    """Verify querying schedules by type and enabled status."""
    async with session_factory() as session:
        s1 = ScheduleDefinition(name="active-recurring", schedule_type="recurring", enabled=True)
        s2 = ScheduleDefinition(name="paused-recurring", schedule_type="recurring", enabled=False)
        s3 = ScheduleDefinition(name="active-onetime", schedule_type="one_time", enabled=True)
        session.add_all([s1, s2, s3])
        await session.commit()

    async with session_factory() as session:
        # Query active recurring
        active_recurring = (
            await session.scalars(
                select(ScheduleDefinition).where(
                    ScheduleDefinition.schedule_type == "recurring",
                    ScheduleDefinition.enabled.is_(True),
                )
            )
        ).all()
        assert len(active_recurring) == 1
        assert active_recurring[0].name == "active-recurring"

        # Query all active
        all_active = (
            await session.scalars(
                select(ScheduleDefinition).where(ScheduleDefinition.enabled.is_(True))
            )
        ).all()
        assert len(all_active) == 2
        assert {s.name for s in all_active} == {"active-recurring", "active-onetime"}
