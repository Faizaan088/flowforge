"""Tests for Schedules and Jobs API endpoints, including RBAC and audit trail integration."""

import os
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL must point to an isolated PostgreSQL database",
)

if TEST_DATABASE_URL:
    os.environ["DATABASE_URL"] = TEST_DATABASE_URL
    from database import Base, get_db
    from main import app
    from models import (
        ScheduleDefinition,
        JobDefinition,
        AuditEvent,
        User,
    )
    from auth import (
        create_access_token,
        create_user,
        ROLE_ADMIN,
        ROLE_OPERATOR,
        ROLE_OBSERVER,
    )
    from tests.test_policy_api import AsyncApiClient


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


@pytest_asyncio.fixture
async def db_session(session_factory):
    async with session_factory() as session:
        yield session


@pytest_asyncio.fixture
async def admin_token(db_session):
    user = await create_user(db_session, "api_admin", "password123", role=ROLE_ADMIN)
    await db_session.commit()
    return create_access_token(user)


@pytest_asyncio.fixture
async def operator_token(db_session):
    user = await create_user(db_session, "api_operator", "password123", role=ROLE_OPERATOR)
    await db_session.commit()
    return create_access_token(user)


@pytest_asyncio.fixture
async def observer_token(db_session):
    user = await create_user(db_session, "api_observer", "password123", role=ROLE_OBSERVER)
    await db_session.commit()
    return create_access_token(user)


@pytest.mark.asyncio
async def test_jobs_api_crud(session_factory, operator_token):
    async def override_get_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    try:
        client = AsyncApiClient(app)

        # Create job directly in DB first
        async with session_factory() as session:
            job = JobDefinition(
                name="step1",
                category="batch",
                priority=1,
                max_retries=3,
                payload={"msg": "hello"},
            )
            session.add(job)
            await session.commit()
            job_id = job.id

        # GET /jobs/
        res = await client.get("/jobs/", headers={"Authorization": f"Bearer {operator_token}"})
        assert res.status_code == 200
        jobs = res.json()
        assert len(jobs) >= 1
        assert any(j["id"] == job_id for j in jobs)

        # GET /jobs/{job_id}
        res = await client.get(f"/jobs/{job_id}", headers={"Authorization": f"Bearer {operator_token}"})
        assert res.status_code == 200
        assert res.json()["name"] == "step1"
        assert res.json()["category"] == "batch"
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.mark.asyncio
async def test_schedules_api_lifecycle(session_factory, operator_token, observer_token):
    async def override_get_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    try:
        client = AsyncApiClient(app)

        # 1. Create schedule via POST /schedules/
        payload = {
            "name": "daily_report",
            "schedule_type": "recurring",
            "enabled": True,
            "configuration": {
                "cron": "0 8 * * *",
                "timezone": "UTC",
                "target_type": "WORKFLOW",
                "target_id": 1,
            },
        }
        res = await client.post(
            "/schedules/",
            json=payload,
            headers={"Authorization": f"Bearer {operator_token}"},
        )
        assert res.status_code == 201
        created = res.json()
        schedule_id = created["id"]
        assert created["name"] == "daily_report"
        assert created["configuration"]["cron"] == "0 8 * * *"
        assert created["enabled"] is True

        # 2. GET /schedules/
        res = await client.get("/schedules/", headers={"Authorization": f"Bearer {operator_token}"})
        assert res.status_code == 200
        schedules = res.json()
        assert any(s["id"] == schedule_id for s in schedules)

        # 3. GET /schedules/{schedule_id}
        res = await client.get(f"/schedules/{schedule_id}", headers={"Authorization": f"Bearer {operator_token}"})
        assert res.status_code == 200
        assert res.json()["id"] == schedule_id

        # 4. Observer cannot PATCH schedule (lacks executions:trigger)
        res = await client.patch(
            f"/schedules/{schedule_id}",
            json={"enabled": False},
            headers={"Authorization": f"Bearer {observer_token}"},
        )
        assert res.status_code == 403

        # 5. Operator can PATCH schedule
        res = await client.patch(
            f"/schedules/{schedule_id}",
            json={"enabled": False, "configuration": {"cron": "0 9 * * *"}},
            headers={"Authorization": f"Bearer {operator_token}"},
        )
        assert res.status_code == 200
        updated = res.json()
        assert updated["enabled"] is False
        assert updated["configuration"]["cron"] == "0 9 * * *"

        # 6. DELETE /schedules/{schedule_id}
        res = await client.delete(
            f"/schedules/{schedule_id}",
            headers={"Authorization": f"Bearer {operator_token}"},
        )
        assert res.status_code == 200

        # Verify schedule deleted
        res = await client.get(f"/schedules/{schedule_id}", headers={"Authorization": f"Bearer {operator_token}"})
        assert res.status_code == 404
    finally:
        app.dependency_overrides.pop(get_db, None)
