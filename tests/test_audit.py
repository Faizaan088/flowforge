"""Tests for Audit Event recording and query endpoints."""

import os
import pytest
import pytest_asyncio
from tests.test_policy_api import AsyncApiClient
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
    from models import AuditEvent, User
    from audit import record_audit_event, list_audit_events
    from auth import create_access_token, create_user, ROLE_ADMIN, ROLE_OPERATOR, ROLE_OBSERVER


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
    user = await create_user(db_session, "audit_admin", "password123", role=ROLE_ADMIN)
    await db_session.commit()
    return create_access_token(user)


@pytest_asyncio.fixture
async def operator_token(db_session):
    user = await create_user(db_session, "audit_operator", "password123", role=ROLE_OPERATOR)
    await db_session.commit()
    return create_access_token(user)


@pytest.mark.asyncio
async def test_audit_event_recording_and_querying(db_session):
    ev1 = await record_audit_event(
        db_session,
        actor_id="1",
        action="USER_CREATED",
        target_type="USER",
        target_id="2",
        metadata={"username": "alice"},
    )
    ev2 = await record_audit_event(
        db_session,
        actor_id="1",
        action="WORKFLOW_TRIGGERED",
        target_type="WORKFLOW",
        target_id="10",
    )
    await db_session.commit()

    all_events = await list_audit_events(db_session)
    assert len(all_events) >= 2

    user_events = await list_audit_events(db_session, action="USER_CREATED")
    assert len(user_events) == 1
    assert user_events[0].target_id == "2"


@pytest.mark.asyncio
async def test_audit_endpoint_permissions(session_factory, admin_token, operator_token):
    async def override_get_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    try:
        client = AsyncApiClient(app)
        # Operator should be forbidden (only admin with users:manage can view all audit)
        res = await client.request(
            "GET",
            "/audit/",
            headers={"Authorization": f"Bearer {operator_token}"},
        )
        assert res.status_code == 403

        # Admin should succeed
        res = await client.request(
            "GET",
            "/audit/",
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert res.status_code == 200
        assert isinstance(res.json(), list)
    finally:
        app.dependency_overrides.pop(get_db, None)
