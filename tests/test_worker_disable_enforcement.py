"""Regression and integration tests for worker-disable enforcement, task-claiming prevention, in-flight task safety, and worker re-enabling."""

from datetime import datetime, timedelta, timezone
import os
import pytest
import pytest_asyncio
from sqlalchemy import select
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
    from models import Execution, Worker, AuditEvent, User
    from execution_claim import (
        claim_execution,
        claim_next_execution,
        start_execution,
        complete_execution,
        renew_lease,
        ExecutionClaim,
    )
    from worker_registry import (
        register_worker,
        disable_worker,
        enable_worker,
        heartbeat_worker,
        is_worker_disabled,
        UnknownWorkerError,
    )
    from auth import create_access_token, create_user, ROLE_ADMIN, ROLE_OPERATOR, ROLE_OBSERVER
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
    user = await create_user(db_session, "worker_admin", "password123", role=ROLE_ADMIN)
    await db_session.commit()
    return create_access_token(user)


@pytest_asyncio.fixture
async def operator_token(db_session):
    user = await create_user(db_session, "worker_operator", "password123", role=ROLE_OPERATOR)
    await db_session.commit()
    return create_access_token(user)


@pytest.mark.asyncio
async def test_disabled_worker_cannot_claim_execution(session_factory):
    """Verify that a disabled worker cannot claim new executions via claim_execution or claim_next_execution."""
    async with session_factory() as session:
        # Register and then disable worker
        await register_worker(session, "worker-dis-1")
        await disable_worker(session, "worker-dis-1")

        # Create two queued executions
        e1 = Execution(status="QUEUED", priority=1)
        e2 = Execution(status="QUEUED", priority=2)
        session.add_all([e1, e2])
        await session.commit()
        e1_id, e2_id = e1.id, e2.id

    async with session_factory() as session:
        # 1. claim_execution must be rejected for disabled worker
        claim1 = await claim_execution(session, e1_id, "worker-dis-1", timedelta(seconds=30))
        assert claim1 is None

        # 2. claim_next_execution must be rejected for disabled worker
        claim2 = await claim_next_execution(session, "worker-dis-1", timedelta(seconds=30))
        assert claim2 is None

        # Verify executions remain in QUEUED state
        row1 = await session.get(Execution, e1_id)
        row2 = await session.get(Execution, e2_id)
        assert row1.status == "QUEUED"
        assert row2.status == "QUEUED"
        assert row1.worker_id is None
        assert row2.worker_id is None


@pytest.mark.asyncio
async def test_disabled_worker_cannot_start_or_renew_execution(session_factory):
    """If a worker was active when claiming, but disabled before starting or renewing, operations are rejected."""
    async with session_factory() as session:
        await register_worker(session, "worker-dis-2")
        e = Execution(status="QUEUED")
        session.add(e)
        await session.commit()
        eid = e.id

        # Worker claims while active
        claim = await claim_execution(session, eid, "worker-dis-2", timedelta(seconds=30))
        assert claim is not None
        await session.commit()

        # Worker is disabled
        await disable_worker(session, "worker-dis-2")

        # start_execution must be rejected
        started = await start_execution(session, claim)
        assert started is False

        # renew_lease must be rejected
        renewed = await renew_lease(session, claim, timedelta(seconds=30))
        assert renewed is None


@pytest.mark.asyncio
async def test_in_flight_tasks_safe_behavior_on_worker_disable(session_factory):
    """When a worker is disabled while holding claimed or running tasks:
    1. CLAIMED tasks are returned to QUEUED so other workers can run them.
    2. RUNNING tasks are transitioned to FAILED with explicit reason and scheduled for retry.
    3. The disabled worker cannot call complete_execution to mark them SUCCEEDED.
    """
    async with session_factory() as session:
        await register_worker(session, "worker-dis-3")

        e_claimed = Execution(status="QUEUED", priority=5)
        e_running = Execution(status="QUEUED", priority=10, max_retries=3, attempt=0)
        session.add_all([e_claimed, e_running])
        await session.commit()
        cid, rid = e_claimed.id, e_running.id

        claim_c = await claim_execution(session, cid, "worker-dis-3", timedelta(seconds=60))
        claim_r = await claim_execution(session, rid, "worker-dis-3", timedelta(seconds=60))
        assert claim_c is not None and claim_r is not None

        # Start the second execution
        started = await start_execution(session, claim_r)
        assert started is True
        await session.commit()

    # Now disable the worker
    async with session_factory() as session:
        await disable_worker(session, "worker-dis-3")
        await session.commit()

    # Check safe states of both executions
    async with session_factory() as session:
        row_c = await session.get(Execution, cid)
        row_r = await session.get(Execution, rid)

        # 1. Claimed task is safely back in QUEUED and unassigned
        assert row_c.status == "QUEUED"
        assert row_c.worker_id is None
        assert row_c.lease_until is None

        # 2. Running task is NOT marked SUCCEEDED; it was transitioned safely (to RETRY_WAIT since attempt < max_retries)
        assert row_r.status in ("FAILED", "RETRY_WAIT")
        assert "disabled while execution was in flight" in (row_r.error_summary or "")

        # 3. If the worker attempts to complete the running execution anyway, it is rejected!
        completed = await complete_execution(session, claim_r)
        assert completed is False

        # Status must never become SUCCEEDED
        await session.refresh(row_r)
        assert row_r.status != "SUCCEEDED"


@pytest.mark.asyncio
async def test_re_enabling_worker_restores_claiming_ability(session_factory):
    """Re-enabling a disabled worker via enable_worker restores its ability to claim and complete tasks."""
    async with session_factory() as session:
        await register_worker(session, "worker-dis-4")
        await disable_worker(session, "worker-dis-4")
        assert await is_worker_disabled(session, "worker-dis-4") is True

        e = Execution(status="QUEUED")
        session.add(e)
        await session.commit()
        eid = e.id

        # Verify claim is blocked
        assert await claim_execution(session, eid, "worker-dis-4", timedelta(seconds=30)) is None

        # Re-enable the worker
        w = await enable_worker(session, "worker-dis-4")
        assert w.status == "ACTIVE"
        assert await is_worker_disabled(session, "worker-dis-4") is False

        # Now claiming succeeds!
        claim = await claim_execution(session, eid, "worker-dis-4", timedelta(seconds=30))
        assert claim is not None
        assert claim.worker_id == "worker-dis-4"

        # Worker can run and complete task cleanly
        assert await start_execution(session, claim) is True
        assert await complete_execution(session, claim) is True

        row = await session.get(Execution, eid)
        assert row.status == "SUCCEEDED"


@pytest.mark.asyncio
async def test_worker_api_disable_and_enable_endpoints(session_factory, admin_token, operator_token):
    """Test POST /workers/{worker_id}/disable and POST /workers/{worker_id}/enable with RBAC and audit."""
    async def override_get_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db
    try:
        client = AsyncApiClient(app)

        # Register worker in DB
        async with session_factory() as session:
            await register_worker(session, "node-api-1")

        # 1. Operator lacks policies:manage -> 403
        dis_res = await client.post(
            "/workers/node-api-1/disable",
            headers={"Authorization": f"Bearer {operator_token}"},
        )
        assert dis_res.status_code == 403

        # 2. Admin can disable worker -> 200
        dis_res = await client.post(
            "/workers/node-api-1/disable",
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert dis_res.status_code == 200
        assert dis_res.json()["status"] == "DISABLED"
        assert dis_res.json()["is_alive"] is False

        # 3. Admin can re-enable worker -> 200
        en_res = await client.post(
            "/workers/node-api-1/enable",
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert en_res.status_code == 200
        assert en_res.json()["status"] == "ACTIVE"
        assert en_res.json()["is_alive"] is True

        # 4. Check audit log has both events
        async with session_factory() as session:
            audit_records = (await session.execute(
                select(AuditEvent).where(AuditEvent.target_id == "node-api-1")
            )).scalars().all()
            actions = {a.action for a in audit_records}
            assert "WORKER_DISABLED" in actions
            assert "WORKER_ENABLED" in actions
    finally:
        app.dependency_overrides.pop(get_db, None)
