"""Focused tests for Role-Based Access Control (RBAC) across FlowForge control-plane endpoints."""

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

    from auth import (
        ROLE_ADMIN,
        ROLE_OBSERVER,
        ROLE_OPERATOR,
        create_access_token,
        create_user,
    )
    from database import Base, get_db
    from main import app
    from models import (
        ConcurrencyLimitPolicy,
        Execution,
        JobDefinition,
        RateLimitPolicy,
        RateLimitRecord,
        User,
        WorkflowDefinition,
        WorkflowRun,
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


@pytest_asyncio.fixture(autouse=True)
async def clear_database(session_factory):
    async with session_factory() as session:
        await session.execute(RateLimitRecord.__table__.delete())
        await session.execute(RateLimitPolicy.__table__.delete())
        await session.execute(ConcurrencyLimitPolicy.__table__.delete())
        await session.execute(WorkflowRun.__table__.delete())
        await session.execute(WorkflowDefinition.__table__.delete())
        await session.execute(Execution.__table__.delete())
        await session.execute(JobDefinition.__table__.delete())
        await session.execute(User.__table__.delete())
        await session.commit()


@pytest_asyncio.fixture
async def raw_client(session_factory):
    """Client without default authentication override for testing RBAC boundaries."""
    async def get_test_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = get_test_db
    try:
        yield AsyncApiClient(app)
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest_asyncio.fixture
async def users_and_tokens(session_factory):
    """Fixture providing initialized users and their bearer tokens for each role."""
    async with session_factory() as session:
        admin = await create_user(session, "admin_test", "adminPass", role=ROLE_ADMIN)
        operator = await create_user(session, "operator_test", "opPass", role=ROLE_OPERATOR)
        observer = await create_user(session, "observer_test", "obsPass", role=ROLE_OBSERVER)
        disabled_op = await create_user(session, "disabled_op", "disPass", role=ROLE_OPERATOR, is_active=False)
        await session.commit()

        return {
            "admin_token": create_access_token(admin),
            "operator_token": create_access_token(operator),
            "observer_token": create_access_token(observer),
            "disabled_token": create_access_token(disabled_op),
            "admin_id": admin.id,
            "operator_id": operator.id,
            "observer_id": observer.id,
        }


# ===========================================================================
# 1. Unauthenticated Requests Return 401
# ===========================================================================
@pytest.mark.asyncio
async def test_unauthenticated_requests_return_401(raw_client):
    """Every control-plane endpoint must reject unauthenticated requests with 401."""
    # Executions
    assert (await raw_client.get("/executions/")).status_code == 401
    assert (await raw_client.get("/executions/1")).status_code == 401
    assert (await raw_client.post("/executions/1/cancel")).status_code == 401
    assert (await raw_client.patch("/executions/1/priority", json={"priority": 10})).status_code == 401

    # Jobs
    assert (await raw_client.post("/jobs/", json={"name": "test-job"})).status_code == 401
    assert (await raw_client.post("/jobs/1/execute")).status_code == 401

    # Policies
    assert (await raw_client.get("/policies/concurrency")).status_code == 401
    assert (await raw_client.post("/policies/concurrency", json={"target_type": "WORKFLOW", "target_id": "w1"})).status_code == 401
    assert (await raw_client.get("/policies/rate-limit")).status_code == 401
    assert (await raw_client.post("/policies/rate-limit", json={"target_type": "CATEGORY", "target_id": "c1"})).status_code == 401

    # Workflows
    assert (await raw_client.post("/workflows/runs/1/cancel")).status_code == 401

    # System
    assert (await raw_client.post("/system/reconcile-queue")).status_code == 401
    assert (await raw_client.post("/system/sweep")).status_code == 401


@pytest.mark.asyncio
async def test_disabled_user_token_returns_401(raw_client, users_and_tokens):
    """A disabled account cannot access protected endpoints even with a valid signature token."""
    token = users_and_tokens["disabled_token"]
    resp = await raw_client.get("/executions/", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 401
    assert "disabled" in resp.json()["detail"]


# ===========================================================================
# 2. Observer Role Boundaries (Read-Only)
# ===========================================================================
@pytest.mark.asyncio
async def test_observer_can_read_but_cannot_mutate(raw_client, session_factory, users_and_tokens):
    """Observer can read executions and policies, but is forbidden from mutating actions (403)."""
    token = users_and_tokens["observer_token"]
    headers = {"Authorization": f"Bearer {token}"}

    async with session_factory() as session:
        job = JobDefinition(name="job-obs")
        session.add(job)
        await session.commit()
        job_id = job.id
        execution = Execution(job_definition_id=job_id, status="QUEUED")
        session.add(execution)
        await session.commit()
        exec_id = execution.id

    # 1. Observer CAN read executions -> 200
    list_resp = await raw_client.get("/executions/", headers=headers)
    assert list_resp.status_code == 200

    get_resp = await raw_client.get(f"/executions/{exec_id}", headers=headers)
    assert get_resp.status_code == 200

    # 2. Observer CAN read policies -> 200
    cp_resp = await raw_client.get("/policies/concurrency", headers=headers)
    assert cp_resp.status_code == 200
    rl_resp = await raw_client.get("/policies/rate-limit", headers=headers)
    assert rl_resp.status_code == 200

    # 3. Observer CANNOT trigger jobs -> 403 Forbidden
    job_trigger = await raw_client.post(f"/jobs/{job_id}/execute", headers=headers)
    assert job_trigger.status_code == 403

    # 4. Observer CANNOT cancel execution -> 403 Forbidden
    cancel_resp = await raw_client.post(f"/executions/{exec_id}/cancel", headers=headers)
    assert cancel_resp.status_code == 403

    # 5. Observer CANNOT update execution priority -> 403 Forbidden
    prio_resp = await raw_client.patch(f"/executions/{exec_id}/priority", json={"priority": 10}, headers=headers)
    assert prio_resp.status_code == 403

    # 6. Observer CANNOT manage policies -> 403 Forbidden
    create_policy = await raw_client.post(
        "/policies/concurrency",
        json={"target_type": "WORKFLOW", "target_id": "wf-obs", "max_concurrency": 2},
        headers=headers,
    )
    assert create_policy.status_code == 403

    # 7. Observer CANNOT manage users -> 403 Forbidden
    user_mgmt = await raw_client.post(
        "/auth/users",
        json={"username": "evil", "password": "pass", "role": "admin"},
        headers=headers,
    )
    assert user_mgmt.status_code == 403


# ===========================================================================
# 3. Operator Role Boundaries (Operations Allowed, Admin Forbidden)
# ===========================================================================
@pytest.mark.asyncio
async def test_operator_permissions_and_restrictions(raw_client, session_factory, users_and_tokens):
    """Operator can manage executions and jobs, but CANNOT manage policies or system administration."""
    op_token = users_and_tokens["operator_token"]
    op_headers = {"Authorization": f"Bearer {op_token}"}

    async with session_factory() as session:
        job = JobDefinition(name="job-op")
        session.add(job)
        await session.commit()
        job_id = job.id

    # 1. Operator CAN create job and trigger execution -> 200
    create_job_resp = await raw_client.post(
        "/jobs/",
        json={"name": "new-job", "priority": 3},
        headers=op_headers,
    )
    assert create_job_resp.status_code == 200

    trigger_resp = await raw_client.post(
        f"/jobs/{job_id}/execute",
        headers=op_headers,
    )
    assert trigger_resp.status_code == 200
    exec_id = trigger_resp.json()["id"]

    # 2. Operator CAN update execution priority -> 200
    prio_resp = await raw_client.patch(
        f"/executions/{exec_id}/priority",
        json={"priority": 50},
        headers=op_headers,
    )
    assert prio_resp.status_code == 200
    assert prio_resp.json()["priority"] == 50

    # 3. Operator CAN cancel execution -> 200
    cancel_resp = await raw_client.post(
        f"/executions/{exec_id}/cancel",
        json={"reason": "Operator cancelled"},
        headers=op_headers,
    )
    assert cancel_resp.status_code == 200

    # 4. Operator CANNOT manage policies -> 403 Forbidden
    policy_resp = await raw_client.post(
        "/policies/concurrency",
        json={"target_type": "WORKFLOW", "target_id": "wf-op", "max_concurrency": 5},
        headers=op_headers,
    )
    assert policy_resp.status_code == 403

    rl_resp = await raw_client.post(
        "/policies/rate-limit",
        json={"target_type": "CATEGORY", "target_id": "cat-op", "max_requests": 20},
        headers=op_headers,
    )
    assert rl_resp.status_code == 403

    # 5. Operator CANNOT perform system administration -> 403 Forbidden
    reconcile_resp = await raw_client.post("/system/reconcile-queue", headers=op_headers)
    assert reconcile_resp.status_code == 403

    sweep_resp = await raw_client.post("/system/sweep", headers=op_headers)
    assert sweep_resp.status_code == 403


# ===========================================================================
# 4. Administrator Role Has Full Access
# ===========================================================================
@pytest.mark.asyncio
async def test_admin_has_full_control_plane_access(raw_client, session_factory, users_and_tokens):
    """Admin has access to all execution, policy, system, and user management operations."""
    admin_token = users_and_tokens["admin_token"]
    admin_headers = {"Authorization": f"Bearer {admin_token}"}

    # 1. Admin can manage concurrency policies -> 200
    cp_resp = await raw_client.post(
        "/policies/concurrency",
        json={"target_type": "WORKFLOW", "target_id": "wf-admin", "max_concurrency": 10},
        headers=admin_headers,
    )
    assert cp_resp.status_code == 200
    pol_id = cp_resp.json()["id"]

    # 2. Admin can disable policy -> 200
    dis_resp = await raw_client.post(f"/policies/concurrency/{pol_id}/disable", headers=admin_headers)
    assert dis_resp.status_code == 200
    assert dis_resp.json()["is_enabled"] is False

    # 3. Admin can delete policy -> 200
    del_resp = await raw_client.delete(f"/policies/concurrency/{pol_id}", headers=admin_headers)
    assert del_resp.status_code == 200

    # 4. Admin can manage rate-limit policies -> 200
    rl_resp = await raw_client.post(
        "/policies/rate-limit",
        json={"target_type": "CATEGORY", "target_id": "cat-admin", "max_requests": 50},
        headers=admin_headers,
    )
    assert rl_resp.status_code == 200

    # 5. Admin can create and list users -> 201 / 200
    create_u = await raw_client.post(
        "/auth/users",
        json={"username": "created_by_admin", "password": "validPassword123", "role": "observer"},
        headers=admin_headers,
    )
    assert create_u.status_code == 201


# ===========================================================================
# 5. Error Semantics Preservation Under Authentication
# ===========================================================================
@pytest.mark.asyncio
async def test_error_semantics_with_authenticated_user(raw_client, users_and_tokens):
    """When properly authenticated, missing resources return 404, not 401 or 403."""
    admin_headers = {"Authorization": f"Bearer {users_and_tokens['admin_token']}"}

    # Non-existent execution -> 404
    resp_exec = await raw_client.get("/executions/999999", headers=admin_headers)
    assert resp_exec.status_code == 404

    # Non-existent policy -> 404
    resp_pol = await raw_client.get("/policies/concurrency/999999", headers=admin_headers)
    assert resp_pol.status_code == 404

    # Invalid policy input -> 400
    bad_pol = await raw_client.post(
        "/policies/concurrency",
        json={"target_type": "INVALID_TYPE", "target_id": "t1", "max_concurrency": -1},
        headers=admin_headers,
    )
    assert bad_pol.status_code == 400
