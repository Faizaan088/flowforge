"""Tests for User Creation, RBAC Enforcement, and Onboarding Credential Delivery."""

import os
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from database import Base, get_db
from models import User, PasswordResetToken, AuditEvent
from auth import (
    create_user,
    authenticate_user,
    create_access_token,
    reset_password_with_token,
    ROLE_ADMIN,
    ROLE_OPERATOR,
    ROLE_OBSERVER,
    verify_password,
)
from tests.test_policy_api import AsyncApiClient
from email_service import clear_sent_emails

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL must point to an isolated PostgreSQL database",
)

if TEST_DATABASE_URL:
    os.environ["DATABASE_URL"] = TEST_DATABASE_URL
    from main import app


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
async def clean_database(session_factory):
    async with session_factory() as session:
        await session.execute(PasswordResetToken.__table__.delete())
        await session.execute(AuditEvent.__table__.delete())
        await session.execute(User.__table__.delete())
        await session.commit()
    clear_sent_emails()


@pytest_asyncio.fixture
async def raw_client(session_factory):
    async def get_test_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = get_test_db
    try:
        yield AsyncApiClient(app)
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest.mark.asyncio
async def test_admin_create_user_with_initial_password(raw_client, session_factory):
    """Admin creates a user with manual initial password; response contains credentials and no password hash."""
    async with session_factory() as session:
        admin = await create_user(session, "super_admin", "AdminPass12345!", role=ROLE_ADMIN)
        await session.commit()
        admin_token = create_access_token(admin)

    resp = await raw_client.post(
        "/auth/users",
        json={
            "username": "operator_jane",
            "password": "JanePassword123!",
            "email": "jane@flowforge.dev",
            "role": "operator",
            "delivery_method": "manual_password",
        },
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 201
    data = resp.json()

    # Public ID, username, assigned role, account status
    assert "id" in data and isinstance(data["id"], int)
    assert data["username"] == "operator_jane"
    assert data["role"] == "operator"
    assert data["is_active"] is True
    assert data["credential_type"] == "initial_password"
    assert data["initial_password"] == "JanePassword123!"
    assert "password_hash" not in data

    # Honest email reporting: in dev without SMTP configured, status is not_configured
    assert data["email_delivery_status"] in ("not_configured", "sent")

    # Verify user can log in with initial password
    async with session_factory() as session:
        authenticated = await authenticate_user(session, "operator_jane", "JanePassword123!")
        assert authenticated is not None
        assert authenticated.id == data["id"]


@pytest.mark.asyncio
async def test_admin_create_user_with_invitation_flow(raw_client, session_factory):
    """Admin creates a user with secure invitation token; token is single-use and allows setting password."""
    async with session_factory() as session:
        admin = await create_user(session, "invite_admin", "AdminPass12345!", role=ROLE_ADMIN)
        await session.commit()
        admin_token = create_access_token(admin)

    resp = await raw_client.post(
        "/auth/users",
        json={
            "username": "invited_user",
            "email": "invited@flowforge.dev",
            "role": "observer",
            "delivery_method": "email_invitation",
        },
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 201
    data = resp.json()

    assert data["username"] == "invited_user"
    assert data["role"] == "observer"
    assert data["credential_type"] == "invitation_link"
    assert data["initial_password"] is None
    assert "invitation_url" in data and "reset-password?token=" in data["invitation_url"]
    assert "invitation_token" in data and len(data["invitation_token"]) >= 20
    assert "password_hash" not in data

    invite_token = data["invitation_token"]

    # Invited user cannot log in before setting password
    async with session_factory() as session:
        uninit_auth = await authenticate_user(session, "invited_user", "")
        assert uninit_auth is None

    # Complete setup with the invitation token
    async with session_factory() as session:
        user = await reset_password_with_token(session, invite_token, "MyNewSecurePassword2026!")
        await session.commit()
        assert verify_password("MyNewSecurePassword2026!", user.password_hash)

    # Logging in with new password succeeds
    async with session_factory() as session:
        auth_user = await authenticate_user(session, "invited_user", "MyNewSecurePassword2026!")
        assert auth_user is not None
        assert auth_user.username == "invited_user"

    # Re-using the invitation token fails (single-use token security)
    async with session_factory() as session:
        with pytest.raises(ValueError, match="Invalid or expired"):
            await reset_password_with_token(session, invite_token, "AnotherAttemptPassword123!")


@pytest.mark.asyncio
async def test_create_user_requires_email_for_invitation(raw_client, session_factory):
    """Creating an invitation-based user requires a valid email address."""
    async with session_factory() as session:
        admin = await create_user(session, "admin_no_email", "AdminPass12345!", role=ROLE_ADMIN)
        await session.commit()
        admin_token = create_access_token(admin)

    resp = await raw_client.post(
        "/auth/users",
        json={
            "username": "user_missing_email",
            "role": "observer",
            "delivery_method": "email_invitation",
        },
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert resp.status_code == 400
    assert "email address is required" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_create_user_rbac_permissions(raw_client, session_factory):
    """Only administrators with 'users:manage' permission can create users."""
    async with session_factory() as session:
        operator = await create_user(session, "plain_op", "OperatorPass123!", role=ROLE_OPERATOR)
        observer = await create_user(session, "plain_obs", "ObserverPass123!", role=ROLE_OBSERVER)
        await session.commit()
        op_token = create_access_token(operator)
        obs_token = create_access_token(observer)

    # Operator cannot create users
    op_resp = await raw_client.post(
        "/auth/users",
        json={"username": "test_by_op", "password": "Password12345!", "role": "observer"},
        headers={"Authorization": f"Bearer {op_token}"},
    )
    assert op_resp.status_code == 403

    # Observer cannot create users
    obs_resp = await raw_client.post(
        "/auth/users",
        json={"username": "test_by_obs", "password": "Password12345!", "role": "observer"},
        headers={"Authorization": f"Bearer {obs_token}"},
    )
    assert obs_resp.status_code == 403


@pytest.mark.asyncio
async def test_create_user_rejects_duplicate_and_invalid_role(raw_client, session_factory):
    """Duplicate usernames return 409 Conflict; invalid roles return 400 Bad Request."""
    async with session_factory() as session:
        admin = await create_user(session, "role_admin", "AdminPass12345!", role=ROLE_ADMIN)
        await create_user(session, "existing_worker", "WorkerPass12345!", role=ROLE_OPERATOR)
        await session.commit()
        admin_token = create_access_token(admin)

    # Duplicate username -> 409
    dup_resp = await raw_client.post(
        "/auth/users",
        json={"username": "existing_worker", "password": "Password12345!", "role": "operator"},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert dup_resp.status_code == 409

    # Invalid role -> 400
    invalid_role_resp = await raw_client.post(
        "/auth/users",
        json={"username": "valid_user_bad_role", "password": "Password12345!", "role": "supergod"},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert invalid_role_resp.status_code == 400
    assert "Invalid role" in invalid_role_resp.json()["detail"]
