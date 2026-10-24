"""Tests for Administrator Password Recovery CLI and Service."""

import os
import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from database import Base
from models import User, AuditEvent
from auth import (
    create_user,
    authenticate_user,
    ROLE_ADMIN,
    ROLE_OPERATOR,
    ROLE_OBSERVER,
    verify_password,
)
from scripts.reset_admin_password import reset_admin_password, list_admin_accounts

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL must point to an isolated PostgreSQL database",
)


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
async def clean_users_and_audit(session_factory):
    async with session_factory() as session:
        await session.execute(AuditEvent.__table__.delete())
        await session.execute(User.__table__.delete())
        await session.commit()


@pytest.mark.asyncio
async def test_admin_recovery_success_and_login(session_factory):
    """Admin password reset succeeds and updates authentication credentials in place."""
    old_password = "OldAdminPassword123!"
    new_password = "NewSuperSecurePassword2026!"

    async with session_factory() as session:
        admin = await create_user(
            session=session,
            username="sec_admin",
            password=old_password,
            role=ROLE_ADMIN,
            email="admin@flowforge.dev",
        )
        await session.commit()
        admin_id = admin.id

    # Reset password using recovery service
    async with session_factory() as session:
        updated_admin = await reset_admin_password(
            session=session,
            username="sec_admin",
            new_password=new_password,
            actor_id="system:recovery_test",
        )
        assert updated_admin.id == admin_id
        assert updated_admin.is_active is True
        assert verify_password(new_password, updated_admin.password_hash)

    # Verify old password fails and new password succeeds in authentication
    async with session_factory() as session:
        failed_auth = await authenticate_user(session, "sec_admin", old_password)
        assert failed_auth is None

        successful_auth = await authenticate_user(session, "sec_admin", new_password)
        assert successful_auth is not None
        assert successful_auth.id == admin_id

    # Verify audit event logged without storing password or hash
    async with session_factory() as session:
        stmt = (
            select(AuditEvent)
            .where(
                AuditEvent.action == "ADMIN_PASSWORD_RESET",
                AuditEvent.target_id == str(admin_id),
            )
        )
        events = (await session.execute(stmt)).scalars().all()
        assert len(events) == 1
        audit_ev = events[0]
        assert audit_ev.actor_id == "system:recovery_test"
        assert audit_ev.target_type == "USER"

        metadata = audit_ev.metadata_ or {}
        # Ensure passwords or hashes are NEVER present in audit metadata
        assert "password" not in metadata
        assert "password_hash" not in metadata
        assert new_password not in str(metadata)
        assert old_password not in str(metadata)


@pytest.mark.asyncio
async def test_admin_recovery_rejects_nonexistent_user(session_factory):
    """Attempting to reset a nonexistent account raises ValueError."""
    async with session_factory() as session:
        with pytest.raises(ValueError, match="was not found in the database"):
            await reset_admin_password(
                session=session,
                username="ghost_admin",
                new_password="ValidNewPassword123!",
            )


@pytest.mark.asyncio
async def test_admin_recovery_rejects_non_admin_target(session_factory):
    """Attempting to reset an operator or observer account is strictly refused."""
    async with session_factory() as session:
        operator = await create_user(
            session=session,
            username="ops_user",
            password="InitialPassword123!",
            role=ROLE_OPERATOR,
        )
        await session.commit()

    async with session_factory() as session:
        with pytest.raises(ValueError, match="only resets accounts with the administrator role"):
            await reset_admin_password(
                session=session,
                username="ops_user",
                new_password="NewSecurePassword123!",
            )


@pytest.mark.asyncio
async def test_admin_recovery_validates_password_strength(session_factory):
    """Password must be between 12 and 128 characters."""
    async with session_factory() as session:
        await create_user(
            session=session,
            username="short_pw_admin",
            password="InitialPassword123!",
            role=ROLE_ADMIN,
        )
        await session.commit()

    async with session_factory() as session:
        # Too short (< 12)
        with pytest.raises(ValueError, match="between 12 and 128 characters"):
            await reset_admin_password(
                session=session,
                username="short_pw_admin",
                new_password="short",
            )

        # Empty password
        with pytest.raises(ValueError, match="between 12 and 128 characters"):
            await reset_admin_password(
                session=session,
                username="short_pw_admin",
                new_password="",
            )


@pytest.mark.asyncio
async def test_list_admin_accounts_excludes_secrets(session_factory):
    """Listing administrator accounts returns usernames and metadata without secrets."""
    async with session_factory() as session:
        await create_user(
            session=session,
            username="admin_one",
            password="PasswordOne12345!",
            role=ROLE_ADMIN,
            email="one@example.com",
        )
        await create_user(
            session=session,
            username="admin_two",
            password="PasswordTwo12345!",
            role=ROLE_ADMIN,
            email=None,
        )
        await create_user(
            session=session,
            username="regular_operator",
            password="PasswordThree123!",
            role=ROLE_OPERATOR,
        )
        await session.commit()

        admins = await list_admin_accounts(session)
        assert len(admins) == 2
        usernames = {a["username"] for a in admins}
        assert usernames == {"admin_one", "admin_two"}
        for a in admins:
            assert "password" not in a
            assert "password_hash" not in a
