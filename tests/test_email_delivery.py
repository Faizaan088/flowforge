"""Tests for real email delivery, SMTP configuration validation, and token security."""

import os
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock
import smtplib
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from database import Base
from models import User, PasswordResetToken, EmailVerificationToken
from auth import (
    create_user,
    request_password_reset,
    reset_password_with_token,
    create_email_verification_token,
    verify_email_with_token,
    ROLE_OPERATOR,
    verify_password,
)
from config import validate_production_configuration
from email_service import (
    is_smtp_configured,
    send_email,
    send_password_reset_email,
    send_invitation_email,
    get_sent_emails,
    clear_sent_emails,
)

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
async def clean_database(session_factory):
    async with session_factory() as session:
        await session.execute(PasswordResetToken.__table__.delete())
        await session.execute(EmailVerificationToken.__table__.delete())
        await session.execute(User.__table__.delete())
        await session.commit()
    clear_sent_emails()


def test_missing_smtp_settings_dev_fallback():
    """When SMTP is not configured, email service records dev fallback without claiming real delivery."""
    with patch.dict(os.environ, {"SMTP_HOST": "", "SMTP_FROM": ""}, clear=False):
        assert is_smtp_configured() is False
        clear_sent_emails()

        success = send_email("alice@example.com", "Test Subject", "Hello world")
        assert success is True

        emails = get_sent_emails()
        assert len(emails) == 1
        assert emails[0]["to"] == "alice@example.com"
        assert emails[0]["delivery_mode"] == "dev_fallback"
        assert emails[0]["simulated"] is True


def test_smtp_delivery_failure_raises_runtime_error():
    """When SMTP is configured but the mail server fails, a clear RuntimeError is raised."""
    env = {
        "SMTP_HOST": "mail.example.com",
        "SMTP_PORT": "587",
        "SMTP_FROM": "notifications@flowforge.dev",
        "SMTP_USER": "testuser",
        "SMTP_PASSWORD": "testpassword",
        "SMTP_TLS": "true",
    }
    with patch.dict(os.environ, env, clear=False):
        assert is_smtp_configured() is True

        with patch("smtplib.SMTP") as mock_smtp:
            instance = MagicMock()
            mock_smtp.return_value = instance
            instance.send_message.side_effect = smtplib.SMTPConnectError(421, b"Service not available")

            with pytest.raises(RuntimeError, match="Email delivery service failure"):
                send_email("bob@example.com", "Test", "Content")


@pytest.mark.asyncio
async def test_request_password_reset_graceful_on_smtp_failure(session_factory):
    """When SMTP delivery fails during password reset, the endpoint handles it gracefully to prevent account enumeration."""
    async with session_factory() as session:
        await create_user(
            session=session,
            username="reset_fail_user",
            password="InitialPassword123!",
            role=ROLE_OPERATOR,
            email="fail_user@example.com",
        )
        await session.commit()

        # Mock send_password_reset_email to fail
        with patch("auth.send_password_reset_email", side_effect=RuntimeError("SMTP connection timeout")):
            success = await request_password_reset(session, "reset_fail_user")
            # Must return True to prevent user enumeration
            assert success is True


@pytest.mark.asyncio
async def test_password_reset_expired_token(session_factory):
    """Expired password reset tokens are rejected."""
    import hashlib
    raw_token = "expired_token_12345678901234567890"
    token_hash = hashlib.sha256(raw_token.encode("utf-8")).hexdigest()

    async with session_factory() as session:
        user = await create_user(
            session=session,
            username="expired_test_user",
            password="InitialPassword123!",
            role=ROLE_OPERATOR,
        )
        await session.flush()

        # Insert expired token (expired 2 hours ago)
        expired_token = PasswordResetToken(
            user_id=user.id,
            token_hash=token_hash,
            expires_at=datetime.now(timezone.utc) - timedelta(hours=2),
        )
        session.add(expired_token)
        await session.commit()

        with pytest.raises(ValueError, match="Invalid or expired password reset token"):
            await reset_password_with_token(session, raw_token, "NewPassword12345!")


@pytest.mark.asyncio
async def test_password_reset_invalid_token(session_factory):
    """Invalid tokens that do not exist in the database are rejected."""
    async with session_factory() as session:
        with pytest.raises(ValueError, match="Invalid or expired password reset token"):
            await reset_password_with_token(session, "completely_bogus_token_xyz", "NewPassword12345!")


@pytest.mark.asyncio
async def test_password_reset_token_reuse_prevented(session_factory):
    """Reset tokens are single-use; reusing a token immediately fails."""
    async with session_factory() as session:
        user = await create_user(
            session=session,
            username="single_use_user",
            password="InitialPassword123!",
            role=ROLE_OPERATOR,
            email="single_use@example.com",
        )
        await session.commit()

        # Request reset
        await request_password_reset(session, "single_use_user")
        await session.commit()

        emails = get_sent_emails()
        token = emails[-1]["metadata"]["token"]

        # First use succeeds
        updated = await reset_password_with_token(session, token, "NewPassword123456!")
        await session.commit()
        assert verify_password("NewPassword123456!", updated.password_hash)

        # Second use fails
        with pytest.raises(ValueError, match="Invalid or expired password reset token"):
            await reset_password_with_token(session, token, "AnotherNewPassword123!")


def test_production_smtp_configuration_validation():
    """Production configuration validator enforces complete and valid SMTP settings when enabled."""
    db_pass = "a" * 24
    redis_pass = "a" * 32
    base_prod_env = {
        "ENVIRONMENT": "production",
        "FLOWFORGE_SECRET_KEY": "a" * 32,
        "FLOWFORGE_ADMIN_USERNAME": "admin",
        "FLOWFORGE_ADMIN_PASSWORD": "AdminPassword2026!",
        "CORS_ALLOWED_ORIGINS": "https://dashboard.example.com",
        "DATABASE_URL": f"postgresql+asyncpg://flowforge_user:{db_pass}@postgres:5432/flowforge",
        "REQUIRE_REDIS_AUTH": "true",
        "REDIS_URL": f"redis://:{redis_pass}@redis:6379/0",
    }

    # 1. REQUIRE_SMTP enabled but missing SMTP_HOST
    with patch.dict(os.environ, {**base_prod_env, "REQUIRE_SMTP": "true", "SMTP_HOST": ""}, clear=True):
        with pytest.raises(ValueError, match="REQUIRE_SMTP is enabled but SMTP_HOST is not configured"):
            validate_production_configuration()

    # 2. Configured SMTP with invalid SMTP_FROM
    with patch.dict(os.environ, {**base_prod_env, "SMTP_HOST": "smtp.example.com", "SMTP_FROM": "invalid-email"}, clear=True):
        with pytest.raises(ValueError, match="SMTP_FROM must be a valid email address"):
            validate_production_configuration()

    # 3. Configured SMTP with invalid port
    with patch.dict(os.environ, {**base_prod_env, "SMTP_HOST": "smtp.example.com", "SMTP_FROM": "notifications@example.com", "SMTP_PORT": "99999"}, clear=True):
        with pytest.raises(ValueError, match="Invalid SMTP_PORT"):
            validate_production_configuration()

    # 4. Configured SMTP with template placeholder password
    with patch.dict(os.environ, {**base_prod_env, "SMTP_HOST": "smtp.example.com", "SMTP_FROM": "notifications@example.com", "SMTP_PASSWORD": "replace-with-your-smtp-password"}, clear=True):
        with pytest.raises(ValueError, match="SMTP_PASSWORD cannot use a template placeholder"):
            validate_production_configuration()

    # 5. Valid SMTP settings pass validation
    with patch.dict(os.environ, {
        **base_prod_env,
        "REQUIRE_SMTP": "true",
        "SMTP_HOST": "smtp.sendgrid.net",
        "SMTP_FROM": "FlowForge System <notifications@flowforge.dev>",
        "SMTP_PORT": "587",
        "SMTP_PASSWORD": "SuperSecureSmtpPassword123!",
    }, clear=True):
        validate_production_configuration()  # Must not raise
