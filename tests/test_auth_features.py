import os
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from database import Base
from auth import (
    create_user,
    get_user_by_username,
    request_password_reset,
    reset_password_with_token,
    create_email_verification_token,
    verify_email_with_token,
    create_api_key,
    authenticate_api_key,
    list_api_keys,
    revoke_api_key,
    delete_user_account,
    revoke_token,
    is_token_revoked,
    ROLE_OPERATOR,
    ROLE_ADMIN,
    verify_password,
)
from models import User, PasswordResetToken, EmailVerificationToken, ApiKey
from email_service import get_sent_emails, clear_sent_emails

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
async def clear_database(session_factory):
    async with session_factory() as session:
        await session.execute(ApiKey.__table__.delete())
        await session.execute(EmailVerificationToken.__table__.delete())
        await session.execute(PasswordResetToken.__table__.delete())
        await session.execute(User.__table__.delete())
        await session.commit()


@pytest.mark.asyncio
async def test_password_reset_flow(session_factory):
    clear_sent_emails()
    async with session_factory() as session:
        user = await create_user(
            session=session,
            username="reset_user_test",
            password="InitialPassword123!",
            role=ROLE_OPERATOR,
            email="reset_user@example.com",
        )
        await session.commit()

        # Request reset
        success = await request_password_reset(session, "reset_user_test", base_url="http://localhost:5173")
        assert success is True
        await session.commit()

        # Check email was generated
        emails = get_sent_emails()
        assert len(emails) >= 1
        reset_email = emails[-1]
        assert reset_email["to"] == "reset_user@example.com"
        assert "token" in reset_email["metadata"]
        token = reset_email["metadata"]["token"]

        # Reset password with token
        updated_user = await reset_password_with_token(session, token, "NewSecurePassword456!")
        await session.commit()
        assert verify_password("NewSecurePassword456!", updated_user.password_hash)

        # Attempting to reuse the token should fail
        with pytest.raises(ValueError, match="Invalid or expired"):
            await reset_password_with_token(session, token, "AnotherPassword789!")


@pytest.mark.asyncio
async def test_email_verification_flow(session_factory):
    clear_sent_emails()
    async with session_factory() as session:
        user = await create_user(
            session=session,
            username="verify_user_test",
            password="InitialPassword123!",
            role=ROLE_OPERATOR,
            email="verify_user@example.com",
        )
        await session.commit()
        assert user.is_verified is False

        # Create verification token
        raw_token = await create_email_verification_token(session, user, base_url="http://localhost:5173")
        await session.commit()

        emails = get_sent_emails()
        assert len(emails) >= 1
        assert emails[-1]["to"] == "verify_user@example.com"

        # Verify email
        verified_user = await verify_email_with_token(session, raw_token)
        await session.commit()
        assert verified_user.is_verified is True

        # Re-using the token fails
        with pytest.raises(ValueError, match="Invalid or expired"):
            await verify_email_with_token(session, raw_token)


@pytest.mark.asyncio
async def test_api_key_lifecycle(session_factory):
    async with session_factory() as session:
        user = await create_user(
            session=session,
            username="apikey_user_test",
            password="InitialPassword123!",
            role=ROLE_OPERATOR,
        )
        await session.commit()

        # Create API key
        key_record, raw_key = await create_api_key(
            session=session,
            user_id=user.id,
            name="Production Ingestion Key",
            expires_in_days=30,
        )
        await session.commit()

        assert raw_key.startswith("ffk_")
        assert key_record.key_prefix == raw_key[:12]
        assert key_record.is_active is True

        # Authenticate with valid key
        auth_user = await authenticate_api_key(session, raw_key)
        assert auth_user is not None
        assert auth_user.id == user.id

        # List keys
        keys = await list_api_keys(session, user.id)
        assert len(keys) == 1
        assert keys[0].id == key_record.id

        # Revoke key
        revoked = await revoke_api_key(session, user.id, key_record.id)
        assert revoked is True
        await session.commit()

        # Authentication with revoked key fails
        auth_revoked = await authenticate_api_key(session, raw_key)
        assert auth_revoked is None


@pytest.mark.asyncio
async def test_token_revocation():
    token_str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.dummy_test_token"
    assert is_token_revoked(token_str) is False

    revoke_token(token_str)
    assert is_token_revoked(token_str) is True


@pytest.mark.asyncio
async def test_account_deletion(session_factory):
    async with session_factory() as session:
        user = await create_user(
            session=session,
            username="delete_user_test",
            password="InitialPassword123!",
            role=ROLE_OPERATOR,
        )
        await session.commit()

        deleted = await delete_user_account(session, user.id)
        assert deleted is True
        await session.commit()

        fetched = await get_user_by_username(session, "delete_user_test")
        assert fetched is None
