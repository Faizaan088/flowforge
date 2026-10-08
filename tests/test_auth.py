"""Comprehensive tests for authentication, user management, and JWT tokens."""

import os
from datetime import timedelta

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
        UserAlreadyExistsError,
        authenticate_user,
        bootstrap_admin,
        create_access_token,
        create_user,
        decode_access_token,
        get_user_by_id,
        get_user_by_username,
        hash_password,
        list_users,
        update_user,
        verify_password,
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
        await session.execute(Execution.__table__.delete())
        await session.execute(JobDefinition.__table__.delete())
        await session.execute(User.__table__.delete())
        await session.commit()


@pytest_asyncio.fixture
async def raw_client(session_factory):
    """Client without default authentication override for testing real auth flows."""
    async def get_test_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = get_test_db
    try:
        yield AsyncApiClient(app)
    finally:
        app.dependency_overrides.pop(get_db, None)


# ===========================================================================
# 1. Password Hashing & Verification
# ===========================================================================
def test_password_hashing_and_verification():
    password = "CorrectHorseBatteryStaple123!"
    hashed = hash_password(password)

    # Must be in standard pbkdf2:sha256 format
    assert hashed.startswith("pbkdf2:sha256:100000$")
    assert password not in hashed

    # Valid password verification
    assert verify_password(password, hashed) is True

    # Invalid password verification
    assert verify_password("WrongPassword", hashed) is False
    assert verify_password("", hashed) is False

    # Corrupted / tampered hashes
    assert verify_password(password, "invalid_hash_string") is False
    assert verify_password(password, "pbkdf2:sha256:notanumber$salt$hash") is False
    assert verify_password(password, "") is False


def test_password_salt_randomness():
    password = "SamePasswordAcrossUsers"
    hash1 = hash_password(password)
    hash2 = hash_password(password)
    assert hash1 != hash2, "Distinct salts must produce distinct hashes for the same password"


# ===========================================================================
# 2. JWT Token Generation & Validation
# ===========================================================================
def test_jwt_lifecycle_and_claims():
    user = User(id=42, username="alice", role=ROLE_OPERATOR, is_active=True)
    token = create_access_token(user, expires_delta=timedelta(minutes=30))

    parts = token.split(".")
    assert len(parts) == 3, "JWT must consist of header, payload, and signature"

    payload = decode_access_token(token)
    assert payload["sub"] == "42"
    assert payload["username"] == "alice"
    assert payload["role"] == ROLE_OPERATOR
    assert "exp" in payload
    assert "iat" in payload


def test_jwt_tampering_rejected():
    user = User(id=1, username="bob", role=ROLE_OBSERVER, is_active=True)
    token = create_access_token(user)

    # Tampered signature
    tampered = token[:-4] + "AAAA"
    with pytest.raises(Exception) as exc:
        decode_access_token(tampered)
    assert "Invalid authentication token signature" in str(exc.value.detail)


def test_jwt_expired_rejected():
    user = User(id=1, username="charlie", role=ROLE_ADMIN, is_active=True)
    expired_token = create_access_token(user, expires_delta=timedelta(seconds=-10))

    with pytest.raises(Exception) as exc:
        decode_access_token(expired_token)
    assert "Authentication token has expired" in str(exc.value.detail)


def test_jwt_malformed_rejected():
    with pytest.raises(Exception) as exc:
        decode_access_token("not.a.valid.jwt.token")
    assert "Malformed" in str(exc.value.detail)


# ===========================================================================
# 3. User Service Layer Operations
# ===========================================================================
@pytest.mark.asyncio
async def test_user_creation_and_retrieval(session_factory):
    async with session_factory() as session:
        user = await create_user(
            session=session,
            username="dave",
            password="securePassword456",
            role=ROLE_OPERATOR,
            email="dave@example.com",
        )
        await session.commit()
        user_id = user.id

    async with session_factory() as session:
        fetched = await get_user_by_id(session, user_id)
        assert fetched is not None
        assert fetched.username == "dave"
        assert fetched.email == "dave@example.com"
        assert fetched.role == ROLE_OPERATOR
        assert fetched.is_active is True
        assert verify_password("securePassword456", fetched.password_hash) is True
        assert "securePassword456" not in fetched.password_hash


@pytest.mark.asyncio
async def test_user_uniqueness_constraints(session_factory):
    async with session_factory() as session:
        await create_user(session, "unique_user", "pass123", email="unique@example.com")
        await session.commit()

    # Duplicate username
    async with session_factory() as session:
        with pytest.raises(UserAlreadyExistsError) as exc:
            await create_user(session, "unique_user", "differentPass")
        assert "already taken" in str(exc.value)

    # Duplicate email
    async with session_factory() as session:
        with pytest.raises(UserAlreadyExistsError) as exc:
            await create_user(session, "another_user", "pass789", email="unique@example.com")
        assert "already registered" in str(exc.value)


@pytest.mark.asyncio
async def test_user_validation_rules(session_factory):
    async with session_factory() as session:
        with pytest.raises(ValueError) as exc:
            await create_user(session, "", "validpass")
        assert "Username cannot be empty" in str(exc.value)

        with pytest.raises(ValueError) as exc:
            await create_user(session, "validuser", "validpass", role="superadmin")
        assert "Invalid role" in str(exc.value)


@pytest.mark.asyncio
async def test_user_authentication_service(session_factory):
    async with session_factory() as session:
        await create_user(session, "eve", "eveSecretPass", role=ROLE_ADMIN)
        await session.commit()

    async with session_factory() as session:
        # Valid credentials
        authed = await authenticate_user(session, "eve", "eveSecretPass")
        assert authed is not None
        assert authed.username == "eve"

        # Invalid password
        bad_pass = await authenticate_user(session, "eve", "wrongPassword")
        assert bad_pass is None

        # Unknown username
        bad_user = await authenticate_user(session, "nonexistent", "somePass")
        assert bad_user is None


@pytest.mark.asyncio
async def test_update_user_service(session_factory):
    async with session_factory() as session:
        u = await create_user(session, "frank", "initPass", role=ROLE_OBSERVER)
        await session.commit()
        uid = u.id

    async with session_factory() as session:
        updated = await update_user(session, uid, role=ROLE_ADMIN, is_active=False, password="newPass456")
        await session.commit()
        assert updated.role == ROLE_ADMIN
        assert updated.is_active is False
        assert verify_password("newPass456", updated.password_hash) is True


@pytest.mark.asyncio
async def test_admin_bootstrap_service(session_factory, monkeypatch):
    monkeypatch.setenv("FLOWFORGE_ADMIN_USERNAME", "sysadmin")
    monkeypatch.setenv("FLOWFORGE_ADMIN_PASSWORD", "bootstrapAdminPass123")

    async with session_factory() as session:
        admin = await bootstrap_admin(session)
        assert admin is not None
        assert admin.username == "sysadmin"
        assert admin.role == ROLE_ADMIN
        assert admin.is_active is True

    # Repeated bootstrap is idempotent
    async with session_factory() as session:
        admin2 = await bootstrap_admin(session)
        assert admin2 is not None
        assert admin2.id == admin.id


# ===========================================================================
# 4. Auth API Endpoints (Login & Identity)
# ===========================================================================
@pytest.mark.asyncio
async def test_auth_login_endpoint_success(raw_client, session_factory):
    async with session_factory() as session:
        await create_user(session, "apiloginuser", "myLoginPassword!", role=ROLE_OPERATOR)
        await session.commit()

    resp = await raw_client.post("/auth/login", json={
        "username": "apiloginuser",
        "password": "myLoginPassword!",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"
    assert data["user"]["username"] == "apiloginuser"
    assert data["user"]["role"] == ROLE_OPERATOR
    assert "password_hash" not in data["user"]
    assert "password" not in data["user"]


@pytest.mark.asyncio
async def test_auth_login_invalid_credentials(raw_client, session_factory):
    async with session_factory() as session:
        await create_user(session, "existinguser", "correctPass")
        await session.commit()

    # Wrong password -> 401
    resp1 = await raw_client.post("/auth/login", json={
        "username": "existinguser",
        "password": "wrongPassword",
    })
    assert resp1.status_code == 401
    assert "Incorrect username or password" in resp1.json()["detail"]

    # Non-existent username -> 401
    resp2 = await raw_client.post("/auth/login", json={
        "username": "nosuchuser",
        "password": "somePassword",
    })
    assert resp2.status_code == 401
    assert "Incorrect username or password" in resp2.json()["detail"]


@pytest.mark.asyncio
async def test_auth_login_disabled_user(raw_client, session_factory):
    async with session_factory() as session:
        await create_user(session, "disableduser", "somePass123", is_active=False)
        await session.commit()

    resp = await raw_client.post("/auth/login", json={
        "username": "disableduser",
        "password": "somePass123",
    })
    assert resp.status_code == 401
    assert "disabled" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_auth_me_endpoint(raw_client, session_factory):
    async with session_factory() as session:
        u = await create_user(session, "meuser", "mepass", role=ROLE_OBSERVER, email="me@example.com")
        await session.commit()
        token = create_access_token(u)

    # 1. Unauthenticated -> 401
    unauth = await raw_client.get("/auth/me")
    assert unauth.status_code == 401

    # 2. Authenticated with Bearer token -> 200
    auth_resp = await raw_client.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
    assert auth_resp.status_code == 200
    data = auth_resp.json()
    assert data["username"] == "meuser"
    assert data["email"] == "me@example.com"
    assert data["role"] == ROLE_OBSERVER
    assert "password_hash" not in data


@pytest.mark.asyncio
async def test_auth_user_management_endpoints(raw_client, session_factory):
    async with session_factory() as session:
        admin = await create_user(session, "adminuser", "adminPass", role=ROLE_ADMIN)
        operator = await create_user(session, "opuser", "opPass", role=ROLE_OPERATOR)
        await session.commit()
        admin_token = create_access_token(admin)
        op_token = create_access_token(operator)

    # 1. Admin can create a new user -> 201
    create_resp = await raw_client.post(
        "/auth/users",
        json={"username": "newworker", "password": "workerPassword123", "role": "operator"},
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert create_resp.status_code == 201
    created_data = create_resp.json()
    assert created_data["username"] == "newworker"
    assert created_data["role"] == "operator"
    assert "password_hash" not in created_data

    # 2. Operator CANNOT create a new user -> 403 Forbidden
    op_create = await raw_client.post(
        "/auth/users",
        json={"username": "hacker", "password": "hackerPassword", "role": "admin"},
        headers={"Authorization": f"Bearer {op_token}"},
    )
    assert op_create.status_code == 403

    # 3. Admin can list users -> 200
    list_resp = await raw_client.get(
        "/auth/users",
        headers={"Authorization": f"Bearer {admin_token}"},
    )
    assert list_resp.status_code == 200
    usernames = [u["username"] for u in list_resp.json()]
    assert "adminuser" in usernames
    assert "opuser" in usernames
    assert "newworker" in usernames
