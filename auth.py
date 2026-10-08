"""Authentication and Role-Based Access Control (RBAC) service for FlowForge control-plane.

Provides:
- Cryptographically secure PBKDF2-HMAC-SHA256 password hashing with random salts.
- RFC 7519 compliant HS256 JWT tokens with signature validation and expiration.
- Database service layer for user management and authentication.
- Explicit RBAC roles (admin, operator, observer) and fine-grained permissions.
- FastAPI dependency injection guards for server-side authentication and authorization.
- Idempotent environment-driven admin bootstrapping.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

from fastapi import Depends, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db
from models import User

# ---------------------------------------------------------------------------
# Configuration & Constants
# ---------------------------------------------------------------------------
SECRET_KEY = os.getenv("FLOWFORGE_SECRET_KEY", "flowforge-dev-secret-key-change-in-production")
TOKEN_EXPIRE_MINUTES = int(os.getenv("FLOWFORGE_TOKEN_EXPIRE_MINUTES", "60"))
ADMIN_USERNAME = os.getenv("FLOWFORGE_ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("FLOWFORGE_ADMIN_PASSWORD")

ROLE_ADMIN = "admin"
ROLE_OPERATOR = "operator"
ROLE_OBSERVER = "observer"
VALID_ROLES = {ROLE_ADMIN, ROLE_OPERATOR, ROLE_OBSERVER}

ROLE_PERMISSIONS: dict[str, set[str]] = {
    ROLE_ADMIN: {
        "executions:read",
        "executions:trigger",
        "executions:cancel",
        "executions:priority",
        "workflows:read",
        "workflows:manage",
        "policies:read",
        "policies:manage",
        "system:reconcile",
        "users:manage",
    },
    ROLE_OPERATOR: {
        "executions:read",
        "executions:trigger",
        "executions:cancel",
        "executions:priority",
        "workflows:read",
        "workflows:manage",
        "policies:read",
    },
    ROLE_OBSERVER: {
        "executions:read",
        "workflows:read",
        "policies:read",
    },
}


class AuthError(Exception):
    """Base exception for authentication errors."""


class UserAlreadyExistsError(AuthError):
    """Raised when attempting to create a user with an existing username or email."""


# ---------------------------------------------------------------------------
# Pydantic Schemas
# ---------------------------------------------------------------------------
class UserResponse(BaseModel):
    id: int
    username: str
    email: Optional[str] = None
    role: str
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class UserCreate(BaseModel):
    username: str = Field(..., min_length=1, max_length=100)
    password: str = Field(..., min_length=4)
    email: Optional[str] = None
    role: str = ROLE_OBSERVER


class LoginRequest(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


# ---------------------------------------------------------------------------
# Password Hashing & Verification
# ---------------------------------------------------------------------------
def hash_password(password: str) -> str:
    """Hash a plaintext password using PBKDF2-HMAC-SHA256 with a cryptographically secure salt."""
    salt = secrets.token_bytes(16)
    iterations = 100_000
    derived_key = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2:sha256:{iterations}${salt.hex()}${derived_key.hex()}"


def verify_password(plain_password: str, password_hash: str) -> bool:
    """Verify a plaintext password against a stored PBKDF2 hash using constant-time comparison."""
    if not password_hash or not plain_password:
        return False
    try:
        header, salt_hex, key_hex = password_hash.split("$")
        parts = header.split(":")
        if len(parts) != 3 or parts[0] != "pbkdf2" or parts[1] != "sha256":
            return False
        iterations = int(parts[2])
        salt = bytes.fromhex(salt_hex)
        expected_key = bytes.fromhex(key_hex)
        actual_key = hashlib.pbkdf2_hmac("sha256", plain_password.encode("utf-8"), salt, iterations)
        return hmac.compare_digest(actual_key, expected_key)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Token Generation & Verification (RFC 7519 HS256 JWT)
# ---------------------------------------------------------------------------
def _base64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _base64url_decode(data_str: str) -> bytes:
    padding = len(data_str) % 4
    if padding:
        data_str += "=" * (4 - padding)
    return base64.urlsafe_b64decode(data_str)


def create_access_token(
    user: User,
    expires_delta: Optional[timedelta] = None,
    secret_key: Optional[str] = None,
) -> str:
    """Generate a signed HS256 JWT access token for the given user."""
    key = secret_key or SECRET_KEY
    now = datetime.now(timezone.utc)
    expires = now + (expires_delta or timedelta(minutes=TOKEN_EXPIRE_MINUTES))

    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "sub": str(user.id),
        "username": user.username,
        "role": user.role,
        "iat": int(now.timestamp()),
        "exp": int(expires.timestamp()),
    }

    header_bytes = json.dumps(header, separators=(",", ":")).encode("utf-8")
    payload_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    header_b64 = _base64url_encode(header_bytes)
    payload_b64 = _base64url_encode(payload_bytes)
    signing_input = f"{header_b64}.{payload_b64}"

    signature = hmac.new(key.encode("utf-8"), signing_input.encode("utf-8"), hashlib.sha256).digest()
    sig_b64 = _base64url_encode(signature)

    return f"{signing_input}.{sig_b64}"


def decode_access_token(token: str, secret_key: Optional[str] = None) -> dict[str, Any]:
    """Validate and decode an HS256 JWT access token.

    Raises HTTPException(401) on invalid signature, malformed token, or expiration.
    """
    key = secret_key or SECRET_KEY
    parts = token.split(".")
    if len(parts) != 3:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Malformed authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    header_b64, payload_b64, sig_b64 = parts
    signing_input = f"{header_b64}.{payload_b64}"

    try:
        expected_sig = hmac.new(key.encode("utf-8"), signing_input.encode("utf-8"), hashlib.sha256).digest()
        actual_sig = _base64url_decode(sig_b64)
        if not hmac.compare_digest(expected_sig, actual_sig):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication token signature",
                headers={"WWW-Authenticate": "Bearer"},
            )
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token format",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        header_json = json.loads(_base64url_decode(header_b64).decode("utf-8"))
        if header_json.get("alg") != "HS256":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Unsupported token algorithm",
                headers={"WWW-Authenticate": "Bearer"},
            )

        payload = json.loads(_base64url_decode(payload_b64).decode("utf-8"))
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token payload",
            headers={"WWW-Authenticate": "Bearer"},
        )

    now_ts = int(datetime.now(timezone.utc).timestamp())
    exp_ts = payload.get("exp")
    if exp_ts is None or exp_ts < now_ts:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return payload


# ---------------------------------------------------------------------------
# Database Service Layer
# ---------------------------------------------------------------------------
async def get_user_by_id(session: AsyncSession, user_id: int) -> Optional[User]:
    """Retrieve a user by primary key ID."""
    return await session.get(User, user_id)


async def get_user_by_username(session: AsyncSession, username: str) -> Optional[User]:
    """Retrieve a user by unique username."""
    stmt = select(User).where(User.username == username.strip())
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def list_users(session: AsyncSession) -> list[User]:
    """List all registered users ordered by ID."""
    stmt = select(User).order_by(User.id.asc())
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def create_user(
    session: AsyncSession,
    username: str,
    password: str,
    role: str = ROLE_OBSERVER,
    email: Optional[str] = None,
    is_active: bool = True,
) -> User:
    """Create a new user with hashed password and role validation."""
    clean_username = username.strip()
    if not clean_username:
        raise ValueError("Username cannot be empty")

    clean_role = role.strip().lower()
    if clean_role not in VALID_ROLES:
        raise ValueError(f"Invalid role '{role}'. Must be one of: {sorted(VALID_ROLES)}")

    clean_email = email.strip() if email else None

    # Check username uniqueness
    existing_user = await get_user_by_username(session, clean_username)
    if existing_user is not None:
        raise UserAlreadyExistsError(f"Username '{clean_username}' is already taken")

    # Check email uniqueness if provided
    if clean_email:
        stmt = select(User).where(User.email == clean_email)
        existing_email = (await session.execute(stmt)).scalar_one_or_none()
        if existing_email is not None:
            raise UserAlreadyExistsError(f"Email '{clean_email}' is already registered")

    user = User(
        username=clean_username,
        email=clean_email,
        password_hash=hash_password(password),
        role=clean_role,
        is_active=is_active,
    )
    session.add(user)
    await session.flush()
    return user


async def authenticate_user(
    session: AsyncSession,
    username: str,
    password: str,
) -> Optional[User]:
    """Authenticate credentials against durable PostgreSQL storage."""
    user = await get_user_by_username(session, username)
    if user is None:
        return None
    if not verify_password(password, user.password_hash):
        return None
    return user


async def update_user(
    session: AsyncSession,
    user_id: int,
    role: Optional[str] = None,
    is_active: Optional[bool] = None,
    password: Optional[str] = None,
) -> Optional[User]:
    """Update user role, active status, or password."""
    user = await session.get(User, user_id)
    if user is None:
        return None

    if role is not None:
        clean_role = role.strip().lower()
        if clean_role not in VALID_ROLES:
            raise ValueError(f"Invalid role '{role}'")
        user.role = clean_role

    if is_active is not None:
        user.is_active = is_active

    if password is not None:
        user.password_hash = hash_password(password)

    await session.flush()
    return user


async def bootstrap_admin(session: AsyncSession) -> Optional[User]:
    """Idempotently bootstrap the initial administrator account if configured.

    Reads from FLOWFORGE_ADMIN_USERNAME and FLOWFORGE_ADMIN_PASSWORD environment variables.
    If FLOWFORGE_ADMIN_PASSWORD is not set, bootstrapping is skipped to prevent insecure defaults.
    """
    admin_user = os.getenv("FLOWFORGE_ADMIN_USERNAME", ADMIN_USERNAME)
    admin_pass = os.getenv("FLOWFORGE_ADMIN_PASSWORD", ADMIN_PASSWORD)

    if not admin_pass:
        return None

    user = await get_user_by_username(session, admin_user)
    if user is None:
        user = User(
            username=admin_user,
            email=None,
            password_hash=hash_password(admin_pass),
            role=ROLE_ADMIN,
            is_active=True,
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)
        print(f"Bootstrapped administrator account '{admin_user}'")
        return user
    else:
        # Ensure role is admin and active
        updated = False
        if user.role != ROLE_ADMIN:
            user.role = ROLE_ADMIN
            updated = True
        if not user.is_active:
            user.is_active = True
            updated = True
        if updated:
            await session.commit()
            await session.refresh(user)
        return user


# ---------------------------------------------------------------------------
# FastAPI Authorization Dependencies
# ---------------------------------------------------------------------------
async def get_current_user(
    request: Request,
    db: AsyncSession = Depends(get_db),
) -> User:
    """Extract and authenticate the principal from the Authorization header.

    Returns the verified User model. Raises HTTP 401 on missing/invalid/expired token or disabled user.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    parts = auth_header.strip().split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization scheme: Bearer token required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = parts[1]
    payload = decode_access_token(token)

    user_id_str = payload.get("sub")
    if not user_id_str:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token claims",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        user_id = int(user_id_str)
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid user identity in token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user = await get_user_by_id(db, user_id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User not found",
            headers={"WWW-Authenticate": "Bearer"},
        )

    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User account is disabled",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user


def require_permission(permission: str) -> Callable[[User], User]:
    """Factory creating an endpoint dependency that enforces fine-grained RBAC permission."""

    async def _permission_dependency(
        current_user: User = Depends(get_current_user),
    ) -> User:
        user_permissions = ROLE_PERMISSIONS.get(current_user.role, set())
        if permission not in user_permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Forbidden: Insufficient permissions for action '{permission}'",
            )
        return current_user

    return _permission_dependency


def require_role(*roles: str) -> Callable[[User], User]:
    """Factory creating an endpoint dependency that enforces explicit role membership."""

    async def _role_dependency(
        current_user: User = Depends(get_current_user),
    ) -> User:
        if current_user.role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Forbidden: Requires one of roles {roles}",
            )
        return current_user

    return _role_dependency
