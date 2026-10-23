"""Centralized configuration and security validation for FlowForge."""

import os
import re
from urllib.parse import urlparse
from typing import List, Any


def safe_int(value: Any, default: int) -> int:
    """Safely parse an integer from environment variables or strings, gracefully handling decimal strings and errors."""
    if value is None:
        return default
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return default


def safe_float(value: Any, default: float) -> float:
    """Safely parse a float from environment variables or strings, gracefully handling errors."""
    if value is None:
        return default
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return default

# Dev fallback secret (strictly forbidden in production)
DEV_DEFAULT_SECRET = "flowforge-dev-secret-key-change-in-production"
INSECURE_SECRETS = {
    DEV_DEFAULT_SECRET,
    "secret",
    "changeme",
    "admin",
    "password",
    "123456",
    "flowforge",
}

# Authentication & JWT configuration
SECRET_KEY = os.getenv("FLOWFORGE_SECRET_KEY", DEV_DEFAULT_SECRET)
TOKEN_EXPIRE_MINUTES = safe_int(os.getenv("FLOWFORGE_TOKEN_EXPIRE_MINUTES", "60"), 60)
ADMIN_USERNAME = os.getenv("FLOWFORGE_ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("FLOWFORGE_ADMIN_PASSWORD")

# Safe default CORS origins for local development
DEFAULT_DEV_CORS_ORIGINS = [
    "http://localhost:5173",
    "http://localhost:5174",
    "http://localhost:3000",
    "http://localhost:8000",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:5174",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:8000",
]


def get_environment() -> str:
    """Return the active environment name in lowercase."""
    return os.getenv("ENVIRONMENT", os.getenv("FLOWFORGE_ENV", "development")).lower()


def is_production() -> bool:
    """Return True if running in production mode."""
    return get_environment() == "production"


def get_cors_origins() -> List[str]:
    """Retrieve allowed CORS origins from environment or fallback to safe defaults.

    In production, explicit origins MUST be configured via CORS_ALLOWED_ORIGINS / CORS_ORIGINS.
    """
    raw_origins = os.getenv("CORS_ALLOWED_ORIGINS") or os.getenv("CORS_ORIGINS")
    if raw_origins:
        origins = [orig.strip() for orig in raw_origins.split(",") if orig.strip()]
        return origins

    if is_production():
        return []

    return list(DEFAULT_DEV_CORS_ORIGINS)


def validate_production_configuration() -> None:
    """Validate critical security settings at startup.

    Raises ValueError if production mode is active and any insecure setting is detected.
    """
    if not is_production():
        return

    # 1. Validate Secret Key
    secret = os.getenv("FLOWFORGE_SECRET_KEY", "")
    if not secret:
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: FLOWFORGE_SECRET_KEY is required in production."
        )
    if (
        secret.lower() in {s.lower() for s in INSECURE_SECRETS}
        or secret == DEV_DEFAULT_SECRET
        or secret.lower().startswith(("replace-with", "change-me", "changeme", "your-secret"))
    ):
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: FLOWFORGE_SECRET_KEY cannot use default or insecure value in production."
        )
    if len(secret) < 32:
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: FLOWFORGE_SECRET_KEY must be at least 32 characters long in production."
        )

    # 2. Validate Admin Password
    admin_pw = os.getenv("FLOWFORGE_ADMIN_PASSWORD", "")
    if not admin_pw:
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: FLOWFORGE_ADMIN_PASSWORD must be configured in production."
        )
    if len(admin_pw) < 12 or len(admin_pw) > 128:
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: FLOWFORGE_ADMIN_PASSWORD must be between 12 and 128 characters in production."
        )
    if admin_pw.casefold() == os.getenv("FLOWFORGE_ADMIN_USERNAME", "admin").casefold():
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: administrator password must differ from the username."
        )
    if admin_pw.lower().startswith(("replace-with", "change-me", "changeme", "your-password")):
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: FLOWFORGE_ADMIN_PASSWORD cannot use a template placeholder in production."
        )

    # 3. Validate CORS
    origins = get_cors_origins()
    if not origins:
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: Explicit CORS_ALLOWED_ORIGINS must be configured in production."
        )
    if "*" in origins:
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: Wildcard '*' CORS origin is strictly prohibited in production."
        )

    # 4. Validate Database URL
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: DATABASE_URL must be configured in production."
        )
    parsed_db = urlparse(db_url)
    if parsed_db.scheme not in {"postgresql+asyncpg", "postgresql"} or not parsed_db.hostname:
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: DATABASE_URL must be a valid PostgreSQL URL in production."
        )
    if not parsed_db.password or parsed_db.password == "flowforge_password":
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: a unique database password is required in production."
        )
    # Compose interpolates this value into DATABASE_URL; constrain it to URL-safe
    # characters so reserved characters cannot corrupt credentials in the URL.
    postgres_password = os.getenv("POSTGRES_PASSWORD") or (parsed_db.password or "")
    if (
        len(postgres_password) < 24
        or not re.fullmatch(r"[A-Za-z0-9_-]+", postgres_password)
        or postgres_password.lower().startswith(("replace-with", "change-me", "changeme", "your-password"))
    ):
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: POSTGRES_PASSWORD must be at least 24 URL-safe characters in production."
        )

    # 5. Validate Redis Configuration
    redis_url = os.getenv("REDIS_URL", "")
    require_redis_auth = os.getenv("REQUIRE_REDIS_AUTH", "false").lower() in ("true", "1", "yes")
    if not require_redis_auth or not redis_url:
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: authenticated REDIS_URL is required in production."
        )
    parsed_redis = urlparse(redis_url)
    if parsed_redis.scheme != "redis" or not parsed_redis.hostname or not parsed_redis.password:
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: REDIS_URL must be a valid authenticated Redis URL in production."
        )
    redis_password = os.getenv("REDIS_PASSWORD") or (parsed_redis.password or "")
    if (
        len(redis_password) < 32
        or not re.fullmatch(r"[A-Za-z0-9_-]+", redis_password)
        or redis_password.lower().startswith(("replace-with", "change-me", "changeme", "your-password"))
    ):
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: REDIS_PASSWORD must be at least 32 URL-safe characters in production."
        )
    if any(origin.startswith("http://") for origin in origins):
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: production CORS origins must use HTTPS."
        )
