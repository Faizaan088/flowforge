"""Centralized configuration and security validation for FlowForge."""

import os
from typing import List

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
TOKEN_EXPIRE_MINUTES = int(os.getenv("FLOWFORGE_TOKEN_EXPIRE_MINUTES", "60"))
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
    if secret.lower() in {s.lower() for s in INSECURE_SECRETS} or secret == DEV_DEFAULT_SECRET:
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
    if len(admin_pw) < 8:
        raise ValueError(
            "CRITICAL SECURITY CONFIGURATION ERROR: FLOWFORGE_ADMIN_PASSWORD must be at least 8 characters long in production."
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
