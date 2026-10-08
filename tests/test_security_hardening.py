"""Tests for production security hardening, configuration validation, and CORS restrictions."""

import os
from unittest.mock import patch
import pytest

from config import (
    DEFAULT_DEV_CORS_ORIGINS,
    DEV_DEFAULT_SECRET,
    get_cors_origins,
    validate_production_configuration,
)


def test_dev_config_validation_succeeds_with_defaults():
    """In development mode, validation succeeds without requiring strict production secrets."""
    with patch.dict(os.environ, {"ENVIRONMENT": "development"}, clear=False):
        validate_production_configuration()  # Must not raise


def test_production_validation_rejects_missing_secret_key():
    """In production mode, missing secret key raises ValueError."""
    with patch.dict(
        os.environ,
        {
            "ENVIRONMENT": "production",
            "FLOWFORGE_SECRET_KEY": "",
            "FLOWFORGE_ADMIN_PASSWORD": "ValidPassword123!",
            "CORS_ALLOWED_ORIGINS": "https://controlplane.example.com",
            "DATABASE_URL": "postgresql+asyncpg://user:pass@localhost:5432/db",
        },
        clear=False,
    ):
        with pytest.raises(ValueError, match="FLOWFORGE_SECRET_KEY is required"):
            validate_production_configuration()


def test_production_validation_rejects_default_secret_key():
    """In production mode, default development secret key is rejected."""
    with patch.dict(
        os.environ,
        {
            "ENVIRONMENT": "production",
            "FLOWFORGE_SECRET_KEY": DEV_DEFAULT_SECRET,
            "FLOWFORGE_ADMIN_PASSWORD": "ValidPassword123!",
            "CORS_ALLOWED_ORIGINS": "https://controlplane.example.com",
            "DATABASE_URL": "postgresql+asyncpg://user:pass@localhost:5432/db",
        },
        clear=False,
    ):
        with pytest.raises(ValueError, match="cannot use default or insecure value"):
            validate_production_configuration()


def test_production_validation_rejects_short_secret_key():
    """In production mode, secret key with fewer than 32 characters is rejected."""
    with patch.dict(
        os.environ,
        {
            "ENVIRONMENT": "production",
            "FLOWFORGE_SECRET_KEY": "too-short-secret-key-12345",
            "FLOWFORGE_ADMIN_PASSWORD": "ValidPassword123!",
            "CORS_ALLOWED_ORIGINS": "https://controlplane.example.com",
            "DATABASE_URL": "postgresql+asyncpg://user:pass@localhost:5432/db",
        },
        clear=False,
    ):
        with pytest.raises(ValueError, match="at least 32 characters long"):
            validate_production_configuration()


def test_production_validation_rejects_missing_admin_password():
    """In production mode, missing admin password is rejected."""
    with patch.dict(
        os.environ,
        {
            "ENVIRONMENT": "production",
            "FLOWFORGE_SECRET_KEY": "a" * 32,
            "FLOWFORGE_ADMIN_PASSWORD": "",
            "CORS_ALLOWED_ORIGINS": "https://controlplane.example.com",
            "DATABASE_URL": "postgresql+asyncpg://user:pass@localhost:5432/db",
        },
        clear=False,
    ):
        with pytest.raises(ValueError, match="FLOWFORGE_ADMIN_PASSWORD must be configured"):
            validate_production_configuration()


def test_production_validation_rejects_short_admin_password():
    """In production mode, admin password under 8 characters is rejected."""
    with patch.dict(
        os.environ,
        {
            "ENVIRONMENT": "production",
            "FLOWFORGE_SECRET_KEY": "a" * 32,
            "FLOWFORGE_ADMIN_PASSWORD": "short",
            "CORS_ALLOWED_ORIGINS": "https://controlplane.example.com",
            "DATABASE_URL": "postgresql+asyncpg://user:pass@localhost:5432/db",
        },
        clear=False,
    ):
        with pytest.raises(ValueError, match="at least 8 characters long"):
            validate_production_configuration()


def test_production_validation_rejects_wildcard_cors():
    """In production mode, wildcard '*' CORS origin is strictly prohibited."""
    with patch.dict(
        os.environ,
        {
            "ENVIRONMENT": "production",
            "FLOWFORGE_SECRET_KEY": "a" * 32,
            "FLOWFORGE_ADMIN_PASSWORD": "ValidPassword123!",
            "CORS_ALLOWED_ORIGINS": "*",
            "DATABASE_URL": "postgresql+asyncpg://user:pass@localhost:5432/db",
        },
        clear=False,
    ):
        with pytest.raises(ValueError, match="Wildcard '\\*' CORS origin is strictly prohibited"):
            validate_production_configuration()


def test_production_validation_rejects_missing_cors_in_prod():
    """In production mode, missing CORS origins configuration is rejected."""
    env = {
        "ENVIRONMENT": "production",
        "FLOWFORGE_SECRET_KEY": "a" * 32,
        "FLOWFORGE_ADMIN_PASSWORD": "ValidPassword123!",
        "DATABASE_URL": "postgresql+asyncpg://user:pass@localhost:5432/db",
    }
    # Clear any CORS origins from environment
    with patch.dict(os.environ, env, clear=True):
        with pytest.raises(ValueError, match="Explicit CORS_ALLOWED_ORIGINS must be configured"):
            validate_production_configuration()


def test_production_validation_succeeds_with_valid_settings():
    """In production mode, properly configured settings pass validation."""
    with patch.dict(
        os.environ,
        {
            "ENVIRONMENT": "production",
            "FLOWFORGE_SECRET_KEY": "secure_production_secret_key_with_at_least_32_characters",
            "FLOWFORGE_ADMIN_PASSWORD": "SecureAdminPassword2026!",
            "CORS_ALLOWED_ORIGINS": "https://dashboard.example.com,https://app.example.com",
            "DATABASE_URL": "postgresql+asyncpg://flowforge_user:secret@postgres:5432/flowforge",
        },
        clear=False,
    ):
        validate_production_configuration()  # Must not raise


def test_cors_origins_dev_defaults():
    """In development without CORS env vars, safe dev origins are returned."""
    env = {"ENVIRONMENT": "development"}
    with patch.dict(os.environ, env, clear=True):
        origins = get_cors_origins()
        assert "http://localhost:5173" in origins
        assert "http://localhost:8000" in origins
        assert "*" not in origins


def test_cors_origins_custom_list():
    """Comma-separated CORS origins are correctly parsed."""
    with patch.dict(
        os.environ,
        {"CORS_ALLOWED_ORIGINS": "https://a.com, https://b.com ,https://c.com"},
        clear=False,
    ):
        origins = get_cors_origins()
        assert origins == ["https://a.com", "https://b.com", "https://c.com"]
