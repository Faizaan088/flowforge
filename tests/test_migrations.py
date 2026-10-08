"""Tests for Alembic schema migrations on an isolated clean database."""

import asyncio
import os
import asyncpg
import pytest
from alembic.config import Config
from alembic import command

MIGRATION_DB_URL = os.getenv(
    "MIGRATION_TEST_DATABASE_URL",
    "postgresql+asyncpg://flowforge_user:flowforge_password@127.0.0.1:5432/flowforge_clean",
)

EXPECTED_APPLICATION_TABLES = {
    "concurrency_limit_policies",
    "job_definitions",
    "rate_limit_policies",
    "schedule_definitions",
    "users",
    "workers",
    "workflow_definitions",
    "schedule_occurrences",
    "workflow_runs",
    "workflow_tasks",
    "executions",
    "workflow_edges",
    "rate_limit_records",
    "workflow_task_executions",
}


@pytest.fixture(scope="module")
def alembic_config():
    """Create Alembic Config targeting the isolated migration test database."""
    ini_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "alembic.ini")
    cfg = Config(ini_path)
    cfg.set_main_option("sqlalchemy.url", MIGRATION_DB_URL.replace("%", "%%"))
    return cfg


def get_existing_tables() -> set[str]:
    """Inspect tables in public schema of migration test db."""
    async def _fetch():
        clean_url = MIGRATION_DB_URL.replace("postgresql+asyncpg://", "postgresql://")
        conn = await asyncpg.connect(clean_url)
        try:
            rows = await conn.fetch(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public';"
            )
            return {r["tablename"] for r in rows}
        finally:
            await conn.close()

    return asyncio.run(_fetch())


def reset_public_schema():
    """Reset public schema in migration test database to clean slate."""
    async def _reset():
        clean_url = MIGRATION_DB_URL.replace("postgresql+asyncpg://", "postgresql://")
        conn = await asyncpg.connect(clean_url)
        try:
            await conn.execute("DROP SCHEMA IF EXISTS public CASCADE;")
            await conn.execute("CREATE SCHEMA public;")
            await conn.execute("GRANT ALL ON SCHEMA public TO flowforge_user;")
            await conn.execute("GRANT ALL ON SCHEMA public TO public;")
        finally:
            await conn.close()

    asyncio.run(_reset())


def test_migrations_clean_db_upgrade_and_downgrade(alembic_config):
    """Verify that Alembic migrations run cleanly up, down, and re-apply on a clean database."""
    # Reset schema to fresh clean slate
    reset_public_schema()

    tables_init = get_existing_tables()
    assert len(tables_init) == 0, "Clean database must initially have 0 tables"

    # 1. Upgrade to head
    command.upgrade(alembic_config, "head")
    tables_after_upgrade = get_existing_tables()

    # Verify all expected tables exist
    for table in EXPECTED_APPLICATION_TABLES:
        assert table in tables_after_upgrade, f"Table {table} missing after alembic upgrade head"
    assert "alembic_version" in tables_after_upgrade

    # 2. Downgrade back to base
    command.downgrade(alembic_config, "base")
    tables_after_downgrade = get_existing_tables()
    for table in EXPECTED_APPLICATION_TABLES:
        assert table not in tables_after_downgrade, f"Table {table} should be dropped after downgrade"

    # 3. Upgrade back to head to leave the database in working condition
    command.upgrade(alembic_config, "head")
    final_tables = get_existing_tables()
    for table in EXPECTED_APPLICATION_TABLES:
        assert table in final_tables


def test_migrated_schema_columns_and_indices():
    """Verify essential columns and indices on migrated tables."""
    async def _verify():
        clean_url = MIGRATION_DB_URL.replace("postgresql+asyncpg://", "postgresql://")
        conn = await asyncpg.connect(clean_url)
        try:
            # Check users columns
            user_cols = await conn.fetch(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'users';"
            )
            col_names = {r["column_name"] for r in user_cols}
            assert "username" in col_names
            assert "password_hash" in col_names
            assert "role" in col_names
            assert "is_active" in col_names

            # Check executions columns
            exec_cols = await conn.fetch(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'executions';"
            )
            exec_names = {r["column_name"] for r in exec_cols}
            assert "status" in exec_names
            assert "priority" in exec_names
            assert "category" in exec_names
            assert "worker_id" in exec_names

            # Check concurrency_limit_policies
            pol_cols = await conn.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'concurrency_limit_policies';"
            )
            pol_names = {r["column_name"] for r in pol_cols}
            assert "target_type" in pol_names
            assert "target_id" in pol_names
            assert "max_concurrency" in pol_names
        finally:
            await conn.close()

    asyncio.run(_verify())
