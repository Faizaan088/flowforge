#!/usr/bin/env python3
"""FlowForge Administrator Password Recovery Script.

This local administrative tool allows system administrators to securely reset
the password of an existing administrator account directly in the database.

Why changing FLOWFORGE_ADMIN_PASSWORD in .env does not change existing accounts:
FlowForge's administrator bootstrap logic (in auth.py:bootstrap_admin) is create-only.
It automatically seeds the initial admin user only if that user does not already exist
in the database. Once created, FlowForge intentionally never overwrites administrator
passwords on restart to prevent accidental privilege rollbacks or credential overrides.
Therefore, resetting an existing administrator password must be done either through
authenticated control-plane endpoints or using this direct administrative CLI script.

Usage:
    # 1. Identify existing administrator accounts safely without exposing secrets:
    python scripts/reset_admin_password.py --list-admins

    # 2. Reset the password for an existing administrator account interactively:
    python scripts/reset_admin_password.py --username admin

    # 3. Non-interactive reset (e.g. CI/CD or automation):
    $env:FLOWFORGE_NEW_ADMIN_PASSWORD="MyNewSecurePassword123!"
    python scripts/reset_admin_password.py --username admin --env-pass
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import os
import sys
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker

from models import User, AuditEvent
from auth import hash_password, ROLE_ADMIN
from audit import record_audit_event


def get_db_url() -> str:
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        db_url = "postgresql+asyncpg://flowforge_user:flowforge_password@localhost:5432/flowforge"
    return db_url


async def list_admin_accounts(session: AsyncSession) -> list[dict]:
    """Query and return existing administrator accounts without exposing any credentials."""
    stmt = select(User).where(User.role == ROLE_ADMIN).order_by(User.id.asc())
    result = await session.execute(stmt)
    admins = result.scalars().all()
    return [
        {
            "id": u.id,
            "username": u.username,
            "email": u.email,
            "is_active": u.is_active,
            "created_at": u.created_at.isoformat() if u.created_at else None,
        }
        for u in admins
    ]


async def reset_admin_password(
    session: AsyncSession,
    username: str,
    new_password: str,
    actor_id: str = "system:cli",
) -> User:
    """Reset the password for an existing administrator account.

    Validates:
    - Target user exists in the database.
    - Target user has the 'admin' role.
    - New password meets length requirements (12 to 128 characters).

    Updates account in place, sets active, and records an audit log event
    without storing the plaintext password or password hash in logs.
    """
    clean_username = username.strip()
    if not clean_username:
        raise ValueError("Target username cannot be empty.")

    if len(new_password) < 12 or len(new_password) > 128:
        raise ValueError("Password must be between 12 and 128 characters long.")

    stmt = select(User).where(User.username == clean_username)
    result = await session.execute(stmt)
    user = result.scalar_one_or_none()

    if user is None:
        raise ValueError(f"User '{clean_username}' was not found in the database.")

    if user.role != ROLE_ADMIN:
        raise ValueError(
            f"User '{clean_username}' has role '{user.role}', not '{ROLE_ADMIN}'. "
            "This administrative tool only resets accounts with the administrator role."
        )

    # Hash using PBKDF2-HMAC-SHA256 with cryptographically secure random salt
    user.password_hash = hash_password(new_password)
    user.is_active = True

    # Record recovery action in audit log (never logs password or hash)
    await record_audit_event(
        session,
        actor_id=actor_id,
        action="ADMIN_PASSWORD_RESET",
        target_type="USER",
        target_id=str(user.id),
        metadata={
            "username": user.username,
            "reset_method": "local_admin_cli",
        },
    )

    await session.commit()
    await session.refresh(user)
    return user


async def async_main(args: argparse.Namespace) -> int:
    db_url = get_db_url()
    engine = create_async_engine(db_url, echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    try:
        async with session_factory() as session:
            if args.list_admins:
                configured_bootstrap = os.getenv("FLOWFORGE_ADMIN_USERNAME", "admin")
                print("=" * 60)
                print("FlowForge Administrator Accounts")
                print("=" * 60)
                print(f"Configured bootstrap username in environment: {configured_bootstrap}")
                print("-" * 60)

                admins = await list_admin_accounts(session)
                if not admins:
                    print("No administrator accounts currently found in the database.")
                    print("Start the backend application with FLOWFORGE_ADMIN_USERNAME and")
                    print("FLOWFORGE_ADMIN_PASSWORD set in .env to bootstrap the initial admin.")
                else:
                    for a in admins:
                        status_str = "ACTIVE" if a["is_active"] else "DISABLED"
                        email_str = f" <{a['email']}>" if a["email"] else ""
                        print(f"  - ID: {a['id']:<4} Username: {a['username']:<20}{email_str:<25} Status: {status_str}")
                print("=" * 60)
                return 0

            if not args.username:
                print("Error: Target administrator username is required (use --username <username>).", file=sys.stderr)
                print("Run with --list-admins to view available administrator accounts.", file=sys.stderr)
                return 1

            # Get new password securely
            new_pass = ""
            if args.env_pass:
                new_pass = os.getenv("FLOWFORGE_NEW_ADMIN_PASSWORD", "")
                if not new_pass:
                    print("Error: FLOWFORGE_NEW_ADMIN_PASSWORD environment variable is not set.", file=sys.stderr)
                    return 1
            elif args.stdin:
                new_pass = sys.stdin.readline().strip()
            elif args.password:
                # Provided via argument (warn about history)
                new_pass = args.password
            else:
                # Interactive secure prompt
                print(f"Resetting password for administrator: '{args.username}'")
                print("Note: Password must be between 12 and 128 characters.")
                p1 = getpass.getpass("Enter new password: ")
                if not p1:
                    print("Error: Password cannot be empty.", file=sys.stderr)
                    return 1
                p2 = getpass.getpass("Confirm new password: ")
                if p1 != p2:
                    print("Error: Passwords do not match.", file=sys.stderr)
                    return 1
                new_pass = p1

            try:
                user = await reset_admin_password(session, args.username, new_pass)
                print("=" * 60)
                print(f"SUCCESS: Administrator '{user.username}' password has been updated.")
                print(f"Account ID: #{user.id} | Role: {user.role} | Status: {'Active' if user.is_active else 'Disabled'}")
                print("An audit record (ADMIN_PASSWORD_RESET) has been logged.")
                print("You can now log in at https://flowforge-098.vercel.app or local frontend.")
                print("=" * 60)
                return 0
            except ValueError as err:
                print(f"ERROR: {err}", file=sys.stderr)
                return 1

    finally:
        await engine.dispose()


def main():
    parser = argparse.ArgumentParser(
        description="FlowForge Administrator Password Recovery Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Important Information on Initial Admin Password:
  Changing FLOWFORGE_ADMIN_PASSWORD in .env only takes effect if the administrator
  account does not yet exist in the database (bootstrap is create-only).
  To reset the password of an existing administrator account, use this script.
        """,
    )
    parser.add_argument(
        "--username",
        "-u",
        help="Target administrator username to reset.",
    )
    parser.add_argument(
        "--list-admins",
        "-l",
        action="store_true",
        help="List existing administrator usernames without revealing secrets.",
    )
    parser.add_argument(
        "--password",
        "-p",
        help="New password (warning: may appear in shell history; prefer interactive prompt or --env-pass).",
    )
    parser.add_argument(
        "--env-pass",
        action="store_true",
        help="Read new password from FLOWFORGE_NEW_ADMIN_PASSWORD environment variable.",
    )
    parser.add_argument(
        "--stdin",
        action="store_true",
        help="Read new password from standard input (first line).",
    )

    args = parser.parse_args()
    sys.exit(asyncio.run(async_main(args)))


if __name__ == "__main__":
    main()
