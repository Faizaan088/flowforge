#!/usr/bin/env python3
"""Automated PostgreSQL restore script for FlowForge.

Supports:
- Verification of SHA-256 checksum
- Uncompressing .sql.gz or streaming uncompressed .sql
- Execution via host psql or docker compose exec postgres psql
- Safety verification prompt (bypassable with --yes)
"""

import argparse
import gzip
import hashlib
import os
import shutil
import subprocess
import sys
from pathlib import Path


def compute_sha256(filepath: Path) -> str:
    sha = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            sha.update(chunk)
    return sha.hexdigest()


def find_psql_tool() -> tuple[str, list[str]]:
    """Determine whether to use native psql or docker compose."""
    if shutil.which("psql"):
        return "native", ["psql"]
    
    if shutil.which("docker"):
        try:
            res = subprocess.run(
                ["docker", "compose", "ps", "--services"],
                capture_output=True,
                text=True,
                check=False,
            )
            if "postgres" in res.stdout:
                return "docker", ["docker", "compose", "exec", "-T", "postgres", "psql"]
        except Exception:
            pass
            
    return "unknown", []


def run_restore(
    backup_path: Path,
    db_name: str = "flowforge",
    db_user: str = "flowforge_user",
    db_host: str = "localhost",
    db_port: str = "5432",
    skip_checksum: bool = False,
    auto_confirm: bool = False,
):
    if not backup_path.exists():
        raise FileNotFoundError(f"Backup file not found: {backup_path}")

    # Verify Checksum if present
    checksum_file = backup_path.parent / f"{backup_path.name}.sha256"
    if checksum_file.exists() and not skip_checksum:
        print("[*] Verifying SHA-256 checksum...")
        expected = checksum_file.read_text(encoding="utf-8").split()[0].strip()
        actual = compute_sha256(backup_path)
        if actual.lower() != expected.lower():
            raise ValueError(f"Checksum mismatch! Expected: {expected}, Actual: {actual}")
        print("[+] Checksum verified successfully.")

    if not auto_confirm:
        confirm = input(f"[!] WARNING: This will overwrite data in database '{db_name}'. Continue? [y/N]: ")
        if confirm.strip().lower() not in ("y", "yes"):
            print("[-] Restore aborted by user.")
            sys.exit(0)

    tool_type, cmd_prefix = find_psql_tool()
    if tool_type == "unknown":
        raise RuntimeError("Neither local 'psql' nor Docker Compose 'postgres' service was found.")

    print(f"[*] Starting FlowForge database restore via [{tool_type}] into '{db_name}'...")
    env = os.environ.copy()
    if "POSTGRES_PASSWORD" in env and "PGPASSWORD" not in env:
        env["PGPASSWORD"] = env["POSTGRES_PASSWORD"]

    if tool_type == "native":
        cmd = cmd_prefix + [
            "-h", db_host,
            "-p", str(db_port),
            "-U", db_user,
            "-d", db_name,
        ]
    else:
        cmd = cmd_prefix + [
            "-U", db_user,
            "-d", db_name,
        ]

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )

    if backup_path.suffix == ".gz":
        with gzip.open(backup_path, "rb") as gz_in:
            shutil.copyfileobj(gz_in, proc.stdin)
    else:
        with open(backup_path, "rb") as f_in:
            shutil.copyfileobj(f_in, proc.stdin)

    proc.stdin.close()
    stdout_data, stderr_data = proc.communicate()

    if proc.returncode != 0:
        raise RuntimeError(f"psql restore failed with code {proc.returncode}: {stderr_data.decode('utf-8', errors='replace')}")

    print(f"[+] Database '{db_name}' restored successfully from {backup_path.name}!")


def main():
    parser = argparse.ArgumentParser(description="FlowForge PostgreSQL Database Restore Tool")
    parser.add_argument("backup_file", type=Path, help="Path to the backup file (.sql.gz or .sql)")
    parser.add_argument("--db-name", default=os.getenv("POSTGRES_DB", "flowforge"), help="Target database name")
    parser.add_argument("--db-user", default=os.getenv("POSTGRES_USER", "flowforge_user"), help="Database user")
    parser.add_argument("--db-host", default=os.getenv("POSTGRES_HOST", "localhost"), help="Database host")
    parser.add_argument("--db-port", default=os.getenv("POSTGRES_PORT", "5432"), help="Database port")
    parser.add_argument("--skip-checksum", action="store_true", help="Skip checksum verification")
    parser.add_argument("--yes", "-y", action="store_true", help="Automatic confirmation without prompt")

    args = parser.parse_args()
    try:
        run_restore(
            backup_path=args.backup_file,
            db_name=args.db_name,
            db_user=args.db_user,
            db_host=args.db_host,
            db_port=args.db_port,
            skip_checksum=args.skip_checksum,
            auto_confirm=args.yes,
        )
    except Exception as exc:
        print(f"[!] Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
