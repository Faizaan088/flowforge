#!/usr/bin/env python3
"""Automated PostgreSQL backup script for FlowForge.

Supports:
- Execution via host pg_dump or docker compose exec postgres pg_dump
- Gzip compression (.sql.gz)
- SHA-256 integrity verification
- Configurable retention policy (default: 7 days / last 7 backups)
- Health check verification of generated dump
"""

import argparse
import gzip
import hashlib
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def compute_sha256(filepath: Path) -> str:
    sha = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(65536):
            sha.update(chunk)
    return sha.hexdigest()


def find_dump_tool() -> tuple[str, list[str]]:
    """Determine whether to use native pg_dump or docker compose."""
    if shutil.which("pg_dump"):
        return "native", ["pg_dump"]
    
    # Check docker compose
    if shutil.which("docker"):
        try:
            res = subprocess.run(
                ["docker", "compose", "ps", "--services"],
                capture_output=True,
                text=True,
                check=False,
            )
            if "postgres" in res.stdout:
                return "docker", ["docker", "compose", "exec", "-T", "postgres", "pg_dump"]
        except Exception:
            pass
            
    return "unknown", []


def run_backup(
    output_dir: Path,
    db_name: str = "flowforge",
    db_user: str = "flowforge_user",
    db_host: str = "localhost",
    db_port: str = "5432",
    retention_count: int = 7,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_filename = f"flowforge_{db_name}_{timestamp}.sql.gz"
    backup_path = output_dir / backup_filename
    checksum_path = output_dir / f"{backup_filename}.sha256"

    tool_type, cmd_prefix = find_dump_tool()
    if tool_type == "unknown":
        raise RuntimeError("Neither local 'pg_dump' nor Docker Compose 'postgres' service was found.")

    print(f"[*] Starting FlowForge database backup via [{tool_type}]...")
    env = os.environ.copy()
    if "POSTGRES_PASSWORD" in env and "PGPASSWORD" not in env:
        env["PGPASSWORD"] = env["POSTGRES_PASSWORD"]

    if tool_type == "native":
        cmd = cmd_prefix + [
            "-h", db_host,
            "-p", str(db_port),
            "-U", db_user,
            "--no-owner",
            "--clean",
            "--if-exists",
            db_name,
        ]
    else:
        cmd = cmd_prefix + [
            "-U", db_user,
            "--no-owner",
            "--clean",
            "--if-exists",
            db_name,
        ]

    start_time = time.time()
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        with gzip.open(backup_path, "wb") as gz_out:
            shutil.copyfileobj(proc.stdout, gz_out)
        
        _, stderr_data = proc.communicate()
        if proc.returncode != 0:
            if backup_path.exists():
                backup_path.unlink()
            raise RuntimeError(f"pg_dump exited with code {proc.returncode}: {stderr_data.decode('utf-8', errors='replace')}")

    except Exception as exc:
        if backup_path.exists():
            backup_path.unlink()
        raise RuntimeError(f"Backup execution failed: {exc}") from exc

    # Verify backup is non-empty and valid gzip
    file_size = backup_path.stat().st_size
    if file_size < 100:
        backup_path.unlink()
        raise RuntimeError(f"Backup file is abnormally small ({file_size} bytes). Aborting.")

    # Validate gzip reading
    try:
        with gzip.open(backup_path, "rb") as gz_in:
            sample = gz_in.read(1024)
            if not sample or b"PostgreSQL database dump" not in sample and b"CREATE" not in sample and b"DROP" not in sample:
                print("[!] Warning: Output file does not contain expected PostgreSQL dump header.")
    except Exception as exc:
        backup_path.unlink()
        raise RuntimeError(f"Backup compression verification failed: {exc}")

    # Compute and record checksum
    checksum = compute_sha256(backup_path)
    checksum_path.write_text(f"{checksum}  {backup_filename}\n", encoding="utf-8")

    duration = time.time() - start_time
    print(f"[+] Backup completed successfully in {duration:.2f}s:")
    print(f"    File:     {backup_path}")
    print(f"    Size:     {file_size / 1024:.2f} KB")
    print(f"    SHA-256:  {checksum}")

    # Enforce retention policy
    if retention_count > 0:
        backups = sorted(output_dir.glob("flowforge_*.sql.gz"), key=lambda p: p.stat().st_mtime)
        if len(backups) > retention_count:
            to_remove = backups[:-retention_count]
            print(f"[*] Enforcing retention ({retention_count} max). Pruning {len(to_remove)} old backups...")
            for old in to_remove:
                old_sha = old.with_suffix(".sql.gz.sha256")
                print(f"    Deleting {old.name}")
                old.unlink(missing_ok=True)
                old_sha.unlink(missing_ok=True)

    return backup_path


def main():
    parser = argparse.ArgumentParser(description="FlowForge PostgreSQL Database Backup Tool")
    parser.add_argument("--output-dir", type=Path, default=Path("backups"), help="Directory to store backups")
    parser.add_argument("--db-name", default=os.getenv("POSTGRES_DB", "flowforge"), help="Database name")
    parser.add_argument("--db-user", default=os.getenv("POSTGRES_USER", "flowforge_user"), help="Database user")
    parser.add_argument("--db-host", default=os.getenv("POSTGRES_HOST", "localhost"), help="Database host")
    parser.add_argument("--db-port", default=os.getenv("POSTGRES_PORT", "5432"), help="Database port")
    parser.add_argument("--retention", type=int, default=7, help="Number of backups to keep (default: 7)")

    args = parser.parse_args()
    try:
        run_backup(
            output_dir=args.output_dir,
            db_name=args.db_name,
            db_user=args.db_user,
            db_host=args.db_host,
            db_port=args.db_port,
            retention_count=args.retention,
        )
    except Exception as exc:
        print(f"[!] Error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
