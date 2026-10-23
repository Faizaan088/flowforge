# FlowForge Backup and Disaster Recovery Guide

This document describes the automated database backup, validation, and restoration procedures for FlowForge production and development environments.

---

## 1. Automated Backup Tool (`scripts/backup.py`)

The backup utility automates PostgreSQL dumps with Gzip compression, SHA-256 integrity hashing, and automatic retention pruning.

### Key Capabilities
- **Dual Execution Modes:** Detects and runs either native host `pg_dump` or containerized `docker compose exec postgres pg_dump`.
- **Gzip Compression:** Output files are stored in `.sql.gz` format to minimize storage footprint.
- **SHA-256 Checksum:** Generates an accompanying `.sql.gz.sha256` checksum file for tamper detection and corruption verification.
- **Integrity Validation:** Verifies the generated archive is valid and contains standard PostgreSQL dump headers before committing.
- **Configurable Retention:** Retains the N most recent backups (default: 7) and automatically cleans up older snapshots.

### Usage

```bash
# Basic usage with default settings (backups stored in ./backups)
python scripts/backup.py

# Custom retention (e.g., retain last 14 backups)
python scripts/backup.py --retention 14

# Custom target database and output directory
python scripts/backup.py --db-name flowforge --output-dir /var/backups/flowforge --retention 30
```

### Automated Scheduling (Cron or Systemd Timer)

To run daily backups at 02:00 UTC, add the following cron entry:

```cron
0 2 * * * cd /opt/flowforge && /opt/flowforge/.venv/bin/python scripts/backup.py --output-dir /var/backups/flowforge --retention 30 >> /var/log/flowforge_backup.log 2>&1
```

---

## 2. Disaster Recovery & Restore Tool (`scripts/restore.py`)

The restore utility recovers a FlowForge database from any valid `.sql.gz` or `.sql` backup.

### Key Capabilities
- **Checksum Verification:** Automatically validates the SHA-256 checksum before executing the restore.
- **Safety Safeguard:** Prompts for explicit confirmation before overwriting the target database (bypassable with `--yes` in automated scripts).
- **Dual Execution Modes:** Works seamlessly with host `psql` or containerized `docker compose exec postgres psql`.

### Usage

```bash
# Standard restore with confirmation prompt
python scripts/restore.py backups/flowforge_flowforge_20261023_144532.sql.gz

# Automated restore to a staging/test database without prompt
python scripts/restore.py backups/flowforge_flowforge_20261023_144532.sql.gz --db-name flowforge_staging --yes

# Skip checksum verification if manually editing SQL
python scripts/restore.py backups/flowforge_backup.sql.gz --skip-checksum --yes
```

---

## 3. Disaster Recovery Runbook (RTO & RPO)

- **Recovery Point Objective (RPO):** Maximum 24 hours with daily backups, or real-time with PostgreSQL WAL archiving.
- **Recovery Time Objective (RTO):** Under 5 minutes for database recreation and schema restore.

### Emergency Recovery Steps
1. Deploy fresh FlowForge containers:
   ```bash
   docker compose -f docker-compose.prod.yml up -d postgres redis
   ```
2. Wait for PostgreSQL container health check to report healthy:
   ```bash
   docker compose ps postgres
   ```
3. Restore from the most recent verified backup:
   ```bash
   python scripts/restore.py backups/latest_backup.sql.gz --yes
   ```
4. Run schema migrations to apply any pending changes:
   ```bash
   docker compose run --rm migration alembic upgrade head
   ```
5. Launch the API and worker services:
   ```bash
   docker compose -f docker-compose.prod.yml up -d
   ```
6. Verify service health endpoint:
   ```bash
   curl -f http://localhost:8000/health
   ```
