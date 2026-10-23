# FlowForge Production Hardening & Deployment Guide

## 1. Overview
FlowForge is hardened for production operations with:
- **Alembic Database Migrations** replacing runtime schema creation (`Base.metadata.create_all()`).
- **Fail-Fast Security Validation** rejecting insecure defaults, weak secrets, or wildcard CORS in production mode.
- **Docker Compose Orchestration** with ordered dependency startup via healthchecks and dedicated migration containers.
- **Audited Logging & Observability** ensuring zero secrets, tokens, or PII enter logs or Prometheus metrics.

---

## 2. Database Migrations & Fresh-DB Setup

### Fresh Database Setup
For a new production installation:
1. Provision a PostgreSQL 15+ database instance.
2. Configure the database connection string in `.env` as `DATABASE_URL`:
   ```bash
   DATABASE_URL=postgresql+asyncpg://<user>:<password>@<host>:5432/<dbname>
   ```
3. Run Alembic migrations to apply all 14 schema tables, indexes, and constraints:
   ```bash
   alembic upgrade head
   ```

### Common Migration Commands
- **Apply all pending migrations:**
  ```bash
  alembic upgrade head
  ```
- **Roll back all migrations to base (clean schema):**
  ```bash
  alembic downgrade base
  ```
- **Check current database revision:**
  ```bash
  alembic current
  ```
- **Show migration history:**
  ```bash
  alembic history --verbose
  ```
- **Generate a new migration:**
  ```bash
  alembic revision --autogenerate -m "describe_change"
  ```

---

## 3. Security Hardening & Environment Configuration

### Fail-Fast Startup Validation
When `ENVIRONMENT=production`, FlowForge validates security configurations before booting the API:
- `FLOWFORGE_SECRET_KEY`:
  - Must be explicitly defined (never default).
  - Must not match common insecure patterns (`secret`, `password`, `changeme`).
  - Must have at least 32 characters.
  - Generate via: `openssl rand -hex 32`
- `FLOWFORGE_ADMIN_PASSWORD`:
  - Must be explicitly configured.
  - Must be between 12 and 128 characters and differ from the administrator username.
- `CORS_ALLOWED_ORIGINS` / `CORS_ORIGINS`:
  - Must be explicitly set to trusted origin domains (e.g. `https://dashboard.flowforge.internal`).
  - Wildcard `*` origin is strictly forbidden in production.
- `DATABASE_URL`:
  - Must be a valid PostgreSQL URL with a unique password.
  - The Compose-interpolated database password must be at least 24 URL-safe characters (`A-Z`, `a-z`, `0-9`, `_`, `-`) because it is embedded directly in the URL.
- `REDIS_URL` / `REDIS_PASSWORD`:
  - Redis authentication must be enabled.
  - The password must be at least 32 URL-safe characters (`A-Z`, `a-z`, `0-9`, `_`, `-`).
- `CORS_ALLOWED_ORIGINS`:
  - Production origins must use HTTPS and match the public frontend origin.
- Login throttling:
  - Shared Redis-backed rate limiting defaults to 10 attempts per username/IP pair and per IP across usernames per 15 minutes.
- `AUTO_CREATE_TABLES`:
  - Defaults to `false`. Production uses Alembic migrations exclusively.

---

## 4. Docker Compose Deployment

The application runs reproducibly via Docker Compose:

```bash
# 1. Copy and populate production configuration
cp .env.example .env
# Edit .env with secure production values

# 2. Build and start services
docker compose -f docker-compose.yml -f docker-compose.prod.yml up --build -d
```

**Do not use the base-only command for public production deployments.** The base file is intended for local development. Configure TLS at a trusted HTTPS reverse proxy/load balancer before exposing the frontend publicly; this Compose overlay only publishes HTTP.

### Service Architecture & Startup Order
1. **`postgres`**: PostgreSQL 15 container with `pg_isready` healthcheck.
2. **`redis`**: Redis 7 container with `redis-cli ping` healthcheck.
3. **`migration`**: Ephemeral container running `alembic upgrade head` after `postgres` becomes healthy.
4. **`api`**: FastAPI control plane booting only after `migration` completes successfully and `redis` is healthy. Verified via `/health` endpoint healthcheck.
5. **`worker`**: Background queue execution worker running after migrations complete.
6. **`frontend`**: Production Nginx container serving the built React dashboard on container port 80, reverse-proxying API and WebSockets to `api`. The host port is controlled by `PROD_HTTP_PORT` (default 80); terminate HTTPS at a trusted reverse proxy/load balancer.
