# FlowForge: Distributed Workflow Orchestration & Task Execution Engine

FlowForge is a resilient, distributed workflow orchestration and task execution platform built with **Python (FastAPI)**, **PostgreSQL**, **Redis**, and **React (TypeScript)**. It provides end-to-end DAG workflow execution, atomic execution claiming, automatic crash recovery, a trusted extensible task registry, durable audit logging, time-based scheduling, granular Role-Based Access Control (RBAC), real-time WebSocket telemetry, and Prometheus observability.

---

## Architecture Overview

```
                      +-----------------------------+
                      |   FlowForge React Console   |
                      |   (Vite + TS + Tailwind)   |
                      +--------------+--------------+
                                     |
                       REST & WS /ws/events (JWT Auth)
                                     v
                      +-----------------------------+
                      |     FastAPI Control Plane   |
                      |  - RBAC & JWT Auth          |
                      |  - DAG Resolution Engine    |
                      |  - Recurring Scheduler Loop |
                      |  - Execution Recovery Loop  |
                      |  - Durable Audit Trail      |
                      +-------+-------------+-------+
                              |             |
           PostgreSQL (Asyncpg)             | Redis (Pub/Sub & Queues)
           - Workflows, Tasks, Edges        | - Task Dispatch
           - Executions (SKIP LOCKED)       | - Real-time WS Events
           - Schedules & Occurrences        | - Worker Coordination
           - Policies & Users               |
                              v             v
                      +-----------------------------+
                      |   Distributed Worker Fleet  |
                      |  - Atomic Lease Claiming    |
                      |  - Active Lease Renewal     |
                      |  - Trusted Task Registry    |
                      |  - Heartbeat & Auto-Recovery|
                      +-----------------------------+
```

---

## Core Capabilities

1. **Atomic Execution Claims & Worker Leases:**
   - Database-level isolation using `SELECT ... FOR UPDATE SKIP LOCKED` enables atomic claiming so competing workers do not claim the same eligible row within the same claim transaction. Lease expiry, worker crashes, retries, and external side effects mean this is not a guarantee of exactly-once task execution; handlers that perform external side effects should use idempotency keys or compensating actions where appropriate.
   - Dynamic worker lease renewals periodically extend active leases while tasks are processing.
   - Background execution recovery loop automatically re-queues expired worker leases or routes exhausted retries to dead-letter state.

2. **Trusted Extensible Task Registry:**
   - Real, verified execution replacing simulations.
   - Built-in handlers: `echo`, `sleep`, `compute_hash`, `fail`, `transform`, `standard`.
   - Extensible registration API (`TaskRegistry.register`) supporting custom async/sync handlers, parameter validation, execution timeouts, error reporting, and output capture.

3. **Workflow Orchestration & Directed Acyclic Graphs (DAGs):**
   - Validates DAG topological sorting, cyclic dependency detection, and upstream-downstream task dependency progression.
   - Seamless upstream output passing to downstream inputs.
   - Concurrency limits and token-bucket / sliding-window rate limit policy enforcement.

4. **Integrated Scheduler Loop:**
   - Background scheduler seamlessly embedded in the control-plane lifespan.
   - Evaluates active cron expressions, recurring intervals, and one-time schedules.
   - Prevents duplicate occurrence generation using atomic database uniqueness constraints.

5. **Granular RBAC & Security Hardening:**
   - Cryptographic JWT bearer tokens with permission enforcement (`Admin`, `Operator`, `Observer`).
   - Production configuration validation rejecting wildcards, insecure default secrets, and default admin credentials.
   - Admin account bootstrapping and worker administrative controls (`/workers/{worker_id}/disable`).

6. **Durable Audit Trail:**
   - Immutable audit logging (`audit_events` table) recording security actions, workflow triggers, schedule modifications, worker status changes, and user management.

7. **Real-time Observability & Telemetry:**
   - WebSocket broadcast (`/ws/events`) streaming live state transitions, heartbeats, and worker updates to connected clients.
   - Prometheus metrics endpoint (`/metrics`) exposing execution counters, claim latencies, and queue depths.
   - Liveness (`/live`), readiness (`/readiness`), and deep health check (`/health`) endpoints.

---

## Prerequisites

- **Docker** and **Docker Compose** (recommended for production and local services)
- **Python 3.12+** (for native backend development)
- **Node.js 22.12+** & **npm** (Vite 8 requires Node.js 20.19+ or 22.12+; use Node.js 22.12+ for development)
- **PostgreSQL 15+** and **Redis 7+**

---

## Quickstart with Docker Compose

To start the complete FlowForge platform (PostgreSQL, Redis, FastAPI Backend, Distributed Worker, and React Frontend Dashboard):

```bash
# 1. Clone the repository and configure your environment
cp .env.example .env

# 2. Build and launch all services in detached mode (Development / Testing)
docker compose up --build -d

# 3. View service status
docker compose ps
```

### Production Deployment

In production environments, apply the production overlay `docker-compose.prod.yml` which requires Docker Compose v2.24.4 or newer and:
- Strips public/host port mappings for PostgreSQL and Redis entirely (accessible only on the internal `flowforge-backend` network).
- Enforces container log rotation limits (`max-size: 20m`, `max-file: 5`).
- Enables `always` restart policies.
- Connects the production frontend via hardened reverse proxy on port 80.
- Enforces strict secret provisioning (fails fast if `FLOWFORGE_SECRET_KEY`, `FLOWFORGE_ADMIN_PASSWORD`, or `POSTGRES_PASSWORD` are missing).

```bash
# Before starting, set unique production values in .env. Generate random secrets, for example:
# python -c "import secrets; print(secrets.token_hex(32))"
# Use a separate generated value for FLOWFORGE_SECRET_KEY, POSTGRES_PASSWORD,
# REDIS_PASSWORD, and FLOWFORGE_ADMIN_PASSWORD. Keep passwords used in URLs
# URL-safe (hex output is suitable) and set CORS_ALLOWED_ORIGINS to your real frontend origin.

# Production launch with docker-compose.prod.yml overlay
docker compose -f docker-compose.yml -f docker-compose.prod.yml up --build -d
```

The services will be available at:
- **Frontend Dashboard:** [http://localhost:3000](http://localhost:3000) (Dev) or [http://localhost:80](http://localhost:80) (Prod)
- **API Documentation (Swagger UI):** [http://localhost:8000/docs](http://localhost:8000/docs) in development. In production, the API has no published host port; access it only through explicitly configured reverse-proxy routes or your private deployment network.
- **API Health Check and Prometheus Metrics:** available on port 8000 in development; keep them private or expose them through authenticated/controlled infrastructure in production.

Default bootstrap administrator credentials:
- **Username:** `admin`
- **Password:** Configure via `FLOWFORGE_ADMIN_PASSWORD` in `.env` (must be at least 8 characters).


---

## Local Development Setup

### 1. Backend Setup

```bash
# Create and activate Python virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .\.venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt

# Start local infrastructure dependencies via Docker
docker compose up postgres redis -d

# Apply database migrations
alembic upgrade head

# Run FastAPI control plane
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# In a separate terminal, run the distributed worker
python worker.py
```

### 2. Frontend Setup

```bash
cd frontend

# Install Node dependencies
npm install

# Run Vite development server
npm run dev
```

Visit [http://localhost:5173](http://localhost:5173) to view the development frontend.

---

## Database Migrations (Alembic)

Database schema versions are managed via Alembic.

```bash
# Run all pending migrations
alembic upgrade head

# Rollback latest migration
alembic downgrade -1

# Generate a new migration after model updates
alembic revision --autogenerate -m "describe_migration"
```

Migration history in `alembic/versions`:
- `94318d5e07b4_add_audit_events_table.py`: Adds the durable `audit_events` table for compliance and operational tracking.

---

## Security & RBAC Configuration

FlowForge implements three distinct role tiers:

| Role | Permissions | Typical Use |
| :--- | :--- | :--- |
| **Admin** | Full access: user management (`users:manage`), policies, schedules, worker management, and audit log inspection. | System administrators, platform DevOps. |
| **Operator** | Read/write workflows, trigger executions (`executions:trigger`), manage schedules, claim & cancel tasks. | Engineers, automation operators. |
| **Observer** | Read-only access: view execution status, workflows, workers, and metrics. | Auditors, dashboards, monitoring agents. |

### Environment Variables

| Variable | Description | Default / Example |
| :--- | :--- | :--- |
| `ENVIRONMENT` | `production`, `development`, or `testing` | `production` |
| `FLOWFORGE_SECRET_KEY` | Secret key for signing JWT tokens (min 32 chars) | Cryptographically secure random string |
| `FLOWFORGE_TOKEN_EXPIRE_MINUTES` | Lifetime of JWT bearer tokens | `60` |
| `FLOWFORGE_ADMIN_USERNAME` | Default administrator username | `admin` |
| `FLOWFORGE_ADMIN_PASSWORD` | Default administrator password | Secure password |
| `DATABASE_URL` | PostgreSQL async connection string | `postgresql+asyncpg://...` |
| `REDIS_URL` | Redis server connection string | `redis://redis:6379/0` |
| `CORS_ALLOWED_ORIGINS` | Trusted origins (comma-separated; no wildcard in prod) | `http://localhost:3000` |
| `SCHEDULER_INTERVAL_SECONDS` | Cadence for schedule occurrence evaluations | `5.0` |
| `EXECUTION_RECOVERY_INTERVAL_SECONDS`| Cadence for expired lease recovery | `10.0` |

---

## Extensible Task Registry

Custom tasks can be easily registered with the default registry in `task_registry.py`:

```python
from task_registry import default_task_registry

async def send_slack_notification(channel: str, message: str) -> dict:
    # Perform external API call or processing
    return {"status": "sent", "channel": channel}

default_task_registry.register(
    task_type="slack_notify",
    handler=send_slack_notification,
    required_params=["channel", "message"],
    timeout_seconds=30.0,
)
```

Workers automatically dispatch jobs with `task_type="slack_notify"` to this handler, capturing output return values, enforcing timeouts, and propagating failures cleanly.

---

## Running Verification Tests & Benchmarks

### Backend Test Suite
The comprehensive backend test suite includes 280 tests covering authentication, RBAC, DAG workflows, atomic execution claims, retries, recovery loops, schedulers, WebSockets, audit logging, user administration, worker disable/enable lifecycle enforcement, and migrations.

```bash
# Run full pytest suite against an isolated PostgreSQL test database
$env:TEST_DATABASE_URL="postgresql+asyncpg://flowforge_user:flowforge_password@localhost:5432/flowforge_test"
pytest tests/ -v
```

### Benchmarks
Run the high-concurrency performance benchmark suite:

```bash
python benchmarks/benchmark_suite.py
```

Benchmark capabilities validated:
- Sequential & concurrent execution claiming throughput (>15 items/sec)
- Lease crash recovery throughput (>2,000 recoveries/sec)
- Scheduler evaluation throughput (>700 occurrences/sec)
- Queue delivery throughput from PostgreSQL to Redis (>800 items/sec)

### Frontend Verification
```bash
cd frontend
npm run lint      # ESLint verification (0 errors, 0 warnings)
npm run build     # Production Vite build
```

---

## Operational API Endpoints

- `POST /auth/login` & `POST /auth/token` — Exchange credentials for JWT access token.
- `GET /workflows/` & `POST /workflows/` — List and create DAG workflows.
- `POST /workflows/{id}/runs` — Trigger a DAG execution run.
- `GET /schedules/` & `POST /schedules/` — Manage recurring cron, interval, and one-time schedules.
- `GET /workers/` — Worker fleet status and liveness timestamps.
- `POST /workers/{id}/disable` & `POST /workers/{id}/enable` — Worker administrative disablement and re-enabling.
- `GET /audit/` — Filter and inspect durable audit logs (Admin only).
- `WS /ws/events` — Live WebSocket telemetry stream.
- `GET /metrics` — Prometheus metrics.
- `GET /health` & `/readiness` & `/live` — Kubernetes and orchestration health checks.

---

## Stopping the Platform

To stop the Docker Compose services:

```bash
# Stop running containers while preserving volumes
docker compose down

# Stop containers and remove persistent database volumes (reset state)
docker compose down -v
```

