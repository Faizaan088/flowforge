# FlowForge Observability, Metrics & Health Monitoring

FlowForge includes production-grade observability designed to provide deep runtime visibility into execution throughput, queue lag, worker liveness, scheduler cycles, DAG workflows, and admission limits without changing orchestration semantics or burdening PostgreSQL with unbounded queries.

---

## 1. Endpoints Overview

| Endpoint | Method | Authentication | Purpose |
|---|---|---|---|
| `/metrics` | `GET` | None (Internal) | Standard Prometheus exposition format telemetry. |
| `/health` | `GET` | None | Container health check (Docker & legacy compatible). |
| `/health/live` | `GET` | None | Liveness probe indicating process is running. |
| `/health/ready` | `GET` | None | Readiness probe validating PostgreSQL and Redis connectivity. |
| `/system/summary` | `GET` | RBAC (`executions:read`) | Lightweight JSON snapshot of active workers and queue depth. |

---

## 2. Prometheus Metrics Reference

All metrics are exposed under the `flowforge_` namespace.

### Executions
- `flowforge_executions_total{status, category}` (Counter)
  - Tracks execution transitions through `queued`, `claimed`, `running`, `succeeded`, `failed`, `cancelled`, `retry_waiting`, and `dead_lettered`.
- `flowforge_execution_duration_seconds{status}` (Histogram)
  - Time elapsed from execution start to terminal success or failure.
  - Buckets: `0.05s`, `0.1s`, `0.25s`, `0.5s`, `1.0s`, `2.5s`, `5.0s`, `10.0s`, `30.0s`, `60.0s`, `120.0s`, `300.0s`.
- `flowforge_execution_queue_wait_seconds` (Histogram)
  - Queue latency from creation to worker claim.
- `flowforge_execution_retries_total` (Counter)
  - Executions scheduled for exponential backoff retry.
- `flowforge_execution_dead_lettered_total` (Counter)
  - Executions permanently failed after retry exhaustion.
- `flowforge_execution_cancelled_total` (Counter)
  - Executions aborted by user or workflow operator.
- `flowforge_execution_recovered_total` (Counter)
  - Orphaned or expired execution leases recovered back to queue.

### Claims & Worker Health
- `flowforge_execution_claims_total{outcome}` (Counter)
  - Outcomes: `claimed`, `rejected`, `empty`.
- `flowforge_execution_claim_failures_total{reason}` (Counter)
  - Reasons: `policy_denied`, `conflict_or_not_found`, `not_available_yet`.
- `flowforge_active_workers` (Gauge)
  - Number of registered workers with heartbeats within the last 60 seconds.
- `flowforge_worker_heartbeats_total{status}` (Counter)
- `flowforge_worker_heartbeat_failures_total{reason}` (Counter)

### Queue Depth (Cached PostgreSQL Aggregation)
- `flowforge_queued_execution_count` (Gauge)
- `flowforge_retry_waiting_execution_count` (Gauge)
- `flowforge_dead_lettered_execution_count` (Gauge)
- `flowforge_running_execution_count` (Gauge)
> [!NOTE]
> Queue gauges are refreshed periodically by the background recovery loop (every 10 seconds) with a short cache TTL, guaranteeing that high-frequency Prometheus scrape intervals never burden the database with full table scans.

### Scheduler
- `flowforge_scheduler_occurrences_created_total` (Counter)
- `flowforge_scheduler_occurrences_skipped_total` (Counter)
- `flowforge_scheduler_occurrences_failed_total` (Counter)

### Workflows
- `flowforge_workflow_runs_total{status}` (Counter)
  - Statuses: `succeeded`, `failed`, `cancelled`.
- `flowforge_workflow_run_duration_seconds` (Histogram)
- `flowforge_workflow_failures_total` (Counter)
- `flowforge_workflow_cancellations_total` (Counter)

### Concurrency & Rate Limit Policies
- `flowforge_concurrency_admission_denied_total{target_type}` (Counter)
  - Target types: `workflow`, `task_type`, `category`.
- `flowforge_rate_limit_admission_denied_total{target_type}` (Counter)
  - Target types: `workflow`, `task_type`, `category`.

### Events & WebSockets
- `flowforge_events_published_total{event_type}` (Counter)
- `flowforge_event_publish_failures_total{event_type}` (Counter)
- `flowforge_websocket_connections` (Gauge)
- `flowforge_websocket_events_sent_total` (Counter)
- `flowforge_websocket_events_dropped_total` (Counter)

---

## 3. Label Cardinality Rules

To maintain bounded memory usage and prevent time-series cardinality explosion in Prometheus/VictoriaMetrics:

1. **Strictly Forbidden as Metric Labels**:
   - `execution_id`
   - `workflow_run_id`
   - `worker_id`
   - `user_id`
   - Unbounded job definitions or raw error strings.
2. **Permitted Labels**:
   - Fixed enumerations only (e.g. `status`, `target_type`, `outcome`, `event_type`).

---

## 4. Request Correlation & Context

FlowForge attaches an `X-Request-ID` header to all incoming and outgoing HTTP responses via [`RequestCorrelationMiddleware`](file:///C:/Users/moham/OneDrive/Desktop/flowforge/metrics.py#L320-L336). Clients can supply custom correlation IDs in requests, or receive a server-generated ID (`req_<uuid12>`).

---

## 5. Health & Readiness Details

- **`/health/live`**: Checks if the web server process is alive. Returns HTTP 200 `{"status": "alive"}`.
- **`/health/ready`**: Verifies connectivity to PostgreSQL (`SELECT 1`) and Redis (`PING`). If any critical dependency fails, returns HTTP 503:
  ```json
  {
    "status": "not_ready",
    "database": "unhealthy",
    "redis": "ok"
  }
  ```

---

## 6. Operational Summary API

Authenticated dashboard clients can retrieve a real-time operational overview via `GET /system/summary`:
```http
GET /system/summary HTTP/1.1
Authorization: Bearer <JWT_TOKEN>
```
Response:
```json
{
  "status": "operational",
  "active_workers": 2,
  "queued_executions": 14,
  "running_executions": 3,
  "retry_waiting": 1,
  "dead_letters": 0
}
```
