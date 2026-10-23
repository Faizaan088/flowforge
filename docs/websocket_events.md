# FlowForge WebSocket Live Execution Events Stream

FlowForge provides a real-time event streaming system over WebSockets (`/ws/events`) designed to provide low-latency execution and workflow notifications. It is a transient notification channel, not a durable event log or a replacement for reading current state from the API.

---

## 1. Architecture & Design Principles

1. **PostgreSQL Remains Durable Source of Truth**:
   - All state transitions (queued, claimed, running, succeeded, failed, retry_waiting, dead_lettered, cancelled, recovered, priority updates) are durably committed to PostgreSQL before any event is published.
   - Event publication occurs strictly **post-transaction commit**.
2. **Transient Transport via Redis Pub/Sub**:
   - Redis Pub/Sub channel `flowforge:events` acts as high-throughput, transient event transport across multi-process or clustered instances.
   - A single background Redis Pub/Sub consumer task runs per application process and fans out events to local active WebSocket sessions (preventing connection explosion on Redis).
3. **Resilience to Redis Failure**:
   - Any Redis connection or network failure during event publication is caught and logged. Event failures never abort or roll back durable database transactions. Because Redis Pub/Sub and per-client WebSocket queues are transient, a publication failure, disconnect, or dropped queue item can cause a client to miss an event. Clients must reconcile against durable API state on connection/reconnection and periodically while open.
4. **Bounded Queues & Backpressure Management**:
   - Each WebSocket client maintains a bounded FIFO queue (`asyncio.Queue(maxsize=200)`). If a slow client fails to consume events fast enough, excess events are safely dropped to protect the server process from memory exhaustion.

---

## 2. Connection & Authentication

The WebSocket endpoint is exposed at:
```text
ws://<host>:<port>/ws/events
```

### Authentication
Connections must be authenticated with a valid JWT token issued by `/auth/login`. The browser frontend sends the token in the `Sec-WebSocket-Protocol` handshake rather than the URL, avoiding query-string token leakage into ordinary access logs. Production rejects query-string tokens. Do not put tokens in URLs.

Example browser handshake:
```http
GET /ws/events HTTP/1.1
Host: localhost:8000
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Protocol: flowforge, token.<JWT_TOKEN>
```

The frontend uses the WebSocket subprotocol list `flowforge` and `token.<JWT_TOKEN>`. Confirm any non-browser client's handshake behavior against the actual server and reverse proxy before deployment; do not assume arbitrary `Authorization` header support.

### RBAC Authorization
- Clients must possess one of the following roles: `observer`, `operator`, or `admin`.
- Unauthenticated or unauthorized connection attempts are immediately closed with WebSocket status code `1008` (`WS_1008_POLICY_VIOLATION`).

Upon successful connection, the server sends a connection acknowledgment frame:
```json
{
  "type": "connected",
  "user": "alice",
  "role": "operator",
  "subscription": {
    "all": true,
    "execution_ids": [],
    "workflow_run_ids": [],
    "workflow_ids": [],
    "job_ids": []
  }
}
```

---

## 3. Subscription Management

By default, newly connected sessions receive all execution and workflow events. Clients can update their subscriptions at any time by sending a JSON message:

### Subscribe to Specific Execution
```json
{
  "action": "subscribe",
  "filter": {
    "execution_id": 105
  }
}
```

### Subscribe to Specific Workflow Run
```json
{
  "action": "subscribe",
  "filter": {
    "workflow_run_id": 42
  }
}
```

### Subscribe to Specific Job
```json
{
  "action": "subscribe",
  "filter": {
    "job_id": 7
  }
}
```

### Subscribe to All Events
```json
{
  "action": "subscribe",
  "filter": "all"
}
```

The server responds with an acknowledgment:
```json
{
  "type": "subscribed",
  "subscription": {
    "all": false,
    "execution_ids": [105],
    "workflow_run_ids": [],
    "workflow_ids": [],
    "job_ids": []
  }
}
```

---

## 4. Heartbeat & Ping / Pong

Clients can send keepalive pings:
```json
{
  "action": "ping"
}
```
*(Plain text `"ping"` is also accepted).*

The server replies:
```json
{
  "type": "pong",
  "timestamp": "2026-10-08T21:55:00.000000+00:00"
}
```

---

## 5. State Snapshots

Clients can request an immediate snapshot of current executions without polling the HTTP REST endpoints:
```json
{
  "action": "snapshot"
}
```
Or for a specific execution:
```json
{
  "action": "snapshot",
  "execution_id": 105
}
```

The server responds with:
```json
{
  "type": "snapshot",
  "executions": [
    {
      "id": 105,
      "job_definition_id": 7,
      "status": "RUNNING",
      "priority": 10,
      "category": "default",
      "attempt": 1
    }
  ]
}
```

---

## 6. Event Schema & Types

### Event Object Schema
Every event pushed to clients is a JSON object with standard fields:

| Field | Type | Description |
|---|---|---|
| `event_id` | `string` | Monotonic unique ID (`evt_<timestamp_ms>_<hex>`) |
| `event_type` | `string` | Categorized event type name |
| `timestamp` | `string` | ISO-8601 UTC timestamp |
| `execution_id` | `int \| null` | Associated Execution ID |
| `job_id` | `int \| null` | Associated Job Definition ID |
| `workflow_id` | `int \| null` | Associated Workflow Definition ID |
| `workflow_run_id` | `int \| null` | Associated Workflow Run ID |
| `workflow_task_execution_id` | `int \| null` | Associated Workflow Task Execution ID |
| `status` | `string \| null` | Execution or Workflow Status |
| `worker_id` | `string \| null` | Identifier of worker processing the task |
| `metadata` | `object` | Arbitrary event-specific payload (reasons, error summaries, priorities, durations) |

### Event Types
- `execution:queued`: Execution inserted into PostgreSQL durable queue.
- `execution:claimed`: Execution atomically claimed by worker under lease fencing.
- `execution:running`: Worker began task execution.
- `execution:succeeded`: Task finished successfully.
- `execution:failed`: Task failed with error or non-zero exit code.
- `execution:retry_waiting`: Retryable failure scheduled for backoff delay.
- `execution:dead_lettered`: Retries exhausted; execution transitioned to terminal dead letter.
- `execution:cancelled`: Execution cancelled by user or workflow operator.
- `execution:recovered`: Expired lease recovered back to queue or retryable pool.
- `execution:priority_changed`: Queue priority dynamically altered.
- `workflow:task_state_changed`: Task node transitioned state (e.g. READY, RUNNING, SUCCEEDED).
- `workflow:run_state_changed`: Workflow run reached terminal state (SUCCEEDED, FAILED).
- `workflow:run_cancelled`: Entire workflow run cancelled and all pending tasks fenced.
