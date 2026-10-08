# FlowForge Performance & Reliability Benchmarks

## Overview

FlowForge orchestrates distributed execution workflows using **PostgreSQL** as the single source of durable truth and **Redis** for transient pub/sub notifications and queue buffering.

This document details reproducible performance benchmarks measuring critical operational paths under concurrency, contention, and failover recovery.

---

## Benchmark Environment & Methodology

- **Host OS**: Windows 11 Enterprise (amd64)
- **Runtime**: Python 3.12 (asyncio, asyncpg, SQLAlchemy 2.0 async)
- **Database Engine**: PostgreSQL 16 (Docker container `flowforge-postgres-1`)
- **Queue / Event Broker**: Redis 7 Alpine (Docker container `flowforge-redis-1`)
- **Isolation**: Clean database state before each test run; transactions committed to disk.
- **Benchmark Suite**: [`benchmarks/benchmark_suite.py`](../benchmarks/benchmark_suite.py)

---

## Baseline Results

| Benchmark Metric | Test Size / Scale | Elapsed Time | Throughput | Average Latency |
| :--- | :--- | :--- | :--- | :--- |
| **Sequential Execution Claim** | 100 executions | 7.61 s | **13.15 claims/s** | **76.06 ms / claim** |
| **Concurrent Worker Claims (Contention)** | 100 executions, 5 concurrent workers | 7.19 s | **13.91 claims/s** | **71.87 ms / claim** |
| **Lease Recovery Latency** | 200 expired leases | 0.061 s | **3,293.45 recoveries/s** | **0.30 ms / recovery** |
| **Scheduler Evaluation & Generation** | 50 active recurring schedules | 2.48 s | **605.14 occurrences/s** | **1.65 ms / occurrence** |
| **Workflow DAG Dispatch Latency** | 20 diamond DAG runs (80 tasks) | 5.61 s | **3.56 workflows/s** | **280.59 ms / workflow** |
| **Queue Delivery Throughput** | 100 queued executions (Postgres $\to$ Redis) | 0.115 s | **867.59 items/s** | **1.15 ms / item** |

---

## Detailed Metric Analysis

### 1. Execution Claim Throughput (`claim_next_execution`)
- **Mechanism**: Row locking (`FOR UPDATE SKIP LOCKED`) combined with priority ordering (`priority DESC, id ASC`) and policy validation (`check_and_record_admission`).
- **Performance**: Averages ~13–14 claims per second per single connection on local Dockerized PostgreSQL over TCP loopback.
- **Observation**: Contention does not induce deadlocks or serial transaction rollbacks due to `SKIP LOCKED`.

### 2. Concurrent Worker Contention
- **Mechanism**: 5 competing asynchronous worker loops contending on the same queue simultaneously.
- **Result**: Zero duplicate claims; exactly 100 out of 100 executions claimed and fenced. PostgreSQL concurrency semantics guarantee exactly-once claim semantics without dual execution.

### 3. Lease Recovery Latency (`recover_expired_executions`)
- **Mechanism**: Bulk conditional SQL query identifying `status IN ('CLAIMED', 'RUNNING') AND lease_until <= now` and transitioning orphaned items to retry or failed state.
- **Performance**: Extremely fast at **3,293+ recoveries per second** (~61 ms for 200 orphaned runs), ensuring fast cluster rebalancing when nodes crash.

### 4. Scheduler Throughput (`evaluate_due_schedules`)
- **Mechanism**: Evaluates active recurring and one-time schedules, computes deterministic next-interval timestamps, and performs batched idempotent `INSERT ... ON CONFLICT DO NOTHING`.
- **Performance**: Evaluates 50 schedules and persists 1,500 due occurrences in **2.48 seconds** (**605+ occurrences/sec**).

### 5. Workflow DAG Dispatch Latency (`resolve_and_dispatch`)
- **Mechanism**: Resolves dependency barriers across multi-stage DAGs (diamond graph), evaluates parent completion states, updates task statuses, and instantiates linked `Execution` records.
- **Performance**: Dispatches 80 tasks across 20 workflow runs in **5.61 seconds** (~280 ms full DAG resolution and execution creation).

### 6. Queue Delivery Throughput (`deliver_executions_to_redis`)
- **Mechanism**: Batched retrieval of PostgreSQL QUEUED records and pipeline push (`LPUSH`) into Redis queue buffers.
- **Performance**: **867.59 items/second** with error-tolerant fallback (if Redis drops, Postgres records remain QUEUED).

---

## Running the Benchmarks

To execute the benchmarks against an active test database:

```bash
# Set environment
export TEST_DATABASE_URL="postgresql+asyncpg://flowforge_user:flowforge_password@127.0.0.1:5432/flowforge_test"
export REDIS_URL="redis://127.0.0.1:6379/0"

# Run suite
python benchmarks/benchmark_suite.py
```
