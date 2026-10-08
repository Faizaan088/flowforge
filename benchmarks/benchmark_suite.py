"""FlowForge Performance and Reliability Benchmark Suite.

Measures:
1. Execution claim throughput (single & batch)
2. Queue delivery throughput (Postgres to Redis queue buffer)
3. Concurrent worker claim contention (serialize & conflict handling)
4. Scheduler throughput (generation & idempotency checks)
5. Workflow dispatch latency
6. Lease recovery latency
"""

import asyncio
from datetime import datetime, timedelta, timezone
import json
import os
import sys
import time
from typing import Any, Dict, List

# Ensure flowforge root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from database import Base
from execution_claim import (
    claim_execution,
    claim_next_execution,
    complete_execution,
    start_execution,
)
from execution_recovery import recover_expired_executions
from models import (
    Execution,
    JobDefinition,
    ScheduleDefinition,
    ScheduleOccurrence,
    Worker,
    WorkflowDefinition,
    WorkflowRun,
    WorkflowTask,
    WorkflowTaskExecution,
)
import redis
from queue_reconciliation import deliver_executions_to_redis
from scheduler import dispatch_due_occurrences, evaluate_due_schedules
from workflow_engine import (
    advance_workflow_on_execution_terminal,
    create_workflow,
    create_workflow_run,
    resolve_and_dispatch,
)

DATABASE_URL = os.getenv(
    "TEST_DATABASE_URL",
    "postgresql+asyncpg://flowforge_user:flowforge_password@127.0.0.1:5432/flowforge_test",
)


async def setup_db():
    engine = create_async_engine(DATABASE_URL, echo=False, poolclass=NullPool)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    return engine, session_maker


async def cleanup_data(session: AsyncSession):
    async with session.begin():
        await session.execute(WorkflowTaskExecution.__table__.delete())
        await session.execute(WorkflowRun.__table__.delete())
        await session.execute(Execution.__table__.delete())
        await session.execute(ScheduleOccurrence.__table__.delete())
        await session.execute(ScheduleDefinition.__table__.delete())
        await session.execute(WorkflowTask.__table__.delete())
        await session.execute(WorkflowDefinition.__table__.delete())
        await session.execute(JobDefinition.__table__.delete())
        await session.execute(Worker.__table__.delete())


async def benchmark_claim_throughput(session_maker, n: int = 100) -> Dict[str, Any]:
    print(f"\n--- Running Benchmark 1: Execution Claim Throughput (n={n}) ---")
    async with session_maker() as session:
        await cleanup_data(session)

        job = JobDefinition(name="bench_job", category="bench_cat", priority=10, max_retries=3)
        session.add(job)
        await session.commit()
        await session.refresh(job)

        for i in range(n):
            exc = Execution(
                job_definition_id=job.id,
                category="bench_cat",
                status="QUEUED",
                priority=100 - (i % 10),
            )
            session.add(exc)
        await session.commit()

    start_time = time.perf_counter()
    claimed_count = 0
    worker_id = "bench-worker-1"

    for _ in range(n):
        async with session_maker() as session:
            claimed = await claim_next_execution(
                session=session,
                worker_id=worker_id,
                lease_duration=timedelta(seconds=30),
            )
            if claimed:
                claimed_count += 1
            else:
                break
    elapsed = time.perf_counter() - start_time
    throughput = claimed_count / elapsed if elapsed > 0 else 0
    latency_p50 = (elapsed / claimed_count * 1000) if claimed_count > 0 else 0

    print(f"Claimed {claimed_count}/{n} in {elapsed:.3f}s -> {throughput:.1f} claims/sec, avg latency {latency_p50:.2f}ms")
    return {
        "operation": "Sequential Execution Claim",
        "total_operations": claimed_count,
        "elapsed_seconds": round(elapsed, 4),
        "throughput_ops_sec": round(throughput, 2),
        "avg_latency_ms": round(latency_p50, 2),
    }


async def benchmark_concurrent_worker_claims(
    session_maker, total_executions: int = 100, workers: int = 5
) -> Dict[str, Any]:
    print(f"\n--- Running Benchmark 2: Concurrent Worker Claims ({workers} workers, n={total_executions}) ---")
    async with session_maker() as session:
        await cleanup_data(session)
        job = JobDefinition(name="bench_contention_job", category="contention", priority=1, max_retries=3)
        session.add(job)
        await session.commit()
        await session.refresh(job)

        for i in range(total_executions):
            exc = Execution(
                job_definition_id=job.id,
                category="contention",
                status="QUEUED",
                priority=i % 10,
            )
            session.add(exc)
        await session.commit()

    async def worker_loop(w_id: str) -> int:
        claims = 0
        while True:
            async with session_maker() as session:
                claimed = await claim_next_execution(
                    session=session, worker_id=w_id, lease_duration=timedelta(seconds=30)
                )
                if claimed:
                    claims += 1
                else:
                    break
        return claims

    start_time = time.perf_counter()
    tasks = [worker_loop(f"contention-worker-{w}") for w in range(workers)]
    results = await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start_time

    total_claimed = sum(results)
    throughput = total_claimed / elapsed if elapsed > 0 else 0
    print(f"Contention: Workers {results} claimed {total_claimed} total in {elapsed:.3f}s ({throughput:.1f} claims/sec)")
    return {
        "operation": f"Concurrent Worker Claims ({workers} workers)",
        "total_operations": total_claimed,
        "elapsed_seconds": round(elapsed, 4),
        "throughput_ops_sec": round(throughput, 2),
        "worker_distribution": results,
    }


async def benchmark_lease_recovery_latency(session_maker, count: int = 200) -> Dict[str, Any]:
    print(f"\n--- Running Benchmark 3: Lease Recovery Latency ({count} expired leases) ---")
    async with session_maker() as session:
        await cleanup_data(session)
        job = JobDefinition(name="bench_lease_job", category="bench_lease", priority=1, max_retries=3)
        session.add(job)
        await session.commit()
        await session.refresh(job)

        expired_time = datetime.now(timezone.utc) - timedelta(minutes=5)
        for i in range(count):
            exc = Execution(
                job_definition_id=job.id,
                category="bench_lease",
                status="RUNNING" if i % 2 == 0 else "CLAIMED",
                worker_id=f"dead-worker-{i % 10}",
                lease_until=expired_time,
                attempt=1,
            )
            session.add(exc)
        await session.commit()

    start_time = time.perf_counter()
    async with session_maker() as session:
        recovered = await recover_expired_executions(session)
        await session.commit()
    elapsed = time.perf_counter() - start_time
    recovery_rate = len(recovered) / elapsed if elapsed > 0 else 0

    print(f"Recovered {len(recovered)} expired leases in {elapsed:.3f}s -> {recovery_rate:.1f} recoveries/sec")
    return {
        "operation": f"Lease Recovery ({count} expired executions)",
        "total_recovered": len(recovered),
        "elapsed_seconds": round(elapsed, 4),
        "throughput_ops_sec": round(recovery_rate, 2),
        "latency_ms": round(elapsed * 1000, 2),
    }


async def benchmark_scheduler_throughput(session_maker, schedules_count: int = 50) -> Dict[str, Any]:
    print(f"\n--- Running Benchmark 4: Scheduler Evaluation & Generation ({schedules_count} schedules) ---")
    async with session_maker() as session:
        await cleanup_data(session)
        job = JobDefinition(name="bench_sched_job", category="bench_sched", priority=1, max_retries=3)
        session.add(job)
        await session.commit()
        await session.refresh(job)

        now = datetime.now(timezone.utc)
        for i in range(schedules_count):
            sched = ScheduleDefinition(
                name=f"sched_{i}",
                schedule_type="recurring",
                enabled=True,
                configuration={"interval_seconds": 10},
                payload={"job_definition_id": job.id},
                created_at=now - timedelta(minutes=5),
            )
            session.add(sched)
        await session.commit()

    start_time = time.perf_counter()
    async with session_maker() as session:
        occurrences = await evaluate_due_schedules(session, now=now)
        await session.commit()
    elapsed = time.perf_counter() - start_time

    rate = len(occurrences) / elapsed if elapsed > 0 else 0
    print(f"Evaluated & generated {len(occurrences)} occurrences in {elapsed:.3f}s ({rate:.1f} occurrences/sec)")
    return {
        "operation": f"Scheduler Evaluation ({schedules_count} schedules)",
        "occurrences_generated": len(occurrences),
        "elapsed_seconds": round(elapsed, 4),
        "throughput_ops_sec": round(rate, 2),
        "latency_ms": round(elapsed * 1000, 2),
    }


async def benchmark_workflow_dispatch_latency(session_maker, workflow_count: int = 20) -> Dict[str, Any]:
    print(f"\n--- Running Benchmark 5: Workflow DAG Dispatch Latency ({workflow_count} runs) ---")
    async with session_maker() as session:
        await cleanup_data(session)

        # Create 4 jobs for diamond DAG
        j_root = JobDefinition(name="root_job", category="etl")
        j_a = JobDefinition(name="branch_a_job", category="etl")
        j_b = JobDefinition(name="branch_b_job", category="etl")
        j_join = JobDefinition(name="join_job", category="etl")
        session.add_all([j_root, j_a, j_b, j_join])
        await session.commit()

        tasks_def = [
            {"name": "root", "job_definition_id": j_root.id, "dependencies": []},
            {"name": "branch_a", "job_definition_id": j_a.id, "dependencies": ["root"]},
            {"name": "branch_b", "job_definition_id": j_b.id, "dependencies": ["root"]},
            {"name": "join", "job_definition_id": j_join.id, "dependencies": ["branch_a", "branch_b"]},
        ]
        wf_def = await create_workflow(session, name="bench_diamond_dag", tasks=tasks_def)
        await session.commit()

        run_ids = []
        for _ in range(workflow_count):
            run = await create_workflow_run(session, workflow_id=wf_def.id)
            run_ids.append(run.id)
        await session.commit()

    start_time = time.perf_counter()
    dispatched_total = 0

    for r_id in run_ids:
        async with session_maker() as session:
            dispatched = await resolve_and_dispatch(session, workflow_run_id=r_id)
            dispatched_total += len(dispatched.dispatched_task_ids)
            await session.commit()

    elapsed = time.perf_counter() - start_time
    avg_latency = (elapsed / workflow_count * 1000) if workflow_count > 0 else 0
    throughput = workflow_count / elapsed if elapsed > 0 else 0

    print(f"Dispatched {dispatched_total} initial tasks across {workflow_count} workflows in {elapsed:.3f}s -> {avg_latency:.2f}ms/wf")
    return {
        "operation": f"Workflow DAG Dispatch ({workflow_count} DAG runs)",
        "tasks_dispatched": dispatched_total,
        "elapsed_seconds": round(elapsed, 4),
        "avg_latency_ms": round(avg_latency, 2),
        "throughput_workflows_sec": round(throughput, 2),
    }


async def benchmark_queue_delivery_throughput(session_maker, n: int = 100) -> Dict[str, Any]:
    print(f"\n--- Running Benchmark 6: Queue Delivery Throughput (PostgreSQL -> Redis, n={n}) ---")
    async with session_maker() as session:
        await cleanup_data(session)

        job = JobDefinition(name="bench_delivery_job", category="bench_queue", priority=5)
        session.add(job)
        await session.commit()
        await session.refresh(job)

        for i in range(n):
            exc = Execution(
                job_definition_id=job.id,
                category="bench_queue",
                status="QUEUED",
                priority=i % 10,
            )
            session.add(exc)
        await session.commit()

    r_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    r_client = redis.from_url(r_url, decode_responses=True)
    start_time = time.perf_counter()
    async with session_maker() as session:
        result = await session.execute(select(Execution.id).filter(Execution.status == "QUEUED"))
        exec_ids = [row[0] for row in result.all()]
        res = deliver_executions_to_redis(r_client, exec_ids)
        delivered_count = res.enqueued
    elapsed = time.perf_counter() - start_time
    throughput = delivered_count / elapsed if elapsed > 0 else 0

    print(f"Delivered {delivered_count}/{n} to Redis queue in {elapsed:.3f}s ({throughput:.1f} items/sec)")
    return {
        "operation": "Queue Delivery Throughput (Postgres to Redis)",
        "total_operations": delivered_count,
        "elapsed_seconds": round(elapsed, 4),
        "throughput_ops_sec": round(throughput, 2),
    }


async def run_all_benchmarks():
    print("=" * 65)
    print("FLOWFORGE SYSTEM PERFORMANCE & RELIABILITY BENCHMARK")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print(f"Database: {DATABASE_URL}")
    print("=" * 65)

    engine, session_maker = await setup_db()

    results = []
    try:
        results.append(await benchmark_claim_throughput(session_maker, n=100))
        results.append(await benchmark_concurrent_worker_claims(session_maker, total_executions=100, workers=5))
        results.append(await benchmark_lease_recovery_latency(session_maker, count=200))
        results.append(await benchmark_scheduler_throughput(session_maker, schedules_count=50))
        results.append(await benchmark_workflow_dispatch_latency(session_maker, workflow_count=20))
        results.append(await benchmark_queue_delivery_throughput(session_maker, n=100))
    finally:
        async with session_maker() as session:
            await cleanup_data(session)
        await engine.dispose()

    print("\n" + "=" * 65)
    print("BENCHMARK SUMMARY")
    print("=" * 65)
    for r in results:
        rate = r.get("throughput_ops_sec") or r.get("throughput_workflows_sec")
        print(f"• {r['operation']}: {rate} items/sec | elapsed: {r['elapsed_seconds']}s")
    print("=" * 65)
    return results


if __name__ == "__main__":
    asyncio.run(run_all_benchmarks())
