import asyncio
from contextlib import suppress
from datetime import timedelta
import logging
import os
import socket
import sys

import redis
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import selectinload

from execution_claim import (
    ExecutionClaim,
    claim_execution,
    claim_next_execution,
    complete_execution,
    fail_execution,
    renew_lease,
    start_execution,
)
from models import Execution, JobDefinition, Worker, WorkflowTask, WorkflowTaskExecution
from task_registry import default_task_registry
from worker_registry import heartbeat_worker, register_worker
from config import safe_int, safe_float

logger = logging.getLogger("flowforge.worker")

DB_URL = os.getenv("DATABASE_URL")
if DB_URL:
    engine = create_async_engine(DB_URL, echo=False)
    AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)
else:
    engine = None
    AsyncSessionLocal = None
WORKER_ID = os.getenv("WORKER_ID", f"{socket.gethostname()}-{os.getpid()}")
LEASE_DURATION = timedelta(seconds=safe_int(os.getenv("LEASE_DURATION_SECONDS", "30"), 30))
HEARTBEAT_INTERVAL_SECONDS = safe_float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "10"), 10.0)
LEASE_RENEW_INTERVAL_SECONDS = safe_float(os.getenv("LEASE_RENEW_INTERVAL_SECONDS", "10"), 10.0)


async def register_current_worker(worker_id: str):
    async with AsyncSessionLocal() as session:
        return await register_worker(session, worker_id)


async def send_heartbeat(worker_id: str):
    async with AsyncSessionLocal() as session:
        return await heartbeat_worker(session, worker_id)


async def heartbeat_forever(worker_id: str):
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
        try:
            await send_heartbeat(worker_id)
        except Exception as error:
            print(f"Worker heartbeat failed for {worker_id}: {error}")


async def renew_lease_periodically(
    claim_holder: list[ExecutionClaim | None],
    lease_lost: asyncio.Event | None = None,
):
    """Periodically extend lease and signal when ownership is lost."""
    while True:
        await asyncio.sleep(LEASE_RENEW_INTERVAL_SECONDS)
        current_claim = claim_holder[0]
        if not current_claim:
            break
        try:
            async with AsyncSessionLocal() as session:
                renewed = await renew_lease(session, current_claim, LEASE_DURATION)
                if renewed:
                    claim_holder[0] = renewed
                else:
                    logger.warning("Lease ownership lost for execution %s", current_claim.execution_id)
                    if lease_lost is not None:
                        lease_lost.set()
                    break
        except Exception as err:
            print(f"Error renewing lease for execution {current_claim.execution_id}: {err}")


async def execute_task_payload(
    session,
    execution: Execution,
) -> tuple[str, str | None, dict | None]:
    """Inspect execution and dispatch to registered task handler."""
    task_type = "standard"
    params = {}
    timeout_sec = None

    # Check if this execution is tied to a workflow task execution
    if execution.workflow_task_execution and execution.workflow_task_execution.workflow_task:
        wf_task = execution.workflow_task_execution.workflow_task
        task_type = wf_task.task_type or "standard"
        if wf_task.config and isinstance(wf_task.config, dict):
            params = wf_task.config.get("params") or wf_task.config
            timeout_sec = wf_task.config.get("timeout_seconds") or wf_task.config.get("timeout")

    # Or if tied to a standalone JobDefinition
    elif execution.job_definition_id:
        job_def = await session.get(JobDefinition, execution.job_definition_id)
        if job_def and job_def.payload and isinstance(job_def.payload, dict):
            task_type = job_def.payload.get("task_type") or job_def.payload.get("type") or "standard"
            params = job_def.payload.get("params") or job_def.payload
            timeout_sec = job_def.payload.get("timeout_seconds") or job_def.payload.get("timeout")

    result = await default_task_registry.execute(
        task_type=task_type,
        params=params,
        timeout_seconds=float(timeout_sec) if timeout_sec else None,
    )
    return result.status, result.error, result.output


async def claim_and_run(execution_id: int, worker_id: str = WORKER_ID, redis_conn=None):
    async with AsyncSessionLocal() as session:
        claim = await claim_execution(
            session,
            execution_id,
            worker_id,
            LEASE_DURATION,
            redis_client=redis_conn,
        )

        if claim is None:
            print(f"Execution {execution_id} was not eligible or not queued; skipping it.")
            return

        print(
            f"Claimed execution {execution_id} as {worker_id} "
            f"until {claim.lease_until.isoformat()}..."
        )

        if not await start_execution(session, claim, redis_client=redis_conn):
            print(f"Execution {execution_id} is no longer owned by {worker_id}; skipping it.")
            return

        # Fetch execution with relationships for task config
        execution_row = await session.get(
            Execution,
            claim.execution_id,
            options=[
                selectinload(Execution.workflow_task_execution).selectinload(
                    WorkflowTaskExecution.workflow_task
                )
            ],
        )

    # Monitor lease ownership while executing. If an administrator disables the
    # worker or ownership is otherwise lost, cancel the async handler rather than
    # allowing it to continue indefinitely after another worker may retry it.
    claim_holder = [claim]
    lease_lost = asyncio.Event()
    renewer_task = asyncio.create_task(renew_lease_periodically(claim_holder, lease_lost))
    async def run_handler():
        async with AsyncSessionLocal() as task_session:
            return await execute_task_payload(task_session, execution_row)
    execution_task = asyncio.create_task(run_handler())
    lease_lost_task = asyncio.create_task(lease_lost.wait())

    try:
        done, _ = await asyncio.wait(
            {execution_task, lease_lost_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if lease_lost_task in done and not execution_task.done():
            execution_task.cancel()
            with suppress(asyncio.CancelledError):
                await execution_task
            # The authoritative transition/retry is performed by the control
            # plane that revoked the lease. Do not overwrite its state here.
            return
        status, error_msg, output = await execution_task

        latest_claim = claim_holder[0] or claim
        async with AsyncSessionLocal() as session:
            if status == "SUCCEEDED":
                logger.info("Execution %s completed successfully", execution_id)
                if not await complete_execution(session, latest_claim, redis_client=redis_conn):
                    print(f"Execution {execution_id} was lost before completion.")
            else:
                logger.warning("Execution %s failed", execution_id)
                if not await fail_execution(session, latest_claim, error_summary=error_msg, redis_client=redis_conn):
                    print(f"Execution {execution_id} was lost before recording failure.")

    except Exception as exc:
        logger.error("Unexpected error executing %s (%s)", execution_id, type(exc).__name__)
        latest_claim = claim_holder[0] or claim
        async with AsyncSessionLocal() as session:
            await fail_execution(
                session, latest_claim,
                error_summary=f"Task execution failed ({type(exc).__name__})",
                redis_client=redis_conn,
            )
    finally:
        for task in (execution_task, lease_lost_task, renewer_task):
            if not task.done():
                task.cancel()
        for task in (execution_task, lease_lost_task, renewer_task):
            with suppress(asyncio.CancelledError, Exception):
                await task


async def run_worker(redis_conn):
    await register_current_worker(WORKER_ID)
    heartbeat_task = asyncio.create_task(heartbeat_forever(WORKER_ID))

    try:
        print(f"Worker {WORKER_ID} is ready! Listening to 'flowforge:queue'...")
        while True:
            # Check if this worker is disabled before pulling or processing work
            async with AsyncSessionLocal() as session:
                worker_record = await session.get(Worker, WORKER_ID)
                if worker_record is not None and worker_record.status == "DISABLED":
                    await asyncio.sleep(2.0)
                    continue

            job = await asyncio.to_thread(
                redis_conn.brpop,
                "flowforge:queue",
                timeout=5,
            )

            if job:
                queue_name, execution_id = job
                print(f"\n--- INCOMING WORK ---")
                print(f"Got a job from Redis! Execution ID: {execution_id}")
                await claim_and_run(int(execution_id), redis_conn=redis_conn)
    finally:
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task


def main():
    print("Worker process starting up...")

    r_url = os.getenv("REDIS_URL")
    if not r_url:
        print("Worker can't find REDIS_URL!")
        sys.exit(1)

    redis_conn = redis.from_url(r_url, decode_responses=True)

    try:
        asyncio.run(run_worker(redis_conn))
    except KeyboardInterrupt:
        print("Worker shutting down...")
    except Exception as error:
        print(f"Worker stopped: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
