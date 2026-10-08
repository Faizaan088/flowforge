"""Focused tests for FlowForge observability, Prometheus metrics, health checks, and summary API."""

from datetime import datetime, timedelta, timezone
import os
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL must point to an isolated PostgreSQL database",
)

if TEST_DATABASE_URL:
    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

    from auth import (
        ROLE_ADMIN,
        ROLE_OBSERVER,
        ROLE_OPERATOR,
        create_access_token,
        create_user,
    )
    from concurrency_policy import (
        check_and_record_admission,
        set_concurrency_limit_policy,
        set_rate_limit_policy,
    )
    from database import Base, get_db
    from events import (
        EVENT_EXECUTION_QUEUED,
        create_event,
        manager,
        publish_event,
    )
    from execution_claim import (
        cancel_execution,
        claim_execution,
        claim_next_execution,
        complete_execution,
        fail_execution,
        start_execution,
    )
    from execution_recovery import recover_expired_executions
    from execution_retry import transition_failed_execution
    from main import app
    from metrics import (
        ACTIVE_WORKERS,
        CONCURRENCY_ADMISSION_DENIED_TOTAL,
        DEAD_LETTERED_EXECUTION_COUNT,
        EVENT_PUBLISH_FAILURES_TOTAL,
        EVENTS_PUBLISHED_TOTAL,
        EXECUTION_CANCELLED_TOTAL,
        EXECUTION_CLAIM_FAILURES_TOTAL,
        EXECUTION_CLAIMS_TOTAL,
        EXECUTION_DEAD_LETTERED_TOTAL,
        EXECUTION_DURATION_SECONDS,
        EXECUTION_QUEUE_WAIT_SECONDS,
        EXECUTION_RECOVERED_TOTAL,
        EXECUTION_RETRIES_TOTAL,
        EXECUTIONS_TOTAL,
        QUEUED_EXECUTION_COUNT,
        RATE_LIMIT_ADMISSION_DENIED_TOTAL,
        RETRY_WAITING_EXECUTION_COUNT,
        RUNNING_EXECUTION_COUNT,
        SCHEDULER_OCCURRENCES_CREATED_TOTAL,
        WEBSOCKET_CONNECTIONS,
        WEBSOCKET_EVENTS_DROPPED_TOTAL,
        WEBSOCKET_EVENTS_SENT_TOTAL,
        WORKFLOW_FAILURES_TOTAL,
        WORKFLOW_RUN_DURATION_SECONDS,
        WORKFLOW_RUNS_TOTAL,
        refresh_queue_gauges,
        registry,
    )
    from models import (
        ConcurrencyLimitPolicy,
        Execution,
        JobDefinition,
        RateLimitPolicy,
        RateLimitRecord,
        ScheduleDefinition,
        ScheduleOccurrence,
        User,
        Worker,
        WorkflowDefinition,
        WorkflowRun,
    )
    from scheduler import evaluate_due_schedules
    from tests.test_policy_api import AsyncApiClient
    from worker_registry import heartbeat_worker, register_worker
    from workflow_engine import (
        create_workflow,
        create_workflow_run,
        resolve_dependencies,
        transition_workflow_run,
    )


@pytest_asyncio.fixture
async def session_factory():
    engine = create_async_engine(TEST_DATABASE_URL, echo=False, poolclass=NullPool)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    try:
        yield async_sessionmaker(engine, expire_on_commit=False)
    finally:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
        await engine.dispose()


@pytest_asyncio.fixture(autouse=True)
async def clear_database(session_factory):
    async with session_factory() as session:
        await session.execute(RateLimitRecord.__table__.delete())
        await session.execute(RateLimitPolicy.__table__.delete())
        await session.execute(ConcurrencyLimitPolicy.__table__.delete())
        await session.execute(WorkflowRun.__table__.delete())
        await session.execute(WorkflowDefinition.__table__.delete())
        await session.execute(ScheduleOccurrence.__table__.delete())
        await session.execute(ScheduleDefinition.__table__.delete())
        await session.execute(Execution.__table__.delete())
        await session.execute(JobDefinition.__table__.delete())
        await session.execute(Worker.__table__.delete())
        await session.execute(User.__table__.delete())
        await session.commit()


@pytest_asyncio.fixture(autouse=True)
async def override_get_db(session_factory):
    async def get_test_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = get_test_db
    try:
        yield
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest_asyncio.fixture
async def client():
    return AsyncApiClient(app)


@pytest_asyncio.fixture
async def observer_token(session_factory):
    async with session_factory() as session:
        user = await create_user(session, "obs_metrics_user", "password123", role=ROLE_OBSERVER)
        await session.commit()
        return create_access_token(user)


# ---------------------------------------------------------------------------
# 1. Endpoints: /metrics, /health, /health/live, /health/ready, /system/summary
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_metrics_endpoint_exposition(client):
    res = await client.request("GET", "/metrics")
    assert res.status_code == 200
    assert "text/plain" in res.headers.get("content-type", "")
    content = res.text
    # Verify core metric families exist in Prometheus output
    assert "flowforge_executions_total" in content
    assert "flowforge_execution_duration_seconds" in content
    assert "flowforge_execution_queue_wait_seconds" in content
    assert "flowforge_active_workers" in content
    assert "flowforge_queued_execution_count" in content


@pytest.mark.asyncio
async def test_request_correlation_middleware(client):
    # Without request ID -> server generates req_<hex>
    res1 = await client.request("GET", "/health")
    assert res1.status_code == 200
    assert "x-request-id" in res1.headers
    assert res1.headers["x-request-id"].startswith("req_")

    # With supplied request ID -> server preserves it
    res2 = await client.request(
        "GET", "/health", headers={"X-Request-ID": "custom-trace-12345"}
    )
    assert res2.status_code == 200
    assert res2.headers.get("x-request-id") == "custom-trace-12345"


@pytest.mark.asyncio
async def test_health_endpoints(client):
    # Standard health check
    res_h = await client.request("GET", "/health")
    assert res_h.status_code == 200
    assert res_h.json() == {"status": "healthy"}

    # Liveness probe
    res_l = await client.request("GET", "/health/live")
    assert res_l.status_code == 200
    assert res_l.json() == {"status": "alive"}

    # Readiness probe
    res_r = await client.request("GET", "/health/ready")
    assert res_r.status_code in (200, 503)
    data = res_r.json()
    assert "status" in data
    assert "database" in data


@pytest.mark.asyncio
async def test_system_summary_endpoint_rbac(client, observer_token, session_factory):
    # Unauthenticated rejected
    res_unauth = await client.request("GET", "/system/summary")
    assert res_unauth.status_code == 401

    # Insert mock records
    async with session_factory() as session:
        job = JobDefinition(name="sum_job")
        session.add(job)
        await session.flush()
        ex = Execution(job_definition_id=job.id, status="QUEUED")
        session.add(ex)
        await session.commit()

    # Authenticated accepted
    headers = {"Authorization": f"Bearer {observer_token}"}
    res = await client.request("GET", "/system/summary", headers=headers)
    assert res.status_code == 200
    data = res.json()
    assert data["status"] == "operational"
    assert data["queued_executions"] >= 1
    assert "active_workers" in data
    assert "running_executions" in data


# ---------------------------------------------------------------------------
# 2. Execution Lifecycle Metrics & Timings
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_execution_claim_and_completion_metrics(session_factory):
    async with session_factory() as db:
        job = JobDefinition(name="obs_job", category="etl")
        db.add(job)
        await db.flush()
        ex = Execution(
            job_definition_id=job.id,
            status="QUEUED",
            category="etl",
            created_at=datetime.now(timezone.utc) - timedelta(seconds=2),
        )
        db.add(ex)
        await db.commit()
        ex_id = ex.id

    # 1. Claim
    claims_before = EXECUTIONS_TOTAL.labels(status="claimed", category="etl")._value.get()
    async with session_factory() as db:
        claim = await claim_execution(db, ex_id, "worker-obs-1", timedelta(seconds=30))
        assert claim is not None
    claims_after = EXECUTIONS_TOTAL.labels(status="claimed", category="etl")._value.get()
    assert claims_after == claims_before + 1

    # 2. Start
    running_before = EXECUTIONS_TOTAL.labels(status="running", category="etl")._value.get()
    async with session_factory() as db:
        assert await start_execution(db, claim) is True
    running_after = EXECUTIONS_TOTAL.labels(status="running", category="etl")._value.get()
    assert running_after == running_before + 1

    # 3. Complete
    succ_before = EXECUTIONS_TOTAL.labels(status="succeeded", category="etl")._value.get()
    async with session_factory() as db:
        assert await complete_execution(db, claim) is True
    succ_after = EXECUTIONS_TOTAL.labels(status="succeeded", category="etl")._value.get()
    assert succ_after == succ_before + 1


@pytest.mark.asyncio
async def test_execution_failure_metrics(session_factory):
    async with session_factory() as db:
        job = JobDefinition(name="fail_job", category="default")
        db.add(job)
        await db.flush()
        ex = Execution(job_definition_id=job.id, status="QUEUED")
        db.add(ex)
        await db.commit()
        ex_id = ex.id

    async with session_factory() as db:
        claim = await claim_execution(db, ex_id, "worker-obs-1", timedelta(seconds=30))
        await start_execution(db, claim)

    failed_before = EXECUTIONS_TOTAL.labels(status="failed", category="default")._value.get()
    async with session_factory() as db:
        assert await fail_execution(db, claim, error_summary="Out of memory") is True
    failed_after = EXECUTIONS_TOTAL.labels(status="failed", category="default")._value.get()
    assert failed_after == failed_before + 1


@pytest.mark.asyncio
async def test_retry_and_dead_letter_metrics(session_factory):
    async with session_factory() as db:
        job = JobDefinition(name="retry_job")
        db.add(job)
        await db.flush()
        ex = Execution(job_definition_id=job.id, status="FAILED", attempt=0)
        db.add(ex)
        await db.commit()
        ex_id = ex.id

    # Transition to RETRY_WAIT
    retries_before = EXECUTION_RETRIES_TOTAL._value.get()
    async with session_factory() as db:
        res = await transition_failed_execution(db, ex_id, max_retries=1)
        assert res == "RETRY_WAIT"
    retries_after = EXECUTION_RETRIES_TOTAL._value.get()
    assert retries_after == retries_before + 1

    # Transition to DEAD_LETTERED
    async with session_factory() as db:
        row = await db.get(Execution, ex_id)
        row.status = "FAILED"
        row.attempt = 1
        await db.commit()

    dl_before = EXECUTION_DEAD_LETTERED_TOTAL._value.get()
    async with session_factory() as db:
        res2 = await transition_failed_execution(db, ex_id, max_retries=1)
        assert res2 == "DEAD_LETTERED"
    dl_after = EXECUTION_DEAD_LETTERED_TOTAL._value.get()
    assert dl_after == dl_before + 1


@pytest.mark.asyncio
async def test_cancellation_metrics(session_factory):
    async with session_factory() as db:
        job = JobDefinition(name="cancel_job")
        db.add(job)
        await db.flush()
        ex = Execution(job_definition_id=job.id, status="QUEUED")
        db.add(ex)
        await db.commit()
        ex_id = ex.id

    cancel_before = EXECUTION_CANCELLED_TOTAL._value.get()
    async with session_factory() as db:
        res = await cancel_execution(db, ex_id, reason="Operator clicked stop")
        assert res.cancelled is True
    cancel_after = EXECUTION_CANCELLED_TOTAL._value.get()
    assert cancel_after == cancel_before + 1


@pytest.mark.asyncio
async def test_recovery_metrics(session_factory):
    expired_time = datetime.now(timezone.utc) - timedelta(minutes=5)
    async with session_factory() as db:
        job = JobDefinition(name="rec_job")
        db.add(job)
        await db.flush()
        ex = Execution(
            job_definition_id=job.id,
            status="RUNNING",
            worker_id="w-dead",
            lease_until=expired_time,
        )
        db.add(ex)
        await db.commit()

    rec_before = EXECUTION_RECOVERED_TOTAL._value.get()
    async with session_factory() as db:
        recovered = await recover_expired_executions(db)
        assert len(recovered) >= 1
    rec_after = EXECUTION_RECOVERED_TOTAL._value.get()
    assert rec_after >= rec_before + 1


# ---------------------------------------------------------------------------
# 3. Policy Denial Metrics
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_concurrency_policy_denial_metrics(session_factory):
    async with session_factory() as db:
        job = JobDefinition(name="conc_metric_job")
        db.add(job)
        await db.flush()
        await set_concurrency_limit_policy(
            db, target_type="TASK_TYPE", target_id="CPU_BOUND", max_concurrency=1
        )
        await db.commit()

    # Pre-create a RUNNING execution simulating active capacity
    async with session_factory() as db:
        wf = await create_workflow(
            db,
            "conc_wf",
            tasks=[
                {"name": "t1", "task_type": "CPU_BOUND"},
                {"name": "t2", "task_type": "CPU_BOUND"},
            ],
        )
        w_run = await create_workflow_run(db, wf.id)
        te1 = w_run.task_executions[0]
        te2 = w_run.task_executions[1]

        ex1 = Execution(job_definition_id=job.id, status="RUNNING")
        db.add(ex1)
        await db.flush()
        te1.execution_id = ex1.id

        ex2 = Execution(job_definition_id=job.id, status="QUEUED")
        db.add(ex2)
        await db.flush()
        te2.execution_id = ex2.id
        await db.commit()

        denials_before = CONCURRENCY_ADMISSION_DENIED_TOTAL.labels(
            target_type="task_type"
        )._value.get()
        admitted, reason = await check_and_record_admission(db, ex2)
        assert admitted is False
        denials_after = CONCURRENCY_ADMISSION_DENIED_TOTAL.labels(
            target_type="task_type"
        )._value.get()
        assert denials_after == denials_before + 1


# ---------------------------------------------------------------------------
# 4. Worker Registry & Heartbeat Metrics
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_heartbeat_metrics(session_factory):
    async with session_factory() as db:
        await register_worker(db, "worker-metric-test")

    async with session_factory() as db:
        hb_ts = await heartbeat_worker(db, "worker-metric-test")
        assert hb_ts is not None


# ---------------------------------------------------------------------------
# 5. Workflow Metrics
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_workflow_completion_metrics(session_factory):
    async with session_factory() as db:
        wf = await create_workflow(db, "metric_wf", tasks=[{"name": "step1"}])
        w_run = await create_workflow_run(db, wf.id)
        w_run.started_at = datetime.now(timezone.utc) - timedelta(seconds=3)
        await db.commit()

    succ_before = WORKFLOW_RUNS_TOTAL.labels(status="succeeded")._value.get()
    transition_workflow_run(w_run, "RUNNING")
    transition_workflow_run(w_run, "SUCCEEDED")
    succ_after = WORKFLOW_RUNS_TOTAL.labels(status="succeeded")._value.get()
    assert succ_after == succ_before + 1


# ---------------------------------------------------------------------------
# 6. Events & WebSocket Metrics
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_events_and_websocket_metrics():
    class MockRedis:
        def publish(self, *args, **kwargs):
            return 1

    ev = create_event(EVENT_EXECUTION_QUEUED, execution_id=101)
    before_pub = EVENTS_PUBLISHED_TOTAL.labels(event_type=EVENT_EXECUTION_QUEUED)._value.get()
    publish_event(MockRedis(), ev)
    after_pub = EVENTS_PUBLISHED_TOTAL.labels(event_type=EVENT_EXECUTION_QUEUED)._value.get()
    assert after_pub == before_pub + 1


# ---------------------------------------------------------------------------
# 7. Security & Cardinality Protections
# ---------------------------------------------------------------------------
def test_metric_label_cardinality_protections():
    """Verify strictly no high-cardinality identifiers exist in metric label keys."""
    forbidden = {"execution_id", "workflow_run_id", "worker_id", "user_id", "job_id", "error"}
    for metric in registry.collect():
        for sample in metric.samples:
            for label_name in sample.labels:
                assert label_name not in forbidden, f"Forbidden label '{label_name}' on {metric.name}"


@pytest.mark.asyncio
async def test_no_sensitive_data_in_metrics_exposition(client, session_factory):
    secret_pass = "SuperSecretPassword123"
    async with session_factory() as session:
        user = await create_user(session, "sensitive_user", secret_pass, role=ROLE_OBSERVER)
        token = create_access_token(user)
        await session.commit()

    res = await client.request("GET", "/metrics")
    text = res.text
    # Neither raw passwords nor JWT tokens appear in the metrics output
    assert secret_pass not in text
    assert token not in text
