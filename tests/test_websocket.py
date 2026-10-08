"""Focused tests for WebSocket real-time live execution events, RBAC, subscriptions, and lifecycle transitions."""

import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timedelta, timezone
import json
import os
from typing import Any, Optional
import urllib.parse
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
    from database import Base, get_db
    from events import (
        EVENT_EXECUTION_CANCELLED,
        EVENT_EXECUTION_CLAIMED,
        EVENT_EXECUTION_DEAD_LETTERED,
        EVENT_EXECUTION_FAILED,
        EVENT_EXECUTION_PRIORITY_CHANGED,
        EVENT_EXECUTION_QUEUED,
        EVENT_EXECUTION_RETRY_WAITING,
        EVENT_EXECUTION_RUNNING,
        EVENT_EXECUTION_SUCCEEDED,
        ExecutionEvent,
        create_event,
        manager,
        publish_event,
    )
    from execution_claim import (
        cancel_execution,
        claim_execution,
        complete_execution,
        start_execution,
        update_execution_priority,
    )
    from execution_retry import transition_failed_execution
    from main import app
    from models import (
        ConcurrencyLimitPolicy,
        Execution,
        JobDefinition,
        RateLimitPolicy,
        RateLimitRecord,
        User,
        WorkflowDefinition,
        WorkflowRun,
    )


class AsyncWebSocketSession:
    """Session wrapper for testing ASGI WebSocket interactions."""

    def __init__(
        self,
        to_server_q: asyncio.Queue,
        from_server_q: asyncio.Queue,
        app_task: asyncio.Task,
    ):
        self.to_server_q = to_server_q
        self.from_server_q = from_server_q
        self.app_task = app_task
        self.accepted = False
        self.close_code: Optional[int] = None

    async def send_text(self, text: str):
        await self.to_server_q.put({"type": "websocket.receive", "text": text})

    async def send_json(self, data: Any):
        await self.send_text(json.dumps(data))

    async def receive_text(self, timeout: float = 2.0) -> str:
        msg = await asyncio.wait_for(self.from_server_q.get(), timeout=timeout)
        if msg["type"] == "websocket.send":
            return msg.get("text", "")
        if msg["type"] == "websocket.close":
            self.close_code = msg.get("code", 1000)
            raise ConnectionResetError(f"WebSocket closed with code {self.close_code}")
        raise RuntimeError(f"Unexpected ASGI message: {msg}")

    async def receive_json(self, timeout: float = 2.0) -> Any:
        text = await self.receive_text(timeout=timeout)
        return json.loads(text)

    async def close(self, code: int = 1000):
        if not self.app_task.done():
            if self.accepted:
                await self.to_server_q.put({"type": "websocket.disconnect", "code": code})
            try:
                await asyncio.wait_for(self.app_task, timeout=1.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                self.app_task.cancel()


class AsyncWebSocketClient:
    """Lightweight ASGI WebSocket test client."""

    def __init__(self, app_instance):
        self.app = app_instance

    @asynccontextmanager
    async def connect(
        self,
        path: str,
        token: Optional[str] = None,
        headers: Optional[dict[str, str]] = None,
        subprotocols: Optional[list[str]] = None,
    ):
        parsed = urllib.parse.urlparse(path)
        query = parsed.query
        if token and "token=" not in query:
            query = f"token={token}" if not query else f"{query}&token={token}"

        raw_headers = []
        if headers:
            for k, v in headers.items():
                raw_headers.append((k.lower().encode("latin1"), v.encode("latin1")))

        subprotocols = subprotocols or []

        scope = {
            "type": "websocket",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "scheme": "ws",
            "path": parsed.path,
            "raw_path": parsed.path.encode("ascii"),
            "query_string": query.encode("ascii"),
            "headers": raw_headers,
            "subprotocols": subprotocols,
        }

        to_server_q = asyncio.Queue()
        from_server_q = asyncio.Queue()

        async def receive():
            return await to_server_q.get()

        async def send(message):
            await from_server_q.put(message)

        await to_server_q.put({"type": "websocket.connect"})
        app_task = asyncio.create_task(self.app(scope, receive, send))
        session = AsyncWebSocketSession(to_server_q, from_server_q, app_task)

        first_resp = await asyncio.wait_for(from_server_q.get(), timeout=2.0)
        if first_resp["type"] == "websocket.accept":
            session.accepted = True
        elif first_resp["type"] == "websocket.close":
            session.accepted = False
            session.close_code = first_resp.get("code", 1000)
        else:
            raise RuntimeError(f"Unexpected initial ASGI response: {first_resp}")

        try:
            yield session
        finally:
            await session.close()


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
        await session.execute(Execution.__table__.delete())
        await session.execute(JobDefinition.__table__.delete())
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


@pytest.fixture(autouse=True)
def reset_ws_manager():
    manager.active_connections.clear()
    yield
    manager.active_connections.clear()


@pytest_asyncio.fixture
async def ws_client():
    return AsyncWebSocketClient(app)


@pytest_asyncio.fixture
async def operator_token(session_factory):
    async with session_factory() as session:
        user = await create_user(session, "operator_user", "password123", role=ROLE_OPERATOR)
        await session.commit()
        return create_access_token(user)


@pytest_asyncio.fixture
async def observer_token(session_factory):
    async with session_factory() as session:
        user = await create_user(session, "observer_user", "password123", role=ROLE_OBSERVER)
        await session.commit()
        return create_access_token(user)


@pytest_asyncio.fixture
async def admin_token(session_factory):
    async with session_factory() as session:
        user = await create_user(session, "admin_user", "password123", role=ROLE_ADMIN)
        await session.commit()
        return create_access_token(user)


# ---------------------------------------------------------------------------
# Authentication & Handshake Tests
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_unauthenticated_connection_rejected(ws_client):
    async with ws_client.connect("/ws/events") as session:
        assert session.accepted is False
        assert session.close_code == 1008


@pytest.mark.asyncio
async def test_invalid_token_rejected(ws_client):
    async with ws_client.connect("/ws/events?token=invalid.jwt.token") as session:
        assert session.accepted is False
        assert session.close_code == 1008


@pytest.mark.asyncio
async def test_authenticated_connection_query_param(ws_client, operator_token):
    async with ws_client.connect(f"/ws/events?token={operator_token}") as session:
        assert session.accepted is True
        ack = await session.receive_json()
        assert ack["type"] == "connected"
        assert ack["user"] == "operator_user"
        assert ack["role"] == ROLE_OPERATOR
        assert ack["subscription"]["all"] is True


@pytest.mark.asyncio
async def test_authenticated_connection_header(ws_client, observer_token):
    headers = {"Authorization": f"Bearer {observer_token}"}
    async with ws_client.connect("/ws/events", headers=headers) as session:
        assert session.accepted is True
        ack = await session.receive_json()
        assert ack["type"] == "connected"
        assert ack["user"] == "observer_user"
        assert ack["role"] == ROLE_OBSERVER


@pytest.mark.asyncio
async def test_authenticated_connection_subprotocol(ws_client, admin_token):
    subprotocols = [f"token.{admin_token}"]
    async with ws_client.connect("/ws/events", subprotocols=subprotocols) as session:
        assert session.accepted is True
        ack = await session.receive_json()
        assert ack["type"] == "connected"
        assert ack["user"] == "admin_user"
        assert ack["role"] == ROLE_ADMIN


# ---------------------------------------------------------------------------
# Heartbeat / Ping-Pong Tests
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_ping_pong_interaction(ws_client, operator_token):
    async with ws_client.connect(f"/ws/events?token={operator_token}") as session:
        await session.receive_json()  # Consume initial connected message
        await session.send_json({"action": "ping"})
        pong = await session.receive_json()
        assert pong["type"] == "pong"
        assert "timestamp" in pong


# ---------------------------------------------------------------------------
# Subscription Filtering Tests
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_subscription_filter_execution_id(ws_client, operator_token):
    async with ws_client.connect(f"/ws/events?token={operator_token}") as session:
        await session.receive_json()  # Connected ACK

        # Subscribe specifically to execution 100
        await session.send_json({"action": "subscribe", "filter": {"execution_id": 100}})
        sub_ack = await session.receive_json()
        assert sub_ack["type"] == "subscribed"
        assert sub_ack["subscription"]["all"] is False
        assert sub_ack["subscription"]["execution_ids"] == [100]

        # Publish an event for execution 999 (should be filtered out)
        event_other = create_event(EVENT_EXECUTION_RUNNING, execution_id=999, status="RUNNING")
        manager.broadcast_local(event_other)

        # Publish an event for execution 100 (should be received)
        event_target = create_event(EVENT_EXECUTION_RUNNING, execution_id=100, status="RUNNING")
        manager.broadcast_local(event_target)

        received = await session.receive_json()
        assert received["execution_id"] == 100
        assert received["event_type"] == EVENT_EXECUTION_RUNNING


@pytest.mark.asyncio
async def test_subscription_filter_workflow_run_id(ws_client, operator_token):
    async with ws_client.connect(f"/ws/events?token={operator_token}") as session:
        await session.receive_json()  # Connected ACK

        # Subscribe specifically to workflow run 55
        await session.send_json({"action": "subscribe", "filter": {"workflow_run_id": 55}})
        sub_ack = await session.receive_json()
        assert sub_ack["type"] == "subscribed"
        assert sub_ack["subscription"]["workflow_run_ids"] == [55]

        # Event for different workflow run
        manager.broadcast_local(create_event(EVENT_EXECUTION_RUNNING, workflow_run_id=99))
        # Event for matching workflow run
        manager.broadcast_local(
            create_event(EVENT_EXECUTION_RUNNING, workflow_run_id=55, execution_id=200)
        )

        received = await session.receive_json()
        assert received["workflow_run_id"] == 55
        assert received["execution_id"] == 200


@pytest.mark.asyncio
async def test_subscription_filter_all_reset(ws_client, operator_token):
    async with ws_client.connect(f"/ws/events?token={operator_token}") as session:
        await session.receive_json()  # Connected ACK

        # First filter down to execution 10
        await session.send_json({"action": "subscribe", "filter": {"execution_id": 10}})
        await session.receive_json()

        # Reset to all
        await session.send_json({"action": "subscribe", "filter": "all"})
        sub_ack = await session.receive_json()
        assert sub_ack["subscription"]["all"] is True

        # Now any event should arrive
        manager.broadcast_local(create_event(EVENT_EXECUTION_QUEUED, execution_id=777))
        received = await session.receive_json()
        assert received["execution_id"] == 777


# ---------------------------------------------------------------------------
# State Snapshot Tests
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_snapshot_request(ws_client, operator_token, session_factory):
    # Insert dummy execution
    async with session_factory() as session:
        job = JobDefinition(name="snapshot_test_job")
        session.add(job)
        await session.flush()
        execution = Execution(
            job_definition_id=job.id,
            status="QUEUED",
            priority=5,
        )
        session.add(execution)
        await session.commit()
        exec_id = execution.id

    async with ws_client.connect(f"/ws/events?token={operator_token}") as session:
        await session.receive_json()  # Connected ACK

        await session.send_json({"action": "snapshot", "execution_id": exec_id})
        snapshot_resp = await session.receive_json()
        assert snapshot_resp["type"] == "snapshot"
        assert len(snapshot_resp["executions"]) == 1
        assert snapshot_resp["executions"][0]["id"] == exec_id
        assert snapshot_resp["executions"][0]["status"] == "QUEUED"


# ---------------------------------------------------------------------------
# Lifecycle Event Publication Tests
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_execution_claim_and_completion_events(ws_client, operator_token, session_factory):
    async with ws_client.connect(f"/ws/events?token={operator_token}") as session:
        await session.receive_json()  # Connected ACK

        async with session_factory() as db:
            job = JobDefinition(name="lifecycle_job")
            db.add(job)
            await db.flush()
            ex = Execution(job_definition_id=job.id, status="QUEUED", priority=0)
            db.add(ex)
            await db.commit()
            ex_id = ex.id

        # 1. Claim execution -> triggers EVENT_EXECUTION_CLAIMED
        async with session_factory() as db:
            claimed = await claim_execution(
                db, ex_id, worker_id="worker-w1", lease_duration=timedelta(seconds=30)
            )
            assert claimed is not None

        event1 = await session.receive_json()
        assert event1["event_type"] == EVENT_EXECUTION_CLAIMED
        assert event1["execution_id"] == ex_id
        assert event1["worker_id"] == "worker-w1"

        # 2. Start execution -> triggers EVENT_EXECUTION_RUNNING
        async with session_factory() as db:
            started = await start_execution(db, claimed)
            assert started is True

        event2 = await session.receive_json()
        assert event2["event_type"] == EVENT_EXECUTION_RUNNING
        assert event2["execution_id"] == ex_id

        # 3. Complete execution -> triggers EVENT_EXECUTION_SUCCEEDED
        async with session_factory() as db:
            completed = await complete_execution(db, claimed)
            assert completed is True

        event3 = await session.receive_json()
        assert event3["event_type"] == EVENT_EXECUTION_SUCCEEDED
        assert event3["execution_id"] == ex_id


@pytest.mark.asyncio
async def test_execution_failure_retry_and_dead_letter_events(
    ws_client, operator_token, session_factory
):
    async with ws_client.connect(f"/ws/events?token={operator_token}") as session:
        await session.receive_json()  # Connected ACK

        async with session_factory() as db:
            job = JobDefinition(name="fail_job", max_retries=1)
            db.add(job)
            await db.flush()
            ex = Execution(job_definition_id=job.id, status="FAILED", attempt=0)
            db.add(ex)
            await db.commit()
            ex_id = ex.id

        # Retryable failure -> triggers EVENT_EXECUTION_RETRY_WAITING
        async with session_factory() as db:
            res = await transition_failed_execution(
                db, ex_id, max_retries=1
            )
            assert res == "RETRY_WAIT"

        event1 = await session.receive_json()
        assert event1["event_type"] == EVENT_EXECUTION_RETRY_WAITING
        assert event1["execution_id"] == ex_id

        # Re-run and fail permanently -> triggers EVENT_EXECUTION_DEAD_LETTERED
        async with session_factory() as db:
            row = await db.get(Execution, ex_id)
            row.status = "FAILED"
            row.attempt = 1
            await db.commit()

        async with session_factory() as db:
            res2 = await transition_failed_execution(
                db, ex_id, max_retries=1
            )
            assert res2 == "DEAD_LETTERED"

        event2 = await session.receive_json()
        assert event2["event_type"] == EVENT_EXECUTION_DEAD_LETTERED
        assert event2["execution_id"] == ex_id


@pytest.mark.asyncio
async def test_execution_cancellation_event(ws_client, operator_token, session_factory):
    async with ws_client.connect(f"/ws/events?token={operator_token}") as session:
        await session.receive_json()  # Connected ACK

        async with session_factory() as db:
            job = JobDefinition(name="cancel_job")
            db.add(job)
            await db.flush()
            ex = Execution(job_definition_id=job.id, status="QUEUED")
            db.add(ex)
            await db.commit()
            ex_id = ex.id

        async with session_factory() as db:
            cancel_res = await cancel_execution(db, ex_id, reason="User clicked abort")
            assert cancel_res.cancelled is True

        event = await session.receive_json()
        assert event["event_type"] == EVENT_EXECUTION_CANCELLED
        assert event["execution_id"] == ex_id
        assert event["metadata"]["reason"] == "User clicked abort"


@pytest.mark.asyncio
async def test_execution_priority_changed_event(ws_client, operator_token, session_factory):
    async with ws_client.connect(f"/ws/events?token={operator_token}") as session:
        await session.receive_json()  # Connected ACK

        async with session_factory() as db:
            job = JobDefinition(name="priority_job")
            db.add(job)
            await db.flush()
            ex = Execution(job_definition_id=job.id, status="QUEUED", priority=1)
            db.add(ex)
            await db.commit()
            ex_id = ex.id

        async with session_factory() as db:
            updated = await update_execution_priority(db, ex_id, priority=50)
            assert updated is not None

        event = await session.receive_json()
        assert event["event_type"] == EVENT_EXECUTION_PRIORITY_CHANGED
        assert event["execution_id"] == ex_id
        assert event["metadata"]["priority"] == 50


# ---------------------------------------------------------------------------
# Resilience & Backpressure Tests
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_redis_failure_does_not_abort_postgres(session_factory):
    class BrokenRedis:
        def publish(self, *args, **kwargs):
            raise ConnectionError("Simulated Redis outage")

    async with session_factory() as db:
        job = JobDefinition(name="redis_outage_job")
        db.add(job)
        await db.flush()
        ex = Execution(job_definition_id=job.id, status="QUEUED")
        db.add(ex)
        await db.commit()
        ex_id = ex.id

    # Claim under broken Redis
    async with session_factory() as db:
        claimed = await claim_execution(
            db,
            ex_id,
            worker_id="worker-w1",
            lease_duration=timedelta(seconds=30),
            redis_client=BrokenRedis(),
        )
        assert claimed is not None
        assert claimed.execution_id == ex_id

    # Verify state in database persisted regardless of Redis failure
    async with session_factory() as db:
        row = await db.get(Execution, ex_id)
        assert row.status == "CLAIMED"
        assert row.worker_id == "worker-w1"


@pytest.mark.asyncio
async def test_backpressure_queue_bounded(ws_client, operator_token):
    async with ws_client.connect(f"/ws/events?token={operator_token}") as session:
        await session.receive_json()  # Connected ACK

        # Find the active connection object in the manager
        conn = next(iter(manager.active_connections))
        # Fill queue to maximum capacity (200)
        for i in range(200):
            conn.queue.put_nowait(create_event(EVENT_EXECUTION_QUEUED, execution_id=i))

        assert conn.queue.full()

        # Broadcasting an additional event should NOT raise QueueFull or crash
        overflow_event = create_event(EVENT_EXECUTION_QUEUED, execution_id=9999)
        manager.broadcast_local(overflow_event)

        # Connection remains alive and healthy
        assert conn.is_active is True
