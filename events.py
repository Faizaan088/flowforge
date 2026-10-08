"""Live Execution Events and WebSocket stream management for FlowForge control-plane.

Provides:
- Strongly typed execution event models with unique event IDs, timestamps, and metadata.
- Durable state-aligned event publishing to Redis Pub/Sub without affecting PostgreSQL durability.
- High-performance FastAPI WebSocket connection manager with RBAC authorization and subscription filtering.
- Single background Redis Pub/Sub consumer task for efficient multi-instance event fan-out.
- Bounded client queues and backpressure management preventing memory leaks.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import time
import urllib.parse
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import WebSocket, status
from redis.exceptions import RedisError
from sqlalchemy.ext.asyncio import AsyncSession

from auth import (
    ROLE_ADMIN,
    ROLE_OBSERVER,
    ROLE_OPERATOR,
    decode_access_token,
    get_user_by_id,
)
from database import AsyncSessionLocal
from models import Execution, User

logger = logging.getLogger("flowforge.events")

# ---------------------------------------------------------------------------
# Event Constants
# ---------------------------------------------------------------------------
EVENT_EXECUTION_QUEUED = "execution:queued"
EVENT_EXECUTION_CLAIMED = "execution:claimed"
EVENT_EXECUTION_RUNNING = "execution:running"
EVENT_EXECUTION_SUCCEEDED = "execution:succeeded"
EVENT_EXECUTION_FAILED = "execution:failed"
EVENT_EXECUTION_RETRY_WAITING = "execution:retry_waiting"
EVENT_EXECUTION_DEAD_LETTERED = "execution:dead_lettered"
EVENT_EXECUTION_CANCELLED = "execution:cancelled"
EVENT_EXECUTION_RECOVERED = "execution:recovered"
EVENT_EXECUTION_PRIORITY_CHANGED = "execution:priority_changed"

EVENT_WORKFLOW_TASK_STATE_CHANGED = "workflow:task_state_changed"
EVENT_WORKFLOW_RUN_STATE_CHANGED = "workflow:run_state_changed"
EVENT_WORKFLOW_RUN_CANCELLED = "workflow:run_cancelled"

EVENTS_CHANNEL = "flowforge:events"


# ---------------------------------------------------------------------------
# Event Data Model
# ---------------------------------------------------------------------------
@dataclass
class ExecutionEvent:
    event_id: str
    event_type: str
    timestamp: str
    execution_id: Optional[int] = None
    job_id: Optional[int] = None
    workflow_id: Optional[int] = None
    workflow_run_id: Optional[int] = None
    workflow_task_execution_id: Optional[int] = None
    status: Optional[str] = None
    worker_id: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "timestamp": self.timestamp,
            "execution_id": self.execution_id,
            "job_id": self.job_id,
            "workflow_id": self.workflow_id,
            "workflow_run_id": self.workflow_run_id,
            "workflow_task_execution_id": self.workflow_task_execution_id,
            "status": self.status,
            "worker_id": self.worker_id,
            "metadata": self.metadata,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ExecutionEvent:
        return cls(
            event_id=data.get("event_id", ""),
            event_type=data.get("event_type", ""),
            timestamp=data.get("timestamp", ""),
            execution_id=data.get("execution_id"),
            job_id=data.get("job_id"),
            workflow_id=data.get("workflow_id"),
            workflow_run_id=data.get("workflow_run_id"),
            workflow_task_execution_id=data.get("workflow_task_execution_id"),
            status=data.get("status"),
            worker_id=data.get("worker_id"),
            metadata=data.get("metadata", {}),
        )

    @classmethod
    def from_json(cls, json_str: str) -> ExecutionEvent:
        return cls.from_dict(json.loads(json_str))


def create_event(
    event_type: str,
    execution_id: Optional[int] = None,
    status: Optional[str] = None,
    job_id: Optional[int] = None,
    workflow_id: Optional[int] = None,
    workflow_run_id: Optional[int] = None,
    workflow_task_execution_id: Optional[int] = None,
    worker_id: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> ExecutionEvent:
    """Create a standardized ExecutionEvent with monotonic ID and ISO-8601 UTC timestamp."""
    current_time = now or datetime.now(timezone.utc)
    event_id = f"evt_{int(current_time.timestamp() * 1000)}_{secrets.token_hex(4)}"
    return ExecutionEvent(
        event_id=event_id,
        event_type=event_type,
        timestamp=current_time.isoformat(),
        execution_id=execution_id,
        job_id=job_id,
        workflow_id=workflow_id,
        workflow_run_id=workflow_run_id,
        workflow_task_execution_id=workflow_task_execution_id,
        status=status,
        worker_id=worker_id,
        metadata=metadata or {},
    )


# ---------------------------------------------------------------------------
# Event Publication Layer
# ---------------------------------------------------------------------------
def publish_event(redis_client, event: ExecutionEvent) -> bool:
    """Publish an event to the Redis events channel.

    CRITICAL INVARIANT:
    - Must be invoked strictly AFTER durable PostgreSQL commit succeeds.
    - Errors in Redis delivery are logged and swallowed; they must NEVER fail
      or roll back PostgreSQL execution state.
    """
    if redis_client is None:
        # Also broadcast to in-process websocket manager
        manager.broadcast_local(event)
        return False
    try:
        redis_client.publish(EVENTS_CHANNEL, event.to_json())
        from metrics import EVENTS_PUBLISHED_TOTAL
        EVENTS_PUBLISHED_TOTAL.labels(event_type=event.event_type).inc()
        manager.broadcast_local(event)
        return True
    except Exception as err:
        from metrics import EVENT_PUBLISH_FAILURES_TOTAL
        EVENT_PUBLISH_FAILURES_TOTAL.labels(event_type=event.event_type).inc()
        logger.warning(
            "Event publication failed for %s (%s): %s",
            event.event_id,
            event.event_type,
            err,
        )
        # Still deliver locally to any connected clients on this node
        manager.broadcast_local(event)
        return False


async def publish_event_async(redis_client, event: ExecutionEvent) -> bool:
    """Async variant of publish_event."""
    if redis_client is None:
        manager.broadcast_local(event)
        return False
    try:
        payload = event.to_json()
        publish_fn = getattr(redis_client, "publish", None)
        if publish_fn is not None:
            if asyncio.iscoroutinefunction(publish_fn):
                await redis_client.publish(EVENTS_CHANNEL, payload)
            else:
                redis_client.publish(EVENTS_CHANNEL, payload)
        manager.broadcast_local(event)
        return True
    except Exception as err:
        logger.warning(
            "Async event publication failed for %s (%s): %s",
            event.event_id,
            event.event_type,
            err,
        )
        manager.broadcast_local(event)
        return False


# ---------------------------------------------------------------------------
# WebSocket Subscriptions and Connections
# ---------------------------------------------------------------------------
@dataclass
class WebSocketSubscription:
    all_events: bool = True
    execution_ids: set[int] = field(default_factory=set)
    workflow_run_ids: set[int] = field(default_factory=set)
    workflow_ids: set[int] = field(default_factory=set)
    job_ids: set[int] = field(default_factory=set)

    def matches(self, event: ExecutionEvent) -> bool:
        if self.all_events:
            return True
        if event.execution_id is not None and event.execution_id in self.execution_ids:
            return True
        if event.workflow_run_id is not None and event.workflow_run_id in self.workflow_run_ids:
            return True
        if event.workflow_id is not None and event.workflow_id in self.workflow_ids:
            return True
        if event.job_id is not None and event.job_id in self.job_ids:
            return True
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "all": self.all_events,
            "execution_ids": sorted(list(self.execution_ids)),
            "workflow_run_ids": sorted(list(self.workflow_run_ids)),
            "workflow_ids": sorted(list(self.workflow_ids)),
            "job_ids": sorted(list(self.job_ids)),
        }


class WebSocketConnection:
    """Represents an active authenticated WebSocket client session."""

    def __init__(self, websocket: WebSocket, user: User):
        self.websocket = websocket
        self.user = user
        self.subscription = WebSocketSubscription(all_events=True)
        self.queue: asyncio.Queue[ExecutionEvent] = asyncio.Queue(maxsize=200)
        self.is_active = True

    def update_subscription(self, filters: dict[str, Any]) -> None:
        """Update subscription filters based on client request."""
        if filters.get("all") is True:
            self.subscription = WebSocketSubscription(all_events=True)
            return

        all_flag = False
        exec_ids = set()
        wf_run_ids = set()
        wf_ids = set()
        j_ids = set()

        if "execution_id" in filters:
            exec_ids.add(int(filters["execution_id"]))
        if "execution_ids" in filters:
            exec_ids.update(int(x) for x in filters["execution_ids"])

        if "workflow_run_id" in filters:
            wf_run_ids.add(int(filters["workflow_run_id"]))
        if "workflow_run_ids" in filters:
            wf_run_ids.update(int(x) for x in filters["workflow_run_ids"])

        if "workflow_id" in filters:
            wf_ids.add(int(filters["workflow_id"]))
        if "workflow_ids" in filters:
            wf_ids.update(int(x) for x in filters["workflow_ids"])

        if "job_id" in filters:
            j_ids.add(int(filters["job_id"]))
        if "job_ids" in filters:
            j_ids.update(int(x) for x in filters["job_ids"])

        # If no specific filters were specified, default back to all
        if not (exec_ids or wf_run_ids or wf_ids or j_ids):
            all_flag = True

        self.subscription = WebSocketSubscription(
            all_events=all_flag,
            execution_ids=exec_ids,
            workflow_run_ids=wf_run_ids,
            workflow_ids=wf_ids,
            job_ids=j_ids,
        )


# ---------------------------------------------------------------------------
# WebSocket Manager (Connection Hub + Redis Consumer)
# ---------------------------------------------------------------------------
class WebSocketManager:
    def __init__(self):
        self.active_connections: set[WebSocketConnection] = set()
        self.listener_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def connect(
        self,
        websocket: WebSocket,
        user: User,
        subprotocol: Optional[str] = None,
    ) -> WebSocketConnection:
        """Accept connection and register client."""
        if subprotocol is None:
            subprotocols = websocket.scope.get("subprotocols", [])
            subprotocol = subprotocols[0] if subprotocols else None
        await websocket.accept(subprotocol=subprotocol)
        conn = WebSocketConnection(websocket, user)
        async with self._lock:
            self.active_connections.add(conn)
        from metrics import WEBSOCKET_CONNECTIONS
        WEBSOCKET_CONNECTIONS.inc()
        return conn

    async def disconnect(self, conn: WebSocketConnection) -> None:
        """Unregister client session."""
        conn.is_active = False
        async with self._lock:
            self.active_connections.discard(conn)
        from metrics import WEBSOCKET_CONNECTIONS
        WEBSOCKET_CONNECTIONS.dec()

    def broadcast_local(self, event: ExecutionEvent) -> None:
        """Distribute an event to connected local clients matching RBAC and filters."""
        for conn in list(self.active_connections):
            if not conn.is_active:
                continue

            if not self._check_rbac(conn.user, event):
                continue

            if conn.subscription.matches(event):
                try:
                    conn.queue.put_nowait(event)
                    from metrics import WEBSOCKET_EVENTS_SENT_TOTAL
                    WEBSOCKET_EVENTS_SENT_TOTAL.inc()
                except asyncio.QueueFull:
                    from metrics import WEBSOCKET_EVENTS_DROPPED_TOTAL
                    WEBSOCKET_EVENTS_DROPPED_TOTAL.inc()
                    # Slow client backpressure: drop event or close if persistently blocked
                    logger.warning("Queue full for client %s, dropping event %s", conn.user.username, event.event_id)

    @staticmethod
    def _check_rbac(user: User, event: ExecutionEvent) -> bool:
        """Enforce server-side RBAC on streamed events."""
        # Admin can view all events
        if user.role == ROLE_ADMIN:
            return True
        # Operator can view all execution and workflow events
        if user.role == ROLE_OPERATOR:
            return True
        # Observer can view execution and workflow read events
        if user.role == ROLE_OBSERVER:
            if event.event_type.startswith("execution:") or event.event_type.startswith("workflow:"):
                return True
        return False

    async def start_redis_listener(self, redis_url: str) -> None:
        """Start the single background Redis Pub/Sub subscriber task."""
        if self.listener_task is not None and not self.listener_task.done():
            return
        self.listener_task = asyncio.create_task(self._redis_listener_loop(redis_url))

    async def stop_redis_listener(self) -> None:
        """Stop the background subscriber task."""
        if self.listener_task:
            self.listener_task.cancel()
            try:
                await self.listener_task
            except asyncio.CancelledError:
                pass
            self.listener_task = None

    async def _redis_listener_loop(self, redis_url: str) -> None:
        """Background loop reading from Redis Pub/Sub channel."""
        import redis.asyncio as aioredis

        while True:
            try:
                r_client = aioredis.from_url(redis_url, decode_responses=True)
                pubsub = r_client.pubsub()
                await pubsub.subscribe(EVENTS_CHANNEL)
                logger.info("Subscribed to Redis event channel '%s'", EVENTS_CHANNEL)

                async for message in pubsub.listen():
                    if message["type"] == "message":
                        try:
                            event = ExecutionEvent.from_json(message["data"])
                            self.broadcast_local(event)
                        except Exception as e:
                            logger.error("Failed to parse Redis event message: %s", e)
            except asyncio.CancelledError:
                break
            except Exception as err:
                logger.warning("Redis Pub/Sub listener encountered error: %s. Reconnecting in 3s...", err)
                await asyncio.sleep(3.0)


manager = WebSocketManager()


# ---------------------------------------------------------------------------
# WebSocket Authentication Helper
# ---------------------------------------------------------------------------
async def authenticate_websocket(websocket: WebSocket, session: AsyncSession) -> Optional[User]:
    """Authenticate WebSocket handshake from query parameters or Authorization header.

    Returns the authenticated User or None if unauthenticated.
    """
    token: Optional[str] = None

    # 1. Query parameter: ?token=<jwt>
    query_params = urllib.parse.parse_qs(websocket.scope.get("query_string", b"").decode("latin1"))
    if "token" in query_params:
        token = query_params["token"][0]

    # 2. Authorization header if provided
    if not token:
        headers = dict(websocket.scope.get("headers", []))
        auth_header = headers.get(b"authorization", b"").decode("latin1")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:].strip()

    # 3. Sec-WebSocket-Protocol (token, <jwt>)
    if not token and "subprotocols" in websocket.scope:
        for proto in websocket.scope.get("subprotocols", []):
            if proto.startswith("token."):
                token = proto[6:]
                break

    if not token:
        return None

    try:
        payload = decode_access_token(token)
        user_id = int(payload["sub"])
        user = await get_user_by_id(session, user_id)
        if user is None or not user.is_active:
            return None
        return user
    except Exception:
        return None
