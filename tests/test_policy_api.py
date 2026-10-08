"""Focused API tests for concurrency and rate limit policy management endpoints."""

import json
import os
import urllib.parse
from typing import Any, Optional

import pytest
import pytest_asyncio

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL,
    reason="TEST_DATABASE_URL must point to an isolated PostgreSQL database",
)

if TEST_DATABASE_URL:
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import NullPool

    os.environ["DATABASE_URL"] = TEST_DATABASE_URL

    from database import Base, get_db
    from main import app
    from models import ConcurrencyLimitPolicy, RateLimitPolicy, RateLimitRecord


class Response:
    def __init__(self, status_code: int, headers: list[tuple[bytes, bytes]], content: bytes):
        self.status_code = status_code
        self.headers = {k.decode("latin1"): v.decode("latin1") for k, v in headers}
        self.content = content

    def json(self) -> Any:
        return json.loads(self.content.decode("utf-8"))

    @property
    def text(self) -> str:
        return self.content.decode("utf-8")


class AsyncApiClient:
    """Lightweight ASGI test client for invoking FastAPI without third-party dependencies."""

    def __init__(self, app_instance):
        self.app = app_instance

    async def request(
        self,
        method: str,
        path: str,
        json_data: Optional[Any] = None,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
    ) -> Response:
        parsed = urllib.parse.urlparse(path)
        url_path = parsed.path
        raw_query = parsed.query

        if params:
            query_parts = []
            if raw_query:
                query_parts.append(raw_query)
            query_parts.append(urllib.parse.urlencode(params))
            raw_query = "&".join(query_parts)

        body_bytes = b""
        req_headers = []
        if headers:
            for k, v in headers.items():
                req_headers.append((k.lower().encode("latin1"), v.encode("latin1")))

        if json_data is not None:
            body_bytes = json.dumps(json_data).encode("utf-8")
            req_headers.append((b"content-type", b"application/json"))
            req_headers.append((b"content-length", str(len(body_bytes)).encode("latin1")))

        response_status = None
        response_headers = []
        response_body = []

        async def receive():
            nonlocal body_bytes
            sent_bytes = body_bytes
            body_bytes = b""
            return {
                "type": "http.request",
                "body": sent_bytes,
                "more_body": False,
            }

        async def send(message):
            nonlocal response_status, response_headers, response_body
            if message["type"] == "http.response.start":
                response_status = message["status"]
                response_headers = message.get("headers", [])
            elif message["type"] == "http.response.body":
                response_body.append(message.get("body", b""))

        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": method.upper(),
            "scheme": "http",
            "path": url_path,
            "raw_path": url_path.encode("latin1"),
            "query_string": raw_query.encode("latin1"),
            "headers": req_headers,
            "server": ("testserver", 80),
            "client": ("testclient", 50000),
        }

        await self.app(scope, receive, send)
        return Response(response_status, response_headers, b"".join(response_body))

    async def get(self, path: str, params: Optional[dict[str, Any]] = None, **kwargs) -> Response:
        return await self.request("GET", path, params=params, **kwargs)

    async def post(self, path: str, json: Optional[Any] = None, **kwargs) -> Response:
        return await self.request("POST", path, json_data=json, **kwargs)

    async def patch(self, path: str, json: Optional[Any] = None, **kwargs) -> Response:
        return await self.request("PATCH", path, json_data=json, **kwargs)

    async def put(self, path: str, json: Optional[Any] = None, **kwargs) -> Response:
        return await self.request("PUT", path, json_data=json, **kwargs)

    async def delete(self, path: str, **kwargs) -> Response:
        return await self.request("DELETE", path, **kwargs)


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
        await session.commit()


@pytest_asyncio.fixture
async def client(session_factory):
    async def get_test_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = get_test_db
    try:
        yield AsyncApiClient(app)
    finally:
        app.dependency_overrides.pop(get_db, None)


# ===========================================================================
# 1. Concurrency Policy API Tests
# ===========================================================================

@pytest.mark.asyncio
async def test_concurrency_policy_create_and_get(client):
    """Test creating a concurrency policy and fetching it by ID and target."""
    create_resp = await client.post(
        "/policies/concurrency",
        json={
            "target_type": "WORKFLOW",
            "target_id": "wf-1",
            "max_concurrency": 2,
            "is_enabled": True,
        },
    )
    assert create_resp.status_code == 200
    data = create_resp.json()
    assert data["target_type"] == "WORKFLOW"
    assert data["target_id"] == "wf-1"
    assert data["max_concurrency"] == 2
    assert data["is_enabled"] is True
    policy_id = data["id"]

    # Get by ID
    get_resp = await client.get(f"/policies/concurrency/{policy_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == policy_id

    # Get by target
    target_resp = await client.get("/policies/concurrency/target/WORKFLOW/wf-1")
    assert target_resp.status_code == 200
    assert target_resp.json()["id"] == policy_id


@pytest.mark.asyncio
async def test_concurrency_policy_duplicate_upsert_behavior(client):
    """Calling create again with the same target upserts rather than failing."""
    resp1 = await client.post(
        "/policies/concurrency",
        json={
            "target_type": "TASK_TYPE",
            "target_id": "image-proc",
            "max_concurrency": 4,
            "is_enabled": True,
        },
    )
    assert resp1.status_code == 200
    id1 = resp1.json()["id"]

    # Upsert with different concurrency limit
    resp2 = await client.post(
        "/policies/concurrency",
        json={
            "target_type": "TASK_TYPE",
            "target_id": "image-proc",
            "max_concurrency": 8,
            "is_enabled": False,
        },
    )
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert data2["id"] == id1
    assert data2["max_concurrency"] == 8
    assert data2["is_enabled"] is False


@pytest.mark.asyncio
async def test_concurrency_policy_list_and_filter(client):
    """List concurrency policies with optional target_type and target_id filtering."""
    await client.post(
        "/policies/concurrency",
        json={"target_type": "WORKFLOW", "target_id": "w1", "max_concurrency": 1},
    )
    await client.post(
        "/policies/concurrency",
        json={"target_type": "WORKFLOW", "target_id": "w2", "max_concurrency": 2},
    )
    await client.post(
        "/policies/concurrency",
        json={"target_type": "TASK_TYPE", "target_id": "t1", "max_concurrency": 3},
    )

    # List all
    all_resp = await client.get("/policies/concurrency")
    assert all_resp.status_code == 200
    assert len(all_resp.json()) == 3

    # Filter by target_type
    wf_resp = await client.get("/policies/concurrency", params={"target_type": "WORKFLOW"})
    assert wf_resp.status_code == 200
    assert len(wf_resp.json()) == 2

    # Filter by target_id
    id_resp = await client.get("/policies/concurrency", params={"target_id": "t1"})
    assert id_resp.status_code == 200
    assert len(id_resp.json()) == 1
    assert id_resp.json()[0]["target_id"] == "t1"


@pytest.mark.asyncio
async def test_concurrency_policy_update(client):
    """Update an existing concurrency policy using PATCH or PUT."""
    create_resp = await client.post(
        "/policies/concurrency",
        json={"target_type": "WORKFLOW", "target_id": "w-update", "max_concurrency": 2},
    )
    pol_id = create_resp.json()["id"]

    patch_resp = await client.patch(
        f"/policies/concurrency/{pol_id}",
        json={"max_concurrency": 5},
    )
    assert patch_resp.status_code == 200
    assert patch_resp.json()["max_concurrency"] == 5
    assert patch_resp.json()["is_enabled"] is True


@pytest.mark.asyncio
async def test_concurrency_policy_enable_and_disable(client):
    """Enable and disable endpoints toggling policy state."""
    create_resp = await client.post(
        "/policies/concurrency",
        json={"target_type": "TASK_TYPE", "target_id": "t-toggle", "max_concurrency": 1, "is_enabled": True},
    )
    pol_id = create_resp.json()["id"]

    disable_resp = await client.post(f"/policies/concurrency/{pol_id}/disable")
    assert disable_resp.status_code == 200
    assert disable_resp.json()["is_enabled"] is False

    enable_resp = await client.post(f"/policies/concurrency/{pol_id}/enable")
    assert enable_resp.status_code == 200
    assert enable_resp.json()["is_enabled"] is True


@pytest.mark.asyncio
async def test_concurrency_policy_delete(client):
    """Delete a concurrency policy by ID."""
    create_resp = await client.post(
        "/policies/concurrency",
        json={"target_type": "WORKFLOW", "target_id": "w-delete", "max_concurrency": 1},
    )
    pol_id = create_resp.json()["id"]

    del_resp = await client.delete(f"/policies/concurrency/{pol_id}")
    assert del_resp.status_code == 200

    # Ensure it no longer exists
    get_resp = await client.get(f"/policies/concurrency/{pol_id}")
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_concurrency_policy_missing_returns_404(client):
    """Operations on non-existent policies return HTTP 404."""
    assert (await client.get("/policies/concurrency/999999")).status_code == 404
    assert (await client.get("/policies/concurrency/target/WORKFLOW/missing")).status_code == 404
    assert (await client.patch("/policies/concurrency/999999", json={"max_concurrency": 2})).status_code == 404
    assert (await client.post("/policies/concurrency/999999/enable")).status_code == 404
    assert (await client.post("/policies/concurrency/999999/disable")).status_code == 404
    assert (await client.delete("/policies/concurrency/999999")).status_code == 404


@pytest.mark.asyncio
async def test_concurrency_policy_invalid_values_return_400(client):
    """Invalid parameter values return HTTP 400 Bad Request."""
    # Invalid target_type
    r1 = await client.post(
        "/policies/concurrency",
        json={"target_type": "INVALID_TYPE", "target_id": "1", "max_concurrency": 2},
    )
    assert r1.status_code == 400
    assert "Invalid target_type" in r1.json()["detail"]

    # Empty target_id
    r2 = await client.post(
        "/policies/concurrency",
        json={"target_type": "WORKFLOW", "target_id": "   ", "max_concurrency": 2},
    )
    assert r2.status_code == 400
    assert "target_id cannot be empty" in r2.json()["detail"]

    # max_concurrency < 1
    r3 = await client.post(
        "/policies/concurrency",
        json={"target_type": "WORKFLOW", "target_id": "wf", "max_concurrency": 0},
    )
    assert r3.status_code == 400
    assert "max_concurrency must be an integer >= 1" in r3.json()["detail"]

    # Create a valid policy then update with invalid max_concurrency
    r_valid = await client.post(
        "/policies/concurrency",
        json={"target_type": "WORKFLOW", "target_id": "wf-valid", "max_concurrency": 2},
    )
    pol_id = r_valid.json()["id"]

    r_patch_invalid = await client.patch(
        f"/policies/concurrency/{pol_id}",
        json={"max_concurrency": -2},
    )
    assert r_patch_invalid.status_code == 400


# ===========================================================================
# 2. Rate Limit Policy API Tests
# ===========================================================================

@pytest.mark.asyncio
async def test_rate_limit_policy_create_and_get(client):
    """Test creating a rate limit policy and fetching it by ID and target."""
    create_resp = await client.post(
        "/policies/rate-limit",
        json={
            "target_type": "CATEGORY",
            "target_id": "emails",
            "max_requests": 20,
            "window_seconds": 60,
            "is_enabled": True,
        },
    )
    assert create_resp.status_code == 200
    data = create_resp.json()
    assert data["target_type"] == "CATEGORY"
    assert data["target_id"] == "emails"
    assert data["max_requests"] == 20
    assert data["window_seconds"] == 60
    assert data["is_enabled"] is True
    policy_id = data["id"]

    # Get by ID
    get_resp = await client.get(f"/policies/rate-limit/{policy_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == policy_id

    # Get by target
    target_resp = await client.get("/policies/rate-limit/target/CATEGORY/emails")
    assert target_resp.status_code == 200
    assert target_resp.json()["id"] == policy_id


@pytest.mark.asyncio
async def test_rate_limit_policy_duplicate_upsert_behavior(client):
    """Calling create again with the same target upserts rather than failing."""
    resp1 = await client.post(
        "/policies/rate-limit",
        json={
            "target_type": "CATEGORY",
            "target_id": "sms",
            "max_requests": 5,
            "window_seconds": 30,
            "is_enabled": True,
        },
    )
    assert resp1.status_code == 200
    id1 = resp1.json()["id"]

    resp2 = await client.post(
        "/policies/rate-limit",
        json={
            "target_type": "CATEGORY",
            "target_id": "sms",
            "max_requests": 15,
            "window_seconds": 60,
            "is_enabled": False,
        },
    )
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert data2["id"] == id1
    assert data2["max_requests"] == 15
    assert data2["window_seconds"] == 60
    assert data2["is_enabled"] is False


@pytest.mark.asyncio
async def test_rate_limit_policy_list_and_filter(client):
    """List rate limit policies with optional target_type and target_id filtering."""
    await client.post(
        "/policies/rate-limit",
        json={"target_type": "CATEGORY", "target_id": "cat-1", "max_requests": 5, "window_seconds": 10},
    )
    await client.post(
        "/policies/rate-limit",
        json={"target_type": "CATEGORY", "target_id": "cat-2", "max_requests": 10, "window_seconds": 20},
    )
    await client.post(
        "/policies/rate-limit",
        json={"target_type": "TASK_TYPE", "target_id": "task-rl", "max_requests": 25, "window_seconds": 60},
    )

    all_resp = await client.get("/policies/rate-limit")
    assert all_resp.status_code == 200
    assert len(all_resp.json()) == 3

    cat_resp = await client.get("/policies/rate-limit", params={"target_type": "CATEGORY"})
    assert cat_resp.status_code == 200
    assert len(cat_resp.json()) == 2

    id_resp = await client.get("/policies/rate-limit", params={"target_id": "task-rl"})
    assert id_resp.status_code == 200
    assert len(id_resp.json()) == 1


@pytest.mark.asyncio
async def test_rate_limit_policy_update(client):
    """Update rate limit policy values."""
    create_resp = await client.post(
        "/policies/rate-limit",
        json={"target_type": "CATEGORY", "target_id": "rl-update", "max_requests": 10, "window_seconds": 60},
    )
    pol_id = create_resp.json()["id"]

    patch_resp = await client.patch(
        f"/policies/rate-limit/{pol_id}",
        json={"max_requests": 50, "window_seconds": 120},
    )
    assert patch_resp.status_code == 200
    data = patch_resp.json()
    assert data["max_requests"] == 50
    assert data["window_seconds"] == 120


@pytest.mark.asyncio
async def test_rate_limit_policy_enable_and_disable(client):
    """Enable and disable endpoints for rate limit policy."""
    create_resp = await client.post(
        "/policies/rate-limit",
        json={"target_type": "CATEGORY", "target_id": "toggle-rl", "max_requests": 5, "window_seconds": 30},
    )
    pol_id = create_resp.json()["id"]

    disable_resp = await client.post(f"/policies/rate-limit/{pol_id}/disable")
    assert disable_resp.status_code == 200
    assert disable_resp.json()["is_enabled"] is False

    enable_resp = await client.post(f"/policies/rate-limit/{pol_id}/enable")
    assert enable_resp.status_code == 200
    assert enable_resp.json()["is_enabled"] is True


@pytest.mark.asyncio
async def test_rate_limit_policy_delete(client):
    """Delete a rate limit policy by ID."""
    create_resp = await client.post(
        "/policies/rate-limit",
        json={"target_type": "CATEGORY", "target_id": "delete-rl", "max_requests": 10, "window_seconds": 60},
    )
    pol_id = create_resp.json()["id"]

    del_resp = await client.delete(f"/policies/rate-limit/{pol_id}")
    assert del_resp.status_code == 200

    get_resp = await client.get(f"/policies/rate-limit/{pol_id}")
    assert get_resp.status_code == 404


@pytest.mark.asyncio
async def test_rate_limit_policy_missing_returns_404(client):
    """Missing rate limit policies return HTTP 404."""
    assert (await client.get("/policies/rate-limit/999999")).status_code == 404
    assert (await client.get("/policies/rate-limit/target/CATEGORY/missing")).status_code == 404
    assert (await client.patch("/policies/rate-limit/999999", json={"max_requests": 5})).status_code == 404
    assert (await client.post("/policies/rate-limit/999999/enable")).status_code == 404
    assert (await client.post("/policies/rate-limit/999999/disable")).status_code == 404
    assert (await client.delete("/policies/rate-limit/999999")).status_code == 404


@pytest.mark.asyncio
async def test_rate_limit_policy_invalid_values_return_400(client):
    """Invalid parameters for rate limit policies return HTTP 400 Bad Request."""
    # Invalid target_type
    r1 = await client.post(
        "/policies/rate-limit",
        json={"target_type": "INVALID_RL", "target_id": "1", "max_requests": 10, "window_seconds": 60},
    )
    assert r1.status_code == 400
    assert "Invalid target_type" in r1.json()["detail"]

    # Empty target_id
    r2 = await client.post(
        "/policies/rate-limit",
        json={"target_type": "CATEGORY", "target_id": "", "max_requests": 10, "window_seconds": 60},
    )
    assert r2.status_code == 400
    assert "target_id cannot be empty" in r2.json()["detail"]

    # max_requests < 1
    r3 = await client.post(
        "/policies/rate-limit",
        json={"target_type": "CATEGORY", "target_id": "cat", "max_requests": 0, "window_seconds": 60},
    )
    assert r3.status_code == 400
    assert "max_requests must be an integer >= 1" in r3.json()["detail"]

    # window_seconds < 1
    r4 = await client.post(
        "/policies/rate-limit",
        json={"target_type": "CATEGORY", "target_id": "cat", "max_requests": 5, "window_seconds": 0},
    )
    assert r4.status_code == 400
    assert "window_seconds must be an integer >= 1" in r4.json()["detail"]

    # Create valid and update with invalid values
    r_valid = await client.post(
        "/policies/rate-limit",
        json={"target_type": "CATEGORY", "target_id": "valid-cat", "max_requests": 5, "window_seconds": 10},
    )
    pol_id = r_valid.json()["id"]

    r_patch_bad_requests = await client.patch(
        f"/policies/rate-limit/{pol_id}",
        json={"max_requests": -5},
    )
    assert r_patch_bad_requests.status_code == 400

    r_patch_bad_window = await client.patch(
        f"/policies/rate-limit/{pol_id}",
        json={"window_seconds": 0},
    )
    assert r_patch_bad_window.status_code == 400
