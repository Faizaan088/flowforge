"""Focused tests for Workflow and Worker REST API endpoints."""

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
    from database import Base, get_db
    from main import app
    from models import (
        Execution,
        User,
        Worker,
        WorkflowDefinition,
        WorkflowEdge,
        WorkflowRun,
        WorkflowTask,
        WorkflowTaskExecution,
    )
    from tests.test_policy_api import AsyncApiClient
    from worker_registry import register_worker
    from workflow_engine import create_workflow


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
        await session.execute(WorkflowTaskExecution.__table__.delete())
        await session.execute(WorkflowRun.__table__.delete())
        await session.execute(WorkflowEdge.__table__.delete())
        await session.execute(WorkflowTask.__table__.delete())
        await session.execute(WorkflowDefinition.__table__.delete())
        await session.execute(Execution.__table__.delete())
        await session.execute(Worker.__table__.delete())
        await session.execute(User.__table__.delete())
        await session.commit()


@pytest_asyncio.fixture
def client(session_factory):
    async def get_test_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = get_test_db
    try:
        yield AsyncApiClient(app)
    finally:
        app.dependency_overrides.pop(get_db, None)


@pytest_asyncio.fixture
async def tokens(session_factory):
    async with session_factory() as session:
        admin = await create_user(session, "wf_admin", "adminPass", role=ROLE_ADMIN)
        operator = await create_user(session, "wf_operator", "opPass", role=ROLE_OPERATOR)
        observer = await create_user(session, "wf_observer", "obsPass", role=ROLE_OBSERVER)
        await session.commit()

        return {
            "admin": create_access_token(admin),
            "operator": create_access_token(operator),
            "observer": create_access_token(observer),
        }


# ===========================================================================
# 1. Worker Endpoints
# ===========================================================================
@pytest.mark.asyncio
async def test_list_workers_endpoint(client, session_factory, tokens):
    token = tokens["observer"]
    headers = {"Authorization": f"Bearer {token}"}

    # Register workers
    async with session_factory() as session:
        await register_worker(session, "worker-node-1")
        await register_worker(session, "worker-node-2")

    resp = await client.get("/workers/", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    ids = [w["worker_id"] for w in data]
    assert "worker-node-1" in ids
    assert "worker-node-2" in ids
    assert all(w["is_alive"] is True for w in data)
    assert all(w["status"] == "ACTIVE" for w in data)


# ===========================================================================
# 2. Workflow Definition Endpoints
# ===========================================================================
@pytest.mark.asyncio
async def test_create_and_list_workflows(client, session_factory, tokens):
    op_headers = {"Authorization": f"Bearer {tokens['operator']}"}
    obs_headers = {"Authorization": f"Bearer {tokens['observer']}"}

    # 1. Observer cannot create workflow (403)
    obs_create = await client.post(
        "/workflows/",
        json={"name": "obs-wf", "tasks": [{"name": "t1"}]},
        headers=obs_headers,
    )
    assert obs_create.status_code == 403

    # 2. Operator CAN create workflow
    create_payload = {
        "name": "etl-pipeline",
        "description": "Standard ETL DAG",
        "tasks": [
            {"name": "extract", "task_type": "IO_BOUND"},
            {"name": "transform", "task_type": "CPU_BOUND"},
            {"name": "load", "task_type": "IO_BOUND"},
        ],
        "edges": [["extract", "transform"], ["transform", "load"]],
    }
    create_resp = await client.post("/workflows/", json=create_payload, headers=op_headers)
    assert create_resp.status_code == 201
    created_wf = create_resp.json()
    assert created_wf["name"] == "etl-pipeline"
    assert len(created_wf["tasks"]) == 3
    assert len(created_wf["edges"]) == 2
    wf_id = created_wf["id"]

    # 3. Observer CAN list workflows
    list_resp = await client.get("/workflows/", headers=obs_headers)
    assert list_resp.status_code == 200
    wfs = list_resp.json()
    assert len(wfs) == 1
    assert wfs[0]["id"] == wf_id

    # 4. Observer CAN get single workflow detail
    get_resp = await client.get(f"/workflows/{wf_id}", headers=obs_headers)
    assert get_resp.status_code == 200
    assert get_resp.json()["name"] == "etl-pipeline"


@pytest.mark.asyncio
async def test_create_invalid_workflow_rejects_with_400(client, tokens):
    op_headers = {"Authorization": f"Bearer {tokens['operator']}"}

    # Cycle DAG
    cyclic_payload = {
        "name": "cyclic-wf",
        "tasks": [{"name": "a"}, {"name": "b"}],
        "edges": [["a", "b"], ["b", "a"]],
    }
    resp = await client.post("/workflows/", json=cyclic_payload, headers=op_headers)
    assert resp.status_code == 400
    assert "Cycle" in resp.json()["detail"] or "cycle" in resp.json()["detail"]


# ===========================================================================
# 3. Workflow Run Endpoints
# ===========================================================================
@pytest.mark.asyncio
async def test_trigger_and_list_workflow_runs(client, session_factory, tokens):
    op_headers = {"Authorization": f"Bearer {tokens['operator']}"}
    obs_headers = {"Authorization": f"Bearer {tokens['observer']}"}

    # Seed a workflow
    async with session_factory() as session:
        wf = await create_workflow(
            session,
            name="batch-flow",
            tasks=[{"name": "step1"}, {"name": "step2"}],
            edges=[["step1", "step2"]],
        )
        await session.commit()
        wf_id = wf.id

    # 1. Observer cannot trigger run (403)
    obs_trigger = await client.post(f"/workflows/{wf_id}/runs", headers=obs_headers)
    assert obs_trigger.status_code == 403

    # 2. Operator CAN trigger run (201)
    run_resp = await client.post(
        f"/workflows/{wf_id}/runs",
        json={"triggered_by": "UI_TEST"},
        headers=op_headers,
    )
    assert run_resp.status_code == 201
    run_data = run_resp.json()
    assert run_data["workflow_id"] == wf_id
    assert run_data["triggered_by"] == "UI_TEST"
    assert len(run_data["task_executions"]) == 2
    run_id = run_data["id"]

    # 3. Observer can list runs
    runs_resp = await client.get("/workflows/runs/", headers=obs_headers)
    assert runs_resp.status_code == 200
    runs = runs_resp.json()
    assert len(runs) >= 1
    assert any(r["id"] == run_id for r in runs)

    # 4. Observer can get single run detail
    single_run_resp = await client.get(f"/workflows/runs/{run_id}", headers=obs_headers)
    assert single_run_resp.status_code == 200
    assert single_run_resp.json()["id"] == run_id

    # 5. Filter runs by workflow_id
    filtered_resp = await client.get(f"/workflows/runs/?workflow_id={wf_id}", headers=obs_headers)
    assert filtered_resp.status_code == 200
    assert len(filtered_resp.json()) >= 1
