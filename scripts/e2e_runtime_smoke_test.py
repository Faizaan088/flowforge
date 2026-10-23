"""Comprehensive runtime smoke test exercising all core FlowForge subsystems using urllib (zero extra dependencies)."""

import json
import sys
import time
import urllib.request
import urllib.error

BASE_URL = "http://127.0.0.1:8000"

def log(msg):
    print(f"[SMOKE TEST] {msg}")

def request(method, path, body=None, headers=None):
    url = f"{BASE_URL}{path}"
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp_body = resp.read().decode("utf-8")
            return resp.status, json.loads(resp_body) if resp_body else {}
    except urllib.error.HTTPError as err:
        err_body = err.read().decode("utf-8")
        try:
            parsed = json.loads(err_body)
        except Exception:
            parsed = {"raw": err_body}
        return err.code, parsed

def run_tests():
    # 1. Health Probe
    log("1. Checking cluster readiness (/health/ready)...")
    status, health_data = request("GET", "/health/ready")
    assert status == 200, f"Expected 200, got {status}: {health_data}"
    assert health_data.get("database") in ("ok", "healthy"), f"Database not healthy: {health_data}"
    assert health_data.get("redis") in ("ok", "healthy"), f"Redis not healthy: {health_data}"
    log(f"   Cluster healthy: DB={health_data.get('database')}, Redis={health_data.get('redis')}")

    # 2. Authentication
    log("2. Testing authentication (/auth/login)...")
    import os
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    admin_user = os.getenv("FLOWFORGE_ADMIN_USERNAME", "admin")
    admin_pass = os.getenv("FLOWFORGE_ADMIN_PASSWORD", "admin")
    login_payload = {"username": admin_user, "password": admin_pass}
    status, token_data = request("POST", "/auth/login", body=login_payload)
    assert status == 200, f"Login failed ({status}): {token_data}"
    token = token_data["access_token"]
    auth_headers = {"Authorization": f"Bearer {token}"}
    log("   Authentication succeeded, JWT token acquired.")

    # 3. Create Workflow DAG
    log("3. Creating workflow definition (/workflows/)...")
    wf_payload = {
        "name": f"smoke-test-pipeline-{int(time.time())}",
        "description": "End-to-end smoke test DAG",
        "tasks": [
            {"name": "fetch_input", "task_type": "IO_BOUND"},
            {"name": "compute_result", "task_type": "CPU_BOUND"},
            {"name": "store_output", "task_type": "IO_BOUND"},
        ],
        "edges": [
            ["fetch_input", "compute_result"],
            ["compute_result", "store_output"],
        ],
    }
    status, wf = request("POST", "/workflows/", body=wf_payload, headers=auth_headers)
    assert status == 201, f"Failed to create workflow ({status}): {wf}"
    wf_id = wf["id"]
    log(f"   Created workflow #{wf_id} with 3 tasks and 2 edges.")

    # 4. Trigger Workflow Run
    log(f"4. Triggering workflow #{wf_id} execution...")
    status, run = request("POST", f"/workflows/{wf_id}/runs", body={}, headers=auth_headers)
    assert status == 201, f"Failed to trigger run ({status}): {run}"
    run_id = run["id"]
    log(f"   Triggered workflow run #{run_id}. Waiting for worker execution...")

    # 5. Monitor Workflow Run to completion
    terminal_status = None
    for attempt in range(30):
        time.sleep(1.5)
        status, run_data = request("GET", f"/workflows/runs/{run_id}", headers=auth_headers)
        assert status == 200
        run_status = run_data["status"]
        log(f"   [attempt {attempt+1}] Run #{run_id} status: {run_status}")
        if run_status in ("SUCCEEDED", "COMPLETED", "FAILED", "CANCELLED"):
            terminal_status = run_status
            break

    log(f"   Workflow run finished with status: {terminal_status}")
    assert terminal_status in ("SUCCEEDED", "COMPLETED"), f"Run did not complete successfully: {terminal_status}"

    # 6. Test API Key Generation and Authentication
    log("5. Testing API Key creation and authentication (/api-keys/)...")
    key_payload = {"name": "Smoke Test Service Key", "expires_in_days": 30}
    status, key_data = request("POST", "/api-keys/", body=key_payload, headers=auth_headers)
    assert status == 201, f"Failed to create API key ({status}): {key_data}"
    raw_api_key = key_data["raw_key"]
    api_key_obj = key_data.get("api_key", key_data)
    api_key_id = api_key_obj["id"]
    log(f"   Generated API Key #{api_key_id} (prefix: {api_key_obj.get('key_prefix', '')})")

    # Access endpoint using API Key
    status, user_me = request("GET", "/auth/me", headers={"X-API-Key": raw_api_key})
    assert status == 200, f"API Key auth failed ({status}): {user_me}"
    assert user_me["username"] == "admin"
    log("   X-API-Key authentication verified successfully on /auth/me.")

    # Clean up API key
    status, _ = request("DELETE", f"/api-keys/{api_key_id}", headers=auth_headers)
    assert status == 200

    # 7. Test Logout and Token Revocation
    log("6. Testing token revocation on logout (/auth/logout)...")
    status, _ = request("POST", "/auth/logout", headers=auth_headers)
    assert status == 200, f"Logout failed ({status})"

    # Verify that revoked token is immediately rejected
    status, _ = request("GET", "/auth/me", headers=auth_headers)
    assert status == 401, f"Expected 401 for revoked token, got {status}"
    log("   Revoked token is rejected with 401 Unauthorized as expected.")

    log("\nALL RUNTIME SMOKE TESTS PASSED CLEANLY!")

if __name__ == "__main__":
    try:
        run_tests()
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
