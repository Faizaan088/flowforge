# FlowForge deployment hardening changes

This file records changes applied during the source audit. It is not a declaration that the application is fully production-ready.

## Changes applied

- Admin bootstrap now creates the configured administrator only when the account does not already exist. It no longer silently restores the role or active status of an existing account on every startup.
- API user creation and password-update schemas now require passwords of at least 12 characters and cap passwords at 128 characters.
- The frontend WebSocket client sends its JWT through a WebSocket subprotocol rather than a query-string parameter, sends keepalive pings, and reconnects with bounded exponential backoff.
- Nginx hides its version, adds baseline security headers, blocks public proxy access to API docs/OpenAPI/metrics routes, and configures longer WebSocket timeouts with buffering disabled.
- The API container health check now uses `/health/ready` rather than the liveness-only `/health` endpoint.
- The deployment guide now uses both Compose files for production and explicitly states that HTTPS termination must be configured externally.
- Schedule creation now restricts schedule types to supported values. Malformed one-time/delayed date values and invalid delay values are skipped with warnings rather than repeatedly crashing the scheduler cycle; recurring catch-up limits are bounded.
- Added a regression assertion that admin bootstrap does not undo intentional account deactivation or role changes.

## Validation performed in this environment

- Python source compilation / AST parsing: passed.
- `pytest -q tests/test_security_hardening.py tests/test_auth.py`: 15 passed, 17 skipped. The skipped tests require an isolated PostgreSQL test database.
- Frontend lint could not run here because the ZIP contains Windows-built `node_modules`; Linux reported `eslint: Permission denied`. Reinstall frontend dependencies on the target development environment and rerun lint/build.
- Docker Compose build/start and production smoke tests were not run because Docker is unavailable in this environment.

## Remaining work before public production deployment

- Rotate all credentials that were included in the original archive/chat and ensure `.env` is never committed or distributed.
- Redis-backed distributed login rate limiting is implemented for username/IP and IP-wide attempts; verify behavior under concurrent requests, Redis outage/fail-closed behavior in production, and configured thresholds during the full PostgreSQL/Redis integration suite.
- Review worker disable/cancellation semantics for tasks with external side effects; retries need idempotency guarantees.
- Add complete audit coverage for user and workflow administration actions.
- Verify URL-encoding/handling for database credentials with special characters.
- Run the complete integration/concurrency suite with PostgreSQL and Redis, run frontend lint/build from a clean install, and perform a production Compose smoke test.
- Configure and verify HTTPS, DNS, backups, monitoring, and secret management on the selected hosting provider.

Do not use this release archive as-is if you have not rotated the credentials in the original `.env` files.
