# Environment configuration audit

This report was produced by checking the supplied local `.env` against the application and both Compose configurations. The private `.env` is intentionally not included in the cleaned source archive; `.env.example` is the distributable template.

## Supplied `.env`: local development

| Setting | Assessment | Reason |
|---|---|---|
| `ENVIRONMENT=development` | Appropriate for local development | Production-only startup validation is not enabled. Change it to `production` only with production values and the production Compose overlay. |
| `FLOWFORGE_SECRET_KEY` | Present and sufficiently long | Suitable format for local use. A production deployment should use a fresh, unique random secret. |
| `FLOWFORGE_ADMIN_USERNAME` / `FLOWFORGE_ADMIN_PASSWORD` | Present | These bootstrap a new administrator only. Changing the environment password does not update an already-existing account's password hash. |
| `DATABASE_URL` and `POSTGRES_*` | Host/user/database values are aligned | The host `localhost` URL is for running the API directly on the host. Compose builds an internal URL using the `postgres` service hostname. |
| `POSTGRES_PASSWORD` | Development-only default | It matches the default database URL, so the local setup is internally consistent, but this is not acceptable for production. Use a unique password of at least 24 URL-safe characters for production. |
| `REDIS_URL` / `REDIS_PASSWORD` | Correct for current local Compose mode | The base Compose Redis service is intentionally unauthenticated on loopback; the `REDIS_PASSWORD` variable is not used by that base service. The production overlay enables Redis authentication and constructs an authenticated service URL. |
| `CORS_ALLOWED_ORIGINS` | Appropriate for local development | It lists localhost/127.0.0.1 origins. Production must replace these with the real HTTPS frontend origin(s). |
| Scheduler/recovery/lease settings | Internally consistent | A 30-second lease with 10-second heartbeat/renew intervals leaves a reasonable renewal margin for the configured local defaults. |
| `AUTO_CREATE_TABLES=false` | Correct | Schema setup is migration-managed rather than automatic table creation. |
| `VITE_API_URL=http://localhost:8000` | Correct for Vite development | The Docker production frontend is built with the same-origin `/api` proxy instead. |

## Production prerequisites

- Set `ENVIRONMENT=production` and use both Compose files.
- Replace the development database password; use at least 24 URL-safe characters so Compose URL interpolation remains valid.
- Use a Redis password of at least 32 URL-safe characters.
- Set a fresh secret key, a strong unique admin password (12–128 characters), and the actual HTTPS frontend origin in `CORS_ALLOWED_ORIGINS`.
- Configure HTTPS at a trusted reverse proxy/load balancer. The Compose overlay itself publishes HTTP only.
- Preserve the existing PostgreSQL volume when changing credentials; changing `.env` alone does not alter the password stored in an already-initialized database.

## `.env` and Git

The repository `.gitignore` ignores `.env` and `.env.*` while allowing `.env.example`. This prevents normal Git adds from tracking those files, but verify with `git check-ignore .env` and `git status --short` before committing. `.gitignore` does not remove a file that was already committed in the past, and it does not protect secrets included in an archive or shared outside Git.
