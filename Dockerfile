FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install runtime dependencies as root, then run the application as an
# unprivileged user. Keep development/test dependencies in a separate image
# or environment rather than installing them into the production image.
COPY requirements-prod.txt .
RUN pip install --no-cache-dir -r requirements-prod.txt \
    && groupadd --system --gid 10001 flowforge \
    && useradd --system --uid 10001 --gid flowforge --home-dir /app --shell /usr/sbin/nologin flowforge

COPY --chown=flowforge:flowforge . .
USER 10001:10001

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
