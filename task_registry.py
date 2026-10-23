"""Task Registry for FlowForge Workers.

Provides a trusted registry of executable task handlers with input validation,
timeouts, errors, and output results.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

logger = logging.getLogger("flowforge.tasks")


@dataclass(frozen=True)
class TaskResult:
    status: str  # "SUCCEEDED" or "FAILED"
    output: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


TaskHandler = Callable[[Dict[str, Any]], Awaitable[TaskResult]]


class TaskRegistry:
    """Registry of trusted task execution handlers."""

    def __init__(self):
        self._handlers: Dict[str, TaskHandler] = {}
        self._register_default_handlers()

    def register(self, name: str, handler: TaskHandler) -> None:
        """Register a named task handler."""
        self._handlers[name.lower()] = handler
        logger.info(f"Registered task handler: {name}")

    def get(self, name: str) -> Optional[TaskHandler]:
        """Lookup a task handler by name."""
        return self._handlers.get(name.lower())

    def list_handlers(self) -> list[str]:
        """List registered task handler names."""
        return sorted(list(self._handlers.keys()))

    async def execute(
        self,
        task_type: str,
        params: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[float] = None,
    ) -> TaskResult:
        """Execute a registered task handler with optional timeout."""
        handler = self.get(task_type)
        if not handler:
            return TaskResult(
                status="FAILED",
                error=f"Unknown task handler type: {task_type!r}. Available: {self.list_handlers()}",
            )

        payload = params or {}
        try:
            if timeout_seconds and timeout_seconds > 0:
                return await asyncio.wait_for(
                    handler(payload),
                    timeout=timeout_seconds,
                )
            else:
                return await handler(payload)
        except asyncio.TimeoutError:
            return TaskResult(
                status="FAILED",
                error=f"Task timed out after {timeout_seconds}s",
            )
        except Exception as exc:
            logger.error("Task handler failed for type %s (%s)", task_type, type(exc).__name__)
            return TaskResult(
                status="FAILED",
                error=f"Task handler raised an error ({type(exc).__name__})",
            )

    def _register_default_handlers(self) -> None:
        """Register built-in standard trusted handlers."""

        async def echo_handler(params: Dict[str, Any]) -> TaskResult:
            """Simple echo/pass-through task."""
            message = params.get("message", "ok")
            return TaskResult(status="SUCCEEDED", output={"echo": message, "input": params})

        async def sleep_handler(params: Dict[str, Any]) -> TaskResult:
            """Simulates processing time."""
            duration = float(params.get("seconds", 1.0))
            # Bound duration to reasonable limit to prevent unbounded stalls
            clamped = min(max(duration, 0.0), 300.0)
            await asyncio.sleep(clamped)
            return TaskResult(status="SUCCEEDED", output={"slept_seconds": clamped})

        async def compute_hash_handler(params: Dict[str, Any]) -> TaskResult:
            """Deterministic hashing computation."""
            data = str(params.get("data", ""))
            algorithm = params.get("algorithm", "sha256").lower()
            if algorithm == "md5":
                h = hashlib.md5(data.encode("utf-8")).hexdigest()
            else:
                h = hashlib.sha256(data.encode("utf-8")).hexdigest()
            return TaskResult(status="SUCCEEDED", output={"hash": h, "algorithm": algorithm})

        async def fail_handler(params: Dict[str, Any]) -> TaskResult:
            """Deliberately fails - useful for failure testing and retries."""
            reason = params.get("error", "Simulated task failure")
            return TaskResult(status="FAILED", error=reason)

        async def transform_handler(params: Dict[str, Any]) -> TaskResult:
            """Simple JSON transformation."""
            items = params.get("items", [])
            operation = params.get("operation", "count")
            if operation == "count":
                res = len(items)
            elif operation == "sum" and isinstance(items, list):
                res = sum(float(x) for x in items if isinstance(x, (int, float)))
            elif operation == "uppercase" and isinstance(items, list):
                res = [str(x).upper() for x in items]
            else:
                res = items
            return TaskResult(status="SUCCEEDED", output={"result": res, "operation": operation})

        self.register("echo", echo_handler)
        self.register("sleep", sleep_handler)
        self.register("compute_hash", compute_hash_handler)
        self.register("fail", fail_handler)
        self.register("transform", transform_handler)
        self.register("standard", echo_handler)
        self.register("io_bound", echo_handler)
        self.register("cpu_bound", compute_hash_handler)
        self.register("generic", echo_handler)


# Global default registry singleton
default_task_registry = TaskRegistry()
