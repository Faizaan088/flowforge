"""Durable FlowForge Scheduler subsystem: evaluation, recurring generation, dispatch, and loop."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from models import Execution, JobDefinition, ScheduleDefinition, ScheduleOccurrence
from queue_reconciliation import ReconciliationResult, deliver_executions_to_redis


def resolve_timezone(tz_name: str | None) -> timezone:
    """Resolve a timezone identifier, offset string, or fallback safely across platforms."""
    if not tz_name or str(tz_name).strip().upper() in ("UTC", "Z", "GMT"):
        return timezone.utc
    name = str(tz_name).strip()
    if name.startswith(("+", "-")):
        parts = name.split(":")
        hours = int(parts[0])
        minutes = int(parts[1]) if len(parts) > 1 else 0
        if hours < 0:
            minutes = -minutes
        return timezone(timedelta(hours=hours, minutes=minutes))
    try:
        import zoneinfo

        return zoneinfo.ZoneInfo(name)
    except Exception:
        pass

    fallback_offsets: dict[str, float] = {
        "EST": -5,
        "EDT": -4,
        "CST": -6,
        "CDT": -5,
        "MST": -7,
        "MDT": -6,
        "PST": -8,
        "PDT": -7,
        "AMERICA/NEW_YORK": -5,
        "AMERICA/CHICAGO": -6,
        "AMERICA/DENVER": -7,
        "AMERICA/LOS_ANGELES": -8,
        "EUROPE/LONDON": 0,
        "EUROPE/PARIS": 1,
        "EUROPE/BERLIN": 1,
        "ASIA/TOKYO": 9,
        "ASIA/KOLKATA": 5.5,
    }
    key = name.upper()
    if key in fallback_offsets:
        offset_hours = fallback_offsets[key]
        return timezone(
            timedelta(hours=int(offset_hours), minutes=int((offset_hours % 1) * 60))
        )
    return timezone.utc


def _parse_cron_field(field_str: str, min_val: int, max_val: int, field_name: str = "") -> set[int]:
    """Parse a single numeric cron field.

    Supported syntax:
      - Wildcard: '*' (all values from min_val to max_val)
      - Exact integer: '5'
      - Comma-separated list: '1,15,30'
      - Range: '1-5'
      - Step: '*/15', '0/20', '1-5/2'

    Explicitly unsupported:
      - Month/day names (e.g. 'JAN', 'MON')
      - Special modifiers ('L', 'W', '?', '#')
    """
    field_str = field_str.strip()
    if not field_str:
        raise ValueError(f"Empty cron field: {field_name}")

    for forbidden in ("?", "L", "W", "#"):
        if forbidden in field_str.upper():
            raise ValueError(
                f"Unsupported cron modifier '{forbidden}' in field {field_name!r}: {field_str!r}. "
                "Only standard numeric expressions (*, commas, ranges, steps) are supported."
            )

    values: set[int] = set()
    for part in field_str.split(","):
        part = part.strip()
        if not part:
            raise ValueError(f"Invalid empty item in cron field {field_name!r}: {field_str!r}")

        if "/" in part:
            sub, step_str = part.split("/", 1)
            try:
                step = int(step_str)
            except ValueError:
                raise ValueError(f"Invalid step {step_str!r} in cron field {field_name!r}")
            if step <= 0:
                raise ValueError(f"Step must be positive in cron field {field_name!r}: {part!r}")

            if sub == "*":
                start, end = min_val, max_val
            elif "-" in sub:
                start_s, end_s = sub.split("-", 1)
                start, end = int(start_s), int(end_s)
            else:
                start, end = int(sub), max_val

            if start < min_val or end > max_val or start > end:
                raise ValueError(f"Range {start}-{end} out of bounds ({min_val}-{max_val}) in field {field_name!r}")

            for v in range(start, end + 1, step):
                values.add(v)

        elif "-" in part:
            start_s, end_s = part.split("-", 1)
            try:
                start, end = int(start_s), int(end_s)
            except ValueError:
                raise ValueError(f"Non-numeric range {part!r} in cron field {field_name!r}")
            if start < min_val or end > max_val or start > end:
                raise ValueError(f"Range {start}-{end} out of bounds ({min_val}-{max_val}) in field {field_name!r}")
            for v in range(start, end + 1):
                values.add(v)

        elif part == "*":
            for v in range(min_val, max_val + 1):
                values.add(v)

        else:
            try:
                v = int(part)
            except ValueError:
                raise ValueError(
                    f"Unsupported or non-numeric token {part!r} in cron field {field_name!r}. "
                    "Only standard integer cron values (*, comma-lists, ranges, steps) are supported."
                )
            if v < min_val or v > max_val:
                raise ValueError(f"Value {v} out of bounds ({min_val}-{max_val}) in field {field_name!r}")
            values.add(v)

    return values


def next_cron_occurrence(cron_expr: str, start_dt: datetime) -> datetime:
    """Calculate the next datetime strictly after start_dt matching a standard 5-field cron expression.

    Supported syntax:
      - Exactly 5 whitespace-separated fields: minute (0-59), hour (0-23), day_of_month (1-31),
        month (1-12), day_of_week (0-7, where both 0 and 7 are Sunday).
      - Wildcards (*), lists (,), ranges (-), steps (/), range-steps (1-5/2).

    Explicitly unsupported:
      - 6-part / 7-part crons with seconds or years (must be exactly 5 fields).
      - Macro strings (@daily, @hourly, @reboot, etc.).
      - Named months/days (JAN, MON, etc.).
      - Special characters (L, W, ?, #).
    """
    expr = cron_expr.strip()
    if expr.startswith("@"):
        raise ValueError(
            f"Unsupported cron macro {expr!r}. Standard 5-field cron expressions are required."
        )

    parts = expr.split()
    if len(parts) != 5:
        raise ValueError(
            f"Invalid cron expression {cron_expr!r}: expected exactly 5 fields, got {len(parts)}."
        )

    minutes = _parse_cron_field(parts[0], 0, 59, "minute")
    hours = _parse_cron_field(parts[1], 0, 23, "hour")
    days = _parse_cron_field(parts[2], 1, 31, "day_of_month")
    months = _parse_cron_field(parts[3], 1, 12, "month")
    raw_dow = _parse_cron_field(parts[4], 0, 7, "day_of_week")
    dows = {d % 7 for d in raw_dow}  # 0 is Sunday, 1 is Monday ... 6 is Saturday

    cur = start_dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
    max_steps = 525600 * 5  # Search limit of 5 years
    steps = 0
    while steps < max_steps:
        if cur.month not in months:
            if cur.month == 12:
                cur = cur.replace(year=cur.year + 1, month=1, day=1, hour=0, minute=0)
            else:
                cur = cur.replace(month=cur.month + 1, day=1, hour=0, minute=0)
            steps += 1
            continue

        cron_dow = (cur.weekday() + 1) % 7
        dom_restricted = parts[2] != "*"
        dow_restricted = parts[4] != "*"
        if dom_restricted and dow_restricted:
            day_matches = (cur.day in days) or (cron_dow in dows)
        elif dom_restricted:
            day_matches = cur.day in days
        elif dow_restricted:
            day_matches = cron_dow in dows
        else:
            day_matches = True

        if not day_matches:
            cur = cur.replace(hour=0, minute=0) + timedelta(days=1)
            steps += 1
            continue

        if cur.hour not in hours:
            cur = cur.replace(minute=0) + timedelta(hours=1)
            steps += 1
            continue

        if cur.minute not in minutes:
            cur += timedelta(minutes=1)
            steps += 1
            continue

        return cur

    raise ValueError(f"No matching occurrence within 5 years for cron: {cron_expr}")



def compute_next_occurrence(
    config: dict[str, Any] | None,
    after_time: datetime,
    timezone_name: str | None = None,
) -> datetime:
    """Calculate deterministic next occurrence time for recurring schedules."""
    cfg = config or {}
    tz_str = timezone_name or cfg.get("timezone") or cfg.get("tz")
    tz = resolve_timezone(tz_str)

    interval_sec = cfg.get("interval_seconds") or cfg.get("interval")
    if interval_sec is not None:
        return after_time + timedelta(seconds=float(interval_sec))

    cron_expr = cfg.get("cron") or cfg.get("cron_expression")
    if cron_expr is not None:
        local_start = after_time.astimezone(tz)
        local_next = next_cron_occurrence(str(cron_expr), local_start)
        return local_next.astimezone(timezone.utc)

    raise ValueError(f"Schedule configuration has neither cron nor interval: {cfg}")


def _parse_datetime(val: Any) -> datetime:
    """Parse various datetime representations into UTC timezone-aware datetime."""
    if isinstance(val, datetime):
        if val.tzinfo is None:
            return val.replace(tzinfo=timezone.utc)
        return val.astimezone(timezone.utc)
    if isinstance(val, (int, float)):
        return datetime.fromtimestamp(val, tz=timezone.utc)
    if isinstance(val, str):
        cleaned = val.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    raise ValueError(f"Cannot parse datetime value: {val!r}")


async def evaluate_due_schedules(
    session: AsyncSession,
    now: datetime | None = None,
) -> list[int]:
    """Evaluate all enabled ScheduleDefinitions against `now` and create due occurrences idempotently.

    Returns list of created occurrence IDs.
    """
    eval_time = now or datetime.now(timezone.utc)
    created_occurrence_ids: list[int] = []

    result = await session.execute(
        select(ScheduleDefinition)
        .where(ScheduleDefinition.enabled.is_(True))
        .order_by(ScheduleDefinition.id)
    )
    schedules = result.scalars().all()

    for schedule in schedules:
        schedule_type = (schedule.schedule_type or "").lower()
        cfg = schedule.configuration or schedule.payload or {}
        if not isinstance(cfg, dict):
            cfg = {}

        if schedule_type == "one_time":
            target_val = (
                cfg.get("run_at")
                or cfg.get("scheduled_for")
                or cfg.get("time")
                or cfg.get("at")
            )
            if target_val is None:
                continue
            run_at = _parse_datetime(target_val)
            if eval_time < run_at:
                continue  # Future; not yet due

            existing_count = await session.scalar(
                select(func.count(ScheduleOccurrence.id)).where(
                    ScheduleOccurrence.schedule_definition_id == schedule.id
                )
            )
            if existing_count and existing_count > 0:
                continue  # One-time schedule already fired

            stmt = (
                insert(ScheduleOccurrence)
                .values(
                    schedule_definition_id=schedule.id,
                    scheduled_for=run_at,
                    status="SCHEDULED",
                )
                .on_conflict_do_nothing(
                    index_elements=["schedule_definition_id", "scheduled_for"]
                )
                .returning(ScheduleOccurrence.id)
            )
            res = await session.execute(stmt)
            occ_id = res.scalar_one_or_none()
            if occ_id:
                created_occurrence_ids.append(occ_id)

        elif schedule_type == "delayed":
            delay_sec = (
                cfg.get("delay_seconds")
                or cfg.get("delay")
                or cfg.get("seconds")
            )
            if delay_sec is not None:
                base_time = schedule.created_at or eval_time
                due_time = base_time + timedelta(seconds=float(delay_sec))
            elif cfg.get("run_at"):
                due_time = _parse_datetime(cfg["run_at"])
            else:
                continue

            if eval_time < due_time:
                continue  # Future; not yet due

            existing_count = await session.scalar(
                select(func.count(ScheduleOccurrence.id)).where(
                    ScheduleOccurrence.schedule_definition_id == schedule.id
                )
            )
            if existing_count and existing_count > 0:
                continue  # Delayed schedule already fired

            stmt = (
                insert(ScheduleOccurrence)
                .values(
                    schedule_definition_id=schedule.id,
                    scheduled_for=due_time,
                    status="SCHEDULED",
                )
                .on_conflict_do_nothing(
                    index_elements=["schedule_definition_id", "scheduled_for"]
                )
                .returning(ScheduleOccurrence.id)
            )
            res = await session.execute(stmt)
            occ_id = res.scalar_one_or_none()
            if occ_id:
                created_occurrence_ids.append(occ_id)

        elif schedule_type == "recurring":
            # Missed-occurrence policy when scheduler was down for multiple periods:
            # - "catch_up" (default): Deterministically evaluate and generate each missed occurrence up to `max_missed_occurrences` (default: 50).
            # - "skip" / "coalesce": Skip intermediate missed occurrences and generate only the single latest due occurrence.
            misfire_policy = str(cfg.get("misfire_policy", "catch_up")).lower()
            max_missed = int(cfg.get("max_missed_occurrences", 50))

            latest_time = await session.scalar(
                select(func.max(ScheduleOccurrence.scheduled_for)).where(
                    ScheduleOccurrence.schedule_definition_id == schedule.id
                )
            )
            base_time = latest_time or schedule.created_at or eval_time

            if misfire_policy in ("skip", "coalesce"):
                curr_time = base_time
                latest_due_time = None
                while True:
                    try:
                        next_time = compute_next_occurrence(cfg, after_time=curr_time)
                    except Exception:
                        break
                    if next_time <= eval_time:
                        latest_due_time = next_time
                        curr_time = next_time
                    else:
                        break

                if latest_due_time is not None:
                    stmt = (
                        insert(ScheduleOccurrence)
                        .values(
                            schedule_definition_id=schedule.id,
                            scheduled_for=latest_due_time,
                            status="SCHEDULED",
                        )
                        .on_conflict_do_nothing(
                            index_elements=["schedule_definition_id", "scheduled_for"]
                        )
                        .returning(ScheduleOccurrence.id)
                    )
                    res = await session.execute(stmt)
                    occ_id = res.scalar_one_or_none()
                    if occ_id:
                        created_occurrence_ids.append(occ_id)

            else:
                curr_time = base_time
                missed_count = 0
                while missed_count < max_missed:
                    try:
                        next_time = compute_next_occurrence(cfg, after_time=curr_time)
                    except Exception:
                        break
                    if next_time <= eval_time:
                        stmt = (
                            insert(ScheduleOccurrence)
                            .values(
                                schedule_definition_id=schedule.id,
                                scheduled_for=next_time,
                                status="SCHEDULED",
                            )
                            .on_conflict_do_nothing(
                                index_elements=["schedule_definition_id", "scheduled_for"]
                            )
                            .returning(ScheduleOccurrence.id)
                        )
                        res = await session.execute(stmt)
                        occ_id = res.scalar_one_or_none()
                        if occ_id:
                            created_occurrence_ids.append(occ_id)
                        curr_time = next_time
                        missed_count += 1
                    else:
                        break


    if created_occurrence_ids:
        await session.commit()

    return created_occurrence_ids


async def dispatch_due_occurrences(
    session: AsyncSession,
    redis_client=None,
    now: datetime | None = None,
    batch_size: int = 100,
) -> tuple[list[int], list[int], ReconciliationResult | None]:
    """Atomically claim due occurrences, transition to QUEUED, create Executions, and deliver to Redis."""
    eval_time = now or datetime.now(timezone.utc)

    # Concurrency-safe selection using FOR UPDATE SKIP LOCKED
    candidate_result = await session.execute(
        select(ScheduleOccurrence.id, ScheduleOccurrence.schedule_definition_id)
        .where(
            ScheduleOccurrence.status == "SCHEDULED",
            ScheduleOccurrence.scheduled_for <= eval_time,
        )
        .order_by(ScheduleOccurrence.scheduled_for, ScheduleOccurrence.id)
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    candidates = candidate_result.all()
    if not candidates:
        return [], [], None

    candidate_ids = [c[0] for c in candidates]

    # Atomically mark claimed occurrences as QUEUED
    await session.execute(
        update(ScheduleOccurrence)
        .where(ScheduleOccurrence.id.in_(candidate_ids))
        .values(status="QUEUED")
    )

    dispatched_occurrence_ids: list[int] = []
    created_execution_ids: list[int] = []

    for occ_id, schedule_def_id in candidates:
        schedule = await session.get(ScheduleDefinition, schedule_def_id)
        job_def_id = None
        priority = 0
        if schedule:
            cfg = schedule.configuration or schedule.payload or {}
            if isinstance(cfg, dict):
                job_def_id = cfg.get("job_definition_id")
                priority = int(cfg.get("priority", 0))

        if job_def_id and priority == 0:
            job_def = await session.get(JobDefinition, job_def_id)
            if job_def and job_def.priority:
                priority = int(job_def.priority)

        execution = Execution(
            job_definition_id=job_def_id,
            schedule_occurrence_id=occ_id,
            status="QUEUED",
            priority=priority,
        )
        session.add(execution)
        await session.flush()
        created_execution_ids.append(execution.id)
        dispatched_occurrence_ids.append(occ_id)

    # Persist durable queue state in PostgreSQL before attempting best-effort Redis delivery
    await session.commit()

    reconciliation = None
    if redis_client is not None and created_execution_ids:
        reconciliation = deliver_executions_to_redis(redis_client, created_execution_ids)

    return dispatched_occurrence_ids, created_execution_ids, reconciliation


@dataclass(frozen=True)
class SchedulerCycleResult:
    occurrences_created: list[int]
    occurrences_dispatched: list[int]
    executions_created: list[int]
    reconciliation: ReconciliationResult | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "occurrences_created": self.occurrences_created,
            "occurrences_dispatched": self.occurrences_dispatched,
            "executions_created": self.executions_created,
            "reconciliation": self.reconciliation.as_dict() if self.reconciliation else None,
        }


async def run_scheduler_cycle(
    session_factory,
    redis_client=None,
    now: datetime | None = None,
) -> SchedulerCycleResult:
    """Execute a single scheduler cycle: evaluate due schedules followed by occurrence dispatch."""
    eval_time = now or datetime.now(timezone.utc)

    async with session_factory() as session:
        created_occurrence_ids = await evaluate_due_schedules(session, now=eval_time)

    async with session_factory() as session:
        dispatched_occ_ids, created_exec_ids, reconciliation = await dispatch_due_occurrences(
            session, redis_client=redis_client, now=eval_time
        )

    return SchedulerCycleResult(
        occurrences_created=created_occurrence_ids,
        occurrences_dispatched=dispatched_occ_ids,
        executions_created=created_exec_ids,
        reconciliation=reconciliation,
    )


async def scheduler_loop(
    session_factory,
    redis_client=None,
    interval_seconds: float = 5.0,
):
    """Background task running periodic scheduler cycles safely."""
    while True:
        try:
            await run_scheduler_cycle(session_factory, redis_client=redis_client)
        except asyncio.CancelledError:
            break
        except Exception as error:
            print(f"Scheduler cycle error: {error}")
        await asyncio.sleep(interval_seconds)
