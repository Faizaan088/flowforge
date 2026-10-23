"""Audit service for durable event tracking."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from models import AuditEvent

logger = logging.getLogger("flowforge.audit")


async def record_audit_event(
    session: AsyncSession,
    actor_id: str,
    action: str,
    target_type: str,
    target_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> AuditEvent:
    """Record an audit trail entry."""
    event = AuditEvent(
        actor_id=str(actor_id),
        action=action,
        target_type=target_type,
        target_id=str(target_id) if target_id is not None else None,
        metadata_=metadata or {},
    )
    session.add(event)
    await session.flush()
    logger.info(
        f"Audit: actor={actor_id} action={action} target={target_type}:{target_id}"
    )
    return event


async def list_audit_events(
    session: AsyncSession,
    actor_id: Optional[str] = None,
    action: Optional[str] = None,
    target_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
) -> List[AuditEvent]:
    """Query audit events with filtering."""
    query = select(AuditEvent).order_by(AuditEvent.created_at.desc(), AuditEvent.id.desc())
    if actor_id:
        query = query.where(AuditEvent.actor_id == actor_id)
    if action:
        query = query.where(AuditEvent.action == action)
    if target_type:
        query = query.where(AuditEvent.target_type == target_type)

    query = query.limit(min(limit, 500)).offset(offset)
    result = await session.execute(query)
    return list(result.scalars().all())
