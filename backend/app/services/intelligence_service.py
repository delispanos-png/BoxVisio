from __future__ import annotations

from datetime import date
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.intelligence.engine import (
    acknowledge_insight,
    insights_counts_by_severity,
    list_insights,
    list_rules,
    run_insights_generation,
    update_rule,
)


async def generate_daily_insights(
    db: AsyncSession,
    *,
    tenant_id: int,
    tenant_slug: str,
    tenant_plan: str,
    tenant_source: str,
    as_of: date | None = None,
) -> dict[str, Any]:
    return await run_insights_generation(
        db,
        tenant_id=tenant_id,
        tenant_slug=tenant_slug,
        tenant_plan=tenant_plan,
        tenant_source=tenant_source,
        as_of=as_of,
    )


async def list_recent_insights(
    db: AsyncSession,
    *,
    limit: int = 20,
    statuses: list[str] | None = None,
    insight_types: list[str] | None = None,
) -> list[dict[str, Any]]:
    status = statuses[0] if statuses else None
    return await list_insights(
        db,
        category=None,
        severity=None,
        status=status,
        rule_codes=insight_types,
        limit=limit,
    )


__all__ = [
    'acknowledge_insight',
    'generate_daily_insights',
    'insights_counts_by_severity',
    'list_insights',
    'list_recent_insights',
    'list_rules',
    'run_insights_generation',
    'update_rule',
    'UUID',
]
