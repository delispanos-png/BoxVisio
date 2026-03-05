from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tenant import Insight, InsightRule, InsightRun
from app.services.intelligence.rules import RULE_SPECS, RULE_SPEC_BY_CODE, RuleSpec
from app.services.intelligence.types import InsightCreate, RuleContext


def _rule_allowed_for_plan(rule: RuleSpec, tenant_plan: str, tenant_source: str) -> bool:
    plan = (tenant_plan or '').lower()
    source = (tenant_source or '').lower()
    if rule.category == 'sales':
        return True
    if rule.category == 'purchases':
        return plan in {'pro', 'enterprise'}
    if rule.category in {'inventory', 'cashflow'}:
        return plan == 'enterprise' and source == 'pharmacyone'
    return False


def _severity_rank(severity: str) -> int:
    return {'critical': 0, 'warning': 1, 'info': 2}.get((severity or '').lower(), 3)


async def ensure_default_rules(db: AsyncSession) -> dict[str, InsightRule]:
    existing = (await db.execute(select(InsightRule))).scalars().all()
    by_code = {r.code: r for r in existing}
    changed = False
    for spec in RULE_SPECS:
        if spec.code in by_code:
            continue
        row = InsightRule(
            code=spec.code,
            name=spec.name,
            description=spec.description,
            category=spec.category,
            severity_default=spec.severity_default,
            enabled=True,
            params_json=spec.params_json,
            scope=spec.scope,
            schedule=spec.schedule,
        )
        db.add(row)
        by_code[spec.code] = row
        changed = True
    if changed:
        await db.flush()
    return by_code


async def _upsert_insight(db: AsyncSession, item: InsightCreate, ctx: RuleContext) -> int:
    entity_type = item.entity_type or 'tenant'
    entity_external_id = item.entity_external_id or ctx.tenant_slug
    existing = (
        await db.execute(
            select(Insight).where(
                Insight.rule_code == item.rule_code,
                Insight.entity_type == entity_type,
                Insight.entity_external_id == entity_external_id,
                Insight.period_from == (item.period_from or ctx.period_from),
                Insight.period_to == (item.period_to or ctx.period_to),
            )
        )
    ).scalar_one_or_none()
    if existing:
        existing.severity = item.severity
        existing.title = item.title
        existing.message = item.message
        existing.entity_name = item.entity_name
        existing.value = item.value
        existing.baseline_value = item.baseline_value
        existing.delta_value = item.delta_value
        existing.delta_pct = item.delta_pct
        existing.metadata_json = item.metadata_json or {}
        if existing.status == 'resolved':
            existing.status = 'open'
            existing.acknowledged_at = None
            existing.acknowledged_by = None
        return 0
    db.add(
        Insight(
            rule_code=item.rule_code,
            category=item.category,
            severity=item.severity,
            title=item.title,
            message=item.message,
            entity_type=entity_type,
            entity_external_id=entity_external_id,
            entity_name=item.entity_name,
            period_from=item.period_from or ctx.period_from,
            period_to=item.period_to or ctx.period_to,
            value=item.value,
            baseline_value=item.baseline_value,
            delta_value=item.delta_value,
            delta_pct=item.delta_pct,
            metadata_json=item.metadata_json or {},
            status='open',
            created_at=datetime.now(timezone.utc),
        )
    )
    return 1


def _period_bounds(as_of: date, window_days: int) -> tuple[date, date, date, date]:
    window = max(1, int(window_days))
    period_to = as_of
    period_from = period_to - timedelta(days=window - 1)
    previous_to = period_from - timedelta(days=1)
    previous_from = previous_to - timedelta(days=window - 1)
    return period_from, period_to, previous_from, previous_to


async def run_insights_generation(
    db: AsyncSession,
    *,
    tenant_id: int,
    tenant_slug: str,
    tenant_plan: str,
    tenant_source: str,
    as_of: date | None = None,
) -> dict[str, Any]:
    as_of_date = as_of or (date.today() - timedelta(days=1))
    rules = await ensure_default_rules(db)
    run = InsightRun(
        started_at=datetime.now(timezone.utc),
        status='success',
        rules_executed=0,
        insights_created=0,
        error=None,
    )
    db.add(run)
    await db.flush()

    created = 0
    executed = 0
    try:
        for rule_code, rule in rules.items():
            if not rule.enabled:
                continue
            spec = RULE_SPEC_BY_CODE.get(rule_code)
            if spec is None:
                continue
            if not _rule_allowed_for_plan(spec, tenant_plan, tenant_source):
                continue
            params = dict(spec.params_json)
            params.update(dict(rule.params_json or {}))
            period_from, period_to, previous_from, previous_to = _period_bounds(
                as_of_date,
                int(params.get('window_days', 30)),
            )
            ctx = RuleContext(
                tenant_id=tenant_id,
                tenant_slug=tenant_slug,
                tenant_plan=tenant_plan,
                tenant_source=tenant_source,
                period_from=period_from,
                period_to=period_to,
                previous_from=previous_from,
                previous_to=previous_to,
                as_of=as_of_date,
            )
            rows = await spec.runner(db, params, ctx)
            executed += 1
            for row in rows:
                if not row.severity:
                    row.severity = rule.severity_default
                created += await _upsert_insight(db, row, ctx)
        run.rules_executed = executed
        run.insights_created = created
        run.finished_at = datetime.now(timezone.utc)
        run.status = 'success'
        await db.commit()
        return {'status': 'success', 'rules_executed': executed, 'insights_created': created, 'run_id': str(run.id)}
    except Exception as exc:
        run.rules_executed = executed
        run.insights_created = created
        run.finished_at = datetime.now(timezone.utc)
        run.status = 'fail'
        run.error = str(exc)[:2000]
        await db.commit()
        return {'status': 'fail', 'rules_executed': executed, 'insights_created': created, 'run_id': str(run.id), 'error': str(exc)}


async def list_insights(
    db: AsyncSession,
    *,
    category: str | None = None,
    severity: str | None = None,
    status: str | None = None,
    rule_codes: list[str] | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    stmt = select(Insight)
    if category:
        stmt = stmt.where(Insight.category == category)
    if severity:
        stmt = stmt.where(Insight.severity == severity)
    if status:
        stmt = stmt.where(Insight.status == status)
    if rule_codes:
        stmt = stmt.where(Insight.rule_code.in_(rule_codes))
    if date_from:
        stmt = stmt.where(Insight.created_at >= datetime.combine(date_from, datetime.min.time(), tzinfo=timezone.utc))
    if date_to:
        stmt = stmt.where(Insight.created_at <= datetime.combine(date_to, datetime.max.time(), tzinfo=timezone.utc))
    rows = (
        await db.execute(stmt.order_by(Insight.created_at.desc()).limit(max(1, min(limit, 500))))
    ).scalars().all()
    rows = sorted(rows, key=lambda r: (_severity_rank(r.severity), -(int(r.created_at.timestamp()) if r.created_at else 0)))
    return [
        {
            'id': str(r.id),
            'type': r.rule_code,
            'rule_code': r.rule_code,
            'category': r.category,
            'severity': r.severity,
            'title': r.title,
            'message': r.message,
            'entity_type': r.entity_type,
            'entity_external_id': r.entity_external_id,
            'entity_name': r.entity_name,
            'period_from': str(r.period_from),
            'period_to': str(r.period_to),
            'value': float(r.value) if r.value is not None else None,
            'baseline_value': float(r.baseline_value) if r.baseline_value is not None else None,
            'delta_value': float(r.delta_value) if r.delta_value is not None else None,
            'delta_pct': float(r.delta_pct) if r.delta_pct is not None else None,
            'status': r.status,
            'created_at': r.created_at.isoformat() if r.created_at else None,
            'metadata_json': r.metadata_json or {},
        }
        for r in rows
    ]


async def acknowledge_insight(db: AsyncSession, *, insight_id: UUID, user_id: UUID | None = None) -> bool:
    insight = (await db.execute(select(Insight).where(Insight.id == insight_id))).scalar_one_or_none()
    if not insight:
        return False
    insight.status = 'acknowledged'
    insight.acknowledged_by = user_id
    insight.acknowledged_at = datetime.now(timezone.utc)
    await db.commit()
    return True


async def list_rules(db: AsyncSession) -> list[InsightRule]:
    await ensure_default_rules(db)
    rows = (await db.execute(select(InsightRule).order_by(InsightRule.category, InsightRule.code))).scalars().all()
    return rows


async def update_rule(
    db: AsyncSession,
    *,
    code: str,
    enabled: bool | None = None,
    severity_default: str | None = None,
    params_json: dict | None = None,
) -> bool:
    row = (await db.execute(select(InsightRule).where(InsightRule.code == code))).scalar_one_or_none()
    if row is None:
        return False
    if enabled is not None:
        row.enabled = bool(enabled)
    if severity_default:
        row.severity_default = severity_default
    if params_json is not None:
        row.params_json = params_json
    row.updated_at = datetime.now(timezone.utc)
    await db.commit()
    return True


async def insights_counts_by_severity(db: AsyncSession) -> dict[str, int]:
    rows = (await db.execute(select(Insight.severity, func.count(Insight.id)).group_by(Insight.severity))).all()
    payload = {'critical': 0, 'warning': 0, 'info': 0}
    for sev, cnt in rows:
        payload[str(sev)] = int(cnt)
    return payload
