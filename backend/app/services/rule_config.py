from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.control import (
    GlobalRuleEntry,
    GlobalRuleSet,
    OperationalStream,
    OverrideMode,
    RuleDomain,
    TenantRuleOverride,
)


def _as_plain_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = dict(base)
    for key, override_value in override.items():
        if key not in out:
            out[key] = override_value
            continue
        base_value = out[key]
        if isinstance(base_value, dict) and isinstance(override_value, dict):
            out[key] = _deep_merge(base_value, override_value)
        else:
            out[key] = override_value
    return out


async def _load_global_rule(
    db: AsyncSession,
    *,
    domain: RuleDomain,
    stream: OperationalStream,
    rule_key: str,
) -> GlobalRuleEntry | None:
    stmt = (
        select(GlobalRuleEntry)
        .join(GlobalRuleSet, GlobalRuleSet.id == GlobalRuleEntry.ruleset_id)
        .where(
            GlobalRuleSet.is_active.is_(True),
            GlobalRuleEntry.is_active.is_(True),
            GlobalRuleEntry.domain == domain,
            GlobalRuleEntry.stream == stream,
            GlobalRuleEntry.rule_key == rule_key,
        )
        .order_by(GlobalRuleSet.priority.desc(), GlobalRuleSet.id.desc(), GlobalRuleEntry.id.desc())
        .limit(1)
    )
    return (await db.execute(stmt)).scalar_one_or_none()


async def _load_tenant_override(
    db: AsyncSession,
    *,
    tenant_id: int,
    domain: RuleDomain,
    stream: OperationalStream,
    rule_key: str,
) -> TenantRuleOverride | None:
    stmt = (
        select(TenantRuleOverride)
        .where(
            TenantRuleOverride.tenant_id == tenant_id,
            TenantRuleOverride.is_active.is_(True),
            TenantRuleOverride.domain == domain,
            TenantRuleOverride.stream == stream,
            TenantRuleOverride.rule_key == rule_key,
        )
        .order_by(TenantRuleOverride.id.desc())
        .limit(1)
    )
    return (await db.execute(stmt)).scalar_one_or_none()


async def resolve_rule_payload(
    db: AsyncSession,
    *,
    tenant_id: int,
    domain: RuleDomain,
    stream: OperationalStream,
    rule_key: str,
    fallback_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    global_rule = await _load_global_rule(db, domain=domain, stream=stream, rule_key=rule_key)
    override_rule = await _load_tenant_override(
        db,
        tenant_id=tenant_id,
        domain=domain,
        stream=stream,
        rule_key=rule_key,
    )
    base_payload = _as_plain_dict(global_rule.payload_json if global_rule else fallback_payload)
    if not override_rule:
        return base_payload

    override_payload = _as_plain_dict(override_rule.payload_json)
    if override_rule.override_mode == OverrideMode.disable:
        return {'enabled': False}
    if override_rule.override_mode == OverrideMode.replace:
        return override_payload
    return _deep_merge(base_payload, override_payload)


async def resolve_source_query_template(
    db: AsyncSession,
    *,
    tenant_id: int,
    stream: OperationalStream,
    fallback_query_template: str,
) -> str:
    payload = await resolve_rule_payload(
        db,
        tenant_id=tenant_id,
        domain=RuleDomain.source_mapping,
        stream=stream,
        rule_key='query_template',
        fallback_payload={'query_template': fallback_query_template},
    )
    candidate = str(payload.get('query_template') or '').strip()
    if candidate:
        return candidate
    return fallback_query_template
