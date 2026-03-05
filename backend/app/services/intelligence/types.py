from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Any


@dataclass(slots=True)
class RuleContext:
    tenant_id: int
    tenant_slug: str
    tenant_plan: str
    tenant_source: str
    period_from: date
    period_to: date
    previous_from: date
    previous_to: date
    as_of: date


@dataclass(slots=True)
class InsightCreate:
    rule_code: str
    category: str
    severity: str
    title: str
    message: str
    entity_type: str | None = None
    entity_external_id: str | None = None
    entity_name: str | None = None
    period_from: date | None = None
    period_to: date | None = None
    value: float | None = None
    baseline_value: float | None = None
    delta_value: float | None = None
    delta_pct: float | None = None
    metadata_json: dict[str, Any] = field(default_factory=dict)
