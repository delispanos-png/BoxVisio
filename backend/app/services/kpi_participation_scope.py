from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Any


_sales_kpi_participation_config: ContextVar[dict[str, Any] | None] = ContextVar(
    'sales_kpi_participation_config',
    default=None,
)


def set_current_sales_kpi_participation_config(payload: dict[str, Any] | None) -> Token:
    config = payload if isinstance(payload, dict) else {}
    return _sales_kpi_participation_config.set(config)


def reset_current_sales_kpi_participation_config(token: Token) -> None:
    _sales_kpi_participation_config.reset(token)


def get_current_sales_kpi_participation_config() -> dict[str, Any]:
    payload = _sales_kpi_participation_config.get()
    return dict(payload or {})
