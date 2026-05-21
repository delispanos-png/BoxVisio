#!/usr/bin/env python
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Awaitable, Callable


sys.path.append(str(Path(__file__).resolve().parents[1] / 'backend'))

from app.db.control_session import ControlSessionLocal  # noqa: E402
from app.db.tenant_manager import get_tenant_db_session  # noqa: E402
from app.models.control import Tenant  # noqa: E402
from app.services.business_advisor import business_advisor_report  # noqa: E402
from app.services.kpi_queries import (  # noqa: E402
    executive_dashboard_cards_summary,
    inventory_items_overview,
    inventory_snapshot,
    purchases_documents_overview,
    purchases_summary,
    sales_documents_overview,
    sales_summary,
)
from app.services.replenishment import build_replenishment_from_facts  # noqa: E402
from sqlalchemy import select  # noqa: E402


async def _tenant(slug: str) -> Tenant:
    async with ControlSessionLocal() as control_db:
        tenant = (await control_db.execute(select(Tenant).where(Tenant.slug == slug))).scalar_one()
        return tenant


async def _measure(label: str, fn: Callable[[], Awaitable[object]]) -> dict[str, object]:
    started = time.perf_counter()
    ok = True
    error = ''
    size_hint = None
    try:
        result = await fn()
        if isinstance(result, dict):
            rows = result.get('rows')
            if isinstance(rows, list):
                size_hint = len(rows)
            elif 'summary' in result:
                size_hint = 'summary'
        elif isinstance(result, list):
            size_hint = len(result)
    except Exception as exc:  # pragma: no cover - ops script
        ok = False
        error = f'{type(exc).__name__}: {exc}'
    elapsed_ms = round((time.perf_counter() - started) * 1000, 1)
    return {'label': label, 'ok': ok, 'elapsed_ms': elapsed_ms, 'size_hint': size_hint, 'error': error}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', default='pharmacy295')
    parser.add_argument('--days', type=int, default=30)
    args = parser.parse_args()

    tenant = await _tenant(args.tenant)
    to_date = date.today()
    from_date = to_date - timedelta(days=max(1, args.days) - 1)
    async for tenant_db in get_tenant_db_session(
        tenant.slug,
        tenant.db_name,
        tenant.db_user,
        tenant.db_password,
    ):
        probes: list[tuple[str, Callable[[], Awaitable[object]]]] = [
            (
                'Dashboard Tenant cards',
                lambda: executive_dashboard_cards_summary(tenant_db, date_from=from_date, date_to=to_date),
            ),
            ('Πωλήσεις summary', lambda: sales_summary(tenant_db, from_date, to_date)),
            ('Πωλήσεις documents', lambda: sales_documents_overview(tenant_db, from_date, to_date, limit=100)),
            ('Αγορές summary', lambda: purchases_summary(tenant_db, from_date, to_date)),
            ('Αγορές documents', lambda: purchases_documents_overview(tenant_db, from_date, to_date, limit=100)),
            ('Αποθήκη snapshot', lambda: inventory_snapshot(tenant_db, to_date)),
            ('Είδη overview', lambda: inventory_items_overview(tenant_db, to_date, limit=100)),
            (
                'Business Advisor',
                lambda: business_advisor_report(tenant_db, date_from=from_date, date_to=to_date),
            ),
            ('Replenishment / Availability', lambda: build_replenishment_from_facts(tenant_db, as_of=to_date)),
        ]
        results = []
        for label, fn in probes:
            results.append(await _measure(label, fn))
        print(json.dumps({'tenant': tenant.slug, 'from': str(from_date), 'to': str(to_date), 'results': results}, ensure_ascii=False, indent=2))
        break


if __name__ == '__main__':
    asyncio.run(main())
