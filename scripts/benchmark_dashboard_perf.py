#!/opt/cloudon-bi/.venv/bin/python
from __future__ import annotations

import asyncio
import time
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta

import httpx
from sqlalchemy import select

from app.core.security import create_access_token
from app.db.control_session import ControlSessionLocal
from app.models.control import RoleName, User
from app.services.kpi_cache import invalidate_tenant_cache


@dataclass
class HitResult:
    url: str
    status: int
    elapsed_ms: float
    bytes_count: int
    api_ms: str | None
    db_ms: str | None
    query_count: str | None
    cache: str | None


def _dt(value: date) -> str:
    return value.isoformat()


def _start_of_week(value: date) -> date:
    return value - timedelta(days=value.weekday())


def _start_of_month(value: date) -> date:
    return value.replace(day=1)


def _start_of_year(value: date) -> date:
    return value.replace(month=1, day=1)


def _same_day_prev_year(value: date) -> date:
    prev_year = value.year - 1
    day = min(value.day, monthrange(prev_year, value.month)[1])
    return date(prev_year, value.month, day)


async def _tenant_headers() -> dict[str, str]:
    async with ControlSessionLocal() as db:
        user = (
            await db.execute(
                select(User).where(
                    User.role == RoleName.tenant_admin,
                    User.is_active.is_(True),
                ).order_by(User.id.asc())
            )
        ).scalars().first()
        if user is None:
            raise RuntimeError('No active tenant_admin user found')
        token = create_access_token(
            subject=str(user.id),
            tenant_id=user.tenant_id,
            role=user.role.value,
            audience='tenant',
        )
    return {'Authorization': f'Bearer {token}', 'Host': 'bi.boxvisio.com'}


async def _hit(client: httpx.AsyncClient, url: str) -> HitResult:
    started = time.perf_counter()
    resp = await client.get(url)
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return HitResult(
        url=url,
        status=resp.status_code,
        elapsed_ms=elapsed_ms,
        bytes_count=len(resp.content),
        api_ms=resp.headers.get('X-KPI-API-Time-Ms'),
        db_ms=resp.headers.get('X-KPI-DB-Time-Ms'),
        query_count=resp.headers.get('X-KPI-DB-Query-Count'),
        cache=resp.headers.get('X-KPI-Cache'),
    )


async def main() -> None:
    await invalidate_tenant_cache()
    headers = await _tenant_headers()
    anchor = date.today()
    day_from = anchor
    week_from = _start_of_week(anchor)
    month_from = _start_of_month(anchor)
    year_from = _start_of_year(anchor)
    prev1 = anchor.year - 1
    prev2 = anchor.year - 2
    prev_ytd_from = date(prev1, 1, 1)
    prev_ytd_to = _same_day_prev_year(anchor)
    prev_full_from = date(prev1, 1, 1)
    prev_full_to = date(prev1, 12, 31)
    prev_month_date = month_from - timedelta(days=1)
    prev_month_from = prev_month_date.replace(day=1)
    prev_month_to = date(prev_month_date.year, prev_month_date.month, min(anchor.day, prev_month_date.day))
    from_30 = anchor - timedelta(days=30)

    old_exec_urls = [
        f'/v1/kpi/sales/summary?from={_dt(day_from)}&to={_dt(anchor)}',
        f'/v1/kpi/sales/summary?from={_dt(week_from)}&to={_dt(anchor)}',
        f'/v1/kpi/sales/summary?from={_dt(month_from)}&to={_dt(anchor)}',
        f'/v1/kpi/sales/summary?from={_dt(year_from)}&to={_dt(anchor)}',
        f'/v1/kpi/sales/summary?from={_dt(prev_ytd_from)}&to={_dt(prev_ytd_to)}',
        f'/v1/kpi/sales/summary?from={_dt(prev_full_from)}&to={_dt(prev_full_to)}',
        f'/v1/kpi/sales/summary?from={_dt(from_30)}&to={_dt(anchor)}',
        f'/v1/kpi/sales/by-branch?from={_dt(day_from)}&to={_dt(anchor)}',
        f'/v1/kpi/sales/by-branch?from={_dt(month_from)}&to={_dt(anchor)}',
        f'/v1/kpi/sales/by-branch?from={_dt(year_from)}&to={_dt(anchor)}',
        f'/v1/kpi/sales/by-branch?from={_dt(prev_month_from)}&to={_dt(prev_month_to)}',
        f'/v1/kpi/sales/trend-monthly?from={_dt(date(anchor.year, 1, 1))}&to={_dt(date(anchor.year, 12, 31))}',
        f'/v1/kpi/sales/trend-monthly?from={_dt(date(prev1, 1, 1))}&to={_dt(date(prev1, 12, 31))}',
        f'/v1/kpi/sales/trend-monthly?from={_dt(date(prev2, 1, 1))}&to={_dt(date(prev2, 12, 31))}',
        f'/v1/kpi/purchases/summary?from={_dt(from_30)}&to={_dt(anchor)}',
        f'/v1/kpi/purchases/summary?from={_dt(year_from)}&to={_dt(anchor)}',
    ]
    old_fin_urls = [
        f'/v1/kpi/receivables/summary?from={_dt(from_30)}&to={_dt(anchor)}',
        f'/v1/kpi/receivables/aging?to={_dt(anchor)}',
        f'/v1/kpi/receivables/collection-trend?from={_dt(from_30)}&to={_dt(anchor)}',
        f'/v1/kpi/suppliers?from={_dt(from_30)}&to={_dt(anchor)}&limit=50',
        f'/v1/kpi/cashflow/summary?from={_dt(from_30)}&to={_dt(anchor)}',
        f'/v1/kpi/cashflow/by-type?from={_dt(from_30)}&to={_dt(anchor)}',
        f'/v1/kpi/cashflow/accounts?from={_dt(from_30)}&to={_dt(anchor)}&limit=50',
    ]

    async with httpx.AsyncClient(base_url='http://127.0.0.1:8000', headers=headers, timeout=120.0) as client:
        t0 = time.perf_counter()
        old_exec = await asyncio.gather(*[_hit(client, url) for url in old_exec_urls])
        old_exec_total = (time.perf_counter() - t0) * 1000.0

        t1 = time.perf_counter()
        old_fin = await asyncio.gather(*[_hit(client, url) for url in old_fin_urls])
        old_fin_total = (time.perf_counter() - t1) * 1000.0

        new_exec = await _hit(client, f'/v1/dashboard/executive-summary?from={_dt(from_30)}&to={_dt(anchor)}')
        new_fin = await _hit(
            client,
            f'/v1/dashboard/finance-summary?from={_dt(from_30)}&to={_dt(anchor)}&supplier_limit=50&account_limit=50',
        )
        new_inventory = await _hit(client, f'/v1/streams/inventory/summary?as_of={_dt(anchor)}')
        new_cash = await _hit(client, f'/v1/streams/cash/summary?from={_dt(from_30)}&to={_dt(anchor)}')
        # warm cache validation
        new_exec_cached = await _hit(client, f'/v1/dashboard/executive-summary?from={_dt(from_30)}&to={_dt(anchor)}')

    print('--- Baseline (legacy multi-call UI) ---')
    print(f'old_executive_total_ms={old_exec_total:.2f} calls={len(old_exec)} bytes={sum(x.bytes_count for x in old_exec)}')
    print(f'old_finance_total_ms={old_fin_total:.2f} calls={len(old_fin)} bytes={sum(x.bytes_count for x in old_fin)}')
    print('--- Consolidated endpoints ---')
    print(
        f'new_executive_ms={new_exec.elapsed_ms:.2f} api={new_exec.api_ms} db={new_exec.db_ms} '
        f'queries={new_exec.query_count} cache={new_exec.cache}'
    )
    print(
        f'new_finance_ms={new_fin.elapsed_ms:.2f} api={new_fin.api_ms} db={new_fin.db_ms} '
        f'queries={new_fin.query_count} cache={new_fin.cache}'
    )
    print(
        f'new_inventory_ms={new_inventory.elapsed_ms:.2f} api={new_inventory.api_ms} db={new_inventory.db_ms} '
        f'queries={new_inventory.query_count} cache={new_inventory.cache}'
    )
    print(
        f'new_cash_ms={new_cash.elapsed_ms:.2f} api={new_cash.api_ms} db={new_cash.db_ms} '
        f'queries={new_cash.query_count} cache={new_cash.cache}'
    )
    print(
        f'new_executive_cached_ms={new_exec_cached.elapsed_ms:.2f} api={new_exec_cached.api_ms} '
        f'db={new_exec_cached.db_ms} queries={new_exec_cached.query_count} cache={new_exec_cached.cache}'
    )


if __name__ == '__main__':
    asyncio.run(main())
