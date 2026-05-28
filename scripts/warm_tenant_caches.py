#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import select

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parents[1]
BACKEND_ROOT = PROJECT_ROOT / 'backend'
sys.path.insert(0, str(BACKEND_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))

from app.core.security import create_access_token  # noqa: E402
from app.db.control_session import ControlSessionLocal  # noqa: E402
from app.models.control import RoleName, Tenant, User  # noqa: E402


WARM_ENDPOINTS = (
    ('tenant_dashboard', '/tenant/dashboard'),
    ('sales_dashboard', '/tenant/sales'),
    ('purchases_dashboard', '/tenant/purchases'),
    ('inventory_dashboard', '/tenant/inventory'),
    ('cashflow_dashboard', '/tenant/cashflow'),
    ('suppliers_dashboard', '/tenant/suppliers'),
    ('customers_dashboard', '/tenant/customers'),
    ('supplier_orders_dashboard', '/tenant/supplier-orders'),
    ('replenishment_dashboard', '/tenant/replenishment'),
    ('executive_summary', '/v1/dashboard/executive-summary?fast=true&from={from_date}&to={to_date}'),
    ('sales_summary', '/v1/kpi/sales/summary?from={from_date}&to={to_date}'),
    ('purchases_summary', '/v1/kpi/purchases/summary?from={from_date}&to={to_date}'),
    ('cashflow_summary', '/v1/kpi/cashflow/summary?from={from_date}&to={to_date}'),
    ('suppliers_api', '/v1/kpi/suppliers?from={from_date}&to={to_date}&limit=20'),
    ('business_advisor', '/v1/kpi/business-advisor?from={from_date}&to={to_date}'),
)


def _today() -> date:
    return datetime.now(UTC).date()


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return value


def _project_path(raw_path: str) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else PROJECT_ROOT / path


async def _load_tenant_and_token(tenant_slug: str) -> tuple[Tenant | None, str | None]:
    async with ControlSessionLocal() as db:
        tenant = (await db.execute(select(Tenant).where(Tenant.slug == tenant_slug))).scalars().first()
        if tenant is None:
            return None, None
        user = (
            await db.execute(
                select(User)
                .where(
                    User.tenant_id == tenant.id,
                    User.is_active.is_(True),
                    User.role.in_([RoleName.tenant_admin, RoleName.tenant_user]),
                )
                .order_by(User.role.asc(), User.id.asc())
            )
        ).scalars().first()
        if user is None:
            return tenant, None
        token = create_access_token(
            subject=str(user.id),
            tenant_id=user.tenant_id,
            role=user.role.value,
            audience='tenant',
        )
        return tenant, token


async def _hit_endpoints(args: argparse.Namespace, token: str, from_date: date, to_date: date) -> list[dict[str, Any]]:
    headers = {'Authorization': f'Bearer {token}', 'Host': args.host}
    cookies = {'access_token': token}
    results: list[dict[str, Any]] = []
    async with httpx.AsyncClient(
        base_url=args.base_url.rstrip('/'),
        headers=headers,
        cookies=cookies,
        timeout=float(args.timeout),
        follow_redirects=False,
    ) as client:
        for round_no in range(1, int(args.rounds) + 1):
            for name, template in WARM_ENDPOINTS:
                url = template.format(from_date=from_date.isoformat(), to_date=to_date.isoformat())
                started = time.perf_counter()
                try:
                    response = await client.get(url)
                    elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
                    results.append(
                        {
                            'round': round_no,
                            'name': name,
                            'url': url,
                            'status': response.status_code,
                            'elapsed_ms': elapsed_ms,
                            'bytes': len(response.content),
                            'ok': 200 <= response.status_code < 400 and len(response.content) > 0,
                            'api_ms': response.headers.get('X-KPI-API-Time-Ms'),
                            'db_ms': response.headers.get('X-KPI-DB-Time-Ms'),
                            'cache': response.headers.get('X-KPI-Cache'),
                        }
                    )
                except Exception as exc:
                    elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
                    results.append({'round': round_no, 'name': name, 'url': url, 'status': 0, 'elapsed_ms': elapsed_ms, 'ok': False, 'error': str(exc)})
    return results


async def run(args: argparse.Namespace) -> dict[str, Any]:
    from_date = date.fromisoformat(args.from_date)
    to_date = date.fromisoformat(args.to_date)
    tenant, token = await _load_tenant_and_token(args.tenant)
    if tenant is None:
        return {'overall_status': 'FAIL', 'error': f'Tenant not found: {args.tenant}'}
    if not token:
        return {'overall_status': 'FAIL', 'tenant': args.tenant, 'error': 'No active tenant user for cache warm-up'}

    results = await _hit_endpoints(args, token, from_date, to_date)
    failed = [row for row in results if not row.get('ok')]
    last_round = [row for row in results if int(row.get('round') or 0) == int(args.rounds)]
    slow = [row for row in last_round if row.get('ok') and float(row.get('elapsed_ms') or 0) > float(args.slow_ms)]
    report = {
        'generated_at': datetime.now(UTC).isoformat(timespec='seconds').replace('+00:00', 'Z'),
        'overall_status': 'FAIL' if failed else ('WARN' if slow else 'PASS'),
        'tenant': {'id': tenant.id, 'slug': tenant.slug, 'name': tenant.name},
        'window': {'from': from_date, 'to': to_date},
        'base_url': args.base_url,
        'rounds': int(args.rounds),
        'slow_ms': int(args.slow_ms),
        'failed': failed,
        'slow_last_round': slow,
        'results': results,
    }
    out_dir = _project_path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
    out_path = out_dir / f"tenant_cache_warmup_{args.tenant}_{stamp}.json"
    out_path.write_text(json.dumps(_json_safe(report), ensure_ascii=False, indent=2), encoding='utf-8')
    report['artifact'] = str(out_path)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Warm tenant dashboard/API caches and produce a delivery artifact.')
    parser.add_argument('--tenant', default='pharmacy295')
    parser.add_argument('--from-date', default='2025-01-01')
    parser.add_argument('--to-date', default=_today().isoformat())
    parser.add_argument('--base-url', default='http://127.0.0.1:8000')
    parser.add_argument('--host', default='bi.boxvisio.com')
    parser.add_argument('--rounds', type=int, default=2)
    parser.add_argument('--timeout', type=float, default=45.0)
    parser.add_argument('--slow-ms', type=int, default=2500)
    parser.add_argument('--out-dir', default='artifacts/cache_warmup')
    return parser.parse_args()


def main() -> None:
    report = asyncio.run(run(parse_args()))
    print(json.dumps(_json_safe({'overall_status': report.get('overall_status'), 'artifact': report.get('artifact'), 'failed': report.get('failed'), 'slow_last_round': report.get('slow_last_round')}), ensure_ascii=False, indent=2))
    if report.get('overall_status') == 'FAIL':
        raise SystemExit(2)
    if report.get('overall_status') == 'WARN':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
