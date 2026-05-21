#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

HTML_ROUTES = [
    ('login_page', '/login', False),
    ('tenant_dashboard', '/tenant/dashboard', True),
    ('sales_dashboard', '/tenant/sales', True),
    ('purchases_dashboard', '/tenant/purchases', True),
    ('inventory_dashboard', '/tenant/inventory', True),
    ('items_dashboard', '/tenant/items', True),
    ('cashflow_dashboard', '/tenant/cashflow', True),
    ('suppliers_dashboard', '/tenant/suppliers', True),
    ('customers_dashboard', '/tenant/customers', True),
    ('supplier_orders_dashboard', '/tenant/supplier-orders', True),
    ('business_advisor_dashboard', '/tenant/business-advisor', True),
    ('replenishment_dashboard', '/tenant/replenishment', True),
    ('era_exploration_dashboard', '/tenant/era-exploration-data', True),
]

API_ROUTES = [
    ('tenant_dashboard_sync_chip_source', '/tenant/dashboard'),
    ('executive_summary', '/v1/dashboard/executive-summary?from={from_date}&to={to_date}'),
    ('sales_summary', '/v1/kpi/sales/summary?from={from_date}&to={to_date}'),
    ('sales_documents', '/v1/kpi/sales/documents?from={from_date}&to={to_date}&limit=5'),
    ('purchases_summary', '/v1/kpi/purchases/summary?from={from_date}&to={to_date}'),
    ('purchases_documents', '/v1/kpi/purchases/documents?from={from_date}&to={to_date}&limit=5'),
    ('expenses_documents', '/v1/kpi/expenses/documents?from={from_date}&to={to_date}&limit=5'),
    ('inventory_snapshot', '/v1/kpi/inventory/snapshot?as_of={to_date}'),
    ('inventory_items', '/v1/kpi/inventory/items?as_of={to_date}&limit=5'),
    ('business_advisor', '/v1/kpi/business-advisor?from={from_date}&to={to_date}'),
]

EXPORT_ROUTES = [
    ('sales_by_branch_csv', '/v1/kpi/sales/by-branch/export.csv?from={from_date}&to={to_date}'),
    ('purchases_by_supplier_csv', '/v1/kpi/purchases/by-supplier/export.csv?from={from_date}&to={to_date}'),
    ('sellout_csv', '/v1/reports/sellout/export.csv?from={from_date}&to={to_date}'),
]


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


async def _hit(client: httpx.AsyncClient, name: str, url: str, *, expect_redirect: bool = False) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        response = await client.get(url)
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        ok = (300 <= response.status_code < 400) if expect_redirect else (200 <= response.status_code < 400)
        return {
            'name': name,
            'url': url,
            'status': response.status_code,
            'elapsed_ms': elapsed_ms,
            'bytes': len(response.content or b''),
            'ok': ok,
            'content_type': response.headers.get('content-type'),
            'cache': response.headers.get('X-KPI-Cache'),
            'api_ms': response.headers.get('X-KPI-API-Time-Ms'),
        }
    except Exception as exc:
        return {'name': name, 'url': url, 'status': 0, 'elapsed_ms': round((time.perf_counter() - started) * 1000, 2), 'ok': False, 'error': str(exc)}


def _rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        for key in ('rows', 'items', 'documents', 'data'):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    return []


async def _first_detail(client: httpx.AsyncClient, list_url: str, detail_template: str, name: str) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        response = await client.get(list_url)
        response.raise_for_status()
        payload = response.json()
        rows = _rows(payload)
        external_id = ''
        for row in rows:
            external_id = str(row.get('external_id') or row.get('document_id') or row.get('id') or '').strip()
            if external_id:
                break
        if not external_id:
            return {'name': name, 'url': list_url, 'status': response.status_code, 'ok': False, 'elapsed_ms': round((time.perf_counter() - started) * 1000, 2), 'error': 'No document id in list response'}
        detail_url = detail_template.format(document_id=external_id)
        detail = await client.get(detail_url)
        return {
            'name': name,
            'url': detail_url,
            'status': detail.status_code,
            'elapsed_ms': round((time.perf_counter() - started) * 1000, 2),
            'bytes': len(detail.content or b''),
            'ok': 200 <= detail.status_code < 400 and len(detail.content or b'') > 0,
        }
    except Exception as exc:
        return {'name': name, 'url': list_url, 'status': 0, 'ok': False, 'elapsed_ms': round((time.perf_counter() - started) * 1000, 2), 'error': str(exc)}


async def run(args: argparse.Namespace) -> dict[str, Any]:
    base_url = args.base_url.rstrip('/')
    token = args.token.strip()
    headers = {'Host': args.host}
    auth_headers = {'Host': args.host, 'Authorization': f'Bearer {token}'}
    cookies = {'access_token': token}
    timeout = httpx.Timeout(60.0)
    results: list[dict[str, Any]] = []

    async with httpx.AsyncClient(base_url=base_url, headers=headers, timeout=timeout, follow_redirects=False) as public_client:
        results.append(await _hit(public_client, 'unauth_tenant_dashboard_redirect', '/tenant/dashboard', expect_redirect=True))
        results.append(await _hit(public_client, 'login_page', '/login'))

    async with httpx.AsyncClient(base_url=base_url, headers=auth_headers, cookies=cookies, timeout=timeout, follow_redirects=False) as client:
        for name, url, _needs_auth in HTML_ROUTES:
            if name == 'login_page':
                continue
            results.append(await _hit(client, name, url))
        for name, template in API_ROUTES:
            results.append(await _hit(client, name, template.format(from_date=args.from_date, to_date=args.to_date)))
        results.append(
            await _first_detail(
                client,
                f'/v1/kpi/sales/documents?from={args.from_date}&to={args.to_date}&limit=5',
                '/v1/kpi/sales/documents/{document_id}/detail',
                'sales_document_drilldown',
            )
        )
        results.append(
            await _first_detail(
                client,
                f'/v1/kpi/purchases/documents?from={args.from_date}&to={args.to_date}&limit=5',
                '/v1/kpi/purchases/documents/{document_id}/detail',
                'purchase_document_drilldown',
            )
        )
        results.append(
            await _first_detail(
                client,
                f'/v1/kpi/expenses/documents?from={args.from_date}&to={args.to_date}&limit=5',
                '/v1/kpi/expenses/documents/{document_id}/detail',
                'expense_document_drilldown',
            )
        )
        for name, template in EXPORT_ROUTES:
            results.append(await _hit(client, name, template.format(from_date=args.from_date, to_date=args.to_date)))
        results.append(await _hit(client, 'tenant_token_admin_area_blocked', '/admin/tenants'))

    failures = [row for row in results if not row.get('ok') or int(row.get('status') or 0) >= 500]
    report = {
        'generated_at': datetime.now(UTC).isoformat(timespec='seconds').replace('+00:00', 'Z'),
        'tenant': args.tenant,
        'window': {'from': args.from_date, 'to': args.to_date},
        'overall_status': 'FAIL' if failures else 'PASS',
        'failures': failures,
        'results': results,
    }
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
    out_file = out_dir / f'uat_real_flows_{args.tenant}_{stamp}.json'
    out_file.write_text(json.dumps(_json_safe(report), ensure_ascii=False, indent=2), encoding='utf-8')
    report['artifact'] = str(out_file)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', default='pharmacy295')
    parser.add_argument('--from-date', default='2026-05-01')
    parser.add_argument('--to-date', default='2026-05-21')
    parser.add_argument('--base-url', default='http://127.0.0.1:8000')
    parser.add_argument('--host', default='bi.boxvisio.com')
    parser.add_argument('--token', required=True)
    parser.add_argument('--out-dir', default='artifacts/uat')
    return parser.parse_args()


def main() -> None:
    report = asyncio.run(run(parse_args()))
    print(json.dumps(_json_safe({'overall_status': report['overall_status'], 'artifact': report['artifact'], 'failures': report['failures'], 'results': report['results']}), ensure_ascii=False, indent=2))
    if report['overall_status'] != 'PASS':
        raise SystemExit(2)


if __name__ == '__main__':
    main()
