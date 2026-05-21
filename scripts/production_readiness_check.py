#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import select, text

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parents[1]
BACKEND_ROOT = PROJECT_ROOT / 'backend'
sys.path.insert(0, str(BACKEND_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))

from app.core.security import create_access_token  # noqa: E402
from app.db.control_session import ControlSessionLocal  # noqa: E402
from app.db.tenant_manager import get_tenant_session_factory  # noqa: E402
from app.models.control import RoleName, Tenant, TenantConnection, User  # noqa: E402
from app.services.ingestion.base import ALL_OPERATIONAL_STREAMS  # noqa: E402
from app.services.ingestion.chunking import stream_chunk_policy  # noqa: E402
from app.services.ingestion.queueing import priority_pool_snapshot, priority_pool_depth  # noqa: E402


FACT_STREAMS: dict[str, dict[str, str]] = {
    'sales_documents': {'table': 'fact_sales', 'date': 'doc_date', 'amount': 'net_value'},
    'purchase_documents': {'table': 'fact_purchases', 'date': 'doc_date', 'amount': 'net_value'},
    'inventory_documents': {'table': 'fact_inventory', 'date': 'doc_date', 'amount': 'value_amount'},
    'cash_transactions': {'table': 'fact_cashflows', 'date': 'doc_date', 'amount': 'amount'},
    'supplier_balances': {'table': 'fact_supplier_balances', 'date': 'balance_date', 'amount': 'open_balance'},
    'customer_balances': {'table': 'fact_customer_balances', 'date': 'balance_date', 'amount': 'open_balance'},
    'operating_expenses': {'table': 'fact_expenses', 'date': 'expense_date', 'amount': 'amount_gross'},
    'supplier_orders': {'table': 'fact_supplier_orders', 'date': 'doc_date', 'amount': 'line_value'},
}

AGG_CHECKS: dict[str, dict[str, str]] = {
    'sales_daily': {'table': 'agg_sales_daily', 'date': 'doc_date'},
    'purchases_daily': {'table': 'agg_purchases_daily', 'date': 'doc_date'},
    'inventory_snapshot': {'table': 'agg_inventory_snapshot_daily', 'date': 'snapshot_date'},
    'cash_daily': {'table': 'agg_cash_daily', 'date': 'doc_date'},
    'supplier_balances_daily': {'table': 'agg_supplier_balances_daily', 'date': 'balance_date'},
    'customer_balances_daily': {'table': 'agg_customer_balances_daily', 'date': 'balance_date'},
    'expenses_daily': {'table': 'agg_expenses_daily', 'date': 'expense_date'},
}

SMOKE_ENDPOINTS = (
    ('tenant_dashboard', '/tenant/dashboard'),
    ('sales_dashboard', '/tenant/sales'),
    ('purchases_dashboard', '/tenant/purchases'),
    ('inventory_dashboard', '/tenant/inventory'),
    ('items_dashboard', '/tenant/items'),
    ('cashflow_dashboard', '/tenant/cashflow'),
    ('suppliers_dashboard', '/tenant/suppliers'),
    ('customers_dashboard', '/tenant/customers'),
    ('supplier_orders_dashboard', '/tenant/supplier-orders'),
    ('replenishment_dashboard', '/tenant/replenishment'),
    ('sales_summary_api', '/v1/kpi/sales/summary?from={from_date}&to={to_date}'),
    ('purchases_summary_api', '/v1/kpi/purchases/summary?from={from_date}&to={to_date}'),
    ('cashflow_summary_api', '/v1/kpi/cashflow/summary?from={from_date}&to={to_date}'),
    ('suppliers_api', '/v1/kpi/suppliers?from={from_date}&to={to_date}&limit=20'),
)


@dataclass
class CheckResult:
    name: str
    status: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


def _today() -> date:
    return datetime.now(UTC).date()


def _status_rank(status: str) -> int:
    return {'PASS': 0, 'WARN': 1, 'FAIL': 2}.get(status, 2)


def _overall_status(checks: list[CheckResult]) -> str:
    if any(check.status == 'FAIL' for check in checks):
        return 'FAIL'
    if any(check.status == 'WARN' for check in checks):
        return 'WARN'
    return 'PASS'


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


def _read_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}


def _bridge_streams() -> set[str]:
    bridge_path = PROJECT_ROOT / 'integrations/softone/boxvisio_bi_bridge.js'
    text_value = bridge_path.read_text(encoding='utf-8', errors='replace')
    streams = set(re.findall(r'stream_code:\s*"([^"]+)"', text_value))
    streams.update(re.findall(r'_bv_stream_result\("([^"]+)"', text_value))
    return {stream for stream in streams if stream in set(ALL_OPERATIONAL_STREAMS)}


def _querypack_streams() -> set[str]:
    facts_dir = PROJECT_ROOT / 'backend/querypacks/pharmacyone/facts'
    mapping = _read_json(PROJECT_ROOT / 'backend/querypacks/pharmacyone/mapping.json')
    streams = set()
    file_map = {
        'sales_facts.sql': 'sales_documents',
        'purchases_facts.sql': 'purchase_documents',
        'inventory_facts.sql': 'inventory_documents',
        'cashflow_facts.sql': 'cash_transactions',
        'supplier_balances_facts.sql': 'supplier_balances',
        'customer_balances_facts.sql': 'customer_balances',
        'expenses_facts.sql': 'operating_expenses',
        'supplier_orders_facts.sql': 'supplier_orders',
    }
    for file_path in facts_dir.glob('*_facts.sql'):
        if file_path.name in file_map:
            streams.add(file_map[file_path.name])
    required_cols = set((mapping.get('required_output_columns') or {}).keys())
    if {'document_behavior_code', 'order_qty', 'covered_qty', 'order_status'} <= required_cols:
        streams.add('supplier_orders')
    return {stream for stream in streams if stream in set(ALL_OPERATIONAL_STREAMS)}


async def _load_tenant(tenant_slug: str) -> tuple[Tenant | None, list[TenantConnection]]:
    async with ControlSessionLocal() as db:
        tenant = (await db.execute(select(Tenant).where(Tenant.slug == tenant_slug))).scalars().first()
        if tenant is None:
            return None, []
        connections = (
            await db.execute(
                select(TenantConnection)
                .where(TenantConnection.tenant_id == tenant.id, TenantConnection.is_active.is_(True))
                .order_by(TenantConnection.id.asc())
            )
        ).scalars().all()
        return tenant, list(connections)


async def _tenant_user_token(tenant: Tenant) -> str | None:
    async with ControlSessionLocal() as db:
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
            return None
        return create_access_token(
            subject=str(user.id),
            tenant_id=user.tenant_id,
            role=user.role.value,
            audience='tenant',
        )


async def _control_migration_check() -> dict[str, Any]:
    async with ControlSessionLocal() as db:
        try:
            row = (await db.execute(text('select version_num from alembic_version_control limit 1'))).first()
            return {'version': row[0] if row else None}
        except Exception as exc:
            return {'error': str(exc)}


async def _tenant_session(tenant: Tenant):
    factory = get_tenant_session_factory(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    )
    return factory()


async def _tenant_scalar(tenant: Tenant, sql: str, params: dict[str, Any] | None = None) -> Any:
    async with await _tenant_session(tenant) as db:
        return (await db.execute(text(sql), params or {})).scalar()


async def _tenant_rows(tenant: Tenant, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    async with await _tenant_session(tenant) as db:
        result = await db.execute(text(sql), params or {})
        return [dict(row._mapping) for row in result]


async def _table_exists(tenant: Tenant, table_name: str) -> bool:
    value = await _tenant_scalar(
        tenant,
        "select to_regclass('public.' || :table_name) is not null",
        {'table_name': table_name},
    )
    return bool(value)


async def _fact_coverage(tenant: Tenant, from_date: date, to_date: date) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for stream, cfg in FACT_STREAMS.items():
        if not await _table_exists(tenant, cfg['table']):
            out[stream] = {'exists': False}
            continue
        rows = await _tenant_rows(
            tenant,
            f"""
            select
                count(*)::bigint as rows,
                count(*) filter (where {cfg['date']} between :from_date and :to_date)::bigint as rows_in_window,
                min({cfg['date']}) as min_date,
                max({cfg['date']}) as max_date,
                coalesce(sum({cfg['amount']}) filter (where {cfg['date']} between :from_date and :to_date), 0)::numeric as amount_in_window,
                count(*) filter (where external_id is null or btrim(external_id) = '')::bigint as missing_external_id
            from {cfg['table']}
            """,
            {'from_date': from_date, 'to_date': to_date},
        )
        out[stream] = {'exists': True, **(rows[0] if rows else {})}
    return out


async def _aggregate_coverage(tenant: Tenant) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for name, cfg in AGG_CHECKS.items():
        if not await _table_exists(tenant, cfg['table']):
            out[name] = {'exists': False}
            continue
        rows = await _tenant_rows(
            tenant,
            f"select count(*)::bigint as rows, min({cfg['date']}) as min_date, max({cfg['date']}) as max_date from {cfg['table']}",
        )
        out[name] = {'exists': True, **(rows[0] if rows else {})}
    return out


async def _data_quality(tenant: Tenant) -> dict[str, Any]:
    checks: dict[str, Any] = {}
    queries = {
        'items_total': "select count(*)::bigint from dim_items",
        'items_missing_name': "select count(*)::bigint from dim_items where name is null or btrim(name) = '' or name = external_id",
        'items_missing_barcode': "select count(*)::bigint from dim_items where coalesce(barcode, alternate_barcodes, '') = ''",
        'items_missing_vat': "select count(*)::bigint from dim_items where vat_rate is null",
        'items_missing_abc': "select count(*)::bigint from dim_items where abc_category is null or btrim(abc_category) = ''",
        'items_missing_commercial_status': "select count(*)::bigint from dim_items where commercial_status is null or btrim(commercial_status) = ''",
        'customers_total': "select count(*)::bigint from dim_customers",
        'customers_name_equals_code': "select count(*)::bigint from dim_customers where name = external_id or name = customer_code",
        'suppliers_total': "select count(*)::bigint from dim_suppliers",
        'suppliers_name_equals_code': "select count(*)::bigint from dim_suppliers where name = external_id",
        'sales_missing_item_link': "select count(*)::bigint from fact_sales where item_id is null and item_code is not null",
        'purchases_missing_item_link': "select count(*)::bigint from fact_purchases where item_id is null and item_code is not null",
        'supplier_orders_missing_supplier': "select count(*)::bigint from fact_supplier_orders where supplier_ext_id is null or supplier_ext_id = ''",
        'supplier_orders_missing_item': "select count(*)::bigint from fact_supplier_orders where item_code is null or item_code = ''",
    }
    for key, sql in queries.items():
        table = sql.split(' from ', 1)[1].split()[0]
        if not await _table_exists(tenant, table):
            checks[key] = None
            continue
        checks[key] = int(await _tenant_scalar(tenant, sql) or 0)
    return checks


async def _smoke_endpoints(tenant: Tenant, token: str | None, base_url: str, from_date: date, to_date: date) -> list[dict[str, Any]]:
    if not token:
        return [{'name': 'auth', 'status': 0, 'elapsed_ms': 0, 'ok': False, 'error': 'No active tenant user'}]
    headers = {'Authorization': f'Bearer {token}', 'Host': 'bi.boxvisio.com'}
    cookies = {'access_token': token}
    results: list[dict[str, Any]] = []
    async with httpx.AsyncClient(
        base_url=base_url.rstrip('/'),
        headers=headers,
        cookies=cookies,
        timeout=45.0,
        follow_redirects=False,
    ) as client:
        for name, template in SMOKE_ENDPOINTS:
            url = template.format(from_date=from_date.isoformat(), to_date=to_date.isoformat())
            started = time.perf_counter()
            try:
                response = await client.get(url)
                elapsed_ms = round((time.perf_counter() - started) * 1000.0, 2)
                results.append(
                    {
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
                results.append({'name': name, 'url': url, 'status': 0, 'elapsed_ms': elapsed_ms, 'ok': False, 'error': str(exc)})
    return results


def _write_reports(report: dict[str, Any], out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
    base = f"production_readiness_{report['tenant']['slug']}_{stamp}"
    json_path = out_dir / f'{base}.json'
    md_path = out_dir / f'{base}.md'
    json_path.write_text(json.dumps(_json_safe(report), ensure_ascii=False, indent=2), encoding='utf-8')
    lines = [
        f"# Production Readiness - {report['tenant']['slug']}",
        '',
        f"Overall: **{report['overall_status']}**",
        f"Window: {report['window']['from']} -> {report['window']['to']}",
        '',
        '## Checks',
    ]
    for check in report['checks']:
        lines.append(f"- **{check['status']}** `{check['name']}`: {check['message']}")
    lines.extend(['', '## Artifacts', f"- JSON: `{json_path}`"])
    md_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    return json_path, md_path


def _make_check(name: str, status: str, message: str, details: dict[str, Any] | None = None) -> CheckResult:
    return CheckResult(name=name, status=status, message=message, details=details or {})


async def run(args: argparse.Namespace) -> dict[str, Any]:
    from_date = date.fromisoformat(args.from_date)
    to_date = date.fromisoformat(args.to_date)
    tenant, connections = await _load_tenant(args.tenant)
    checks: list[CheckResult] = []
    if tenant is None:
        return {
            'overall_status': 'FAIL',
            'tenant': {'slug': args.tenant},
            'window': {'from': from_date, 'to': to_date},
            'checks': [_make_check('tenant_exists', 'FAIL', 'Tenant not found').__dict__],
        }

    active_connection = connections[0] if connections else None
    checks.append(
        _make_check(
            'tenant_connection',
            'PASS' if active_connection else 'FAIL',
            'Active connector found' if active_connection else 'No active connector',
            {'connections': [{'id': c.id, 'type': c.connector_type, 'status': c.sync_status} for c in connections]},
        )
    )

    expected_streams = set(ALL_OPERATIONAL_STREAMS)
    enabled_streams = set(active_connection.enabled_streams or []) if active_connection else set()
    supported_streams = set(active_connection.supported_streams or []) if active_connection else set()
    missing_enabled = sorted(expected_streams - enabled_streams)
    missing_supported = sorted(expected_streams - supported_streams)
    mapping = active_connection.stream_query_mapping if active_connection and isinstance(active_connection.stream_query_mapping, dict) else {}
    missing_mapping = sorted(stream for stream in expected_streams if not str(mapping.get(stream) or '').strip())
    checks.append(
        _make_check(
            'connector_stream_parity',
            'PASS' if not missing_enabled and not missing_supported and not missing_mapping else 'FAIL',
            'Connector has all production streams enabled, supported and mapped'
            if not missing_enabled and not missing_supported and not missing_mapping
            else 'Connector stream parity gap',
            {'missing_enabled': missing_enabled, 'missing_supported': missing_supported, 'missing_mapping': missing_mapping},
        )
    )

    bridge_streams = _bridge_streams()
    querypack_streams = _querypack_streams()
    checks.append(
        _make_check(
            'sql_js_bridge_parity',
            'PASS' if bridge_streams == querypack_streams == expected_streams else 'FAIL',
            'SQL querypack and JavaScript bridge expose the same streams'
            if bridge_streams == querypack_streams == expected_streams
            else 'SQL querypack and JavaScript bridge mismatch',
            {
                'expected': sorted(expected_streams),
                'bridge': sorted(bridge_streams),
                'querypack': sorted(querypack_streams),
                'bridge_missing': sorted(expected_streams - bridge_streams),
                'querypack_missing': sorted(expected_streams - querypack_streams),
            },
        )
    )

    control_migration = await _control_migration_check()
    tenant_migration = {}
    try:
        tenant_migration['version'] = await _tenant_scalar(tenant, 'select version_num from alembic_version_tenant limit 1')
    except Exception as exc:
        tenant_migration['error'] = str(exc)
    checks.append(
        _make_check(
            'migrations_present',
            'PASS' if control_migration.get('version') and tenant_migration.get('version') else 'FAIL',
            'Control and tenant alembic versions are present',
            {'control': control_migration, 'tenant': tenant_migration},
        )
    )

    fact_coverage = await _fact_coverage(tenant, from_date, to_date)
    empty_streams = sorted(stream for stream, row in fact_coverage.items() if row.get('exists') and int(row.get('rows_in_window') or 0) == 0)
    missing_tables = sorted(stream for stream, row in fact_coverage.items() if not row.get('exists'))
    checks.append(
        _make_check(
            'fact_coverage',
            'FAIL' if missing_tables else ('WARN' if empty_streams else 'PASS'),
            'Facts have rows in the requested window' if not empty_streams and not missing_tables else 'Some fact streams are missing or empty',
            {'missing_tables': missing_tables, 'empty_streams': empty_streams, 'coverage': fact_coverage},
        )
    )

    aggregate_coverage = await _aggregate_coverage(tenant)
    empty_aggs = sorted(name for name, row in aggregate_coverage.items() if row.get('exists') and int(row.get('rows') or 0) == 0)
    missing_aggs = sorted(name for name, row in aggregate_coverage.items() if not row.get('exists'))
    checks.append(
        _make_check(
            'aggregate_coverage',
            'WARN' if empty_aggs or missing_aggs else 'PASS',
            'Aggregates are populated' if not empty_aggs and not missing_aggs else 'Some aggregates are missing or empty',
            {'missing': missing_aggs, 'empty': empty_aggs, 'coverage': aggregate_coverage},
        )
    )

    dq = await _data_quality(tenant)
    dq_warn_keys = [
        key
        for key in (
            'items_missing_name',
            'items_missing_barcode',
            'items_missing_vat',
            'items_missing_abc',
            'items_missing_commercial_status',
            'customers_name_equals_code',
            'suppliers_name_equals_code',
            'sales_missing_item_link',
            'purchases_missing_item_link',
        )
        if int(dq.get(key) or 0) > 0
    ]
    checks.append(
        _make_check(
            'data_quality',
            'WARN' if dq_warn_keys else 'PASS',
            'Core dimensions/facts pass data quality checks' if not dq_warn_keys else 'Data quality issues detected',
            {'warn_keys': dq_warn_keys, 'metrics': dq},
        )
    )

    token = await _tenant_user_token(tenant)
    smoke = await _smoke_endpoints(tenant, token, args.base_url, from_date, to_date)
    failed_smoke = [row for row in smoke if not row.get('ok')]
    slow_smoke = [row for row in smoke if row.get('ok') and float(row.get('elapsed_ms') or 0) > float(args.slow_ms)]
    checks.append(
        _make_check(
            'dashboard_smoke_and_performance',
            'FAIL' if failed_smoke else ('WARN' if slow_smoke else 'PASS'),
            'Dashboards/API endpoints respond within threshold' if not failed_smoke and not slow_smoke else 'Dashboard smoke/performance issues detected',
            {'failed': failed_smoke, 'slow': slow_smoke, 'results': smoke, 'slow_ms': args.slow_ms},
        )
    )

    pool = {'depth': priority_pool_depth(), 'rows': priority_pool_snapshot(25)}
    chunk_policy = stream_chunk_policy(int(args.chunk_days))
    checks.append(
        _make_check(
            'ingestion_runtime_policy',
            'PASS',
            'Priority pool and stream chunk policy are available',
            {'priority_pool': pool, 'chunk_policy': chunk_policy},
        )
    )

    report = {
        'generated_at': datetime.now(UTC).isoformat(timespec='seconds').replace('+00:00', 'Z'),
        'overall_status': _overall_status(checks),
        'tenant': {
            'id': tenant.id,
            'slug': tenant.slug,
            'name': tenant.name,
            'plan': getattr(tenant.plan, 'value', str(tenant.plan)),
            'status': getattr(tenant.status, 'value', str(tenant.status)),
            'subscription_status': getattr(tenant.subscription_status, 'value', str(tenant.subscription_status)),
        },
        'window': {'from': from_date, 'to': to_date},
        'checks': [_json_safe(check.__dict__) for check in sorted(checks, key=lambda c: (_status_rank(c.status), c.name), reverse=True)],
    }
    json_path, md_path = _write_reports(report, Path(args.out_dir))
    report['artifacts'] = {'json': str(json_path), 'markdown': str(md_path)}
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run production readiness checks for one BI tenant.')
    parser.add_argument('--tenant', default='pharmacy295')
    parser.add_argument('--from-date', default='2025-01-01')
    parser.add_argument('--to-date', default=_today().isoformat())
    parser.add_argument('--base-url', default='http://127.0.0.1:8000')
    parser.add_argument('--out-dir', default='artifacts/production_readiness')
    parser.add_argument('--slow-ms', type=int, default=2500)
    parser.add_argument('--chunk-days', type=int, default=31)
    return parser.parse_args()


def main() -> None:
    report = asyncio.run(run(parse_args()))
    print(json.dumps(_json_safe({'overall_status': report['overall_status'], 'artifacts': report.get('artifacts'), 'checks': report['checks']}), ensure_ascii=False, indent=2))
    if report['overall_status'] == 'FAIL':
        raise SystemExit(2)
    if report['overall_status'] == 'WARN':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
