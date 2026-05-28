#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import sys
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import select, text

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parents[1]
BACKEND_ROOT = PROJECT_ROOT / 'backend'
sys.path.insert(0, str(BACKEND_ROOT))
sys.path.insert(0, str(PROJECT_ROOT))

from app.db.control_session import ControlSessionLocal  # noqa: E402
from app.db.tenant_manager import get_tenant_session_factory  # noqa: E402
from app.models.control import Tenant  # noqa: E402


FIELD_ALIASES = {
    'item_code': ('item_code', 'item_external_id', 'external_id', 'code', 'mtrl_code', 'κωδικος', 'κωδικός'),
    'barcode': ('barcode', 'code1', 'ean', 'ean13', 'bar_code'),
    'alternate_barcodes': ('alternate_barcodes', 'alt_barcodes', 'secondary_barcodes'),
    'vat_rate': ('vat_rate', 'vat', 'fpa', 'softone_vat_code'),
    'vat_label': ('vat_label', 'vat_name', 'fpa_label'),
    'commercial_status': ('commercial_status', 'item_commercial_status', 'utbl05', 'status'),
    'manual_order_category': ('manual_order_category', 'utbl04', 'order_category'),
}


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


def _clean_header(value: object) -> str:
    return str(value or '').strip().lower().replace(' ', '_').replace('-', '_')


def _pick(row: dict[str, str], key: str) -> str:
    normalized = {_clean_header(k): str(v or '').strip() for k, v in row.items()}
    for alias in FIELD_ALIASES[key]:
        value = normalized.get(_clean_header(alias))
        if value:
            return value
    return ''


def _optional_decimal(value: str) -> Decimal | None:
    clean = str(value or '').strip().replace(',', '.')
    if not clean:
        return None
    try:
        return Decimal(clean)
    except Exception:
        return None


async def _load_tenant(tenant_slug: str) -> Tenant | None:
    async with ControlSessionLocal() as db:
        return (await db.execute(select(Tenant).where(Tenant.slug == tenant_slug))).scalars().first()


async def _tenant_session(tenant: Tenant):
    factory = get_tenant_session_factory(
        tenant_key=str(tenant.id),
        db_name=tenant.db_name,
        db_user=tenant.db_user,
        db_password=tenant.db_password,
    )
    return factory()


async def export_gaps(args: argparse.Namespace) -> dict[str, Any]:
    tenant = await _load_tenant(args.tenant)
    if tenant is None:
        return {'overall_status': 'FAIL', 'error': f'Tenant not found: {args.tenant}'}
    out_path = _project_path(args.file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    async with await _tenant_session(tenant) as db:
        rows = (
            await db.execute(
                text(
                    """
                    select
                        external_id as item_code,
                        coalesce(name, '') as item_name,
                        coalesce(barcode, '') as barcode,
                        coalesce(alternate_barcodes, '') as alternate_barcodes,
                        coalesce(vat_rate::text, '') as vat_rate,
                        coalesce(vat_label, '') as vat_label,
                        coalesce(commercial_status, '') as commercial_status,
                        coalesce(manual_order_category, '') as manual_order_category,
                        (coalesce(barcode, alternate_barcodes, '') = '') as missing_barcode,
                        (vat_rate is null) as missing_vat,
                        (coalesce(commercial_status, '') = '') as missing_commercial_status
                    from dim_items
                    where coalesce(barcode, alternate_barcodes, '') = ''
                       or vat_rate is null
                       or coalesce(commercial_status, '') = ''
                    order by external_id
                    """
                )
            )
        ).mappings().all()
    fieldnames = [
        'item_code',
        'item_name',
        'barcode',
        'alternate_barcodes',
        'vat_rate',
        'vat_label',
        'commercial_status',
        'manual_order_category',
        'missing_barcode',
        'missing_vat',
        'missing_commercial_status',
    ]
    with out_path.open('w', encoding='utf-8-sig', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})
    return {'overall_status': 'PASS', 'tenant': args.tenant, 'rows': len(rows), 'file': str(out_path)}


async def import_csv(args: argparse.Namespace) -> dict[str, Any]:
    tenant = await _load_tenant(args.tenant)
    if tenant is None:
        return {'overall_status': 'FAIL', 'error': f'Tenant not found: {args.tenant}'}
    in_path = _project_path(args.file)
    if not in_path.exists():
        return {'overall_status': 'FAIL', 'error': f'CSV not found: {in_path}'}

    rows: list[dict[str, Any]] = []
    with in_path.open('r', encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            item_code = _pick(raw, 'item_code')
            if not item_code:
                continue
            rows.append(
                {
                    'item_code': item_code[:128],
                    'barcode': (_pick(raw, 'barcode') or None),
                    'alternate_barcodes': (_pick(raw, 'alternate_barcodes') or None),
                    'vat_rate': _optional_decimal(_pick(raw, 'vat_rate')),
                    'vat_label': (_pick(raw, 'vat_label') or None),
                    'commercial_status': (_pick(raw, 'commercial_status') or None),
                    'manual_order_category': (_pick(raw, 'manual_order_category') or None),
                }
            )

    updated = 0
    matched = 0
    async with await _tenant_session(tenant) as db:
        for row in rows:
            result = await db.execute(
                text(
                    """
                    update dim_items
                    set
                        barcode = coalesce(nullif(barcode, ''), :barcode),
                        alternate_barcodes = coalesce(nullif(alternate_barcodes, ''), :alternate_barcodes),
                        vat_rate = coalesce(vat_rate, :vat_rate),
                        vat_label = coalesce(nullif(vat_label, ''), :vat_label),
                        commercial_status = coalesce(nullif(commercial_status, ''), :commercial_status),
                        manual_order_category = coalesce(nullif(manual_order_category, ''), :manual_order_category),
                        updated_at = now()
                    where external_id = :item_code
                      and (
                          (coalesce(barcode, '') = '' and :barcode is not null)
                          or (coalesce(alternate_barcodes, '') = '' and :alternate_barcodes is not null)
                          or (vat_rate is null and :vat_rate is not null)
                          or (coalesce(vat_label, '') = '' and :vat_label is not null)
                          or (coalesce(commercial_status, '') = '' and :commercial_status is not null)
                          or (coalesce(manual_order_category, '') = '' and :manual_order_category is not null)
                      )
                    """
                ),
                row,
            )
            if int(result.rowcount or 0) > 0:
                updated += int(result.rowcount or 0)
            matched += int(
                await db.scalar(text('select count(*) from dim_items where external_id = :item_code'), {'item_code': row['item_code']})
                or 0
            )
        if args.dry_run:
            await db.rollback()
        else:
            await db.commit()
    return {
        'overall_status': 'PASS',
        'tenant': args.tenant,
        'dry_run': bool(args.dry_run),
        'input_rows': len(rows),
        'matched_items': matched,
        'updated_items': updated,
        'file': str(in_path),
    }


async def run(args: argparse.Namespace) -> dict[str, Any]:
    if args.mode == 'export-gaps':
        return await export_gaps(args)
    return await import_csv(args)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Export/import item master enrichment CSV for barcode/VAT/status gaps.')
    parser.add_argument('mode', choices=['export-gaps', 'import'])
    parser.add_argument('--tenant', default='pharmacy295')
    parser.add_argument('--file', default='artifacts/enrichment/item_master_gaps.csv')
    parser.add_argument('--dry-run', action='store_true')
    return parser.parse_args()


def main() -> None:
    report = asyncio.run(run(parse_args()))
    print(json.dumps(_json_safe(report), ensure_ascii=False, indent=2))
    if report.get('overall_status') == 'FAIL':
        raise SystemExit(2)


if __name__ == '__main__':
    main()
