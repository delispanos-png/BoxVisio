#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
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


async def _scalar(db: Any, sql: str, params: dict[str, Any] | None = None) -> Any:
    return (await db.execute(text(sql), params or {})).scalar()


async def _metrics(db: Any) -> dict[str, int]:
    row = (
        await db.execute(
            text(
                """
                select
                    count(*)::bigint as items_total,
                    count(*) filter (where coalesce(barcode, alternate_barcodes, '') = '')::bigint as items_missing_barcode,
                    count(*) filter (where vat_rate is null)::bigint as items_missing_vat,
                    count(*) filter (where abc_category is null or btrim(abc_category) = '')::bigint as items_missing_abc,
                    count(*) filter (where commercial_status is null or btrim(commercial_status) = '')::bigint as items_missing_commercial_status
                from dim_items
                """
            )
        )
    ).mappings().first()
    return {str(k): int(v or 0) for k, v in dict(row or {}).items()}


async def _update_from_inventory_payload(db: Any, *, dry_run: bool) -> dict[str, int]:
    sql = """
    with latest as (
        select *
        from (
            select
                fi.item_code,
                nullif(btrim(fi.source_payload_json ->> 'barcode'), '') as barcode,
                nullif(btrim(fi.source_payload_json ->> 'alternate_barcodes'), '') as alternate_barcodes,
                case
                    when replace(nullif(btrim(fi.source_payload_json ->> 'vat_rate'), ''), ',', '.') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                    then replace(nullif(btrim(fi.source_payload_json ->> 'vat_rate'), ''), ',', '.')::numeric
                    else null
                end as vat_rate,
                nullif(btrim(fi.source_payload_json ->> 'vat_label'), '') as vat_label,
                nullif(btrim(fi.source_payload_json ->> 'commercial_status'), '') as commercial_status,
                nullif(btrim(fi.source_payload_json ->> 'manual_order_category'), '') as manual_order_category,
                row_number() over (
                    partition by fi.item_code
                    order by fi.doc_date desc nulls last, fi.updated_at desc nulls last, fi.id desc
                ) as rn
            from fact_inventory fi
            where fi.item_code is not null
              and btrim(fi.item_code) <> ''
              and fi.source_payload_json is not null
        ) x
        where rn = 1
    ),
    candidates as (
        select di.id
        from dim_items di
        join latest l on l.item_code = di.external_id
        where
            (coalesce(di.barcode, '') = '' and l.barcode is not null)
            or (coalesce(di.alternate_barcodes, '') = '' and l.alternate_barcodes is not null)
            or (di.vat_rate is null and l.vat_rate is not null)
            or (coalesce(di.vat_label, '') = '' and l.vat_label is not null)
            or (coalesce(di.commercial_status, '') = '' and l.commercial_status is not null)
            or (coalesce(di.manual_order_category, '') = '' and l.manual_order_category is not null)
    )
    update dim_items di
    set
        barcode = coalesce(nullif(di.barcode, ''), l.barcode),
        alternate_barcodes = coalesce(nullif(di.alternate_barcodes, ''), l.alternate_barcodes),
        vat_rate = coalesce(di.vat_rate, l.vat_rate),
        vat_label = coalesce(nullif(di.vat_label, ''), l.vat_label),
        commercial_status = coalesce(nullif(di.commercial_status, ''), l.commercial_status),
        manual_order_category = coalesce(nullif(di.manual_order_category, ''), l.manual_order_category),
        updated_at = now()
    from latest l
    where di.external_id = l.item_code
      and di.id in (select id from candidates)
    returning di.id
    """
    if dry_run:
        count_sql = """
        with latest as (
            select *
            from (
                select
                    fi.item_code,
                    nullif(btrim(fi.source_payload_json ->> 'barcode'), '') as barcode,
                    nullif(btrim(fi.source_payload_json ->> 'alternate_barcodes'), '') as alternate_barcodes,
                    case
                        when replace(nullif(btrim(fi.source_payload_json ->> 'vat_rate'), ''), ',', '.') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                        then replace(nullif(btrim(fi.source_payload_json ->> 'vat_rate'), ''), ',', '.')::numeric
                        else null
                    end as vat_rate,
                    nullif(btrim(fi.source_payload_json ->> 'vat_label'), '') as vat_label,
                    nullif(btrim(fi.source_payload_json ->> 'commercial_status'), '') as commercial_status,
                    nullif(btrim(fi.source_payload_json ->> 'manual_order_category'), '') as manual_order_category,
                    row_number() over (
                        partition by fi.item_code
                        order by fi.doc_date desc nulls last, fi.updated_at desc nulls last, fi.id desc
                    ) as rn
                from fact_inventory fi
                where fi.item_code is not null
                  and btrim(fi.item_code) <> ''
                  and fi.source_payload_json is not null
            ) x
            where rn = 1
        )
        select
            count(*) filter (where coalesce(di.barcode, '') = '' and l.barcode is not null)::bigint as barcode,
            count(*) filter (where coalesce(di.alternate_barcodes, '') = '' and l.alternate_barcodes is not null)::bigint as alternate_barcodes,
            count(*) filter (where di.vat_rate is null and l.vat_rate is not null)::bigint as vat_rate,
            count(*) filter (where coalesce(di.commercial_status, '') = '' and l.commercial_status is not null)::bigint as commercial_status,
            count(*) filter (where coalesce(di.manual_order_category, '') = '' and l.manual_order_category is not null)::bigint as manual_order_category
        from dim_items di
        join latest l on l.item_code = di.external_id
        """
        row = (await db.execute(text(count_sql))).mappings().first()
        return {str(k): int(v or 0) for k, v in dict(row or {}).items()}
    result = await db.execute(text(sql))
    return {'items_updated': int(result.rowcount or 0)}


async def _derive_abc_from_sales(
    db: Any,
    *,
    from_date: date,
    to_date: date,
    fill_no_sales_abc: bool,
    dry_run: bool,
) -> dict[str, int]:
    base_sql = """
    with item_sales as (
        select
            di.id,
            di.external_id,
            coalesce(sum(greatest(fs.net_value, 0)), 0)::numeric as revenue
        from dim_items di
        left join fact_sales fs
          on fs.item_id = di.id
         and fs.doc_date between :from_date and :to_date
        where di.abc_category is null or btrim(di.abc_category) = ''
        group by di.id, di.external_id
    ),
    ranked as (
        select
            id,
            revenue,
            sum(revenue) over () as total_revenue,
            sum(revenue) over (order by revenue desc, external_id asc rows between unbounded preceding and current row) as running_revenue
        from item_sales
        where revenue > 0 or :fill_no_sales_abc
    ),
    classified as (
        select
            id,
            case
                when revenue <= 0 then 'D'
                when total_revenue <= 0 then 'D'
                when running_revenue / nullif(total_revenue, 0) <= 0.80 then 'A'
                when running_revenue / nullif(total_revenue, 0) <= 0.95 then 'B'
                else 'C'
            end as abc_category
        from ranked
    )
    """
    if dry_run:
        rows = (
            await db.execute(
                text(
                    base_sql
                    + """
                    select abc_category, count(*)::bigint as items
                    from classified
                    group by abc_category
                    order by abc_category
                    """
                ),
                {'from_date': from_date, 'to_date': to_date, 'fill_no_sales_abc': fill_no_sales_abc},
            )
        ).mappings().all()
        return {str(row['abc_category']): int(row['items'] or 0) for row in rows}
    result = await db.execute(
        text(
            base_sql
            + """
            update dim_items di
            set abc_category = c.abc_category,
                updated_at = now()
            from classified c
            where di.id = c.id
            returning di.id
            """
        ),
        {'from_date': from_date, 'to_date': to_date, 'fill_no_sales_abc': fill_no_sales_abc},
    )
    return {'items_updated': int(result.rowcount or 0)}


async def _derive_vat_from_sales(db: Any, *, dry_run: bool) -> dict[str, int]:
    # dim_items.vat_rate stores the Soft1 VAT code, not the percentage.
    # We only map rates that are already dominant in this tenant and can be
    # inferred cleanly from sales line VAT/net value.
    base_sql = """
    with inferred as (
        select
            di.id,
            round((sum(fs.vat_amount) / nullif(sum(fs.net_value), 0) * 100)::numeric, 1) as pct,
            count(*)::bigint as rows,
            sum(fs.net_value)::numeric as net_value
        from dim_items di
        join fact_sales fs on fs.item_code = di.external_id
        where di.vat_rate is null
          and fs.vat_amount is not null
          and fs.net_value > 0
        group by di.id
        having count(*) >= 3 and sum(fs.net_value) > 0
    ),
    classified as (
        select
            id,
            case pct
                when 0.0 then 0.00
                when 6.0 then 1060.00
                when 13.0 then 1131.00
                when 24.0 then 1410.00
                else null
            end as vat_rate,
            case pct
                when 0.0 then 'ΦΠΑ 0%'
                when 6.0 then 'ΦΠΑ 6%'
                when 13.0 then 'ΦΠΑ 13%'
                when 24.0 then 'ΦΠΑ 24%'
                else null
            end as vat_label
        from inferred
    )
    """
    if dry_run:
        rows = (
            await db.execute(
                text(
                    base_sql
                    + """
                    select vat_rate, vat_label, count(*)::bigint as items
                    from classified
                    where vat_rate is not null
                    group by vat_rate, vat_label
                    order by items desc
                    """
                )
            )
        ).mappings().all()
        return {f"{row['vat_rate']}|{row['vat_label']}": int(row['items'] or 0) for row in rows}
    result = await db.execute(
        text(
            base_sql
            + """
            update dim_items di
            set vat_rate = c.vat_rate,
                vat_label = coalesce(nullif(di.vat_label, ''), c.vat_label),
                updated_at = now()
            from classified c
            where di.id = c.id
              and c.vat_rate is not null
            returning di.id
            """
        )
    )
    return {'items_updated': int(result.rowcount or 0)}


async def run(args: argparse.Namespace) -> dict[str, Any]:
    tenant = await _load_tenant(args.tenant)
    if tenant is None:
        return {'status': 'FAIL', 'error': f'Tenant not found: {args.tenant}'}

    from_date = date.fromisoformat(args.from_date)
    to_date = date.fromisoformat(args.to_date)
    async with await _tenant_session(tenant) as db:
        before = await _metrics(db)
        inventory_payload = await _update_from_inventory_payload(db, dry_run=args.dry_run)
        abc = await _derive_abc_from_sales(
            db,
            from_date=from_date,
            to_date=to_date,
            fill_no_sales_abc=args.fill_no_sales_abc,
            dry_run=args.dry_run,
        )
        vat_from_sales = await _derive_vat_from_sales(db, dry_run=args.dry_run)
        if args.dry_run:
            await db.rollback()
        else:
            await db.commit()
        after = before if args.dry_run else await _metrics(db)

    report = {
        'generated_at': datetime.now(UTC).isoformat(timespec='seconds').replace('+00:00', 'Z'),
        'tenant': args.tenant,
        'window': {'from': from_date, 'to': to_date},
        'dry_run': bool(args.dry_run),
        'fill_no_sales_abc': bool(args.fill_no_sales_abc),
        'before': before,
        'actions': {
            'inventory_payload_enrichment': inventory_payload,
            'bi_derived_abc': abc,
            'bi_derived_vat_from_sales': vat_from_sales,
        },
        'after': after,
        'notes': [
            'Barcode, VAT, commercial status and manual order category are filled only from real inventory payload fields.',
            'ABC is BI-derived from sales net value in the selected window; no-sales items become D only when --fill-no-sales-abc is used.',
            'Missing VAT is filled only when sales lines infer a standard tenant VAT code with at least 3 positive-net rows.',
            'No synthetic barcodes or fake commercial statuses are generated.',
        ],
    }
    out_dir = _project_path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
    out_path = out_dir / f"tenant_dimension_enrichment_{args.tenant}_{stamp}.json"
    out_path.write_text(json.dumps(_json_safe(report), ensure_ascii=False, indent=2), encoding='utf-8')
    report['artifact'] = str(out_path)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Safely enrich tenant dimensions from imported BI facts.')
    parser.add_argument('--tenant', default='pharmacy295')
    parser.add_argument('--from-date', default='2025-01-01')
    parser.add_argument('--to-date', default=_today().isoformat())
    parser.add_argument('--out-dir', default='artifacts/enrichment')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--fill-no-sales-abc', action='store_true')
    return parser.parse_args()


def main() -> None:
    report = asyncio.run(run(parse_args()))
    print(json.dumps(_json_safe(report), ensure_ascii=False, indent=2))
    if report.get('status') == 'FAIL':
        raise SystemExit(2)


if __name__ == '__main__':
    main()
