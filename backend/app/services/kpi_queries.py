from datetime import date, timedelta

from sqlalchemy import Date, Integer, String, case, cast, func, literal, literal_column, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql import over

from app.services.intelligence_service import list_recent_insights
from app.models.tenant import (
    AggPurchasesDaily,
    AggSalesDaily,
    DimBranch,
    DimBrand,
    DimCategory,
    DimGroup,
    DimItem,
    DimSupplier,
    DimWarehouse,
    FactCashflow,
    FactInventory,
    FactPurchases,
    FactSales,
    SupplierTarget,
    SupplierTargetItem,
)


def _date_range(column, date_from: date, date_to: date):
    return column >= date_from, column <= date_to


def _clean_item_name(name: str | None, fallback: str | None = None) -> str:
    raw = str(name or fallback or '').strip()
    if not raw:
        return 'N/A'
    cleaned = raw
    if cleaned.endswith(')') and '(ITM' in cleaned.upper():
        left = cleaned.rfind('(ITM')
        if left >= 0:
            cleaned = cleaned[:left].strip()
    parts = cleaned.split()
    if parts and parts[-1].upper().startswith('ITM') and parts[-1][3:].isdigit():
        cleaned = ' '.join(parts[:-1]).strip()
    return cleaned or raw


def _apply_sales_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None):
    if branches:
        stmt = stmt.where(AggSalesDaily.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(AggSalesDaily.warehouse_ext_id.in_(warehouses))
    if brands:
        stmt = stmt.where(AggSalesDaily.brand_ext_id.in_(brands))
    if categories:
        stmt = stmt.where(AggSalesDaily.category_ext_id.in_(categories))
    if groups:
        stmt = stmt.where(AggSalesDaily.group_ext_id.in_(groups))
    return stmt


def _apply_purchase_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None):
    if branches:
        stmt = stmt.where(AggPurchasesDaily.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(AggPurchasesDaily.warehouse_ext_id.in_(warehouses))
    if brands:
        stmt = stmt.where(AggPurchasesDaily.brand_ext_id.in_(brands))
    if categories:
        stmt = stmt.where(AggPurchasesDaily.category_ext_id.in_(categories))
    if groups:
        stmt = stmt.where(AggPurchasesDaily.group_ext_id.in_(groups))
    return stmt


def _apply_fact_sales_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None):
    if branches:
        stmt = stmt.where(FactSales.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactSales.warehouse_ext_id.in_(warehouses))
    if brands:
        stmt = stmt.where(FactSales.brand_ext_id.in_(brands))
    if categories:
        stmt = stmt.where(FactSales.category_ext_id.in_(categories))
    if groups:
        stmt = stmt.where(FactSales.group_ext_id.in_(groups))
    return stmt


def _season_case(date_col):
    month_expr = cast(func.extract('month', date_col), Integer)
    return case(
        (month_expr.in_([12, 1, 2]), 'winter'),
        (month_expr.in_([3, 4, 5]), 'spring'),
        (month_expr.in_([6, 7, 8]), 'summer'),
        else_='autumn',
    )


def _inventory_base_stmt():
    return (
        select(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
    )


def _apply_inventory_filters(stmt, branches=None, warehouses=None, brands=None, categories=None, groups=None):
    if branches:
        stmt = stmt.where(DimBranch.external_id.in_(branches))
    if warehouses:
        stmt = stmt.where(DimWarehouse.external_id.in_(warehouses))
    if brands:
        stmt = stmt.where(DimBrand.external_id.in_(brands))
    if categories:
        stmt = stmt.where(DimCategory.external_id.in_(categories))
    if groups:
        stmt = stmt.where(DimGroup.external_id.in_(groups))
    return stmt


async def sales_summary(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    stmt = (
        select(
            func.count(AggSalesDaily.id),
            func.coalesce(func.sum(AggSalesDaily.qty), 0),
            func.coalesce(func.sum(AggSalesDaily.net_value), 0),
            func.coalesce(func.sum(AggSalesDaily.gross_value), 0),
        )
        .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_sales_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    row = (await db.execute(stmt)).one()
    return {
        'records': int(row[0] or 0),
        'qty': float(row[1] or 0),
        'net_value': float(row[2] or 0),
        'gross_value': float(row[3] or 0),
    }


async def sales_by_branch(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    stmt = (
        select(
            AggSalesDaily.branch_ext_id,
            func.coalesce(func.max(DimBranch.name), AggSalesDaily.branch_ext_id).label('branch_name'),
            func.coalesce(func.sum(AggSalesDaily.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggSalesDaily.gross_value), 0).label('gross_value'),
        )
        .join(DimBranch, DimBranch.external_id == AggSalesDaily.branch_ext_id, isouter=True)
        .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_sales_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(AggSalesDaily.branch_ext_id).order_by(func.sum(AggSalesDaily.net_value).desc())
    rows = (await db.execute(stmt)).all()
    return [
        {
            'branch': r[1] or r[0] or 'N/A',
            'branch_code': r[0] or 'N/A',
            'net_value': float(r[2] or 0),
            'gross_value': float(r[3] or 0),
        }
        for r in rows
    ]


async def sales_by_brand(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    stmt = (
        select(
            AggSalesDaily.brand_ext_id,
            func.coalesce(func.max(DimBrand.name), AggSalesDaily.brand_ext_id).label('brand_name'),
            func.coalesce(func.sum(AggSalesDaily.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggSalesDaily.gross_value), 0).label('gross_value'),
        )
        .join(DimBrand, DimBrand.external_id == AggSalesDaily.brand_ext_id, isouter=True)
        .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_sales_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(AggSalesDaily.brand_ext_id).order_by(func.sum(AggSalesDaily.net_value).desc())
    rows = (await db.execute(stmt)).all()
    return [
        {
            'brand': r[1] or r[0] or 'N/A',
            'brand_code': r[0] or 'N/A',
            'net_value': float(r[2] or 0),
            'gross_value': float(r[3] or 0),
        }
        for r in rows
    ]


async def sales_by_category(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    stmt = (
        select(
            AggSalesDaily.category_ext_id,
            func.coalesce(func.max(DimCategory.name), AggSalesDaily.category_ext_id).label('category_name'),
            func.coalesce(func.sum(AggSalesDaily.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggSalesDaily.gross_value), 0).label('gross_value'),
        )
        .join(DimCategory, DimCategory.external_id == AggSalesDaily.category_ext_id, isouter=True)
        .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_sales_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(AggSalesDaily.category_ext_id).order_by(func.sum(AggSalesDaily.net_value).desc())
    rows = (await db.execute(stmt)).all()
    return [
        {
            'category': r[1] or r[0] or 'N/A',
            'category_code': r[0] or 'N/A',
            'net_value': float(r[2] or 0),
            'gross_value': float(r[3] or 0),
        }
        for r in rows
    ]


async def sales_by_group(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    stmt = (
        select(
            AggSalesDaily.group_ext_id,
            func.coalesce(func.max(DimGroup.name), AggSalesDaily.group_ext_id).label('group_name'),
            func.coalesce(func.sum(AggSalesDaily.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggSalesDaily.gross_value), 0).label('gross_value'),
        )
        .join(DimGroup, DimGroup.external_id == AggSalesDaily.group_ext_id, isouter=True)
        .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_sales_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(AggSalesDaily.group_ext_id).order_by(func.sum(AggSalesDaily.net_value).desc())
    rows = (await db.execute(stmt)).all()
    return [
        {
            'group': r[1] or r[0] or 'N/A',
            'group_code': r[0] or 'N/A',
            'net_value': float(r[2] or 0),
            'gross_value': float(r[3] or 0),
        }
        for r in rows
    ]


async def purchases_summary(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    stmt = (
        select(
            func.count(AggPurchasesDaily.id),
            func.coalesce(func.sum(AggPurchasesDaily.qty), 0),
            func.coalesce(func.sum(AggPurchasesDaily.net_value), 0),
            func.coalesce(func.sum(AggPurchasesDaily.cost_amount), 0),
        )
        .where(*_date_range(AggPurchasesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_purchase_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    row = (await db.execute(stmt)).one()
    return {
        'records': int(row[0] or 0),
        'qty': float(row[1] or 0),
        'net_value': float(row[2] or 0),
        'cost_amount': float(row[3] or 0),
    }


async def purchases_by_supplier(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    stmt = (
        select(
            AggPurchasesDaily.supplier_ext_id,
            func.coalesce(func.max(DimSupplier.name), AggPurchasesDaily.supplier_ext_id).label('supplier_name'),
            func.coalesce(func.sum(AggPurchasesDaily.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggPurchasesDaily.cost_amount), 0).label('cost_amount'),
        )
        .join(DimSupplier, DimSupplier.external_id == AggPurchasesDaily.supplier_ext_id, isouter=True)
        .where(*_date_range(AggPurchasesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_purchase_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(AggPurchasesDaily.supplier_ext_id).order_by(func.sum(AggPurchasesDaily.net_value).desc())
    rows = (await db.execute(stmt)).all()
    return [
        {
            'supplier': r[1] or r[0] or 'N/A',
            'supplier_code': r[0] or 'N/A',
            'net_value': float(r[2] or 0),
            'cost_amount': float(r[3] or 0),
        }
        for r in rows
    ]


async def purchases_monthly_trend(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    month_start_expr = cast(func.date_trunc(literal_column("'month'"), AggPurchasesDaily.doc_date), Date)
    stmt = (
        select(
            month_start_expr.label('month_start'),
            func.coalesce(func.sum(AggPurchasesDaily.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggPurchasesDaily.cost_amount), 0).label('cost_amount'),
            func.coalesce(func.sum(AggPurchasesDaily.qty), 0).label('qty'),
        )
        .where(*_date_range(AggPurchasesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_purchase_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(month_start_expr).order_by(month_start_expr)
    rows = (await db.execute(stmt)).all()
    return [
        {
            'month_start': str(r[0]),
            'net_value': float(r[1] or 0),
            'cost_amount': float(r[2] or 0),
            'qty': float(r[3] or 0),
        }
        for r in rows
    ]


async def purchases_margin_by_supplier(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    rows = await purchases_by_supplier(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    enriched = []
    for r in rows:
        net_value = float(r['net_value'])
        cost_amount = float(r['cost_amount'])
        margin_value = net_value - cost_amount
        margin_pct = (margin_value / net_value * 100.0) if net_value > 0 else 0.0
        enriched.append(
            {
                'supplier': r['supplier'],
                'net_value': net_value,
                'cost_amount': cost_amount,
                'margin_value': margin_value,
                'margin_pct': margin_pct,
            }
        )
    return enriched


async def purchases_cost_change_detection(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    days = max(1, (date_to - date_from).days + 1)
    prev_to = date_from.fromordinal(date_from.toordinal() - 1)
    prev_from = prev_to.fromordinal(prev_to.toordinal() - days + 1)
    current = await purchases_by_supplier(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    previous = await purchases_by_supplier(
        db,
        date_from=prev_from,
        date_to=prev_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    prev_map = {str(x['supplier']): float(x['cost_amount']) for x in previous}
    changes = []
    for row in current:
        supplier = str(row['supplier'])
        cur_cost = float(row['cost_amount'])
        prev_cost = prev_map.get(supplier, 0.0)
        if prev_cost > 0:
            delta_pct = ((cur_cost - prev_cost) / prev_cost) * 100.0
        else:
            delta_pct = None
        if delta_pct is not None and abs(delta_pct) >= 10:
            changes.append(
                {
                    'supplier': supplier,
                    'current_cost': cur_cost,
                    'previous_cost': prev_cost,
                    'delta_pct': delta_pct,
                }
            )
    changes.sort(key=lambda x: abs(float(x['delta_pct'] or 0)), reverse=True)
    return {
        'period': {
            'from': str(date_from),
            'to': str(date_to),
            'prev_from': str(prev_from),
            'prev_to': str(prev_to),
        },
        'alerts': changes[:10],
    }


async def purchases_decision_pack(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    summary = await purchases_summary(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    trend = await purchases_monthly_trend(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    supplier_distribution = await purchases_by_supplier(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    margin_by_supplier = await purchases_margin_by_supplier(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    cost_change = await purchases_cost_change_detection(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    seasonal = await purchases_seasonality(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    new_codes = await new_item_codes_activity(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        limit=10,
    )
    return {
        'summary': summary,
        'purchase_trend': trend,
        'supplier_distribution': supplier_distribution,
        'margin_by_supplier': margin_by_supplier,
        'cost_change_detection': cost_change,
        'seasonality': seasonal,
        'new_codes': new_codes,
    }


async def _distinct_dimension_values(
    db: AsyncSession,
    source_date_column,
    source_dim_column,
    date_from: date,
    date_to: date,
    filters_applier,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    stmt = (
        select(source_dim_column)
        .where(*_date_range(source_date_column, date_from, date_to))
        .where(source_dim_column.is_not(None))
        .where(source_dim_column != '')
    )
    stmt = filters_applier(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    stmt = stmt.distinct().order_by(source_dim_column)
    rows = (await db.execute(stmt)).scalars().all()
    return [str(v) for v in rows if v]


async def _dimension_label_map(db: AsyncSession, model) -> dict[str, str]:
    if model is DimCategory:
        parent = aliased(DimCategory)
        grand_parent = aliased(DimCategory)
        rows = (
            await db.execute(
                select(
                    DimCategory.external_id,
                    DimCategory.name,
                    parent.name.label('parent_name'),
                    grand_parent.name.label('grand_parent_name'),
                )
                .select_from(DimCategory)
                .join(parent, DimCategory.parent_id == parent.id, isouter=True)
                .join(grand_parent, parent.parent_id == grand_parent.id, isouter=True)
                .where(DimCategory.external_id.is_not(None))
            )
        ).all()
        label_map: dict[str, str] = {}
        for ext, name, parent_name, grand_parent_name in rows:
            if not ext:
                continue
            path_parts = [str(x).strip() for x in [grand_parent_name, parent_name, name] if x and str(x).strip()]
            if path_parts:
                label_map[str(ext)] = " > ".join(path_parts)
            else:
                label_map[str(ext)] = str(ext)
        return label_map

    rows = (await db.execute(select(model.external_id, model.name).where(model.external_id.is_not(None)))).all()
    return {str(ext): str(name or ext) for ext, name in rows if ext}


async def sales_filter_options(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    labels = {
        'branches': await _dimension_label_map(db, DimBranch),
        'warehouses': await _dimension_label_map(db, DimWarehouse),
        'brands': await _dimension_label_map(db, DimBrand),
        'categories': await _dimension_label_map(db, DimCategory),
        'groups': await _dimension_label_map(db, DimGroup),
    }
    return {
        'branches': await _distinct_dimension_values(
            db,
            AggSalesDaily.doc_date,
            AggSalesDaily.branch_ext_id,
            date_from,
            date_to,
            _apply_sales_filters,
            branches=None,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
        'warehouses': await _distinct_dimension_values(
            db,
            AggSalesDaily.doc_date,
            AggSalesDaily.warehouse_ext_id,
            date_from,
            date_to,
            _apply_sales_filters,
            branches=branches,
            warehouses=None,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
        'brands': await _distinct_dimension_values(
            db,
            AggSalesDaily.doc_date,
            AggSalesDaily.brand_ext_id,
            date_from,
            date_to,
            _apply_sales_filters,
            branches=branches,
            warehouses=warehouses,
            brands=None,
            categories=categories,
            groups=groups,
        ),
        'categories': await _distinct_dimension_values(
            db,
            AggSalesDaily.doc_date,
            AggSalesDaily.category_ext_id,
            date_from,
            date_to,
            _apply_sales_filters,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=None,
            groups=groups,
        ),
        'groups': await _distinct_dimension_values(
            db,
            AggSalesDaily.doc_date,
            AggSalesDaily.group_ext_id,
            date_from,
            date_to,
            _apply_sales_filters,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=None,
        ),
        'labels': labels,
    }


async def purchases_filter_options(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    labels = {
        'branches': await _dimension_label_map(db, DimBranch),
        'warehouses': await _dimension_label_map(db, DimWarehouse),
        'brands': await _dimension_label_map(db, DimBrand),
        'categories': await _dimension_label_map(db, DimCategory),
        'groups': await _dimension_label_map(db, DimGroup),
    }
    return {
        'branches': await _distinct_dimension_values(
            db,
            AggPurchasesDaily.doc_date,
            AggPurchasesDaily.branch_ext_id,
            date_from,
            date_to,
            _apply_purchase_filters,
            branches=None,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
        'warehouses': await _distinct_dimension_values(
            db,
            AggPurchasesDaily.doc_date,
            AggPurchasesDaily.warehouse_ext_id,
            date_from,
            date_to,
            _apply_purchase_filters,
            branches=branches,
            warehouses=None,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
        'brands': await _distinct_dimension_values(
            db,
            AggPurchasesDaily.doc_date,
            AggPurchasesDaily.brand_ext_id,
            date_from,
            date_to,
            _apply_purchase_filters,
            branches=branches,
            warehouses=warehouses,
            brands=None,
            categories=categories,
            groups=groups,
        ),
        'categories': await _distinct_dimension_values(
            db,
            AggPurchasesDaily.doc_date,
            AggPurchasesDaily.category_ext_id,
            date_from,
            date_to,
            _apply_purchase_filters,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=None,
            groups=groups,
        ),
        'groups': await _distinct_dimension_values(
            db,
            AggPurchasesDaily.doc_date,
            AggPurchasesDaily.group_ext_id,
            date_from,
            date_to,
            _apply_purchase_filters,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=None,
        ),
        'labels': labels,
    }


async def sales_top_products(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    limit: int = 10,
):
    stmt = (
        select(
            FactSales.item_code,
            func.coalesce(func.max(DimItem.name), FactSales.item_code).label('item_name'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
            func.coalesce(func.sum(FactSales.qty), 0).label('qty'),
        )
        .join(DimItem, DimItem.external_id == FactSales.item_code, isouter=True)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    if branches:
        stmt = stmt.where(FactSales.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactSales.warehouse_ext_id.in_(warehouses))
    if brands:
        stmt = stmt.where(FactSales.brand_ext_id.in_(brands))
    if categories:
        stmt = stmt.where(FactSales.category_ext_id.in_(categories))
    if groups:
        stmt = stmt.where(FactSales.group_ext_id.in_(groups))
    stmt = stmt.group_by(FactSales.item_code).order_by(func.sum(FactSales.net_value).desc()).limit(max(1, min(limit, 50)))
    rows = (await db.execute(stmt)).all()
    return [
        {
            'item_code': r[0] or 'N/A',
            'item_name': _clean_item_name(r[1], r[0] or 'N/A'),
            'net_value': float(r[2] or 0),
            'qty': float(r[3] or 0),
        }
        for r in rows
    ]


async def sales_slow_fast_movers(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    base_stmt = (
        select(
            FactSales.item_code,
            func.coalesce(func.max(DimItem.name), FactSales.item_code).label('item_name'),
            func.coalesce(func.sum(FactSales.qty), 0).label('qty'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
        )
        .join(DimItem, DimItem.external_id == FactSales.item_code, isouter=True)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    if branches:
        base_stmt = base_stmt.where(FactSales.branch_ext_id.in_(branches))
    if warehouses:
        base_stmt = base_stmt.where(FactSales.warehouse_ext_id.in_(warehouses))
    if brands:
        base_stmt = base_stmt.where(FactSales.brand_ext_id.in_(brands))
    if categories:
        base_stmt = base_stmt.where(FactSales.category_ext_id.in_(categories))
    if groups:
        base_stmt = base_stmt.where(FactSales.group_ext_id.in_(groups))

    grouped = base_stmt.group_by(FactSales.item_code).where(FactSales.item_code.is_not(None))
    fast_rows = (await db.execute(grouped.order_by(func.sum(FactSales.qty).desc()).limit(5))).all()
    slow_rows = (
        await db.execute(grouped.having(func.sum(FactSales.qty) > 0).order_by(func.sum(FactSales.qty).asc()).limit(5))
    ).all()
    fast = [
        {
            'item_code': r[0],
            'item_name': _clean_item_name(r[1], r[0] or 'N/A'),
            'qty': float(r[2] or 0),
            'net_value': float(r[3] or 0),
        }
        for r in fast_rows
    ]
    slow = [
        {
            'item_code': r[0],
            'item_name': _clean_item_name(r[1], r[0] or 'N/A'),
            'qty': float(r[2] or 0),
            'net_value': float(r[3] or 0),
        }
        for r in slow_rows
    ]

    # Real purchases for the same fast-moving items and same filter period
    fast_item_codes = [str(r['item_code']) for r in fast if r.get('item_code')]
    item_name_map: dict[str, str] = {
        str(r['item_code']): str(r.get('item_name') or r['item_code'])
        for r in fast
        if r.get('item_code')
    }
    for r in slow:
        code = str(r.get('item_code') or '')
        if code and code not in item_name_map:
            item_name_map[code] = str(r.get('item_name') or code)

    purchases_map: dict[str, float] = {}
    if fast_item_codes:
        purchases_stmt = (
            select(
                FactPurchases.item_code,
                func.coalesce(func.max(DimItem.name), FactPurchases.item_code).label('item_name'),
                func.coalesce(func.sum(FactPurchases.qty), 0).label('qty'),
            )
            .join(DimItem, DimItem.external_id == FactPurchases.item_code, isouter=True)
            .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
            .where(FactPurchases.item_code.in_(fast_item_codes))
        )
        if branches:
            purchases_stmt = purchases_stmt.where(FactPurchases.branch_ext_id.in_(branches))
        if warehouses:
            purchases_stmt = purchases_stmt.where(FactPurchases.warehouse_ext_id.in_(warehouses))
        if brands:
            purchases_stmt = purchases_stmt.where(FactPurchases.brand_ext_id.in_(brands))
        if categories:
            purchases_stmt = purchases_stmt.where(FactPurchases.category_ext_id.in_(categories))
        if groups:
            purchases_stmt = purchases_stmt.where(FactPurchases.group_ext_id.in_(groups))
        purchases_stmt = purchases_stmt.group_by(FactPurchases.item_code)
        purchase_rows = (await db.execute(purchases_stmt)).all()
        purchases_map = {str(r[0]): float(r[2] or 0) for r in purchase_rows if r[0]}
        for r in purchase_rows:
            code = str(r[0] or '')
            if code:
                item_name_map[code] = _clean_item_name(r[1], code)

    purchases = [
        {
            'item_code': code,
            'item_name': item_name_map.get(code, code),
            'qty': float(purchases_map.get(code, 0.0)),
        }
        for code in fast_item_codes
    ]
    return {'fast': fast, 'slow': slow, 'purchases': purchases}


async def sales_monthly_trend(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    month_start_expr = cast(func.date_trunc(literal_column("'month'"), AggSalesDaily.doc_date), Date)
    stmt = (
        select(
            month_start_expr.label('month_start'),
            func.coalesce(func.sum(AggSalesDaily.net_value), 0).label('net_value'),
            func.coalesce(func.sum(AggSalesDaily.gross_value), 0).label('gross_value'),
            func.coalesce(func.sum(AggSalesDaily.qty), 0).label('qty'),
        )
        .where(*_date_range(AggSalesDaily.doc_date, date_from, date_to))
    )
    stmt = _apply_sales_filters(stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups)
    stmt = stmt.group_by(month_start_expr).order_by(month_start_expr)
    rows = (await db.execute(stmt)).all()
    return [
        {
            'month_start': str(r[0]),
            'net_value': float(r[1] or 0),
            'gross_value': float(r[2] or 0),
            'qty': float(r[3] or 0),
        }
        for r in rows
    ]


async def sales_decision_pack(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    top_n: int = 10,
):
    current_stmt = (
        select(
            func.count(FactSales.id),
            func.coalesce(func.sum(FactSales.qty), 0),
            func.coalesce(func.sum(FactSales.net_value), 0),
            func.coalesce(func.sum(FactSales.cost_amount), 0),
        )
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    current_stmt = _apply_fact_sales_filters(
        current_stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    current_row = (await db.execute(current_stmt)).one()
    current = {
        'records': int(current_row[0] or 0),
        'qty': float(current_row[1] or 0),
        'net_value': float(current_row[2] or 0),
        'cost_amount': float(current_row[3] or 0),
    }
    days = max(1, (date_to - date_from).days + 1)
    prev_to = date_from.fromordinal(date_from.toordinal() - 1)
    prev_from = prev_to.fromordinal(prev_to.toordinal() - days + 1)
    previous_stmt = (
        select(
            func.count(FactSales.id),
            func.coalesce(func.sum(FactSales.qty), 0),
            func.coalesce(func.sum(FactSales.net_value), 0),
            func.coalesce(func.sum(FactSales.cost_amount), 0),
        )
        .where(*_date_range(FactSales.doc_date, prev_from, prev_to))
    )
    previous_stmt = _apply_fact_sales_filters(
        previous_stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    previous_row = (await db.execute(previous_stmt)).one()
    previous = {
        'records': int(previous_row[0] or 0),
        'qty': float(previous_row[1] or 0),
        'net_value': float(previous_row[2] or 0),
        'cost_amount': float(previous_row[3] or 0),
    }
    turnover = float(current['net_value'])
    cost = float(current['cost_amount'])
    qty = float(current['qty'])
    gross_profit = turnover - cost
    margin_pct = (gross_profit / turnover * 100.0) if turnover > 0 else 0.0
    avg_basket = (turnover / current['records']) if current['records'] > 0 else 0.0
    avg_sale_per_day = turnover / days
    prev_turnover = float(previous['net_value'])
    growth_pct = ((turnover - prev_turnover) / prev_turnover * 100.0) if prev_turnover > 0 else None
    prev_cost = float(previous['cost_amount'])
    prev_margin_pct = ((prev_turnover - prev_cost) / prev_turnover * 100.0) if prev_turnover > 0 else None

    branch_stmt = (
        select(
            FactSales.branch_ext_id,
            func.coalesce(func.max(DimBranch.name), FactSales.branch_ext_id).label('branch_name'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
            func.coalesce(func.sum(FactSales.cost_amount), 0).label('cost_amount'),
        )
        .join(DimBranch, DimBranch.external_id == FactSales.branch_ext_id, isouter=True)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    branch_stmt = _apply_fact_sales_filters(
        branch_stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    branch_stmt = branch_stmt.group_by(FactSales.branch_ext_id).order_by(func.sum(FactSales.net_value).desc())
    branch_rows = (await db.execute(branch_stmt)).all()
    by_branch = []
    for r in branch_rows:
        net_value = float(r[2] or 0)
        cost_amount = float(r[3] or 0)
        branch_margin_pct = ((net_value - cost_amount) / net_value * 100.0) if net_value > 0 else 0.0
        contribution_pct = (net_value / turnover * 100.0) if turnover > 0 else 0.0
        by_branch.append(
            {
                'branch': r[1] or r[0] or 'N/A',
                'branch_code': r[0] or 'N/A',
                'net_value': net_value,
                'cost_amount': cost_amount,
                'margin_pct': branch_margin_pct,
                'contribution_pct': contribution_pct,
            }
        )
    avg_margin_per_branch = 0.0
    if by_branch:
        avg_margin_per_branch = sum(float(x['margin_pct']) for x in by_branch) / len(by_branch)
    by_brand = await sales_by_brand(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    by_category = await sales_by_category(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    by_group = await sales_by_group(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    top_products = await sales_top_products(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        limit=top_n,
    )
    movers = await sales_slow_fast_movers(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    trend = await sales_monthly_trend(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    seasonal = await sales_seasonality(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    new_codes = await new_item_codes_activity(
        db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        limit=10,
    )

    insights: list[str] = []
    margin_alerts: list[str] = []
    if growth_pct is not None:
        if growth_pct <= -10:
            insights.append(f'Turnover decreased {abs(growth_pct):.1f}% vs previous period.')
        elif growth_pct >= 10:
            insights.append(f'Turnover increased {growth_pct:.1f}% vs previous period.')
    if by_branch and turnover > 0:
        top_branch = by_branch[0]
        top_share = (float(top_branch['net_value']) / turnover) * 100.0
        insights.append(f"Το κορυφαίο κατάστημα {top_branch['branch']} συνεισφέρει {top_share:.1f}% του τζίρου.")
    if margin_pct < 15:
        insights.append('Το περιθώριο είναι κάτω από 15%. Έλεγξε εκπτώσεις και κόστος αγοράς.')
        margin_alerts.append('Το τρέχον περιθώριο είναι κάτω από 15%.')
    if prev_margin_pct is not None:
        erosion_pp = margin_pct - prev_margin_pct
        if erosion_pp <= -2.0:
            margin_alerts.append(f'Εντοπίστηκε διάβρωση περιθωρίου: {abs(erosion_pp):.2f} μονάδες έναντι προηγούμενης περιόδου.')
        elif erosion_pp >= 2.0:
            margin_alerts.append(f'Βελτίωση περιθωρίου: {erosion_pp:.2f} μονάδες έναντι προηγούμενης περιόδου.')
    if not margin_alerts:
        margin_alerts.append('Δεν εντοπίστηκε σημαντική διάβρωση περιθωρίου στο επιλεγμένο διάστημα.')
    if not insights:
        insights.append('Η απόδοση πωλήσεων είναι σταθερή στο επιλεγμένο διάστημα.')
    insight_records = await list_recent_insights(
        db,
        limit=15,
        statuses=['open'],
        insight_types=[
            'SALES_DROP_PERIOD',
            'SALES_SPIKE_PERIOD',
            'PROFIT_DROP_PERIOD',
            'MARGIN_DROP_PERIOD',
            'BRANCH_UNDERPERFORM',
            'CATEGORY_DROP',
            'SUPPLIER_DEPENDENCY',
            'DEAD_STOCK',
            'LOW_COVERAGE',
            'OVERSTOCK_RISK',
        ],
    )

    def _name_with_fallback(entity: str, name: str | None, code: str | None) -> str:
        raw_name = str(name or '').strip()
        raw_code = str(code or '').strip()
        if entity == 'product':
            return _clean_item_name(raw_name or raw_code, raw_code or raw_name)
        return raw_name or raw_code or 'N/A'

    async def _best_for_entity(
        entity: str,
        code_col,
        dim_name_col=None,
        join_model=None,
        join_on=None,
    ):
        base = (
            select(
                code_col.label('code'),
                func.coalesce(func.max(dim_name_col), code_col).label('name')
                if dim_name_col is not None
                else code_col.label('name'),
                func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
                func.coalesce(func.sum(FactSales.cost_amount), 0).label('cost_amount'),
                func.coalesce(func.sum(FactSales.qty), 0).label('qty'),
            )
            .select_from(FactSales)
            .where(*_date_range(FactSales.doc_date, date_from, date_to))
            .where(code_col.is_not(None))
            .where(code_col != '')
        )
        if join_model is not None and join_on is not None:
            base = base.join(join_model, join_on, isouter=True)
        base = _apply_fact_sales_filters(
            base, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
        )
        base = base.group_by(code_col)
        rows = (await db.execute(base)).all()
        if not rows:
            return None

        totals = []
        for r in rows:
            code = str(r[0] or '').strip()
            name = _name_with_fallback(entity, str(r[1] or '').strip(), code)
            net_value = float(r[2] or 0)
            cost_amount = float(r[3] or 0)
            qty_value = float(r[4] or 0)
            profit_value = net_value - cost_amount
            margin_pct_val = (profit_value / net_value * 100.0) if net_value > 0 else 0.0
            totals.append(
                {
                    'code': code or 'N/A',
                    'name': name,
                    'net_value': net_value,
                    'cost_amount': cost_amount,
                    'qty': qty_value,
                    'profit_value': profit_value,
                    'margin_pct': margin_pct_val,
                }
            )

        best_profit = max(totals, key=lambda x: x['profit_value'])
        best_qty = max(totals, key=lambda x: x['qty'])

        season_expr = _season_case(FactSales.doc_date).label('season')
        season_stmt = (
            select(
                code_col.label('code'),
                func.coalesce(func.max(dim_name_col), code_col).label('name')
                if dim_name_col is not None
                else code_col.label('name'),
                season_expr,
                func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
            )
            .select_from(FactSales)
            .where(*_date_range(FactSales.doc_date, date_from, date_to))
            .where(code_col.is_not(None))
            .where(code_col != '')
        )
        if join_model is not None and join_on is not None:
            season_stmt = season_stmt.join(join_model, join_on, isouter=True)
        season_stmt = _apply_fact_sales_filters(
            season_stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
        )
        season_stmt = season_stmt.group_by(code_col, season_expr)
        season_rows = (await db.execute(season_stmt)).all()

        season_map: dict[str, dict[str, float | str]] = {}
        for r in season_rows:
            code = str(r[0] or '').strip()
            if not code:
                continue
            name = _name_with_fallback(entity, str(r[1] or '').strip(), code)
            season = str(r[2] or 'unknown')
            net_value = float(r[3] or 0)
            cur = season_map.get(code)
            if cur is None:
                cur = {'name': name, 'total': 0.0, 'best_season': season, 'best_season_net': net_value}
                season_map[code] = cur
            cur['total'] = float(cur.get('total', 0.0)) + net_value
            if net_value >= float(cur.get('best_season_net', 0.0)):
                cur['best_season'] = season
                cur['best_season_net'] = net_value

        best_seasonality = None
        for code, cur in season_map.items():
            total_val = float(cur.get('total', 0.0))
            top_season_val = float(cur.get('best_season_net', 0.0))
            share_pct = (top_season_val / total_val * 100.0) if total_val > 0 else 0.0
            candidate = {
                'code': code,
                'name': str(cur.get('name') or code),
                'season': str(cur.get('best_season') or 'unknown'),
                'season_net_value': top_season_val,
                'season_share_pct': share_pct,
            }
            if best_seasonality is None or share_pct > float(best_seasonality.get('season_share_pct', 0.0)):
                best_seasonality = candidate

        return {
            'profitability': best_profit,
            'quantity': best_qty,
            'seasonality': best_seasonality,
        }

    best_entities = {
        'product': await _best_for_entity('product', FactSales.item_code, DimItem.name, DimItem, DimItem.external_id == FactSales.item_code),
        'brand': await _best_for_entity('brand', FactSales.brand_ext_id, DimBrand.name, DimBrand, DimBrand.external_id == FactSales.brand_ext_id),
        'category': await _best_for_entity(
            'category', FactSales.category_ext_id, DimCategory.name, DimCategory, DimCategory.external_id == FactSales.category_ext_id
        ),
        'group': await _best_for_entity('group', FactSales.group_ext_id, DimGroup.name, DimGroup, DimGroup.external_id == FactSales.group_ext_id),
        'branch': await _best_for_entity('branch', FactSales.branch_ext_id, DimBranch.name, DimBranch, DimBranch.external_id == FactSales.branch_ext_id),
    }

    return {
        'period': {
            'from': str(date_from),
            'to': str(date_to),
            'prev_from': str(prev_from),
            'prev_to': str(prev_to),
        },
        'cards': {
            'turnover': turnover,
            'gross_profit': gross_profit,
            'margin_pct': margin_pct,
            'qty_sold': qty,
            'avg_basket_value': avg_basket,
            'growth_pct': growth_pct,
            'avg_sale_per_day': avg_sale_per_day,
            'avg_margin_per_branch': avg_margin_per_branch,
        },
        'current_summary': current,
        'previous_summary': previous,
        'trend_monthly': trend,
        'by_branch': by_branch,
        'by_brand': by_brand,
        'by_category': by_category,
        'by_group': by_group,
        'top_products': top_products,
        'movers': movers,
        'seasonality': seasonal,
        'new_codes': new_codes,
        'best_entities': best_entities,
        'insights': insights,
        'margin_alerts': margin_alerts,
        'insight_records': insight_records,
    }


async def sales_entity_ranking(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    entity: str,
    metric: str = 'profitability',
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    limit: int = 50,
):
    entity_key = (entity or '').strip().lower()
    metric_key = (metric or '').strip().lower()
    if entity_key not in {'product', 'brand', 'category', 'group', 'branch'}:
        raise ValueError('unsupported_entity')
    if metric_key not in {'profitability', 'quantity', 'seasonality'}:
        raise ValueError('unsupported_metric')

    entity_map = {
        'product': (FactSales.item_code, DimItem, DimItem.external_id == FactSales.item_code, DimItem.name, 'item'),
        'brand': (FactSales.brand_ext_id, DimBrand, DimBrand.external_id == FactSales.brand_ext_id, DimBrand.name, 'brand'),
        'category': (
            FactSales.category_ext_id,
            DimCategory,
            DimCategory.external_id == FactSales.category_ext_id,
            DimCategory.name,
            'category',
        ),
        'group': (FactSales.group_ext_id, DimGroup, DimGroup.external_id == FactSales.group_ext_id, DimGroup.name, 'group'),
        'branch': (FactSales.branch_ext_id, DimBranch, DimBranch.external_id == FactSales.branch_ext_id, DimBranch.name, 'branch'),
    }
    code_col, dim_model, join_on, dim_name_col, label_key = entity_map[entity_key]

    base_stmt = (
        select(
            code_col.label('code'),
            func.coalesce(func.max(dim_name_col), code_col).label('name'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
            func.coalesce(func.sum(FactSales.cost_amount), 0).label('cost_amount'),
            func.coalesce(func.sum(FactSales.qty), 0).label('qty'),
        )
        .select_from(FactSales)
        .join(dim_model, join_on, isouter=True)
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
        .where(code_col.is_not(None))
        .where(code_col != '')
    )
    base_stmt = _apply_fact_sales_filters(
        base_stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    base_stmt = base_stmt.group_by(code_col)
    base_rows = (await db.execute(base_stmt)).all()

    totals: list[dict[str, float | str]] = []
    for r in base_rows:
        code = str(r[0] or '').strip()
        if not code:
            continue
        raw_name = str(r[1] or code).strip()
        if entity_key == 'product':
            raw_name = _clean_item_name(raw_name, code)
        net_value = float(r[2] or 0)
        cost_amount = float(r[3] or 0)
        qty = float(r[4] or 0)
        profit_value = net_value - cost_amount
        margin_pct = (profit_value / net_value * 100.0) if net_value > 0 else 0.0
        totals.append(
            {
                'code': code,
                'name': raw_name or code,
                'net_value': net_value,
                'cost_amount': cost_amount,
                'qty': qty,
                'profit_value': profit_value,
                'margin_pct': margin_pct,
            }
        )

    if not totals:
        return {
            'entity': entity_key,
            'metric': metric_key,
            'label': label_key,
            'period': {'from': str(date_from), 'to': str(date_to)},
            'rows': [],
        }

    rows: list[dict[str, float | str]] = []
    if metric_key == 'profitability':
        sorted_rows = sorted(totals, key=lambda x: float(x['profit_value']), reverse=True)
        for row in sorted_rows[: max(1, min(limit, 200))]:
            rows.append(
                {
                    'code': row['code'],
                    'name': row['name'],
                    'metric_value': float(row['profit_value']),
                    'baseline_value': float(row['margin_pct']),
                    'delta_pct': float(row['net_value']),
                    'net_value': float(row['net_value']),
                    'qty': float(row['qty']),
                }
            )
    elif metric_key == 'quantity':
        total_qty = sum(float(x['qty']) for x in totals)
        sorted_rows = sorted(totals, key=lambda x: float(x['qty']), reverse=True)
        for row in sorted_rows[: max(1, min(limit, 200))]:
            qty_val = float(row['qty'])
            share_pct = (qty_val / total_qty * 100.0) if total_qty > 0 else 0.0
            rows.append(
                {
                    'code': row['code'],
                    'name': row['name'],
                    'metric_value': qty_val,
                    'baseline_value': share_pct,
                    'delta_pct': float(row['net_value']),
                    'net_value': float(row['net_value']),
                    'qty': qty_val,
                }
            )
    else:
        season_expr = _season_case(FactSales.doc_date).label('season')
        season_stmt = (
            select(
                code_col.label('code'),
                func.coalesce(func.max(dim_name_col), code_col).label('name'),
                season_expr,
                func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
            )
            .select_from(FactSales)
            .join(dim_model, join_on, isouter=True)
            .where(*_date_range(FactSales.doc_date, date_from, date_to))
            .where(code_col.is_not(None))
            .where(code_col != '')
        )
        season_stmt = _apply_fact_sales_filters(
            season_stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
        )
        season_stmt = season_stmt.group_by(code_col, season_expr)
        season_rows = (await db.execute(season_stmt)).all()

        season_map: dict[str, dict[str, float | str]] = {}
        for r in season_rows:
            code = str(r[0] or '').strip()
            if not code:
                continue
            raw_name = str(r[1] or code).strip()
            if entity_key == 'product':
                raw_name = _clean_item_name(raw_name, code)
            season = str(r[2] or 'unknown')
            net_value = float(r[3] or 0)
            cur = season_map.get(code)
            if cur is None:
                cur = {'name': raw_name or code, 'total': 0.0, 'best_season': season, 'best_season_value': net_value}
                season_map[code] = cur
            cur['total'] = float(cur.get('total', 0.0)) + net_value
            if net_value >= float(cur.get('best_season_value', 0.0)):
                cur['best_season'] = season
                cur['best_season_value'] = net_value

        season_ranked = []
        for code, item in season_map.items():
            total_val = float(item.get('total', 0.0))
            best_val = float(item.get('best_season_value', 0.0))
            share_pct = (best_val / total_val * 100.0) if total_val > 0 else 0.0
            season_ranked.append(
                {
                    'code': code,
                    'name': str(item.get('name') or code),
                    'season': str(item.get('best_season') or 'unknown'),
                    'metric_value': best_val,
                    'baseline_value': share_pct,
                    'delta_pct': total_val,
                    'net_value': total_val,
                }
            )
        season_ranked.sort(key=lambda x: (float(x['baseline_value']), float(x['metric_value'])), reverse=True)
        rows = season_ranked[: max(1, min(limit, 200))]

    return {
        'entity': entity_key,
        'metric': metric_key,
        'label': label_key,
        'period': {'from': str(date_from), 'to': str(date_to)},
        'rows': rows,
    }


async def inventory_snapshot(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    snapshot_date_stmt = select(func.max(FactInventory.doc_date)).where(FactInventory.doc_date <= as_of)
    snapshot_date = (await db.execute(snapshot_date_stmt)).scalar_one_or_none()
    if snapshot_date is None:
        return {
            'snapshot_date': None,
            'qty_on_hand': 0.0,
            'qty_reserved': 0.0,
            'cost_amount': 0.0,
            'value_amount': 0.0,
        }

    stmt = (
        select(
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0),
            func.coalesce(func.sum(FactInventory.qty_reserved), 0),
            func.coalesce(func.sum(FactInventory.cost_amount), 0),
            func.coalesce(func.sum(FactInventory.value_amount), 0),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date == snapshot_date)
    )
    stmt = _apply_inventory_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    row = (await db.execute(stmt)).one()
    return {
        'snapshot_date': str(snapshot_date),
        'qty_on_hand': float(row[0] or 0),
        'qty_reserved': float(row[1] or 0),
        'cost_amount': float(row[2] or 0),
        'value_amount': float(row[3] or 0),
    }


async def stock_aging(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    d_30 = as_of - timedelta(days=30)
    d_60 = as_of - timedelta(days=60)
    d_90 = as_of - timedelta(days=90)
    bucket = case(
        (FactInventory.doc_date >= d_30, '0_30'),
        (FactInventory.doc_date >= d_60, '31_60'),
        (FactInventory.doc_date >= d_90, '61_90'),
        else_='90_plus',
    )

    stmt = (
        select(
            bucket.label('bucket'),
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(FactInventory.value_amount), 0).label('value_amount'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date <= as_of)
    )
    stmt = _apply_inventory_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    stmt = stmt.group_by(bucket).order_by(bucket)

    rows = (await db.execute(stmt)).all()
    out = {k: {'qty_on_hand': 0.0, 'value_amount': 0.0} for k in ['0_30', '31_60', '61_90', '90_plus']}
    for row in rows:
        out[str(row[0])] = {
            'qty_on_hand': float(row[1] or 0),
            'value_amount': float(row[2] or 0),
        }
    return out


async def cashflow_summary(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
):
    entry_lower = func.lower(FactCashflow.entry_type)
    inflow_case = case((entry_lower.in_(['in', 'inflow', 'credit', 'income']), FactCashflow.amount), else_=0)
    outflow_case = case((entry_lower.in_(['out', 'outflow', 'debit', 'expense']), FactCashflow.amount), else_=0)

    stmt = (
        select(
            func.count(FactCashflow.id),
            func.coalesce(func.sum(inflow_case), 0),
            func.coalesce(func.sum(outflow_case), 0),
        )
        .select_from(FactCashflow)
        .join(DimBranch, FactCashflow.branch_id == DimBranch.id, isouter=True)
        .where(*_date_range(FactCashflow.doc_date, date_from, date_to))
    )

    if branches:
        stmt = stmt.where(DimBranch.external_id.in_(branches))

    row = (await db.execute(stmt)).one()
    inflows = float(row[1] or 0)
    outflows = float(row[2] or 0)
    return {
        'entries': int(row[0] or 0),
        'inflows': inflows,
        'outflows': outflows,
        'net': inflows - outflows,
    }


async def inventory_by_brand(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    limit: int = 12,
):
    stmt = (
        select(
            func.coalesce(DimBrand.name, DimBrand.external_id, literal('N/A')).label('brand'),
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(FactInventory.value_amount), 0).label('value_amount'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date <= as_of)
    )
    stmt = _apply_inventory_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    stmt = (
        stmt.group_by(DimBrand.name, DimBrand.external_id)
        .order_by(func.sum(FactInventory.value_amount).desc())
        .limit(max(1, min(limit, 100)))
    )
    rows = (await db.execute(stmt)).all()
    return [
        {
            'brand': str(r[0] or 'N/A'),
            'qty_on_hand': float(r[1] or 0),
            'value_amount': float(r[2] or 0),
        }
        for r in rows
    ]


async def inventory_by_commercial_category(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    limit: int = 12,
):
    stmt = (
        select(
            func.coalesce(DimGroup.name, DimGroup.external_id, literal('N/A')).label('commercial_category'),
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(FactInventory.value_amount), 0).label('value_amount'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date <= as_of)
    )
    stmt = _apply_inventory_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    stmt = (
        stmt.group_by(DimGroup.name, DimGroup.external_id)
        .order_by(func.sum(FactInventory.value_amount).desc())
        .limit(max(1, min(limit, 100)))
    )
    rows = (await db.execute(stmt)).all()
    return [
        {
            'commercial_category': str(r[0] or 'N/A'),
            'qty_on_hand': float(r[1] or 0),
            'value_amount': float(r[2] or 0),
        }
        for r in rows
    ]


async def inventory_by_manufacturer(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    lookback_days: int = 365,
    limit: int = 12,
):
    supplier_ranked = (
        select(
            FactPurchases.item_code.label('item_code'),
            FactPurchases.supplier_ext_id.label('supplier_ext_id'),
            over(
                func.row_number(),
                partition_by=FactPurchases.item_code,
                order_by=(FactPurchases.doc_date.desc(), FactPurchases.updated_at.desc()),
            ).label('rn'),
        )
        .where(FactPurchases.item_code.is_not(None))
        .where(FactPurchases.doc_date <= as_of)
        .where(FactPurchases.doc_date >= (as_of - timedelta(days=max(30, lookback_days))))
        .subquery('supplier_ranked')
    )
    latest_supplier = (
        select(supplier_ranked.c.item_code, supplier_ranked.c.supplier_ext_id)
        .where(supplier_ranked.c.rn == 1)
        .subquery('latest_supplier')
    )

    stmt = (
        select(
            func.coalesce(DimSupplier.name, latest_supplier.c.supplier_ext_id, literal('N/A')).label('manufacturer'),
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(FactInventory.value_amount), 0).label('value_amount'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .join(latest_supplier, DimItem.external_id == latest_supplier.c.item_code, isouter=True)
        .join(DimSupplier, DimSupplier.external_id == latest_supplier.c.supplier_ext_id, isouter=True)
        .where(FactInventory.doc_date <= as_of)
    )
    stmt = _apply_inventory_filters(
        stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    stmt = (
        stmt.group_by(DimSupplier.name, latest_supplier.c.supplier_ext_id)
        .order_by(func.sum(FactInventory.value_amount).desc())
        .limit(max(1, min(limit, 100)))
    )
    rows = (await db.execute(stmt)).all()
    return [
        {
            'manufacturer': str(r[0] or 'N/A'),
            'qty_on_hand': float(r[1] or 0),
            'value_amount': float(r[2] or 0),
        }
        for r in rows
    ]


async def inventory_filter_options(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    labels = {
        'branches': await _dimension_label_map(db, DimBranch),
        'warehouses': await _dimension_label_map(db, DimWarehouse),
        'brands': await _dimension_label_map(db, DimBrand),
        'categories': await _dimension_label_map(db, DimCategory),
        'groups': await _dimension_label_map(db, DimGroup),
    }
    base = (
        select(
            DimBranch.external_id.label('branch'),
            DimWarehouse.external_id.label('warehouse'),
            DimBrand.external_id.label('brand'),
            DimCategory.external_id.label('category'),
            DimGroup.external_id.label('group'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date <= as_of)
    )
    base = _apply_inventory_filters(
        base,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    ).subquery('inv_dims')

    async def _distinct(col_name: str) -> list[str]:
        col = getattr(base.c, col_name)
        rows = (await db.execute(select(col).where(col.is_not(None)).distinct().order_by(col))).scalars().all()
        return [str(x) for x in rows if x]

    return {
        'branches': await _distinct('branch'),
        'warehouses': await _distinct('warehouse'),
        'brands': await _distinct('brand'),
        'categories': await _distinct('category'),
        'groups': await _distinct('group'),
        'labels': labels,
    }


async def cashflow_by_entry_type(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
):
    stmt = (
        select(
            func.lower(FactCashflow.entry_type).label('entry_type'),
            func.count(FactCashflow.id).label('entries'),
            func.coalesce(func.sum(FactCashflow.amount), 0).label('amount'),
        )
        .select_from(FactCashflow)
        .join(DimBranch, FactCashflow.branch_id == DimBranch.id, isouter=True)
        .where(*_date_range(FactCashflow.doc_date, date_from, date_to))
    )
    if branches:
        stmt = stmt.where(DimBranch.external_id.in_(branches))
    stmt = stmt.group_by(func.lower(FactCashflow.entry_type)).order_by(func.sum(FactCashflow.amount).desc())
    rows = (await db.execute(stmt)).all()
    return [
        {
            'entry_type': str(r[0] or 'unknown'),
            'entries': int(r[1] or 0),
            'amount': float(r[2] or 0),
        }
        for r in rows
    ]


async def cashflow_monthly_trend(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
):
    entry_lower = func.lower(FactCashflow.entry_type)
    inflow_case = case((entry_lower.in_(['in', 'inflow', 'credit', 'income']), FactCashflow.amount), else_=0)
    outflow_case = case((entry_lower.in_(['out', 'outflow', 'debit', 'expense']), FactCashflow.amount), else_=0)
    base = (
        select(
            func.date_trunc('month', cast(FactCashflow.doc_date, Date)).label('month'),
            inflow_case.label('inflow_amount'),
            outflow_case.label('outflow_amount'),
        )
        .select_from(FactCashflow)
        .join(DimBranch, FactCashflow.branch_id == DimBranch.id, isouter=True)
        .where(*_date_range(FactCashflow.doc_date, date_from, date_to))
    )
    if branches:
        base = base.where(DimBranch.external_id.in_(branches))
    base = base.subquery('cashflow_monthly_base')

    stmt = (
        select(
            base.c.month,
            func.coalesce(func.sum(base.c.inflow_amount), 0).label('inflows'),
            func.coalesce(func.sum(base.c.outflow_amount), 0).label('outflows'),
        )
        .group_by(base.c.month)
        .order_by(base.c.month)
    )
    rows = (await db.execute(stmt)).all()
    out = []
    for r in rows:
        inflows = float(r[1] or 0)
        outflows = float(r[2] or 0)
        out.append(
            {
                'month': str(r[0].date() if hasattr(r[0], 'date') else r[0]),
                'inflows': inflows,
                'outflows': outflows,
                'net': inflows - outflows,
            }
        )
    return out


async def sales_seasonality(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    season_expr = _season_case(FactSales.doc_date).label('season')
    stmt = (
        select(
            season_expr,
            func.coalesce(func.sum(FactSales.net_value), 0).label('net_value'),
            func.coalesce(func.sum(FactSales.qty), 0).label('qty'),
        )
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    stmt = _apply_fact_sales_filters(
        stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    stmt = stmt.group_by(season_expr)
    rows = (await db.execute(stmt)).all()
    order = ['winter', 'spring', 'summer', 'autumn']
    m = {str(r[0]): {'season': str(r[0]), 'net_value': float(r[1] or 0), 'qty': float(r[2] or 0)} for r in rows}
    out = [m.get(s, {'season': s, 'net_value': 0.0, 'qty': 0.0}) for s in order]
    total = sum(x['net_value'] for x in out)
    for x in out:
        x['share_pct'] = (x['net_value'] / total * 100.0) if total > 0 else 0.0
    return out


async def purchases_seasonality(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    season_expr = _season_case(FactPurchases.doc_date).label('season')
    stmt = (
        select(
            season_expr,
            func.coalesce(func.sum(FactPurchases.net_value), 0).label('net_value'),
            func.coalesce(func.sum(FactPurchases.qty), 0).label('qty'),
        )
        .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
    )
    if branches:
        stmt = stmt.where(FactPurchases.branch_ext_id.in_(branches))
    if warehouses:
        stmt = stmt.where(FactPurchases.warehouse_ext_id.in_(warehouses))
    if brands:
        stmt = stmt.where(FactPurchases.brand_ext_id.in_(brands))
    if categories:
        stmt = stmt.where(FactPurchases.category_ext_id.in_(categories))
    if groups:
        stmt = stmt.where(FactPurchases.group_ext_id.in_(groups))
    stmt = stmt.group_by(season_expr)
    rows = (await db.execute(stmt)).all()
    order = ['winter', 'spring', 'summer', 'autumn']
    m = {str(r[0]): {'season': str(r[0]), 'net_value': float(r[1] or 0), 'qty': float(r[2] or 0)} for r in rows}
    out = [m.get(s, {'season': s, 'net_value': 0.0, 'qty': 0.0}) for s in order]
    total = sum(x['net_value'] for x in out)
    for x in out:
        x['share_pct'] = (x['net_value'] / total * 100.0) if total > 0 else 0.0
    return out


async def new_item_codes_activity(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    limit: int = 12,
):
    first_inventory = (
        select(
            DimItem.external_id.label('item_code'),
            func.min(FactInventory.doc_date).label('first_seen'),
        )
        .select_from(FactInventory)
        .join(DimItem, FactInventory.item_id == DimItem.id)
        .group_by(DimItem.external_id)
        .having(func.min(FactInventory.doc_date) >= date_from)
        .having(func.min(FactInventory.doc_date) <= date_to)
        .subquery('first_inventory')
    )

    sales_stmt = (
        select(
            FactSales.item_code.label('item_code'),
            func.coalesce(func.sum(FactSales.qty), 0).label('sales_qty'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('sales_value'),
        )
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
    )
    sales_stmt = _apply_fact_sales_filters(
        sales_stmt, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    sales_stmt = sales_stmt.group_by(FactSales.item_code).subquery('sales_stmt')

    purchases_stmt = (
        select(
            FactPurchases.item_code.label('item_code'),
            func.coalesce(func.sum(FactPurchases.qty), 0).label('purchases_qty'),
            func.coalesce(func.sum(FactPurchases.net_value), 0).label('purchases_value'),
        )
        .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
    )
    if branches:
        purchases_stmt = purchases_stmt.where(FactPurchases.branch_ext_id.in_(branches))
    if warehouses:
        purchases_stmt = purchases_stmt.where(FactPurchases.warehouse_ext_id.in_(warehouses))
    if brands:
        purchases_stmt = purchases_stmt.where(FactPurchases.brand_ext_id.in_(brands))
    if categories:
        purchases_stmt = purchases_stmt.where(FactPurchases.category_ext_id.in_(categories))
    if groups:
        purchases_stmt = purchases_stmt.where(FactPurchases.group_ext_id.in_(groups))
    purchases_stmt = purchases_stmt.group_by(FactPurchases.item_code).subquery('purchases_stmt')

    inv_latest_date = (await db.execute(select(func.max(FactInventory.doc_date)))).scalar_one_or_none()
    inv_latest = (
        select(
            DimItem.external_id.label('item_code'),
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_on_hand'),
        )
        .select_from(FactInventory)
        .join(DimItem, FactInventory.item_id == DimItem.id)
        .where(FactInventory.doc_date == inv_latest_date if inv_latest_date is not None else literal(False))
        .group_by(DimItem.external_id)
        .subquery('inv_latest')
    )

    stmt = (
        select(
            first_inventory.c.item_code,
            first_inventory.c.first_seen,
            func.coalesce(func.max(DimItem.name), first_inventory.c.item_code).label('item_name'),
            func.coalesce(sales_stmt.c.sales_qty, 0).label('sales_qty'),
            func.coalesce(sales_stmt.c.sales_value, 0).label('sales_value'),
            func.coalesce(purchases_stmt.c.purchases_qty, 0).label('purchases_qty'),
            func.coalesce(purchases_stmt.c.purchases_value, 0).label('purchases_value'),
            func.coalesce(inv_latest.c.qty_on_hand, 0).label('qty_on_hand'),
        )
        .select_from(first_inventory)
        .join(DimItem, DimItem.external_id == first_inventory.c.item_code, isouter=True)
        .join(sales_stmt, sales_stmt.c.item_code == first_inventory.c.item_code, isouter=True)
        .join(purchases_stmt, purchases_stmt.c.item_code == first_inventory.c.item_code, isouter=True)
        .join(inv_latest, inv_latest.c.item_code == first_inventory.c.item_code, isouter=True)
        .group_by(
            first_inventory.c.item_code,
            first_inventory.c.first_seen,
            sales_stmt.c.sales_qty,
            sales_stmt.c.sales_value,
            purchases_stmt.c.purchases_qty,
            purchases_stmt.c.purchases_value,
            inv_latest.c.qty_on_hand,
        )
        .order_by(first_inventory.c.first_seen.desc(), func.coalesce(sales_stmt.c.sales_value, 0).desc())
        .limit(max(1, min(limit, 50)))
    )
    rows = (await db.execute(stmt)).all()
    return [
        {
            'item_code': str(r[0] or 'N/A'),
            'first_seen': str(r[1]) if r[1] else None,
            'item_name': _clean_item_name(r[2], r[0] or 'N/A'),
            'sales_qty': float(r[3] or 0),
            'sales_value': float(r[4] or 0),
            'purchases_qty': float(r[5] or 0),
            'purchases_value': float(r[6] or 0),
            'qty_on_hand': float(r[7] or 0),
        }
        for r in rows
    ]


async def inventory_items_overview(
    db: AsyncSession,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    status: str = 'all',
    movement: str = 'all',
    q: str | None = None,
    limit: int = 200,
):
    latest_date_stmt = select(func.max(FactInventory.doc_date)).where(FactInventory.doc_date <= as_of)
    latest_date = (await db.execute(latest_date_stmt)).scalar_one_or_none()
    if latest_date is None:
        return {'snapshot_date': None, 'summary': {}, 'rows': []}

    inv_base = (
        select(
            DimItem.external_id.label('item_code'),
            func.coalesce(func.max(DimItem.name), DimItem.external_id).label('item_name'),
            func.coalesce(func.max(DimItem.sku), literal('')).label('barcode'),
            func.coalesce(func.max(DimBrand.name), literal('N/A')).label('brand'),
            func.coalesce(func.max(DimCategory.name), literal('N/A')).label('category'),
            func.coalesce(func.max(DimGroup.name), literal('N/A')).label('group_name'),
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(FactInventory.value_amount), 0).label('stock_value'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date == latest_date)
    )
    inv_base = _apply_inventory_filters(
        inv_base,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    inv_base = inv_base.group_by(DimItem.external_id).subquery('inv_base')

    sales_30 = (
        select(
            FactSales.item_code.label('item_code'),
            func.coalesce(func.sum(FactSales.qty), 0).label('sales_qty_30'),
            func.max(FactSales.doc_date).label('last_sale_date'),
        )
        .where(FactSales.doc_date >= (as_of - timedelta(days=30)))
        .where(FactSales.doc_date <= as_of)
    )
    sales_30 = _apply_fact_sales_filters(
        sales_30, branches=branches, warehouses=warehouses, brands=brands, categories=categories, groups=groups
    )
    sales_30 = sales_30.group_by(FactSales.item_code).subquery('sales_30')

    purch_30 = (
        select(
            FactPurchases.item_code.label('item_code'),
            func.coalesce(func.sum(FactPurchases.qty), 0).label('purchases_qty_30'),
        )
        .where(FactPurchases.doc_date >= (as_of - timedelta(days=30)))
        .where(FactPurchases.doc_date <= as_of)
    )
    if branches:
        purch_30 = purch_30.where(FactPurchases.branch_ext_id.in_(branches))
    if warehouses:
        purch_30 = purch_30.where(FactPurchases.warehouse_ext_id.in_(warehouses))
    if brands:
        purch_30 = purch_30.where(FactPurchases.brand_ext_id.in_(brands))
    if categories:
        purch_30 = purch_30.where(FactPurchases.category_ext_id.in_(categories))
    if groups:
        purch_30 = purch_30.where(FactPurchases.group_ext_id.in_(groups))
    purch_30 = purch_30.group_by(FactPurchases.item_code).subquery('purch_30')

    first_seen = (
        select(
            DimItem.external_id.label('item_code'),
            func.min(FactInventory.doc_date).label('first_seen'),
        )
        .select_from(FactInventory)
        .join(DimItem, FactInventory.item_id == DimItem.id)
        .group_by(DimItem.external_id)
        .subquery('first_seen')
    )

    stmt = (
        select(
            inv_base.c.item_code,
            inv_base.c.item_name,
            inv_base.c.barcode,
            inv_base.c.brand,
            inv_base.c.category,
            inv_base.c.group_name,
            inv_base.c.qty_on_hand,
            inv_base.c.stock_value,
            func.coalesce(sales_30.c.sales_qty_30, 0).label('sales_qty_30'),
            sales_30.c.last_sale_date,
            func.coalesce(purch_30.c.purchases_qty_30, 0).label('purchases_qty_30'),
            first_seen.c.first_seen,
        )
        .select_from(inv_base)
        .join(sales_30, sales_30.c.item_code == inv_base.c.item_code, isouter=True)
        .join(purch_30, purch_30.c.item_code == inv_base.c.item_code, isouter=True)
        .join(first_seen, first_seen.c.item_code == inv_base.c.item_code, isouter=True)
    )

    q_clean = (q or '').strip().lower()
    if q_clean:
        stmt = stmt.where(
            func.lower(cast(inv_base.c.item_name, String)).like(f'%{q_clean}%')
            | func.lower(cast(inv_base.c.item_code, String)).like(f'%{q_clean}%')
            | func.lower(cast(inv_base.c.barcode, String)).like(f'%{q_clean}%')
        )

    rows = (await db.execute(stmt)).all()
    mapped = []
    for r in rows:
        sales_qty_30 = float(r[8] or 0)
        last_sale_date = r[9]
        is_active = bool(last_sale_date and last_sale_date >= (as_of - timedelta(days=60)))
        movement_level = 'fast' if sales_qty_30 >= 50 else ('slow' if sales_qty_30 <= 5 else 'normal')
        mapped.append(
            {
                'item_code': str(r[0] or 'N/A'),
                'item_name': _clean_item_name(r[1], r[0] or 'N/A'),
                'barcode': str(r[2] or ''),
                'brand': str(r[3] or 'N/A'),
                'category': str(r[4] or 'N/A'),
                'group': str(r[5] or 'N/A'),
                'qty_on_hand': float(r[6] or 0),
                'stock_value': float(r[7] or 0),
                'sales_qty_30': sales_qty_30,
                'last_sale_date': str(last_sale_date) if last_sale_date else None,
                'purchases_qty_30': float(r[10] or 0),
                'first_seen': str(r[11]) if r[11] else None,
                'status': 'active' if is_active else 'inactive',
                'movement': movement_level,
            }
        )

    if status in {'active', 'inactive'}:
        mapped = [x for x in mapped if x['status'] == status]
    if movement in {'fast', 'slow', 'normal'}:
        mapped = [x for x in mapped if x['movement'] == movement]
    mapped.sort(key=lambda x: (x['stock_value'], x['qty_on_hand']), reverse=True)
    mapped = mapped[: max(1, min(limit, 500))]

    summary = {
        'total_items': len(rows),
        'active_items': sum(1 for x in rows if x[9] and x[9] >= (as_of - timedelta(days=60))),
        'inactive_items': sum(1 for x in rows if not (x[9] and x[9] >= (as_of - timedelta(days=60)))),
        'fast_items': sum(1 for x in rows if float(x[8] or 0) >= 50),
        'slow_items': sum(1 for x in rows if float(x[8] or 0) <= 5),
        'stock_value': float(sum(float(x[7] or 0) for x in rows)),
    }
    return {'snapshot_date': str(latest_date), 'summary': summary, 'rows': mapped}


async def inventory_item_detail(
    db: AsyncSession,
    item_code: str,
    as_of: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
):
    code = (item_code or '').strip()
    if not code:
        raise ValueError('Missing item code')

    latest_date = (await db.execute(select(func.max(FactInventory.doc_date)).where(FactInventory.doc_date <= as_of))).scalar_one_or_none()
    if latest_date is None:
        raise ValueError('No inventory snapshot found')

    dim_row = (
        await db.execute(
            select(
                cast(DimItem.id, String),
                DimItem.external_id,
                DimItem.sku,
                DimItem.name,
                cast(DimItem.brand_id, String),
                DimBrand.external_id,
                DimBrand.name,
                cast(DimItem.category_id, String),
                DimCategory.external_id,
                DimCategory.name,
                cast(DimItem.group_id, String),
                DimGroup.external_id,
                DimGroup.name,
                DimItem.updated_at,
                DimItem.created_at,
            )
            .select_from(DimItem)
            .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
            .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
            .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
            .where(DimItem.external_id == code)
        )
    ).one_or_none()

    inv_stmt = (
        select(
            func.coalesce(func.sum(FactInventory.qty_on_hand), 0).label('qty_on_hand'),
            func.coalesce(func.sum(FactInventory.qty_reserved), 0).label('qty_reserved'),
            func.coalesce(func.sum(FactInventory.cost_amount), 0).label('cost_amount'),
            func.coalesce(func.sum(FactInventory.value_amount), 0).label('stock_value'),
            func.max(FactInventory.updated_at).label('inv_updated_at'),
            func.min(FactInventory.created_at).label('inv_created_at'),
            func.count(FactInventory.id).label('inv_rows'),
            func.count(func.distinct(FactInventory.branch_id)).label('branch_count'),
            func.count(func.distinct(FactInventory.warehouse_id)).label('warehouse_count'),
        )
        .select_from(FactInventory)
        .join(DimBranch, FactInventory.branch_id == DimBranch.id, isouter=True)
        .join(DimWarehouse, FactInventory.warehouse_id == DimWarehouse.id, isouter=True)
        .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
        .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
        .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
        .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
        .where(FactInventory.doc_date == latest_date)
        .where(DimItem.external_id == code)
    )
    inv_stmt = _apply_inventory_filters(
        inv_stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    inv_row = (await db.execute(inv_stmt)).one()

    sales_30_stmt = (
        select(
            func.coalesce(func.sum(FactSales.qty), 0).label('sales_qty_30'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('sales_value_30'),
            func.coalesce(func.sum(FactSales.gross_value), 0).label('gross_value_30'),
            func.coalesce(func.sum(FactSales.cost_amount), 0).label('sales_cost_30'),
            func.coalesce(func.sum(FactSales.profit_amount), 0).label('sales_profit_30'),
            func.max(FactSales.doc_date).label('last_sale_date'),
            func.count(FactSales.id).label('sales_rows_30'),
        )
        .where(FactSales.item_code == code)
        .where(FactSales.doc_date >= (as_of - timedelta(days=30)))
        .where(FactSales.doc_date <= as_of)
    )
    sales_30_stmt = _apply_fact_sales_filters(
        sales_30_stmt,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    sales_30_row = (await db.execute(sales_30_stmt)).one()

    purch_30_stmt = (
        select(
            func.coalesce(func.sum(FactPurchases.qty), 0).label('purchases_qty_30'),
            func.coalesce(func.sum(FactPurchases.net_value), 0).label('purchases_value_30'),
            func.coalesce(func.sum(FactPurchases.cost_amount), 0).label('purchases_cost_30'),
            func.max(FactPurchases.doc_date).label('last_purchase_date'),
            func.count(FactPurchases.id).label('purchase_rows_30'),
        )
        .where(FactPurchases.item_code == code)
        .where(FactPurchases.doc_date >= (as_of - timedelta(days=30)))
        .where(FactPurchases.doc_date <= as_of)
    )
    if branches:
        purch_30_stmt = purch_30_stmt.where(FactPurchases.branch_ext_id.in_(branches))
    if warehouses:
        purch_30_stmt = purch_30_stmt.where(FactPurchases.warehouse_ext_id.in_(warehouses))
    if brands:
        purch_30_stmt = purch_30_stmt.where(FactPurchases.brand_ext_id.in_(brands))
    if categories:
        purch_30_stmt = purch_30_stmt.where(FactPurchases.category_ext_id.in_(categories))
    if groups:
        purch_30_stmt = purch_30_stmt.where(FactPurchases.group_ext_id.in_(groups))
    purch_30_row = (await db.execute(purch_30_stmt)).one()

    first_seen_row = (
        await db.execute(
            select(func.min(FactInventory.doc_date), func.max(FactInventory.doc_date))
            .select_from(FactInventory)
            .join(DimItem, FactInventory.item_id == DimItem.id, isouter=True)
            .where(DimItem.external_id == code)
        )
    ).one()

    qty_on_hand = float(inv_row[0] or 0)
    qty_reserved = float(inv_row[1] or 0)
    cost_amount = float(inv_row[2] or 0)
    stock_value = float(inv_row[3] or 0)

    sales_qty_30 = float(sales_30_row[0] or 0)
    sales_value_30 = float(sales_30_row[1] or 0)
    gross_value_30 = float(sales_30_row[2] or 0)
    sales_cost_30 = float(sales_30_row[3] or 0)
    sales_profit_30 = float(sales_30_row[4] or 0)
    last_sale_date = sales_30_row[5]
    sales_rows_30 = int(sales_30_row[6] or 0)

    purchases_qty_30 = float(purch_30_row[0] or 0)
    purchases_value_30 = float(purch_30_row[1] or 0)
    purchases_cost_30 = float(purch_30_row[2] or 0)
    last_purchase_date = purch_30_row[3]
    purchase_rows_30 = int(purch_30_row[4] or 0)

    first_seen = first_seen_row[0]
    last_inventory_date = first_seen_row[1]

    is_active = bool(last_sale_date and last_sale_date >= (as_of - timedelta(days=60)))
    movement_level = 'fast' if sales_qty_30 >= 50 else ('slow' if sales_qty_30 <= 5 else 'normal')

    item_name = _clean_item_name(dim_row[3], code) if dim_row else code
    barcode = str(dim_row[2] or '') if dim_row else ''
    brand_name = str(dim_row[6] or 'N/A') if dim_row else 'N/A'
    category_name = str(dim_row[9] or 'N/A') if dim_row else 'N/A'
    group_name = str(dim_row[12] or 'N/A') if dim_row else 'N/A'

    raw_fields = [
        {'key': 'item_id', 'label': 'Item UUID', 'value': str(dim_row[0] or '-') if dim_row else '-'},
        {'key': 'external_id', 'label': 'External ID', 'value': str(dim_row[1] or code) if dim_row else code},
        {'key': 'barcode', 'label': 'Barcode / SKU', 'value': barcode or '-'},
        {'key': 'brand_id', 'label': 'Brand UUID', 'value': str(dim_row[4] or '-') if dim_row else '-'},
        {'key': 'brand_external_id', 'label': 'Brand Ext ID', 'value': str(dim_row[5] or '-') if dim_row else '-'},
        {'key': 'brand_name', 'label': 'Brand Name', 'value': brand_name or '-'},
        {'key': 'category_id', 'label': 'Category UUID', 'value': str(dim_row[7] or '-') if dim_row else '-'},
        {'key': 'category_external_id', 'label': 'Category Ext ID', 'value': str(dim_row[8] or '-') if dim_row else '-'},
        {'key': 'category_name', 'label': 'Category Name', 'value': category_name or '-'},
        {'key': 'group_id', 'label': 'Group UUID', 'value': str(dim_row[10] or '-') if dim_row else '-'},
        {'key': 'group_external_id', 'label': 'Group Ext ID', 'value': str(dim_row[11] or '-') if dim_row else '-'},
        {'key': 'group_name', 'label': 'Group Name', 'value': group_name or '-'},
        {'key': 'item_created_at', 'label': 'Item Created At', 'value': str(dim_row[14]) if dim_row and dim_row[14] else '-'},
        {'key': 'item_updated_at', 'label': 'Item Updated At', 'value': str(dim_row[13]) if dim_row and dim_row[13] else '-'},
        {'key': 'snapshot_date', 'label': 'Snapshot Date', 'value': str(latest_date)},
        {'key': 'inventory_rows', 'label': 'Inventory Rows', 'value': int(inv_row[6] or 0)},
        {'key': 'branch_count', 'label': 'Branches', 'value': int(inv_row[7] or 0)},
        {'key': 'warehouse_count', 'label': 'Warehouses', 'value': int(inv_row[8] or 0)},
        {'key': 'qty_on_hand', 'label': 'Qty On Hand', 'value': qty_on_hand},
        {'key': 'qty_reserved', 'label': 'Qty Reserved', 'value': qty_reserved},
        {'key': 'cost_amount', 'label': 'Cost Amount', 'value': cost_amount},
        {'key': 'stock_value', 'label': 'Stock Value', 'value': stock_value},
        {'key': 'sales_rows_30', 'label': 'Sales Rows 30d', 'value': sales_rows_30},
        {'key': 'sales_qty_30', 'label': 'Sales Qty 30d', 'value': sales_qty_30},
        {'key': 'sales_value_30', 'label': 'Sales Net 30d', 'value': sales_value_30},
        {'key': 'sales_gross_30', 'label': 'Sales Gross 30d', 'value': gross_value_30},
        {'key': 'sales_cost_30', 'label': 'Sales Cost 30d', 'value': sales_cost_30},
        {'key': 'sales_profit_30', 'label': 'Sales Profit 30d', 'value': sales_profit_30},
        {'key': 'purchase_rows_30', 'label': 'Purchases Rows 30d', 'value': purchase_rows_30},
        {'key': 'purchases_qty_30', 'label': 'Purchases Qty 30d', 'value': purchases_qty_30},
        {'key': 'purchases_value_30', 'label': 'Purchases Net 30d', 'value': purchases_value_30},
        {'key': 'purchases_cost_30', 'label': 'Purchases Cost 30d', 'value': purchases_cost_30},
        {'key': 'last_sale_date', 'label': 'Last Sale Date', 'value': str(last_sale_date) if last_sale_date else '-'},
        {'key': 'last_purchase_date', 'label': 'Last Purchase Date', 'value': str(last_purchase_date) if last_purchase_date else '-'},
        {'key': 'first_seen', 'label': 'First Inventory Date', 'value': str(first_seen) if first_seen else '-'},
        {'key': 'last_inventory_date', 'label': 'Last Inventory Date', 'value': str(last_inventory_date) if last_inventory_date else '-'},
        {'key': 'status', 'label': 'Status', 'value': 'active' if is_active else 'inactive'},
        {'key': 'movement', 'label': 'Movement', 'value': movement_level},
    ]

    return {
        'item_code': code,
        'item_name': item_name,
        'barcode': barcode,
        'brand': brand_name,
        'category': category_name,
        'group': group_name,
        'qty_on_hand': qty_on_hand,
        'qty_reserved': qty_reserved,
        'cost_amount': cost_amount,
        'stock_value': stock_value,
        'sales_qty_30': sales_qty_30,
        'purchases_qty_30': purchases_qty_30,
        'last_sale_date': str(last_sale_date) if last_sale_date else None,
        'first_seen': str(first_seen) if first_seen else None,
        'status': 'active' if is_active else 'inactive',
        'movement': movement_level,
        'snapshot_date': str(latest_date),
        'raw_fields': raw_fields,
    }


async def price_control_filter_options(
    db: AsyncSession,
    supplier_ext_id: str | None = None,
    target_id: str | None = None,
):
    category_labels = await _dimension_label_map(db, DimCategory)
    group_labels = await _dimension_label_map(db, DimGroup)

    suppliers_rows = (
        await db.execute(
            select(DimSupplier.external_id, DimSupplier.name)
            .where(DimSupplier.external_id.is_not(None))
            .order_by(DimSupplier.name.asc())
        )
    ).all()
    categories_rows = (
        await db.execute(
            select(DimCategory.external_id)
            .where(DimCategory.external_id.is_not(None))
            .order_by(DimCategory.external_id.asc())
        )
    ).all()
    groups_rows = (
        await db.execute(
            select(DimGroup.external_id)
            .where(DimGroup.external_id.is_not(None))
            .order_by(DimGroup.external_id.asc())
        )
    ).all()
    targets_rows = (
        await db.execute(
            select(
                cast(SupplierTarget.id, String),
                SupplierTarget.name,
                SupplierTarget.supplier_ext_id,
                SupplierTarget.target_year,
                SupplierTarget.rebate_percent,
            )
            .where(SupplierTarget.is_active.is_(True))
            .order_by(SupplierTarget.target_year.desc(), SupplierTarget.name.asc())
        )
    ).all()

    selected_target_supplier: str | None = None
    selected_target_rebate = 0.0
    target_item_codes: set[str] = set()
    if target_id:
        target = (
            await db.execute(
                select(SupplierTarget).where(cast(SupplierTarget.id, String) == target_id, SupplierTarget.is_active.is_(True))
            )
        ).scalar_one_or_none()
        if target:
            selected_target_supplier = target.supplier_ext_id
            selected_target_rebate = float(target.rebate_percent or 0)
            t_items = (
                await db.execute(
                    select(SupplierTargetItem.item_external_id).where(SupplierTargetItem.supplier_target_id == target.id)
                )
            ).scalars().all()
            target_item_codes = {str(x) for x in t_items if x}

    effective_supplier = selected_target_supplier or (supplier_ext_id or None)

    items_stmt = (
        select(
            FactPurchases.item_code,
            func.coalesce(func.max(DimItem.name), FactPurchases.item_code).label('item_name'),
        )
        .join(DimItem, DimItem.external_id == FactPurchases.item_code, isouter=True)
        .where(FactPurchases.item_code.is_not(None))
    )
    if effective_supplier:
        items_stmt = items_stmt.where(FactPurchases.supplier_ext_id == effective_supplier)
    if target_item_codes:
        items_stmt = items_stmt.where(FactPurchases.item_code.in_(list(target_item_codes)))
    items_stmt = items_stmt.group_by(FactPurchases.item_code).order_by(func.max(DimItem.name).asc()).limit(1000)
    item_rows = (await db.execute(items_stmt)).all()

    categories = [
        {'value': str(r[0]), 'label': str(category_labels.get(str(r[0]), r[0]))}
        for r in categories_rows
        if r[0]
    ]
    groups = [
        {'value': str(r[0]), 'label': str(group_labels.get(str(r[0]), r[0]))}
        for r in groups_rows
        if r[0]
    ]
    categories.sort(key=lambda x: x['label'].casefold())
    groups.sort(key=lambda x: x['label'].casefold())

    return {
        'suppliers': [{'value': str(r[0]), 'label': str(r[1] or r[0])} for r in suppliers_rows],
        'categories': categories,
        'groups': groups,
        'targets': [
            {
                'value': str(r[0]),
                'label': f'{str(r[1] or "Target")} ({int(r[3] or 0)})',
                'supplier_ext_id': str(r[2] or ''),
                'rebate_percent': float(r[4] or 0),
            }
            for r in targets_rows
        ],
        'items': [{'value': str(r[0]), 'label': _clean_item_name(r[1], r[0])} for r in item_rows if r[0]],
        'target_supplier_ext_id': selected_target_supplier,
        'target_rebate_percent': selected_target_rebate,
    }


async def price_control_items(
    db: AsyncSession,
    date_from: date,
    date_to: date,
    supplier_ext_id: str | None = None,
    target_id: str | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    item_codes: list[str] | None = None,
    target_margin_pct: float = 35.0,
    discount_pct: float = 0.0,
    limit: int = 500,
):
    selected_target_supplier: str | None = None
    selected_target_rebate = 0.0
    target_item_codes: set[str] = set()
    if target_id:
        target = (
            await db.execute(
                select(SupplierTarget).where(cast(SupplierTarget.id, String) == target_id, SupplierTarget.is_active.is_(True))
            )
        ).scalar_one_or_none()
        if target:
            selected_target_supplier = target.supplier_ext_id
            selected_target_rebate = float(target.rebate_percent or 0)
            t_items = (
                await db.execute(
                    select(SupplierTargetItem.item_external_id).where(SupplierTargetItem.supplier_target_id == target.id)
                )
            ).scalars().all()
            target_item_codes = {str(x) for x in t_items if x}

    effective_supplier = selected_target_supplier or (supplier_ext_id or None)
    effective_discount_pct = float(discount_pct if discount_pct > 0 else selected_target_rebate)
    effective_discount_pct = max(0.0, min(99.0, effective_discount_pct))
    target_margin_pct = max(0.0, min(95.0, float(target_margin_pct)))

    purchases_stmt = (
        select(
            FactPurchases.item_code.label('item_code'),
            func.coalesce(func.sum(FactPurchases.qty), 0).label('purchases_qty'),
            func.coalesce(func.sum(FactPurchases.net_value), 0).label('purchases_value'),
            func.coalesce(
                func.sum(
                    case(
                        (func.coalesce(FactPurchases.cost_amount, 0) > 0, FactPurchases.cost_amount),
                        else_=FactPurchases.net_value,
                    )
                ),
                0,
            ).label('purchases_cost'),
            func.coalesce(func.max(FactPurchases.supplier_ext_id), literal('')).label('supplier_ext_id'),
        )
        .where(*_date_range(FactPurchases.doc_date, date_from, date_to))
        .where(FactPurchases.item_code.is_not(None))
    )
    if effective_supplier:
        purchases_stmt = purchases_stmt.where(FactPurchases.supplier_ext_id == effective_supplier)
    if categories:
        purchases_stmt = purchases_stmt.where(FactPurchases.category_ext_id.in_(categories))
    if groups:
        purchases_stmt = purchases_stmt.where(FactPurchases.group_ext_id.in_(groups))
    if target_item_codes:
        purchases_stmt = purchases_stmt.where(FactPurchases.item_code.in_(list(target_item_codes)))
    if item_codes:
        purchases_stmt = purchases_stmt.where(FactPurchases.item_code.in_(item_codes))
    purchases_stmt = purchases_stmt.group_by(FactPurchases.item_code).subquery('pc_purchases')

    sales_stmt = (
        select(
            FactSales.item_code.label('item_code'),
            func.coalesce(func.sum(FactSales.qty), 0).label('sales_qty'),
            func.coalesce(func.sum(FactSales.net_value), 0).label('sales_value'),
        )
        .where(*_date_range(FactSales.doc_date, date_from, date_to))
        .where(FactSales.item_code.is_not(None))
    )
    if categories:
        sales_stmt = sales_stmt.where(FactSales.category_ext_id.in_(categories))
    if groups:
        sales_stmt = sales_stmt.where(FactSales.group_ext_id.in_(groups))
    if target_item_codes:
        sales_stmt = sales_stmt.where(FactSales.item_code.in_(list(target_item_codes)))
    if item_codes:
        sales_stmt = sales_stmt.where(FactSales.item_code.in_(item_codes))
    sales_stmt = sales_stmt.group_by(FactSales.item_code).subquery('pc_sales')

    item_codes_rows = (await db.execute(select(purchases_stmt.c.item_code))).scalars().all()
    code_set = {str(x) for x in item_codes_rows if x}
    sales_codes = (await db.execute(select(sales_stmt.c.item_code))).scalars().all()
    code_set.update({str(x) for x in sales_codes if x})
    if not code_set:
        return {
            'summary': {'items': 0, 'avg_margin_pct': 0.0, 'target_margin_pct': target_margin_pct},
            'rows': [],
            'effective_discount_pct': effective_discount_pct,
        }

    meta_rows = (
        await db.execute(
            select(
                DimItem.external_id,
                func.coalesce(DimItem.name, DimItem.external_id).label('item_name'),
                func.coalesce(DimItem.sku, literal('')).label('barcode'),
                func.coalesce(DimBrand.name, literal('N/A')).label('brand'),
                func.coalesce(DimCategory.name, literal('N/A')).label('category'),
                func.coalesce(DimGroup.name, literal('N/A')).label('group_name'),
            )
            .select_from(DimItem)
            .join(DimBrand, DimItem.brand_id == DimBrand.id, isouter=True)
            .join(DimCategory, DimItem.category_id == DimCategory.id, isouter=True)
            .join(DimGroup, DimItem.group_id == DimGroup.id, isouter=True)
            .where(DimItem.external_id.in_(list(code_set)))
        )
    ).all()
    meta = {
        str(r[0]): {
            'item_name': _clean_item_name(r[1], r[0]),
            'barcode': str(r[2] or ''),
            'brand': str(r[3] or 'N/A'),
            'category': str(r[4] or 'N/A'),
            'group': str(r[5] or 'N/A'),
        }
        for r in meta_rows
    }

    supplier_name_map = {
        str(r[0]): str(r[1] or r[0])
        for r in (
            await db.execute(select(DimSupplier.external_id, DimSupplier.name).where(DimSupplier.external_id.is_not(None)))
        ).all()
    }

    purchases_map = {
        str(r[0]): {
            'qty': float(r[1] or 0),
            'value': float(r[2] or 0),
            'cost': float(r[3] or 0),
            'supplier_ext_id': str(r[4] or ''),
        }
        for r in (await db.execute(select(purchases_stmt))).all()
    }
    sales_map = {
        str(r[0]): {'qty': float(r[1] or 0), 'value': float(r[2] or 0)}
        for r in (await db.execute(select(sales_stmt))).all()
    }

    rows: list[dict[str, object]] = []
    for code in sorted(code_set):
        p = purchases_map.get(code, {'qty': 0.0, 'value': 0.0, 'cost': 0.0, 'supplier_ext_id': ''})
        s = sales_map.get(code, {'qty': 0.0, 'value': 0.0})

        wholesale_unit = (p['cost'] / p['qty']) if p['qty'] > 0 else 0.0
        acquisition_after_discount = wholesale_unit * (1 - effective_discount_pct / 100.0)
        sale_unit = (s['value'] / s['qty']) if s['qty'] > 0 else 0.0
        unit_profit = sale_unit - acquisition_after_discount
        margin_pct = ((unit_profit / sale_unit) * 100.0) if sale_unit > 0 else 0.0

        required_total_discount_pct = None
        recommended_extra_discount_pct = None
        if wholesale_unit > 0 and sale_unit > 0:
            max_cost_for_target = sale_unit * (1 - target_margin_pct / 100.0)
            req_total = max(0.0, min(99.0, (1 - (max_cost_for_target / wholesale_unit)) * 100.0))
            required_total_discount_pct = req_total
            recommended_extra_discount_pct = max(0.0, req_total - effective_discount_pct)

        m = meta.get(code, {'item_name': code, 'barcode': '', 'brand': 'N/A', 'category': 'N/A', 'group': 'N/A'})
        supplier_code = str(p.get('supplier_ext_id') or effective_supplier or '')
        rows.append(
            {
                'item_code': code,
                'item_name': m['item_name'],
                'barcode': m['barcode'],
                'supplier': supplier_name_map.get(supplier_code, supplier_code or 'N/A'),
                'brand': m['brand'],
                'category': m['category'],
                'group': m['group'],
                'sales_qty': float(s['qty']),
                'sales_value': float(s['value']),
                'purchases_qty': float(p['qty']),
                'wholesale_unit': wholesale_unit,
                'discount_pct': effective_discount_pct,
                'acquisition_after_discount': acquisition_after_discount,
                'sale_unit': sale_unit,
                'unit_profit': unit_profit,
                'margin_pct': margin_pct,
                'target_margin_pct': target_margin_pct,
                'required_total_discount_pct': required_total_discount_pct,
                'recommended_extra_discount_pct': recommended_extra_discount_pct,
            }
        )

    rows.sort(key=lambda x: float(x.get('sales_value', 0) or 0), reverse=True)
    rows = rows[: max(1, min(limit, 2000))]
    avg_margin = (sum(float(r['margin_pct']) for r in rows) / len(rows)) if rows else 0.0

    return {
        'summary': {
            'items': len(rows),
            'avg_margin_pct': round(avg_margin, 2),
            'target_margin_pct': round(target_margin_pct, 2),
        },
        'effective_discount_pct': round(effective_discount_pct, 2),
        'rows': rows,
    }
