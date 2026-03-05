from datetime import date, timedelta
from io import StringIO
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_request_tenant, get_tenant_db
from app.models.control import PlanName, Tenant
from app.services.supplier_targets import (
    create_supplier_target,
    list_supplier_targets,
    supplier_target_filter_options,
    update_supplier_target,
)
from app.services.kpi_queries import (
    cashflow_by_entry_type,
    cashflow_monthly_trend,
    cashflow_summary,
    inventory_by_brand,
    inventory_by_commercial_category,
    inventory_by_manufacturer,
    inventory_filter_options,
    inventory_item_detail,
    inventory_items_overview,
    inventory_snapshot,
    purchases_filter_options,
    purchases_decision_pack,
    purchases_by_supplier,
    purchases_summary,
    price_control_filter_options,
    price_control_items,
    sales_filter_options,
    sales_entity_ranking,
    sales_decision_pack,
    sales_by_branch,
    sales_by_brand,
    sales_by_category,
    sales_summary,
    stock_aging,
)
from app.services.intelligence_service import acknowledge_insight, list_insights
from celery import Celery
from app.core.config import settings

router = APIRouter(tags=['kpi'])
celery_client = Celery('kpi_sender', broker=settings.celery_broker_url)


class SupplierTargetCreateIn(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    supplier_ext_id: str = Field(min_length=1, max_length=120)
    supplier_name: str | None = Field(default=None, max_length=160)
    target_year: int = Field(ge=2000, le=2100)
    target_amount: float = Field(ge=0)
    rebate_percent: float = Field(ge=0, le=100)
    rebate_amount: float = Field(default=0, ge=0)
    item_external_ids: list[str] = Field(default_factory=list)
    notes: str | None = Field(default=None, max_length=500)


class SupplierTargetUpdateIn(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    supplier_ext_id: str | None = Field(default=None, min_length=1, max_length=120)
    supplier_name: str | None = Field(default=None, max_length=160)
    target_year: int | None = Field(default=None, ge=2000, le=2100)
    target_amount: float | None = Field(default=None, ge=0)
    rebate_percent: float | None = Field(default=None, ge=0, le=100)
    rebate_amount: float | None = Field(default=None, ge=0)
    item_external_ids: list[str] | None = None
    notes: str | None = Field(default=None, max_length=500)
    is_active: bool | None = None


def _default_from() -> date:
    return date.today() - timedelta(days=30)


def _default_to() -> date:
    return date.today()


def _csv_response(filename: str, header: list[str], rows: list[list[str]]) -> StreamingResponse:
    buf = StringIO()
    buf.write(','.join(header) + '\n')
    for row in rows:
        buf.write(','.join(row) + '\n')
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'},
    )


@router.get('/v1/kpi/sales/summary')
@router.get('/kpi/sales/summary')
async def get_sales_summary(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await sales_summary(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/sales/decision-pack')
async def get_sales_decision_pack(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    top_n: int = Query(default=10, ge=1, le=50),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await sales_decision_pack(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        top_n=top_n,
    )


@router.get('/v1/kpi/sales/ranking')
async def get_sales_entity_ranking(
    entity: str = Query(...),
    metric: str = Query(default='profitability'),
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    limit: int = Query(default=30, ge=1, le=200),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    try:
        return await sales_entity_ranking(
            tenant_db,
            date_from=date_from,
            date_to=date_to,
            entity=entity,
            metric=metric,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
            limit=limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get('/v1/kpi/sales/filter-options')
async def get_sales_filter_options(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await sales_filter_options(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/intelligence/insights')
async def get_recent_intelligence_insights(
    limit: int = Query(default=20, ge=1, le=100),
    category: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    status: str | None = Query(default=None),
    date_from: date | None = Query(default=None, alias='from'),
    date_to: date | None = Query(default=None, alias='to'),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await list_insights(
        tenant_db,
        category=category,
        severity=severity,
        status=status,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
    )


@router.post('/v1/intelligence/insights/{insight_id}/acknowledge')
async def acknowledge_intelligence_insight(
    insight_id: UUID,
    _user=Depends(get_current_user),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    ok = await acknowledge_insight(tenant_db, insight_id=insight_id, user_id=None)
    if not ok:
        raise HTTPException(status_code=404, detail='Insight not found')
    return {'status': 'acknowledged', 'insight_id': str(insight_id)}


@router.post('/v1/intelligence/run-now')
async def run_intelligence_now(
    tenant: Tenant = Depends(get_request_tenant),
    _user=Depends(get_current_user),
):
    task = celery_client.send_task(
        'worker.tasks.generate_insights_for_tenant',
        kwargs={'tenant_slug': tenant.slug},
        queue='ingest',
    )
    return {'status': 'queued', 'tenant': tenant.slug, 'task_id': task.id}


@router.get('/v1/kpi/sales/by-branch')
@router.get('/kpi/sales/by-branch')
async def get_sales_by_branch(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await sales_by_branch(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/sales/by-brand')
@router.get('/kpi/sales/by-brand')
async def get_sales_by_brand(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await sales_by_brand(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/sales/by-category')
@router.get('/kpi/sales/by-category')
async def get_sales_by_category(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await sales_by_category(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/sales/compare')
@router.get('/kpi/sales/compare')
async def get_sales_compare(
    a_from: date = Query(alias='A_from'),
    a_to: date = Query(alias='A_to'),
    b_from: date = Query(alias='B_from'),
    b_to: date = Query(alias='B_to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return {
        'A': await sales_summary(
            tenant_db,
            date_from=a_from,
            date_to=a_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
        'B': await sales_summary(
            tenant_db,
            date_from=b_from,
            date_to=b_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    }


@router.get('/v1/kpi/purchases/summary')
@router.get('/kpi/purchases/summary')
async def get_purchases_summary(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    if tenant.plan == PlanName.standard:
        raise HTTPException(status_code=403, detail='Upgrade required for purchases KPIs')
    return await purchases_summary(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/purchases/by-supplier')
@router.get('/kpi/purchases/by-supplier')
async def get_purchases_by_supplier(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    if tenant.plan == PlanName.standard:
        raise HTTPException(status_code=403, detail='Upgrade required for purchases KPIs')
    return await purchases_by_supplier(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/purchases/compare')
@router.get('/kpi/purchases/compare')
async def get_purchases_compare(
    a_from: date = Query(alias='A_from'),
    a_to: date = Query(alias='A_to'),
    b_from: date = Query(alias='B_from'),
    b_to: date = Query(alias='B_to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    if tenant.plan == PlanName.standard:
        raise HTTPException(status_code=403, detail='Upgrade required for purchases KPIs')
    return {
        'A': await purchases_summary(
            tenant_db,
            date_from=a_from,
            date_to=a_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
        'B': await purchases_summary(
            tenant_db,
            date_from=b_from,
            date_to=b_to,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    }


@router.get('/v1/kpi/purchases/decision-pack')
async def get_purchases_decision_pack(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    if tenant.plan == PlanName.standard:
        raise HTTPException(status_code=403, detail='Upgrade required for purchases KPIs')
    return await purchases_decision_pack(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/purchases/filter-options')
async def get_purchases_filter_options(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    if tenant.plan == PlanName.standard:
        raise HTTPException(status_code=403, detail='Upgrade required for purchases KPIs')
    return await purchases_filter_options(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/inventory/snapshot')
@router.get('/kpi/inventory/snapshot')
async def get_inventory_snapshot(
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await inventory_snapshot(
        tenant_db,
        as_of=as_of,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/inventory/stock-aging')
@router.get('/kpi/inventory/stock-aging')
async def get_stock_aging(
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await stock_aging(
        tenant_db,
        as_of=as_of,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/inventory/by-brand')
@router.get('/kpi/inventory/by-brand')
async def get_inventory_by_brand(
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    limit: int = Query(default=12, ge=1, le=100),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await inventory_by_brand(
        tenant_db,
        as_of=as_of,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        limit=limit,
    )


@router.get('/v1/kpi/inventory/by-commercial-category')
@router.get('/kpi/inventory/by-commercial-category')
async def get_inventory_by_commercial_category(
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    limit: int = Query(default=12, ge=1, le=100),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await inventory_by_commercial_category(
        tenant_db,
        as_of=as_of,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        limit=limit,
    )


@router.get('/v1/kpi/inventory/by-manufacturer')
@router.get('/kpi/inventory/by-manufacturer')
async def get_inventory_by_manufacturer(
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    lookback_days: int = Query(default=365, ge=30, le=1825),
    limit: int = Query(default=12, ge=1, le=100),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await inventory_by_manufacturer(
        tenant_db,
        as_of=as_of,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        lookback_days=lookback_days,
        limit=limit,
    )


@router.get('/v1/kpi/inventory/filter-options')
@router.get('/kpi/inventory/filter-options')
async def get_inventory_filter_options(
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await inventory_filter_options(
        tenant_db,
        as_of=as_of,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )


@router.get('/v1/kpi/inventory/items')
@router.get('/kpi/inventory/items')
async def get_inventory_items(
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    status: str = Query(default='all'),
    movement: str = Query(default='all'),
    q: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await inventory_items_overview(
        tenant_db,
        as_of=as_of,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
        status=status,
        movement=movement,
        q=q,
        limit=limit,
    )


@router.get('/v1/kpi/inventory/items/{item_code}/detail')
@router.get('/kpi/inventory/items/{item_code}/detail')
async def get_inventory_item_detail(
    item_code: str,
    as_of: date = Query(default_factory=_default_to),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    try:
        return await inventory_item_detail(
            tenant_db,
            item_code=item_code,
            as_of=as_of,
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get('/v1/kpi/cashflow/summary')
@router.get('/kpi/cashflow/summary')
@router.get('/v1/kpi/cashflows/summary')
@router.get('/kpi/cashflows/summary')
async def get_cashflow_summary(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await cashflow_summary(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
    )


@router.get('/v1/kpi/cashflow/by-type')
@router.get('/kpi/cashflow/by-type')
@router.get('/v1/kpi/cashflows/by-type')
@router.get('/kpi/cashflows/by-type')
async def get_cashflow_by_type(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await cashflow_by_entry_type(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
    )


@router.get('/v1/kpi/cashflow/trend-monthly')
@router.get('/kpi/cashflow/trend-monthly')
@router.get('/v1/kpi/cashflows/trend-monthly')
@router.get('/kpi/cashflows/trend-monthly')
async def get_cashflow_trend_monthly(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await cashflow_monthly_trend(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
    )


@router.get('/v1/kpi/sales/by-branch/export.csv')
async def export_sales_by_branch_csv(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    rows = await sales_by_branch(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    csv_rows = [[r['branch'], str(r['net_value']), str(r['gross_value'])] for r in rows]
    return _csv_response('sales_by_branch.csv', ['branch', 'net_value', 'gross_value'], csv_rows)


@router.get('/v1/kpi/purchases/by-supplier/export.csv')
async def export_purchases_by_supplier_csv(
    date_from: date = Query(default_factory=_default_from, alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    branches: list[str] | None = Query(default=None),
    warehouses: list[str] | None = Query(default=None),
    brands: list[str] | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    tenant: Tenant = Depends(get_request_tenant),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    if tenant.plan == PlanName.standard:
        raise HTTPException(status_code=403, detail='Upgrade required for purchases KPIs')
    rows = await purchases_by_supplier(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        branches=branches,
        warehouses=warehouses,
        brands=brands,
        categories=categories,
        groups=groups,
    )
    csv_rows = [[r['supplier'], str(r['net_value']), str(r['cost_amount'])] for r in rows]
    return _csv_response('purchases_by_supplier.csv', ['supplier', 'net_value', 'cost_amount'], csv_rows)


@router.get('/v1/kpi/supplier-targets/filter-options')
async def get_supplier_targets_filter_options(
    supplier_ext_id: str | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await supplier_target_filter_options(tenant_db, supplier_ext_id=supplier_ext_id)


@router.get('/v1/kpi/supplier-targets')
async def get_supplier_targets(
    year: int = Query(default_factory=lambda: date.today().year, ge=2000, le=2100),
    as_of: date | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return {
        'year': year,
        'as_of': as_of.isoformat() if as_of else None,
        'targets': await list_supplier_targets(tenant_db, year=year, as_of=as_of),
    }


@router.post('/v1/kpi/supplier-targets')
async def post_supplier_target(
    payload: SupplierTargetCreateIn,
    _user=Depends(get_current_user),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    try:
        created = await create_supplier_target(
            tenant_db,
            name=payload.name,
            supplier_ext_id=payload.supplier_ext_id,
            supplier_name=payload.supplier_name,
            target_year=payload.target_year,
            target_amount=payload.target_amount,
            rebate_percent=payload.rebate_percent,
            rebate_amount=payload.rebate_amount,
            item_external_ids=payload.item_external_ids,
            notes=payload.notes,
        )
    except ValueError as exc:
        if str(exc) == 'duplicate_target':
            raise HTTPException(
                status_code=409,
                detail='Υπάρχει ήδη στόχος με ίδιο όνομα για τον ίδιο προμηθευτή και έτος.',
            ) from exc
        raise
    return {'status': 'created', **created}


@router.patch('/v1/kpi/supplier-targets/{target_id}')
async def patch_supplier_target(
    target_id: UUID,
    payload: SupplierTargetUpdateIn,
    _user=Depends(get_current_user),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    try:
        ok = await update_supplier_target(
            tenant_db,
            target_id=target_id,
            name=payload.name,
            supplier_ext_id=payload.supplier_ext_id,
            supplier_name=payload.supplier_name,
            target_year=payload.target_year,
            target_amount=payload.target_amount,
            rebate_percent=payload.rebate_percent,
            rebate_amount=payload.rebate_amount,
            item_external_ids=payload.item_external_ids,
            notes=payload.notes,
            is_active=payload.is_active,
        )
    except ValueError as exc:
        if str(exc) == 'duplicate_target':
            raise HTTPException(
                status_code=409,
                detail='Υπάρχει ήδη στόχος με ίδιο όνομα για τον ίδιο προμηθευτή και έτος.',
            ) from exc
        raise
    if not ok:
        raise HTTPException(status_code=404, detail='Supplier target not found')
    return {'status': 'updated', 'id': str(target_id)}


@router.get('/v1/kpi/price-control/filter-options')
async def get_price_control_filter_options(
    supplier_ext_id: str | None = Query(default=None),
    target_id: str | None = Query(default=None),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await price_control_filter_options(
        tenant_db,
        supplier_ext_id=supplier_ext_id,
        target_id=target_id,
    )


@router.get('/v1/kpi/price-control/items')
async def get_price_control_items(
    date_from: date = Query(default_factory=lambda: date(date.today().year, 1, 1), alias='from'),
    date_to: date = Query(default_factory=_default_to, alias='to'),
    supplier_ext_id: str | None = Query(default=None),
    target_id: str | None = Query(default=None),
    categories: list[str] | None = Query(default=None),
    groups: list[str] | None = Query(default=None),
    item_codes: list[str] | None = Query(default=None),
    target_margin_pct: float = Query(default=35.0, ge=0, le=95),
    discount_pct: float = Query(default=0.0, ge=0, le=99),
    limit: int = Query(default=500, ge=1, le=2000),
    tenant_db: AsyncSession = Depends(get_tenant_db),
):
    return await price_control_items(
        tenant_db,
        date_from=date_from,
        date_to=date_to,
        supplier_ext_id=supplier_ext_id,
        target_id=target_id,
        categories=categories,
        groups=groups,
        item_codes=item_codes,
        target_margin_pct=target_margin_pct,
        discount_pct=discount_pct,
        limit=limit,
    )
