from __future__ import annotations

from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tenant import AggInventorySnapshotDaily, AggSalesItemDaily, DimItem
from app.services.intelligence.types import InsightCreate, RuleContext


def _meta(*, why: str, formula: str, drilldown: str, actions: list[str], extras: dict | None = None) -> dict:
    payload = {
        'why': why,
        'formula': formula,
        'drilldown': drilldown,
        'actions': [x for x in actions if x],
    }
    if extras:
        payload.update(extras)
    return payload


async def _latest_snapshot_date(db: AsyncSession, as_of):
    return (
        await db.execute(select(func.max(AggInventorySnapshotDaily.snapshot_date)).where(AggInventorySnapshotDaily.snapshot_date <= as_of))
    ).scalar_one_or_none()


async def _previous_snapshot_date(db: AsyncSession, latest_snapshot):
    return (
        await db.execute(select(func.max(AggInventorySnapshotDaily.snapshot_date)).where(AggInventorySnapshotDaily.snapshot_date < latest_snapshot))
    ).scalar_one_or_none()


async def _last_sale_map(db: AsyncSession, item_ids: list[str]):
    rows = (
        await db.execute(
            select(AggSalesItemDaily.item_external_id, func.max(AggSalesItemDaily.doc_date))
            .where(AggSalesItemDaily.item_external_id.in_(item_ids))
            .group_by(AggSalesItemDaily.item_external_id)
        )
    ).all()
    return {str(r[0]): r[1] for r in rows}


async def _item_name_map(db: AsyncSession, item_ids: list[str]):
    rows = (await db.execute(select(DimItem.external_id, DimItem.name).where(DimItem.external_id.in_(item_ids)))).all()
    return {str(r[0]): str(r[1] or r[0]) for r in rows}


async def run_dead_stock_value(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    no_sales_days = int(params.get('no_sales_days', 60))
    min_value = float(params.get('min_value', 300))
    snapshot = await _latest_snapshot_date(db, ctx.as_of)
    if snapshot is None:
        return []
    cutoff = ctx.as_of - timedelta(days=no_sales_days)

    inv_rows = (
        await db.execute(
            select(AggInventorySnapshotDaily.item_external_id, func.coalesce(func.sum(AggInventorySnapshotDaily.value_amount), 0))
            .where(AggInventorySnapshotDaily.snapshot_date == snapshot)
            .group_by(AggInventorySnapshotDaily.item_external_id)
            .having(func.coalesce(func.sum(AggInventorySnapshotDaily.value_amount), 0) >= min_value)
        )
    ).all()
    if not inv_rows:
        return []
    item_ids = [str(r[0]) for r in inv_rows if r[0] is not None]
    last_sale = await _last_sale_map(db, item_ids)
    names = await _item_name_map(db, item_ids)

    out: list[InsightCreate] = []
    for row in inv_rows:
        item = str(row[0])
        value = float(row[1] or 0)
        ls = last_sale.get(item)
        if ls is not None and ls >= cutoff:
            continue
        out.append(
            InsightCreate(
                rule_code='INV_DEAD_STOCK',
                category='inventory',
                severity='critical',
                title='Νεκρό Απόθεμα',
                message=f'Προϊόντα αξίας {value:.2f}€ δεν έχουν κινηθεί για {no_sales_days} ημέρες.',
                entity_type='item',
                entity_external_id=item,
                entity_name=names.get(item, item),
                period_from=cutoff,
                period_to=ctx.as_of,
                value=value,
                baseline_value=min_value,
                delta_value=value - min_value,
                delta_pct=None,
                metadata_json=_meta(
                    why='Δεσμεύει κεφάλαιο και χώρο χωρίς να παράγει τζίρο.',
                    formula='no movement days >= no_sales_days AND stock_value >= min_value',
                    drilldown=f'/tenant/inventory?item={item}&view=dead-stock',
                    actions=['Προωθητική ενέργεια.', 'Επιστροφή σε προμηθευτή.'],
                    extras={'item_label': names.get(item, item)},
                ),
            )
        )
    return out


async def run_stock_aging_spike(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    increase_pct = float(params.get('increase_pct', 15))
    aging_days = int(params.get('aging_days', 90))

    latest = await _latest_snapshot_date(db, ctx.as_of)
    if latest is None:
        return []
    previous = await _previous_snapshot_date(db, latest)
    if previous is None:
        return []

    current_rows = (
        await db.execute(
            select(AggInventorySnapshotDaily.item_external_id, func.coalesce(func.sum(AggInventorySnapshotDaily.value_amount), 0))
            .where(AggInventorySnapshotDaily.snapshot_date == latest)
            .group_by(AggInventorySnapshotDaily.item_external_id)
        )
    ).all()
    item_ids = [str(r[0]) for r in current_rows if r[0] is not None]
    if not item_ids:
        return []
    last_sale = await _last_sale_map(db, item_ids)

    current_aged = 0.0
    for row in current_rows:
        item = str(row[0])
        value = float(row[1] or 0)
        ls = last_sale.get(item)
        if ls is None or (latest - ls).days >= aging_days:
            current_aged += value

    previous_rows = (
        await db.execute(
            select(AggInventorySnapshotDaily.item_external_id, func.coalesce(func.sum(AggInventorySnapshotDaily.value_amount), 0))
            .where(AggInventorySnapshotDaily.snapshot_date == previous)
            .group_by(AggInventorySnapshotDaily.item_external_id)
        )
    ).all()
    prev_ids = [str(r[0]) for r in previous_rows if r[0] is not None]
    prev_last_sale = await _last_sale_map(db, prev_ids) if prev_ids else {}
    prev_aged = 0.0
    for row in previous_rows:
        item = str(row[0])
        value = float(row[1] or 0)
        ls = prev_last_sale.get(item)
        if ls is None or (previous - ls).days >= aging_days:
            prev_aged += value

    if prev_aged <= 0:
        return []
    delta_pct = ((current_aged - prev_aged) / prev_aged) * 100.0
    if delta_pct < increase_pct:
        return []
    severity = 'critical' if delta_pct >= 30 else 'warning'
    return [
        InsightCreate(
            rule_code='INV_AGING_SPIKE',
            category='inventory',
            severity=severity,
            title='Αύξηση Παλαιού Αποθέματος',
            message=f'Το απόθεμα >90 ημερών αυξήθηκε κατά {delta_pct:.2f}%.',
            entity_type='tenant',
            entity_external_id=ctx.tenant_slug,
            entity_name=ctx.tenant_slug,
            period_from=previous,
            period_to=latest,
            value=current_aged,
            baseline_value=prev_aged,
            delta_value=current_aged - prev_aged,
            delta_pct=delta_pct,
            metadata_json=_meta(
                why='Η αύξηση παλαιού αποθέματος αυξάνει κίνδυνο απομείωσης.',
                formula='aging_90plus_current vs previous snapshot increase_pct threshold',
                drilldown='/tenant/inventory?view=aging',
                actions=['Έλεγξε κατηγορίες με υψηλή συσσώρευση.', 'Προγραμμάτισε αποσυμφόρηση stock.'],
            ),
        )
    ]


async def run_low_coverage_top_sellers(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    min_coverage_days = float(params.get('min_coverage_days', 7))
    top_n = int(params.get('top_n', 50))
    window_days = int(params.get('window_days', 30))

    snapshot = await _latest_snapshot_date(db, ctx.as_of)
    if snapshot is None:
        return []
    sales_from = ctx.as_of - timedelta(days=max(1, window_days) - 1)

    sales_rows = (
        await db.execute(
            select(AggSalesItemDaily.item_external_id, func.coalesce(func.sum(AggSalesItemDaily.qty), 0))
            .where(AggSalesItemDaily.doc_date >= sales_from, AggSalesItemDaily.doc_date <= ctx.as_of)
            .group_by(AggSalesItemDaily.item_external_id)
            .order_by(func.sum(AggSalesItemDaily.qty).desc())
            .limit(max(1, top_n))
        )
    ).all()
    item_ids = [str(r[0]) for r in sales_rows if r[0] is not None]
    if not item_ids:
        return []
    inv_rows = (
        await db.execute(
            select(AggInventorySnapshotDaily.item_external_id, func.coalesce(func.sum(AggInventorySnapshotDaily.qty_on_hand), 0))
            .where(AggInventorySnapshotDaily.snapshot_date == snapshot, AggInventorySnapshotDaily.item_external_id.in_(item_ids))
            .group_by(AggInventorySnapshotDaily.item_external_id)
        )
    ).all()
    stock_map = {str(r[0]): float(r[1] or 0) for r in inv_rows}
    names = await _item_name_map(db, item_ids)

    out: list[InsightCreate] = []
    for row in sales_rows:
        item = str(row[0])
        qty = float(row[1] or 0)
        if qty <= 0:
            continue
        avg_daily = qty / max(1, window_days)
        cov = stock_map.get(item, 0.0) / avg_daily if avg_daily > 0 else 9999
        if cov >= min_coverage_days:
            continue
        severity = 'critical' if cov < 3 else 'warning'
        out.append(
            InsightCreate(
                rule_code='INV_LOW_COVERAGE',
                category='inventory',
                severity=severity,
                title='Κίνδυνος Έλλειψης',
                message=f'Top προϊόντα έχουν κάλυψη μικρότερη από {min_coverage_days:.2f} ημέρες.',
                entity_type='item',
                entity_external_id=item,
                entity_name=names.get(item, item),
                period_from=sales_from,
                period_to=ctx.as_of,
                value=cov,
                baseline_value=min_coverage_days,
                delta_value=cov - min_coverage_days,
                delta_pct=((cov - min_coverage_days) / min_coverage_days * 100.0) if min_coverage_days > 0 else None,
                metadata_json=_meta(
                    why='Χαμηλή κάλυψη σε top sellers δημιουργεί απώλεια πωλήσεων.',
                    formula='coverage_days = stock_qty / avg_daily_sales; trigger if < min_coverage_days',
                    drilldown=f'/tenant/inventory?item={item}&view=coverage',
                    actions=['Άμεση αναπλήρωση για top sellers.', 'Έλεγχος lead time προμηθευτή.'],
                    extras={'item_label': names.get(item, item)},
                ),
            )
        )
    return out


async def run_overstock_slow_movers(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    min_value = float(params.get('min_value', 1000))
    slow_qty = float(params.get('slow_qty', 2))
    window_days = int(params.get('window_days', 30))

    snapshot = await _latest_snapshot_date(db, ctx.as_of)
    if snapshot is None:
        return []
    sales_from = ctx.as_of - timedelta(days=max(1, window_days) - 1)

    sales_rows = (
        await db.execute(
            select(AggSalesItemDaily.item_external_id, func.coalesce(func.sum(AggSalesItemDaily.qty), 0))
            .where(AggSalesItemDaily.doc_date >= sales_from, AggSalesItemDaily.doc_date <= ctx.as_of)
            .group_by(AggSalesItemDaily.item_external_id)
        )
    ).all()
    slow_ids = [str(r[0]) for r in sales_rows if float(r[1] or 0) <= slow_qty]
    if not slow_ids:
        return []
    value = (
        await db.execute(
            select(func.coalesce(func.sum(AggInventorySnapshotDaily.value_amount), 0)).where(
                AggInventorySnapshotDaily.snapshot_date == snapshot,
                AggInventorySnapshotDaily.item_external_id.in_(slow_ids),
            )
        )
    ).scalar_one()
    total = float(value or 0)
    if total < min_value:
        return []
    severity = 'critical' if total >= 2 * min_value else 'warning'
    return [
        InsightCreate(
            rule_code='INV_OVERSTOCK_SLOW',
            category='inventory',
            severity=severity,
            title='Υπερβολικό Απόθεμα Αργών Προϊόντων',
            message=f'Αξία {total:.2f}€ σε προϊόντα με χαμηλή κίνηση.',
            entity_type='tenant',
            entity_external_id=ctx.tenant_slug,
            entity_name=ctx.tenant_slug,
            period_from=sales_from,
            period_to=ctx.as_of,
            value=total,
            baseline_value=min_value,
            delta_value=total - min_value,
            delta_pct=((total - min_value) / min_value * 100.0) if min_value > 0 else None,
            metadata_json=_meta(
                why='Υπερβολικό stock σε αργά προϊόντα μειώνει κεφαλαιακή απόδοση.',
                formula='slow_movers_value > min_value AND low turnover',
                drilldown='/tenant/inventory?view=slow-movers',
                actions=['Μείωση αγορών σε slow movers.', 'Στοχευμένες εκπτώσεις για αποδέσμευση stock.'],
            ),
        )
    ]
