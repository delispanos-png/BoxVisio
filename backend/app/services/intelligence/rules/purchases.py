from __future__ import annotations

from datetime import timedelta
from statistics import mean, pstdev

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tenant import AggPurchasesDaily, AggSalesDaily, DimSupplier, FactSupplierBalance
from app.services.intelligence.types import InsightCreate, RuleContext


def _pct_change(current: float, previous: float) -> float | None:
    if previous <= 0:
        return None
    return ((current - previous) / previous) * 100.0


def _margin_pct(net: float, gross: float) -> float | None:
    if net <= 0:
        return None
    return ((gross - net) / net) * 100.0


def _meta(*, why: str, formula: str, drilldown: str, actions: list[str]) -> dict:
    return {
        'why': why,
        'formula': formula,
        'drilldown': drilldown,
        'actions': [x for x in actions if x],
    }


async def run_purchases_spike_period(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    increase_pct = float(params.get('increase_pct', 15))
    cur = (
        await db.execute(select(func.coalesce(func.sum(AggPurchasesDaily.net_value), 0)).where(AggPurchasesDaily.doc_date >= ctx.period_from, AggPurchasesDaily.doc_date <= ctx.period_to))
    ).scalar_one()
    prev = (
        await db.execute(select(func.coalesce(func.sum(AggPurchasesDaily.net_value), 0)).where(AggPurchasesDaily.doc_date >= ctx.previous_from, AggPurchasesDaily.doc_date <= ctx.previous_to))
    ).scalar_one()
    value = float(cur or 0)
    baseline = float(prev or 0)
    delta_pct = _pct_change(value, baseline)
    if delta_pct is None or delta_pct < increase_pct:
        return []
    severity = 'critical' if delta_pct >= 30 else 'warning'
    return [
        InsightCreate(
            rule_code='PUR_SPIKE_PERIOD',
            category='purchases',
            severity=severity,
            title='Αύξηση Αγορών',
            message=f'Οι αγορές αυξήθηκαν κατά {delta_pct:.2f}% σε σχέση με την προηγούμενη περίοδο.',
            entity_type='tenant',
            entity_external_id=ctx.tenant_slug,
            entity_name=ctx.tenant_slug,
            period_from=ctx.period_from,
            period_to=ctx.period_to,
            value=value,
            baseline_value=baseline,
            delta_value=value - baseline,
            delta_pct=delta_pct,
            metadata_json=_meta(
                why='Η απότομη αύξηση αγορών επηρεάζει ρευστότητα και stock mix.',
                formula='purchases_A > purchases_prev_A * (1+increase_pct)',
                drilldown='/tenant/purchases',
                actions=['Έλεγξε αν η αύξηση συνδέεται με εποχικότητα ή έκτακτη αναπλήρωση.'],
            ),
        )
    ]


async def run_purchases_drop_period(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    drop_pct = float(params.get('drop_pct', 20))
    cur = (
        await db.execute(select(func.coalesce(func.sum(AggPurchasesDaily.net_value), 0)).where(AggPurchasesDaily.doc_date >= ctx.period_from, AggPurchasesDaily.doc_date <= ctx.period_to))
    ).scalar_one()
    prev = (
        await db.execute(select(func.coalesce(func.sum(AggPurchasesDaily.net_value), 0)).where(AggPurchasesDaily.doc_date >= ctx.previous_from, AggPurchasesDaily.doc_date <= ctx.previous_to))
    ).scalar_one()
    value = float(cur or 0)
    baseline = float(prev or 0)
    delta_pct = _pct_change(value, baseline)
    if delta_pct is None or delta_pct > -drop_pct:
        return []
    severity = 'warning' if delta_pct <= -30 else 'info'
    return [
        InsightCreate(
            rule_code='PUR_DROP_PERIOD',
            category='purchases',
            severity=severity,
            title='Μείωση Αγορών',
            message=f'Οι αγορές μειώθηκαν κατά {abs(delta_pct):.2f}%.',
            entity_type='tenant',
            entity_external_id=ctx.tenant_slug,
            entity_name=ctx.tenant_slug,
            period_from=ctx.period_from,
            period_to=ctx.period_to,
            value=value,
            baseline_value=baseline,
            delta_value=value - baseline,
            delta_pct=delta_pct,
            metadata_json=_meta(
                why='Η έντονη μείωση αγορών μπορεί να αυξήσει τον κίνδυνο ελλείψεων.',
                formula='purchases_A < purchases_prev_A * (1-drop_pct)',
                drilldown='/tenant/purchases/trend',
                actions=['Έλεγξε top πωλήσεις για πιθανό out-of-stock risk.'],
            ),
        )
    ]


async def run_supplier_dependency(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    top_supplier_share_pct = float(params.get('top_supplier_share_pct', 40))
    rows = (
        await db.execute(
            select(AggPurchasesDaily.supplier_ext_id, func.coalesce(func.sum(AggPurchasesDaily.net_value), 0))
            .where(AggPurchasesDaily.doc_date >= ctx.period_from, AggPurchasesDaily.doc_date <= ctx.period_to)
            .group_by(AggPurchasesDaily.supplier_ext_id)
            .order_by(func.sum(AggPurchasesDaily.net_value).desc())
        )
    ).all()
    total = sum(float(r[1] or 0) for r in rows)
    if total <= 0 or not rows:
        return []
    supplier = str(rows[0][0] or 'N/A')
    value = float(rows[0][1] or 0)
    share = (value / total) * 100.0
    if share < top_supplier_share_pct:
        return []
    severity = 'critical' if share > 55 else 'warning'
    name_rows = (await db.execute(select(DimSupplier.external_id, DimSupplier.name).where(DimSupplier.external_id == supplier))).all()
    supplier_name = str(name_rows[0][1]) if name_rows else supplier
    return [
        InsightCreate(
            rule_code='SUP_DEPENDENCY',
            category='purchases',
            severity=severity,
            title='Υψηλή Εξάρτηση από Προμηθευτή',
            message=f'Ο προμηθευτής {supplier_name} καλύπτει το {share:.2f}% των αγορών.',
            entity_type='supplier',
            entity_external_id=supplier,
            entity_name=supplier_name,
            period_from=ctx.period_from,
            period_to=ctx.period_to,
            value=share,
            baseline_value=top_supplier_share_pct,
            delta_value=share - top_supplier_share_pct,
            delta_pct=share,
            metadata_json=_meta(
                why='Υψηλή συγκέντρωση σε έναν προμηθευτή αυξάνει τον λειτουργικό κίνδυνο.',
                formula='top_supplier_share > top_supplier_share_pct',
                drilldown=f'/tenant/purchases?supplier={supplier}',
                actions=['Διαφοροποίησε προμηθευτές στα κρίσιμα προϊόντα.'],
            ),
        )
    ]


async def run_supplier_cost_increase(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    increase_pct = float(params.get('increase_pct', 15))
    min_baseline = float(params.get('min_baseline', 500))
    cur_rows = (
        await db.execute(
            select(AggPurchasesDaily.supplier_ext_id, func.coalesce(func.sum(AggPurchasesDaily.cost_amount), 0))
            .where(AggPurchasesDaily.doc_date >= ctx.period_from, AggPurchasesDaily.doc_date <= ctx.period_to)
            .group_by(AggPurchasesDaily.supplier_ext_id)
        )
    ).all()
    prev_rows = (
        await db.execute(
            select(AggPurchasesDaily.supplier_ext_id, func.coalesce(func.sum(AggPurchasesDaily.cost_amount), 0))
            .where(AggPurchasesDaily.doc_date >= ctx.previous_from, AggPurchasesDaily.doc_date <= ctx.previous_to)
            .group_by(AggPurchasesDaily.supplier_ext_id)
        )
    ).all()
    prev_map = {str(r[0] or 'N/A'): float(r[1] or 0) for r in prev_rows}
    name_rows = (await db.execute(select(DimSupplier.external_id, DimSupplier.name))).all()
    name_map = {str(r[0]): str(r[1]) for r in name_rows}
    out: list[InsightCreate] = []
    for row in cur_rows:
        supplier = str(row[0] or 'N/A')
        value = float(row[1] or 0)
        baseline = prev_map.get(supplier, 0.0)
        if baseline < min_baseline:
            continue
        delta_pct = _pct_change(value, baseline)
        if delta_pct is None or delta_pct < increase_pct:
            continue
        severity = 'critical' if delta_pct >= 30 else 'warning'
        out.append(
            InsightCreate(
                rule_code='SUP_COST_UP',
                category='purchases',
                severity=severity,
                title='Αύξηση Κόστους Προμηθευτή',
                message=f'Ο προμηθευτής {name_map.get(supplier, supplier)} αύξησε κόστος κατά {delta_pct:.2f}%.',
                entity_type='supplier',
                entity_external_id=supplier,
                entity_name=name_map.get(supplier, supplier),
                period_from=ctx.period_from,
                period_to=ctx.period_to,
                value=value,
                baseline_value=baseline,
                delta_value=value - baseline,
                delta_pct=delta_pct,
                metadata_json=_meta(
                    why='Η αύξηση κόστους προμηθευτή συμπιέζει το περιθώριο κέρδους.',
                    formula='supplier_cost_delta_pct > increase_pct with min_baseline',
                    drilldown=f'/tenant/purchases?supplier={supplier}&metric=cost',
                    actions=['Έλεγξε ανατιμήσεις και renegotiation όρους.'],
                ),
            )
        )
    return out


async def run_supplier_price_volatility(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    window_days = int(params.get('window_days', 30))
    volatility_threshold = float(params.get('volatility_threshold', 0.3))
    min_daily_mean = float(params.get('min_daily_mean', 20))
    rows = (
        await db.execute(
            select(AggPurchasesDaily.supplier_ext_id, AggPurchasesDaily.doc_date, func.coalesce(func.sum(AggPurchasesDaily.cost_amount), 0))
            .where(AggPurchasesDaily.doc_date >= ctx.period_to - timedelta(days=max(1, window_days) - 1), AggPurchasesDaily.doc_date <= ctx.period_to)
            .group_by(AggPurchasesDaily.supplier_ext_id, AggPurchasesDaily.doc_date)
        )
    ).all()
    by_supplier: dict[str, list[float]] = {}
    for r in rows:
        sup = str(r[0] or 'N/A')
        by_supplier.setdefault(sup, []).append(float(r[2] or 0))
    name_rows = (await db.execute(select(DimSupplier.external_id, DimSupplier.name))).all()
    name_map = {str(r[0]): str(r[1]) for r in name_rows}
    out: list[InsightCreate] = []
    for supplier, vals in by_supplier.items():
        if len(vals) < 7:
            continue
        avg = mean(vals)
        if avg < min_daily_mean:
            continue
        cv = pstdev(vals) / avg if avg > 0 and len(vals) > 1 else 0.0
        if cv <= volatility_threshold:
            continue
        severity = 'critical' if cv >= 0.5 else 'warning'
        out.append(
            InsightCreate(
                rule_code='SUP_VOLATILITY',
                category='purchases',
                severity=severity,
                title='Αστάθεια Τιμών Προμηθευτή',
                message='Μεγάλη διακύμανση κόστους τον τελευταίο μήνα.',
                entity_type='supplier',
                entity_external_id=supplier,
                entity_name=name_map.get(supplier, supplier),
                period_from=ctx.period_to - timedelta(days=max(1, window_days) - 1),
                period_to=ctx.period_to,
                value=cv,
                baseline_value=volatility_threshold,
                delta_value=cv - volatility_threshold,
                delta_pct=None,
                metadata_json=_meta(
                    why='Αστάθεια τιμών δυσκολεύει pricing και προβλέψεις περιθωρίου.',
                    formula='rolling_cv_30d > volatility_threshold',
                    drilldown=f'/tenant/purchases?supplier={supplier}&metric=volatility',
                    actions=['Ενίσχυσε monitoring τιμών και εναλλακτικούς προμηθευτές.'],
                ),
            )
        )
    return out


async def run_purchases_margin_pressure(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    purchases_up_pct = float(params.get('purchases_up_pct', 10))
    margin_down_points = float(params.get('margin_down_points', 1.5))

    cur_pur = (
        await db.execute(select(func.coalesce(func.sum(AggPurchasesDaily.net_value), 0)).where(AggPurchasesDaily.doc_date >= ctx.period_from, AggPurchasesDaily.doc_date <= ctx.period_to))
    ).scalar_one()
    prev_pur = (
        await db.execute(select(func.coalesce(func.sum(AggPurchasesDaily.net_value), 0)).where(AggPurchasesDaily.doc_date >= ctx.previous_from, AggPurchasesDaily.doc_date <= ctx.previous_to))
    ).scalar_one()
    purchases_delta = _pct_change(float(cur_pur or 0), float(prev_pur or 0))

    cur_sales = (
        await db.execute(select(func.coalesce(func.sum(AggSalesDaily.net_value), 0), func.coalesce(func.sum(AggSalesDaily.gross_value), 0)).where(AggSalesDaily.doc_date >= ctx.period_from, AggSalesDaily.doc_date <= ctx.period_to))
    ).one()
    prev_sales = (
        await db.execute(select(func.coalesce(func.sum(AggSalesDaily.net_value), 0), func.coalesce(func.sum(AggSalesDaily.gross_value), 0)).where(AggSalesDaily.doc_date >= ctx.previous_from, AggSalesDaily.doc_date <= ctx.previous_to))
    ).one()
    cur_margin = _margin_pct(float(cur_sales[0] or 0), float(cur_sales[1] or 0))
    prev_margin = _margin_pct(float(prev_sales[0] or 0), float(prev_sales[1] or 0))
    if purchases_delta is None or cur_margin is None or prev_margin is None:
        return []
    margin_delta = cur_margin - prev_margin
    if purchases_delta < purchases_up_pct or margin_delta > -margin_down_points:
        return []
    severity = 'critical' if purchases_delta >= 20 and margin_delta <= -3 else 'warning'
    return [
        InsightCreate(
            rule_code='PUR_MARGIN_PRESSURE',
            category='purchases',
            severity=severity,
            title='Πίεση Περιθωρίου λόγω Κόστους',
            message='Αύξηση αγορών + πτώση margin.',
            entity_type='tenant',
            entity_external_id=ctx.tenant_slug,
            entity_name=ctx.tenant_slug,
            period_from=ctx.period_from,
            period_to=ctx.period_to,
            value=purchases_delta,
            baseline_value=margin_delta,
            delta_value=margin_delta,
            delta_pct=purchases_delta,
            metadata_json=_meta(
                why='Ο συνδυασμός αυξημένου κόστους και πτώσης margin απαιτεί άμεση παρέμβαση.',
                formula='purchases_delta >= purchases_up_pct AND margin_delta <= -margin_down_points',
                drilldown='/tenant/purchases/compare',
                actions=['Έλεγξε κατηγορίες με μεγαλύτερη επιβάρυνση κόστους.'],
            ),
        )
    ]


async def run_supplier_overdue_exposure(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    min_open_balance = float(params.get('min_open_balance', 1000))
    overdue_ratio_threshold = float(params.get('overdue_ratio_threshold', 0.35))
    min_overdue_delta = float(params.get('min_overdue_delta', 200))

    rows = (
        await db.execute(
            select(
                FactSupplierBalance.supplier_ext_id,
                FactSupplierBalance.balance_date,
                func.coalesce(func.sum(FactSupplierBalance.open_balance), 0).label('open_balance'),
                func.coalesce(func.sum(FactSupplierBalance.overdue_balance), 0).label('overdue_balance'),
            )
            .where(FactSupplierBalance.balance_date <= ctx.period_to)
            .group_by(FactSupplierBalance.supplier_ext_id, FactSupplierBalance.balance_date)
            .order_by(FactSupplierBalance.supplier_ext_id.asc(), FactSupplierBalance.balance_date.desc())
        )
    ).all()
    if not rows:
        return []

    series: dict[str, list[tuple]] = {}
    for row in rows:
        supplier = str(row[0] or '').strip()
        if not supplier:
            continue
        series.setdefault(supplier, []).append((row[1], float(row[2] or 0), float(row[3] or 0)))

    if not series:
        return []

    supplier_rows = (await db.execute(select(DimSupplier.external_id, DimSupplier.name))).all()
    supplier_name = {str(r[0] or ''): str(r[1] or '') for r in supplier_rows}

    out: list[InsightCreate] = []
    for supplier, snapshots in series.items():
        latest_date, open_balance, overdue_balance = snapshots[0]
        if open_balance < min_open_balance:
            continue

        overdue_ratio = (overdue_balance / open_balance) if open_balance > 0 else 0.0
        prev_overdue = snapshots[1][2] if len(snapshots) > 1 else 0.0
        overdue_delta = overdue_balance - prev_overdue

        if overdue_ratio < overdue_ratio_threshold and overdue_delta < min_overdue_delta:
            continue

        severity = 'critical' if overdue_ratio >= max(0.6, overdue_ratio_threshold + 0.2) else 'warning'
        out.append(
            InsightCreate(
                rule_code='SUP_OVERDUE_EXPOSURE',
                category='purchases',
                severity=severity,
                title='Έκθεση Ληξιπρόθεσμου Υπολοίπου',
                message=(
                    f'Ο προμηθευτής {supplier_name.get(supplier, supplier)} έχει ληξιπρόθεσμο υπόλοιπο '
                    f'{overdue_balance:.2f} ({overdue_ratio * 100:.1f}% του ανοικτού).'
                ),
                entity_type='supplier',
                entity_external_id=supplier,
                entity_name=supplier_name.get(supplier, supplier) or supplier,
                period_from=latest_date,
                period_to=latest_date,
                value=overdue_balance,
                baseline_value=open_balance,
                delta_value=overdue_delta,
                delta_pct=overdue_ratio * 100.0,
                metadata_json=_meta(
                    why='Αύξηση ληξιπρόθεσμου υπολοίπου αυξάνει λειτουργικό και πιστωτικό κίνδυνο.',
                    formula='overdue/open >= overdue_ratio_threshold OR overdue_delta >= min_overdue_delta',
                    drilldown='/tenant/suppliers',
                    actions=['Επανέλεγχος όρων πληρωμής και προγραμματισμός διακανονισμού.'],
                ),
            )
        )
    return out
