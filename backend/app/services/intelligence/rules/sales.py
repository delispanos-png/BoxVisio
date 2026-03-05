from __future__ import annotations

from datetime import timedelta
from statistics import mean, pstdev

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tenant import AggSalesDaily, AggSalesItemDaily, DimBranch, DimBrand, DimCategory
from app.services.intelligence.types import InsightCreate, RuleContext


def _pct_change(current: float, previous: float) -> float | None:
    if previous <= 0:
        return None
    return ((current - previous) / previous) * 100.0


def _margin_pct(net: float, gross: float) -> float | None:
    if net <= 0:
        return None
    return ((gross - net) / net) * 100.0


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


async def run_sales_drop_period(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    drop_pct = float(params.get('drop_pct', 10))
    cur = (
        await db.execute(
            select(func.coalesce(func.sum(AggSalesDaily.net_value), 0)).where(
                AggSalesDaily.doc_date >= ctx.period_from,
                AggSalesDaily.doc_date <= ctx.period_to,
            )
        )
    ).scalar_one()
    prev = (
        await db.execute(
            select(func.coalesce(func.sum(AggSalesDaily.net_value), 0)).where(
                AggSalesDaily.doc_date >= ctx.previous_from,
                AggSalesDaily.doc_date <= ctx.previous_to,
            )
        )
    ).scalar_one()
    value = float(cur or 0)
    baseline = float(prev or 0)
    delta_pct = _pct_change(value, baseline)
    if delta_pct is None or delta_pct > -drop_pct:
        return []
    severity = 'critical' if delta_pct <= -20 else 'warning'
    return [
        InsightCreate(
            rule_code='SLS_DROP_PERIOD',
            category='sales',
            severity=severity,
            title='Πτώση τζίρου',
            message=f'Ο τζίρος έπεσε {abs(delta_pct):.2f}% ({baseline:.2f}->{value:.2f}) σε σχέση με την προηγούμενη περίοδο.',
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
                why='Η μείωση τζίρου μπορεί να επηρεάσει άμεσα ρευστότητα και κερδοφορία.',
                formula='turnover_A < turnover_prev_A * (1 - drop_pct)',
                drilldown='/tenant/sales',
                actions=[
                    'Έλεγξε top κατηγορίες.',
                    'Έλεγξε branch performance.',
                    'Δες αν μειώθηκαν οι μέσες τιμές ή ο όγκος πωλήσεων.',
                ],
            ),
        )
    ]


async def run_sales_spike_period(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    increase_pct = float(params.get('increase_pct', 15))
    cur = (
        await db.execute(
            select(func.coalesce(func.sum(AggSalesDaily.net_value), 0)).where(
                AggSalesDaily.doc_date >= ctx.period_from,
                AggSalesDaily.doc_date <= ctx.period_to,
            )
        )
    ).scalar_one()
    prev = (
        await db.execute(
            select(func.coalesce(func.sum(AggSalesDaily.net_value), 0)).where(
                AggSalesDaily.doc_date >= ctx.previous_from,
                AggSalesDaily.doc_date <= ctx.previous_to,
            )
        )
    ).scalar_one()
    value = float(cur or 0)
    baseline = float(prev or 0)
    delta_pct = _pct_change(value, baseline)
    if delta_pct is None or delta_pct < increase_pct:
        return []
    severity = 'warning' if delta_pct >= 25 else 'info'
    return [
        InsightCreate(
            rule_code='SLS_SPIKE_PERIOD',
            category='sales',
            severity=severity,
            title='Απότομη αύξηση τζίρου',
            message=f'Απότομη αύξηση τζίρου {delta_pct:.2f}% ({baseline:.2f}->{value:.2f}).',
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
                why='Η έντονη αύξηση μπορεί να κρύβει ευκαιρία αλλά και κίνδυνο stock-out.',
                formula='turnover_A > turnover_prev_A * (1 + increase_pct)',
                drilldown='/tenant/sales/compare',
                actions=[
                    'Εντόπισε ποια προϊόντα οδήγησαν την αύξηση.',
                    'Εξασφάλισε επάρκεια αποθέματος.',
                ],
            ),
        )
    ]


async def run_profit_drop_period(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    drop_pct = float(params.get('drop_pct', 12))
    cur = (
        await db.execute(
            select(
                func.coalesce(func.sum(AggSalesDaily.net_value), 0),
                func.coalesce(func.sum(AggSalesDaily.gross_value), 0),
            ).where(AggSalesDaily.doc_date >= ctx.period_from, AggSalesDaily.doc_date <= ctx.period_to)
        )
    ).one()
    prev = (
        await db.execute(
            select(
                func.coalesce(func.sum(AggSalesDaily.net_value), 0),
                func.coalesce(func.sum(AggSalesDaily.gross_value), 0),
            ).where(AggSalesDaily.doc_date >= ctx.previous_from, AggSalesDaily.doc_date <= ctx.previous_to)
        )
    ).one()
    value = float(cur[1] or 0) - float(cur[0] or 0)
    baseline = float(prev[1] or 0) - float(prev[0] or 0)
    delta_pct = _pct_change(value, baseline)
    if delta_pct is None or delta_pct > -drop_pct:
        return []
    severity = 'critical' if delta_pct <= -20 else 'warning'
    return [
        InsightCreate(
            rule_code='PRF_DROP_PERIOD',
            category='sales',
            severity=severity,
            title='Πτώση Κερδοφορίας',
            message=f'Τα μικτά κέρδη μειώθηκαν κατά {abs(delta_pct):.2f}%.',
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
                why='Η πτώση κερδοφορίας μειώνει την ανθεκτικότητα του φαρμακείου.',
                formula='profit_A = sum(gross)-sum(net), compare vs previous period',
                drilldown='/tenant/sales?metric=profit',
                actions=[
                    'Έλεγξε cost vs selling price.',
                    'Εντόπισε κατηγορίες με margin erosion.',
                ],
            ),
        )
    ]


async def run_margin_drop_points(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    drop_points = float(params.get('drop_points', 2.0))
    cur = (
        await db.execute(
            select(
                func.coalesce(func.sum(AggSalesDaily.net_value), 0),
                func.coalesce(func.sum(AggSalesDaily.gross_value), 0),
            ).where(AggSalesDaily.doc_date >= ctx.period_from, AggSalesDaily.doc_date <= ctx.period_to)
        )
    ).one()
    prev = (
        await db.execute(
            select(
                func.coalesce(func.sum(AggSalesDaily.net_value), 0),
                func.coalesce(func.sum(AggSalesDaily.gross_value), 0),
            ).where(AggSalesDaily.doc_date >= ctx.previous_from, AggSalesDaily.doc_date <= ctx.previous_to)
        )
    ).one()
    value = _margin_pct(float(cur[0] or 0), float(cur[1] or 0))
    baseline = _margin_pct(float(prev[0] or 0), float(prev[1] or 0))
    if value is None or baseline is None:
        return []
    delta = value - baseline
    if delta > -drop_points:
        return []
    severity = 'critical' if delta <= -4 else 'warning'
    return [
        InsightCreate(
            rule_code='MRG_DROP_POINTS',
            category='sales',
            severity=severity,
            title='Μείωση Περιθωρίου Κέρδους',
            message=f'Το περιθώριο μειώθηκε από {baseline:.2f}% σε {value:.2f}% (-{abs(delta):.2f} μονάδες).',
            entity_type='tenant',
            entity_external_id=ctx.tenant_slug,
            entity_name=ctx.tenant_slug,
            period_from=ctx.period_from,
            period_to=ctx.period_to,
            value=value,
            baseline_value=baseline,
            delta_value=delta,
            delta_pct=delta,
            metadata_json=_meta(
                why='Μικρές μεταβολές margin έχουν μεγάλο αντίκτυπο στην τελική κερδοφορία.',
                formula='margin_pct = (gross-net)/net*100, trigger on drop_points',
                drilldown='/tenant/sales/compare',
                actions=[
                    'Έλεγξε αγορές τελευταίων ημερών.',
                    'Επανέλεγξε τιμολογιακή πολιτική.',
                ],
            ),
        )
    ]


async def run_branch_underperform(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    under_pct = float(params.get('under_pct', 15))
    rows = (
        await db.execute(
            select(AggSalesDaily.branch_ext_id, func.coalesce(func.sum(AggSalesDaily.net_value), 0))
            .where(AggSalesDaily.doc_date >= ctx.period_from, AggSalesDaily.doc_date <= ctx.period_to)
            .group_by(AggSalesDaily.branch_ext_id)
        )
    ).all()
    if not rows:
        return []
    vals = [float(r[1] or 0) for r in rows]
    avg = sum(vals) / len(vals) if vals else 0.0
    if avg <= 0:
        return []
    name_rows = (await db.execute(select(DimBranch.external_id, DimBranch.name))).all()
    name_map = {str(r[0]): str(r[1]) for r in name_rows}
    out: list[InsightCreate] = []
    for row in rows:
        branch = str(row[0] or 'N/A')
        value = float(row[1] or 0)
        delta_pct = ((value - avg) / avg) * 100.0
        if delta_pct > -under_pct:
            continue
        severity = 'critical' if delta_pct <= -30 else 'warning'
        out.append(
            InsightCreate(
                rule_code='BR_UNDERPERFORM',
                category='sales',
                severity=severity,
                title='Υποαπόδοση Καταστήματος',
                message=f'Το κατάστημα {name_map.get(branch, branch)} είναι {abs(delta_pct):.2f}% κάτω από τον μέσο όρο.',
                entity_type='branch',
                entity_external_id=branch,
                entity_name=name_map.get(branch, branch),
                period_from=ctx.period_from,
                period_to=ctx.period_to,
                value=value,
                baseline_value=avg,
                delta_value=value - avg,
                delta_pct=delta_pct,
                metadata_json=_meta(
                    why='Η υστέρηση καταστήματος μειώνει τη συνολική απόδοση δικτύου.',
                    formula='branch_turnover < avg_branch_turnover*(1-under_pct)',
                    drilldown=f'/tenant/sales?branch={branch}',
                    actions=[
                        'Σύγκρινε κατηγορίες.',
                        'Έλεγξε τοπικές προωθήσεις.',
                    ],
                ),
            )
        )
    return out


async def run_branch_margin_low(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    gap_points = float(params.get('gap_points', 2.0))
    tenant = (
        await db.execute(
            select(func.coalesce(func.sum(AggSalesDaily.net_value), 0), func.coalesce(func.sum(AggSalesDaily.gross_value), 0)).where(
                AggSalesDaily.doc_date >= ctx.period_from,
                AggSalesDaily.doc_date <= ctx.period_to,
            )
        )
    ).one()
    tenant_margin = _margin_pct(float(tenant[0] or 0), float(tenant[1] or 0))
    if tenant_margin is None:
        return []
    rows = (
        await db.execute(
            select(
                AggSalesDaily.branch_ext_id,
                func.coalesce(func.sum(AggSalesDaily.net_value), 0),
                func.coalesce(func.sum(AggSalesDaily.gross_value), 0),
            )
            .where(AggSalesDaily.doc_date >= ctx.period_from, AggSalesDaily.doc_date <= ctx.period_to)
            .group_by(AggSalesDaily.branch_ext_id)
        )
    ).all()
    name_rows = (await db.execute(select(DimBranch.external_id, DimBranch.name))).all()
    name_map = {str(r[0]): str(r[1]) for r in name_rows}
    out: list[InsightCreate] = []
    for row in rows:
        branch = str(row[0] or 'N/A')
        branch_margin = _margin_pct(float(row[1] or 0), float(row[2] or 0))
        if branch_margin is None:
            continue
        gap = branch_margin - tenant_margin
        if gap > -gap_points:
            continue
        severity = 'critical' if gap <= -4 else 'warning'
        out.append(
            InsightCreate(
                rule_code='BR_MARGIN_LOW',
                category='sales',
                severity=severity,
                title='Χαμηλό Περιθώριο σε Κατάστημα',
                message=f'Το περιθώριο του {name_map.get(branch, branch)} είναι χαμηλότερο κατά {abs(gap):.2f} μονάδες.',
                entity_type='branch',
                entity_external_id=branch,
                entity_name=name_map.get(branch, branch),
                period_from=ctx.period_from,
                period_to=ctx.period_to,
                value=branch_margin,
                baseline_value=tenant_margin,
                delta_value=gap,
                delta_pct=gap,
                metadata_json=_meta(
                    why='Χαμηλό margin σε κατάστημα υποδεικνύει πρόβλημα μίγματος ή τιμολόγησης.',
                    formula='branch_margin < tenant_margin - gap_points',
                    drilldown=f'/tenant/sales?branch={branch}&metric=margin',
                    actions=[
                        'Έλεγξε προϊόντα με υψηλές εκπτώσεις.',
                        'Σύγκρινε προμηθευτικό κόστος ανά κατάστημα.',
                    ],
                ),
            )
        )
    return out


async def run_category_drop(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    drop_pct = float(params.get('drop_pct', 12))
    min_baseline = float(params.get('min_baseline', 500))
    cur_rows = (
        await db.execute(
            select(AggSalesDaily.category_ext_id, func.coalesce(func.sum(AggSalesDaily.net_value), 0))
            .where(AggSalesDaily.doc_date >= ctx.period_from, AggSalesDaily.doc_date <= ctx.period_to)
            .group_by(AggSalesDaily.category_ext_id)
        )
    ).all()
    prev_rows = (
        await db.execute(
            select(AggSalesDaily.category_ext_id, func.coalesce(func.sum(AggSalesDaily.net_value), 0))
            .where(AggSalesDaily.doc_date >= ctx.previous_from, AggSalesDaily.doc_date <= ctx.previous_to)
            .group_by(AggSalesDaily.category_ext_id)
        )
    ).all()
    prev_map = {str(r[0] or 'N/A'): float(r[1] or 0) for r in prev_rows}
    name_rows = (await db.execute(select(DimCategory.external_id, DimCategory.name))).all()
    name_map = {str(r[0]): str(r[1]) for r in name_rows}
    out: list[InsightCreate] = []
    for row in cur_rows:
        cat = str(row[0] or 'N/A')
        value = float(row[1] or 0)
        baseline = prev_map.get(cat, 0.0)
        if baseline < min_baseline:
            continue
        delta_pct = _pct_change(value, baseline)
        if delta_pct is None or delta_pct > -drop_pct:
            continue
        severity = 'critical' if delta_pct <= -25 else 'warning'
        out.append(
            InsightCreate(
                rule_code='CAT_DROP',
                category='sales',
                severity=severity,
                title='Πτώση Κατηγορίας',
                message=f'Η κατηγορία {name_map.get(cat, cat)} έπεσε {abs(delta_pct):.2f}% ({baseline:.2f}->{value:.2f}).',
                entity_type='category',
                entity_external_id=cat,
                entity_name=name_map.get(cat, cat),
                period_from=ctx.period_from,
                period_to=ctx.period_to,
                value=value,
                baseline_value=baseline,
                delta_value=value - baseline,
                delta_pct=delta_pct,
                metadata_json=_meta(
                    why='Πτώση κατηγορίας επηρεάζει τζίρο και εμπορική θέση.',
                    formula='category_turnover drop > drop_pct with baseline filter',
                    drilldown=f'/tenant/sales?category={cat}',
                    actions=[
                        'Έλεγξε top προϊόντα της κατηγορίας.',
                        'Επανέλεγξε pricing και προωθητικές ενέργειες.',
                    ],
                ),
            )
        )
    return out


async def run_category_margin_erosion(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    drop_points = float(params.get('drop_points', 2.0))
    cur_rows = (
        await db.execute(
            select(
                AggSalesDaily.category_ext_id,
                func.coalesce(func.sum(AggSalesDaily.net_value), 0),
                func.coalesce(func.sum(AggSalesDaily.gross_value), 0),
            )
            .where(AggSalesDaily.doc_date >= ctx.period_from, AggSalesDaily.doc_date <= ctx.period_to)
            .group_by(AggSalesDaily.category_ext_id)
        )
    ).all()
    prev_rows = (
        await db.execute(
            select(
                AggSalesDaily.category_ext_id,
                func.coalesce(func.sum(AggSalesDaily.net_value), 0),
                func.coalesce(func.sum(AggSalesDaily.gross_value), 0),
            )
            .where(AggSalesDaily.doc_date >= ctx.previous_from, AggSalesDaily.doc_date <= ctx.previous_to)
            .group_by(AggSalesDaily.category_ext_id)
        )
    ).all()
    prev_map = {str(r[0] or 'N/A'): (float(r[1] or 0), float(r[2] or 0)) for r in prev_rows}
    name_rows = (await db.execute(select(DimCategory.external_id, DimCategory.name))).all()
    name_map = {str(r[0]): str(r[1]) for r in name_rows}
    out: list[InsightCreate] = []
    for row in cur_rows:
        cat = str(row[0] or 'N/A')
        value = _margin_pct(float(row[1] or 0), float(row[2] or 0))
        prev_pair = prev_map.get(cat)
        baseline = _margin_pct(prev_pair[0], prev_pair[1]) if prev_pair else None
        if value is None or baseline is None:
            continue
        delta = value - baseline
        if delta > -drop_points:
            continue
        severity = 'critical' if delta <= -4 else 'warning'
        out.append(
            InsightCreate(
                rule_code='CAT_MARGIN_EROSION',
                category='sales',
                severity=severity,
                title='Διάβρωση Περιθωρίου Κατηγορίας',
                message=f'Το περιθώριο της κατηγορίας {name_map.get(cat, cat)} μειώθηκε από {baseline:.2f}% σε {value:.2f}%.',
                entity_type='category',
                entity_external_id=cat,
                entity_name=name_map.get(cat, cat),
                period_from=ctx.period_from,
                period_to=ctx.period_to,
                value=value,
                baseline_value=baseline,
                delta_value=delta,
                delta_pct=delta,
                metadata_json=_meta(
                    why='Η πτώση margin ανά κατηγορία μειώνει την πραγματική απόδοση πωλήσεων.',
                    formula='category margin points drop > drop_points',
                    drilldown=f'/tenant/sales?category={cat}&metric=margin',
                    actions=[
                        'Ανάλυσε μεταβολές κόστους στην κατηγορία.',
                        'Διόρθωσε τιμολογιακές αποκλίσεις.',
                    ],
                ),
            )
        )
    return out


async def run_brand_drop(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    drop_pct = float(params.get('drop_pct', 12))
    min_baseline = float(params.get('min_baseline', 300))
    cur_rows = (
        await db.execute(
            select(AggSalesDaily.brand_ext_id, func.coalesce(func.sum(AggSalesDaily.net_value), 0))
            .where(AggSalesDaily.doc_date >= ctx.period_from, AggSalesDaily.doc_date <= ctx.period_to)
            .group_by(AggSalesDaily.brand_ext_id)
        )
    ).all()
    prev_rows = (
        await db.execute(
            select(AggSalesDaily.brand_ext_id, func.coalesce(func.sum(AggSalesDaily.net_value), 0))
            .where(AggSalesDaily.doc_date >= ctx.previous_from, AggSalesDaily.doc_date <= ctx.previous_to)
            .group_by(AggSalesDaily.brand_ext_id)
        )
    ).all()
    prev_map = {str(r[0] or 'N/A'): float(r[1] or 0) for r in prev_rows}
    name_rows = (await db.execute(select(DimBrand.external_id, DimBrand.name))).all()
    name_map = {str(r[0]): str(r[1]) for r in name_rows}
    out: list[InsightCreate] = []
    for row in cur_rows:
        brand = str(row[0] or 'N/A')
        value = float(row[1] or 0)
        baseline = prev_map.get(brand, 0.0)
        if baseline < min_baseline:
            continue
        delta_pct = _pct_change(value, baseline)
        if delta_pct is None or delta_pct > -drop_pct:
            continue
        severity = 'critical' if delta_pct <= -25 else 'warning'
        out.append(
            InsightCreate(
                rule_code='BRAND_DROP',
                category='sales',
                severity=severity,
                title='Πτώση Brand',
                message=f'Το brand {name_map.get(brand, brand)} παρουσιάζει πτώση {abs(delta_pct):.2f}%.',
                entity_type='brand',
                entity_external_id=brand,
                entity_name=name_map.get(brand, brand),
                period_from=ctx.period_from,
                period_to=ctx.period_to,
                value=value,
                baseline_value=baseline,
                delta_value=value - baseline,
                delta_pct=delta_pct,
                metadata_json=_meta(
                    why='Η απώλεια απόδοσης σε brand επηρεάζει πιστότητα και μεικτό κέρδος.',
                    formula='brand turnover drop > drop_pct with min_baseline',
                    drilldown=f'/tenant/sales?brand={brand}',
                    actions=[
                        'Έλεγξε διαθεσιμότητα και τιμή brand.',
                        'Σύγκρινε με ανταγωνιστικά brands.',
                    ],
                ),
            )
        )
    return out


async def run_top_products_dependency(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    top_n = int(params.get('top_n', 10))
    share_pct = float(params.get('share_pct', 35))
    rows = (
        await db.execute(
            select(AggSalesItemDaily.item_external_id, func.coalesce(func.sum(AggSalesItemDaily.net_value), 0))
            .where(AggSalesItemDaily.doc_date >= ctx.period_from, AggSalesItemDaily.doc_date <= ctx.period_to)
            .group_by(AggSalesItemDaily.item_external_id)
            .order_by(func.sum(AggSalesItemDaily.net_value).desc())
        )
    ).all()
    total = sum(float(r[1] or 0) for r in rows)
    if total <= 0:
        return []
    top = rows[: max(1, top_n)]
    top_sum = sum(float(r[1] or 0) for r in top)
    share = (top_sum / total) * 100.0
    if share < share_pct:
        return []
    severity = 'critical' if share >= 50 else 'warning'
    return [
        InsightCreate(
            rule_code='TOP_DEPENDENCY',
            category='sales',
            severity=severity,
            title='Υψηλή Εξάρτηση από Top Προϊόντα',
            message=f'Υψηλή εξάρτηση: Top {max(1, top_n)} προϊόντα = {share:.2f}% του τζίρου.',
            entity_type='tenant',
            entity_external_id=ctx.tenant_slug,
            entity_name=ctx.tenant_slug,
            period_from=ctx.period_from,
            period_to=ctx.period_to,
            value=share,
            baseline_value=share_pct,
            delta_value=share - share_pct,
            delta_pct=share,
            metadata_json=_meta(
                why='Υψηλή συγκέντρωση αυξάνει επιχειρησιακό ρίσκο.',
                formula='sum(top_n products)/total_turnover > share_pct',
                drilldown='/tenant/sales?view=top-products',
                actions=['Διαφοροποίηση χαρτοφυλακίου.'],
                extras={'top_n': max(1, top_n)},
            ),
        )
    ]


async def run_sales_volatility_high(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    window_days = int(params.get('window_days', 14))
    volatility_threshold = float(params.get('volatility_threshold', 0.25))
    start = ctx.as_of - timedelta(days=max(1, window_days) - 1)
    rows = (
        await db.execute(
            select(AggSalesDaily.doc_date, func.coalesce(func.sum(AggSalesDaily.net_value), 0))
            .where(AggSalesDaily.doc_date >= start, AggSalesDaily.doc_date <= ctx.as_of)
            .group_by(AggSalesDaily.doc_date)
            .order_by(AggSalesDaily.doc_date.asc())
        )
    ).all()
    values = [float(r[1] or 0) for r in rows]
    if len(values) < 5:
        return []
    avg = mean(values)
    if avg <= 0:
        return []
    cv = pstdev(values) / avg if len(values) > 1 else 0.0
    if cv <= volatility_threshold:
        return []
    severity = 'warning' if cv >= 0.4 else 'info'
    return [
        InsightCreate(
            rule_code='SLS_VOLATILITY',
            category='sales',
            severity=severity,
            title='Υψηλή Μεταβλητότητα Πωλήσεων',
            message='Οι πωλήσεις παρουσιάζουν έντονη διακύμανση τις τελευταίες ημέρες.',
            entity_type='tenant',
            entity_external_id=ctx.tenant_slug,
            entity_name=ctx.tenant_slug,
            period_from=start,
            period_to=ctx.as_of,
            value=cv,
            baseline_value=volatility_threshold,
            delta_value=cv - volatility_threshold,
            delta_pct=None,
            metadata_json=_meta(
                why='Μεταβλητότητα δυσκολεύει forecasting και προγραμματισμό αγορών.',
                formula='stddev(daily_turnover)/mean(daily_turnover) > volatility_threshold',
                drilldown='/tenant/sales/trend',
                actions=[
                    'Έλεγξε ακραίες ημέρες πωλήσεων.',
                    'Προσαρμογή αποθεμάτων σε αιχμές/κοιλάδες.',
                ],
            ),
        )
    ]


async def run_weekend_shift(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    share_points = float(params.get('share_points', 10.0))
    weekends = int(params.get('weekends', 4))
    days = max(1, weekends * 7)
    cur_from = ctx.as_of - timedelta(days=days - 1)
    prev_to = cur_from - timedelta(days=1)
    prev_from = prev_to - timedelta(days=days - 1)

    cur_rows = (
        await db.execute(
            select(AggSalesDaily.doc_date, func.coalesce(func.sum(AggSalesDaily.net_value), 0))
            .where(AggSalesDaily.doc_date >= cur_from, AggSalesDaily.doc_date <= ctx.as_of)
            .group_by(AggSalesDaily.doc_date)
        )
    ).all()
    prev_rows = (
        await db.execute(
            select(AggSalesDaily.doc_date, func.coalesce(func.sum(AggSalesDaily.net_value), 0))
            .where(AggSalesDaily.doc_date >= prev_from, AggSalesDaily.doc_date <= prev_to)
            .group_by(AggSalesDaily.doc_date)
        )
    ).all()

    def _share(rows):
        total = sum(float(r[1] or 0) for r in rows)
        if total <= 0:
            return None
        weekend = sum(float(r[1] or 0) for r in rows if r[0] is not None and r[0].weekday() >= 5)
        return (weekend / total) * 100.0

    cur_share = _share(cur_rows)
    prev_share = _share(prev_rows)
    if cur_share is None or prev_share is None:
        return []
    delta = cur_share - prev_share
    if abs(delta) < share_points:
        return []
    severity = 'warning' if abs(delta) >= 15 else 'info'
    direction = 'αυξήθηκε' if delta > 0 else 'μειώθηκε'
    return [
        InsightCreate(
            rule_code='WEEKEND_SHIFT',
            category='sales',
            severity=severity,
            title='Αλλαγή Συμπεριφοράς Σαββατοκύριακου',
            message=f'Οι πωλήσεις ΣΚ μεταβλήθηκαν κατά {abs(delta):.2f} μονάδες ({prev_share:.2f}%->{cur_share:.2f}%).',
            entity_type='tenant',
            entity_external_id=ctx.tenant_slug,
            entity_name=ctx.tenant_slug,
            period_from=cur_from,
            period_to=ctx.as_of,
            value=cur_share,
            baseline_value=prev_share,
            delta_value=delta,
            delta_pct=delta,
            metadata_json=_meta(
                why='Η μετατόπιση weekend mix επηρεάζει staffing και promo planning.',
                formula='weekend_share change vs previous 4 weekends > share_points',
                drilldown='/tenant/sales/trend?segment=weekend',
                actions=[
                    'Προσαρμογή staffing σε ΣΚ.',
                    'Προσαρμογή προωθητικών ενεργειών.',
                ],
                extras={'direction': direction},
            ),
        )
    ]
