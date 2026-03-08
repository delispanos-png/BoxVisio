from __future__ import annotations

from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tenant import AggCashDaily
from app.services.intelligence.types import InsightCreate, RuleContext



def _meta(*, why: str, formula: str, drilldown: str, actions: list[str]) -> dict:
    return {
        'why': why,
        'formula': formula,
        'drilldown': drilldown,
        'actions': [x for x in actions if x],
    }


async def _sum_inflows(db: AsyncSession, date_from, date_to, subcategories: list[str]) -> float:
    agg_has_rows = (await db.execute(select(AggCashDaily.doc_date).limit(1))).first() is not None
    if not agg_has_rows:
        return 0.0
    return float(
        (
            await db.execute(
                select(func.coalesce(func.sum(AggCashDaily.inflows), 0))
                .where(AggCashDaily.doc_date >= date_from, AggCashDaily.doc_date <= date_to)
                .where(AggCashDaily.subcategory.in_(subcategories))
            )
        ).scalar_one()
        or 0
    )


async def _sum_outflows(db: AsyncSession, date_from, date_to, subcategories: list[str]) -> float:
    agg_has_rows = (await db.execute(select(AggCashDaily.doc_date).limit(1))).first() is not None
    if not agg_has_rows:
        return 0.0
    return float(
        (
            await db.execute(
                select(func.coalesce(func.sum(AggCashDaily.outflows), 0))
                .where(AggCashDaily.doc_date >= date_from, AggCashDaily.doc_date <= date_to)
                .where(AggCashDaily.subcategory.in_(subcategories))
            )
        ).scalar_one()
        or 0
    )


async def run_cash_net_negative(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    min_negative = float(params.get('min_negative', 500))
    inflows = await _sum_inflows(db, ctx.period_from, ctx.period_to, ['customer_collections', 'customer_transfers', 'financial_accounts'])
    outflows = await _sum_outflows(db, ctx.period_from, ctx.period_to, ['supplier_payments', 'supplier_transfers', 'financial_accounts'])
    net = inflows - outflows
    if net >= -min_negative:
        return []
    severity = 'critical' if net <= -(min_negative * 2) else 'warning'
    return [
        InsightCreate(
            rule_code='CASH_NET_NEG',
            category='cashflow',
            severity=severity,
            title='Αρνητική Καθαρή Ροή',
            message=f'Η καθαρή ροή είναι αρνητική ({net:.2f}€) στο διάστημα.',
            entity_type='tenant',
            entity_external_id=ctx.tenant_slug,
            entity_name=ctx.tenant_slug,
            period_from=ctx.period_from,
            period_to=ctx.period_to,
            value=net,
            baseline_value=-min_negative,
            delta_value=net + min_negative,
            delta_pct=None,
            metadata_json=_meta(
                why='Η αρνητική ροή ταμείου επηρεάζει άμεσα τη ρευστότητα.',
                formula='net_cash < -min_negative',
                drilldown='/tenant/cashflow',
                actions=['Δες εισπράξεις/πληρωμές ανά κατηγορία.', 'Έλεγξε πιθανές καθυστερήσεις είσπραξης.'],
            ),
        )
    ]


async def run_cash_collections_drop(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    drop_pct = float(params.get('drop_pct', 15))
    window_days = int(params.get('window_days', 30))
    prev_to = ctx.period_from - timedelta(days=1)
    prev_from = prev_to - timedelta(days=max(1, window_days) - 1)

    current = await _sum_inflows(db, ctx.period_from, ctx.period_to, ['customer_collections'])
    previous = await _sum_inflows(db, prev_from, prev_to, ['customer_collections'])

    if previous <= 0:
        return []
    delta_pct = ((current - previous) / previous) * 100.0
    if delta_pct > -drop_pct:
        return []

    severity = 'critical' if delta_pct <= -(drop_pct * 2) else 'warning'
    return [
        InsightCreate(
            rule_code='CASH_COLL_DROP',
            category='cashflow',
            severity=severity,
            title='Πτώση Εισπράξεων',
            message=f'Οι εισπράξεις πελατών έπεσαν {abs(delta_pct):.2f}% σε σχέση με την προηγούμενη περίοδο.',
            entity_type='tenant',
            entity_external_id=ctx.tenant_slug,
            entity_name=ctx.tenant_slug,
            period_from=ctx.period_from,
            period_to=ctx.period_to,
            value=current,
            baseline_value=previous,
            delta_value=current - previous,
            delta_pct=delta_pct,
            metadata_json=_meta(
                why='Η πτώση εισπράξεων μειώνει τη ρευστότητα και δημιουργεί πίεση στο ταμείο.',
                formula='collections_A < collections_prev_A * (1 - drop_pct)',
                drilldown='/tenant/cashflow/customer_collections',
                actions=['Έλεγξε εισπράξεις ανά πελάτη.', 'Εντόπισε καθυστερήσεις.'],
            ),
        )
    ]


async def run_cash_payment_imbalance(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    ratio = float(params.get('outflow_over_inflow_ratio', 1.2))
    inflows = await _sum_inflows(db, ctx.period_from, ctx.period_to, ['customer_collections', 'customer_transfers'])
    outflows = await _sum_outflows(db, ctx.period_from, ctx.period_to, ['supplier_payments', 'supplier_transfers'])
    if inflows <= 0:
        return []
    if outflows <= inflows * ratio:
        return []

    return [
        InsightCreate(
            rule_code='CASH_OUT_IMBALANCE',
            category='cashflow',
            severity='warning',
            title='Υψηλότερες Πληρωμές από Εισπράξεις',
            message=f'Οι πληρωμές προμηθευτών ({outflows:.2f}€) υπερβαίνουν τις εισπράξεις ({inflows:.2f}€).',
            entity_type='tenant',
            entity_external_id=ctx.tenant_slug,
            entity_name=ctx.tenant_slug,
            period_from=ctx.period_from,
            period_to=ctx.period_to,
            value=outflows,
            baseline_value=inflows,
            delta_value=outflows - inflows,
            delta_pct=((outflows - inflows) / inflows) * 100.0 if inflows else None,
            metadata_json=_meta(
                why='Η ανισορροπία πληρωμών/εισπράξεων πιέζει το κεφάλαιο κίνησης.',
                formula='supplier_outflows > collections * ratio',
                drilldown='/tenant/cashflow/supplier_payments',
                actions=['Επανέλεγξε όρους πληρωμών.', 'Ενίσχυσε εισπράξεις.'],
            ),
        )
    ]
