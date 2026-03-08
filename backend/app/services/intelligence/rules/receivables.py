from __future__ import annotations

from datetime import date

from sqlalchemy import String, cast, func, literal, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import over

from app.models.tenant import FactCustomerBalance
from app.services.intelligence.types import InsightCreate, RuleContext


def _pct_change(current: float, previous: float) -> float | None:
    if previous <= 0:
        return None
    return ((current - previous) / previous) * 100.0


def _meta(*, why: str, formula: str, drilldown: str, actions: list[str]) -> dict:
    return {
        'why': why,
        'formula': formula,
        'drilldown': drilldown,
        'actions': [x for x in actions if x],
    }


def _customer_key_expr():
    customer_ext = func.nullif(func.btrim(cast(func.coalesce(FactCustomerBalance.customer_ext_id, literal('')), String)), '')
    customer_name = func.nullif(func.btrim(cast(func.coalesce(FactCustomerBalance.customer_name, literal('')), String)), '')
    return func.coalesce(customer_ext, customer_name, cast(FactCustomerBalance.external_id, String))


async def _latest_customer_balance_snapshots(db: AsyncSession, *, as_of: date) -> list[dict[str, object]]:
    key_expr = _customer_key_expr()
    by_day = (
        select(
            key_expr.label('customer_id'),
            func.coalesce(func.max(FactCustomerBalance.customer_ext_id), literal('')).label('customer_code'),
            func.coalesce(func.max(FactCustomerBalance.customer_name), literal('')).label('customer_name'),
            FactCustomerBalance.balance_date.label('balance_date'),
            func.coalesce(func.sum(FactCustomerBalance.open_balance), 0).label('open_balance'),
            func.coalesce(func.sum(FactCustomerBalance.overdue_balance), 0).label('overdue_balance'),
            func.max(FactCustomerBalance.last_collection_date).label('last_collection_date'),
        )
        .select_from(FactCustomerBalance)
        .where(FactCustomerBalance.balance_date <= as_of)
        .group_by(key_expr, FactCustomerBalance.balance_date)
    ).subquery('customer_balances_by_day')

    ranked = (
        select(
            by_day.c.customer_id,
            by_day.c.customer_code,
            by_day.c.customer_name,
            by_day.c.balance_date,
            by_day.c.open_balance,
            by_day.c.overdue_balance,
            by_day.c.last_collection_date,
            over(
                func.row_number(),
                partition_by=by_day.c.customer_id,
                order_by=by_day.c.balance_date.desc(),
            ).label('rn'),
        )
    ).subquery('customer_balances_ranked')

    rows = (await db.execute(select(ranked).where(ranked.c.rn == 1))).mappings().all()
    out: list[dict[str, object]] = []
    for row in rows:
        customer_id = str(row.get('customer_id') or '').strip()
        if not customer_id:
            continue
        out.append(
            {
                'customer_id': customer_id,
                'customer_code': str(row.get('customer_code') or customer_id).strip(),
                'customer_name': str(row.get('customer_name') or customer_id).strip(),
                'balance_date': row.get('balance_date'),
                'open_balance': float(row.get('open_balance') or 0),
                'overdue_balance': float(row.get('overdue_balance') or 0),
                'last_collection_date': row.get('last_collection_date'),
            }
        )
    return out


async def run_customer_overdue_spike(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    overdue_spike_pct = float(params.get('overdue_spike_pct', 20))
    min_overdue_baseline = float(params.get('min_overdue_baseline', 500))

    cur_rows = await _latest_customer_balance_snapshots(db, as_of=ctx.period_to)
    prev_rows = await _latest_customer_balance_snapshots(db, as_of=ctx.previous_to)
    cur_overdue = float(sum(float(r.get('overdue_balance') or 0) for r in cur_rows))
    prev_overdue = float(sum(float(r.get('overdue_balance') or 0) for r in prev_rows))

    if prev_overdue < min_overdue_baseline:
        return []

    delta_pct = _pct_change(cur_overdue, prev_overdue)
    if delta_pct is None or delta_pct < overdue_spike_pct:
        return []

    severity = 'critical' if delta_pct >= overdue_spike_pct * 2 else 'warning'
    return [
        InsightCreate(
            rule_code='CUSTOMER_OVERDUE_SPIKE',
            category='receivables',
            severity=severity,
            title='Αύξηση Ληξιπρόθεσμων Απαιτήσεων',
            message=f'Οι ληξιπρόθεσμες απαιτήσεις αυξήθηκαν κατά {delta_pct:.2f}% σε σχέση με την προηγούμενη περίοδο.',
            entity_type='tenant',
            entity_external_id=ctx.tenant_slug,
            entity_name=ctx.tenant_slug,
            period_from=ctx.period_from,
            period_to=ctx.period_to,
            value=cur_overdue,
            baseline_value=prev_overdue,
            delta_value=cur_overdue - prev_overdue,
            delta_pct=delta_pct,
            metadata_json=_meta(
                why='Η απότομη αύξηση ληξιπρόθεσμων απαιτήσεων αυξάνει τον πιστωτικό κίνδυνο.',
                formula='overdue_current vs overdue_previous >= overdue_spike_pct',
                drilldown='/tenant/customers',
                actions=['Έλεγξε top οφειλέτες και ενεργοποίησε πλάνο είσπραξης.'],
            ),
        )
    ]


async def run_receivables_growth_alert(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    growth_pct_threshold = float(params.get('growth_pct', 15))
    min_open_baseline = float(params.get('min_open_baseline', 1000))

    cur_rows = await _latest_customer_balance_snapshots(db, as_of=ctx.period_to)
    prev_rows = await _latest_customer_balance_snapshots(db, as_of=ctx.previous_to)
    cur_open = float(sum(float(r.get('open_balance') or 0) for r in cur_rows))
    prev_open = float(sum(float(r.get('open_balance') or 0) for r in prev_rows))

    if prev_open < min_open_baseline:
        return []

    delta_pct = _pct_change(cur_open, prev_open)
    if delta_pct is None or delta_pct < growth_pct_threshold:
        return []

    severity = 'critical' if delta_pct >= growth_pct_threshold * 2 else 'warning'
    return [
        InsightCreate(
            rule_code='RECEIVABLES_GROWTH_ALERT',
            category='receivables',
            severity=severity,
            title='Αύξηση Ανοικτών Απαιτήσεων',
            message=f'Το ανοικτό υπόλοιπο πελατών αυξήθηκε κατά {delta_pct:.2f}% σε σχέση με την προηγούμενη περίοδο.',
            entity_type='tenant',
            entity_external_id=ctx.tenant_slug,
            entity_name=ctx.tenant_slug,
            period_from=ctx.period_from,
            period_to=ctx.period_to,
            value=cur_open,
            baseline_value=prev_open,
            delta_value=cur_open - prev_open,
            delta_pct=delta_pct,
            metadata_json=_meta(
                why='Η αυξητική τάση στα ανοικτά υπόλοιπα επηρεάζει ρευστότητα και credit risk.',
                formula='open_current vs open_previous >= growth_pct',
                drilldown='/tenant/customers',
                actions=['Αναθεώρησε πιστωτικά όρια και όρους πίστωσης ανά πελάτη.'],
            ),
        )
    ]


async def run_top_customer_exposure(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    share_pct_threshold = float(params.get('share_pct_threshold', 25))
    min_total_open = float(params.get('min_total_open', 1000))

    cur_rows = await _latest_customer_balance_snapshots(db, as_of=ctx.period_to)
    if not cur_rows:
        return []

    total_open = float(sum(float(r.get('open_balance') or 0) for r in cur_rows))
    if total_open < min_total_open:
        return []

    top = max(cur_rows, key=lambda row: float(row.get('open_balance') or 0))
    top_open = float(top.get('open_balance') or 0)
    share_pct = (top_open / total_open) * 100.0 if total_open > 0 else 0.0
    if share_pct < share_pct_threshold:
        return []

    customer_id = str(top.get('customer_id') or '')
    customer_name = str(top.get('customer_name') or customer_id)
    severity = 'critical' if share_pct >= share_pct_threshold + 15 else 'warning'
    return [
        InsightCreate(
            rule_code='TOP_CUSTOMER_EXPOSURE',
            category='receivables',
            severity=severity,
            title='Υψηλή Έκθεση σε Κορυφαίο Πελάτη',
            message=f'Ο πελάτης {customer_name} αντιστοιχεί στο {share_pct:.2f}% των ανοικτών απαιτήσεων.',
            entity_type='customer',
            entity_external_id=customer_id,
            entity_name=customer_name,
            period_from=ctx.period_from,
            period_to=ctx.period_to,
            value=share_pct,
            baseline_value=share_pct_threshold,
            delta_value=share_pct - share_pct_threshold,
            delta_pct=share_pct,
            metadata_json=_meta(
                why='Υψηλή συγκέντρωση απαιτήσεων σε έναν πελάτη αυξάνει τον κίνδυνο είσπραξης.',
                formula='top_customer_open / total_open >= share_pct_threshold',
                drilldown='/tenant/customers',
                actions=['Θέσε όρια έκθεσης και πλάνο συλλογής για τον πελάτη υψηλού κινδύνου.'],
            ),
        )
    ]


async def run_collection_delay_alert(db: AsyncSession, params: dict, ctx: RuleContext) -> list[InsightCreate]:
    delay_days_threshold = int(params.get('days_since_last_collection', 45))
    min_overdue_balance = float(params.get('min_overdue_balance', 300))
    top_n = max(1, int(params.get('top_n', 10)))

    cur_rows = await _latest_customer_balance_snapshots(db, as_of=ctx.period_to)
    if not cur_rows:
        return []

    candidates: list[tuple[int, dict[str, object]]] = []
    for row in cur_rows:
        overdue = float(row.get('overdue_balance') or 0)
        if overdue < min_overdue_balance:
            continue
        last_collection = row.get('last_collection_date')
        if isinstance(last_collection, date):
            delay_days = max(0, (ctx.period_to - last_collection).days)
        else:
            delay_days = delay_days_threshold + 1
        if delay_days < delay_days_threshold:
            continue
        candidates.append((delay_days, row))

    if not candidates:
        return []

    candidates.sort(key=lambda item: (item[0], float(item[1].get('overdue_balance') or 0)), reverse=True)
    out: list[InsightCreate] = []
    for delay_days, row in candidates[:top_n]:
        customer_id = str(row.get('customer_id') or '')
        customer_name = str(row.get('customer_name') or customer_id)
        overdue = float(row.get('overdue_balance') or 0)
        open_balance = float(row.get('open_balance') or 0)
        severity = 'critical' if delay_days >= delay_days_threshold + 30 else 'warning'
        out.append(
            InsightCreate(
                rule_code='COLLECTION_DELAY_ALERT',
                category='receivables',
                severity=severity,
                title='Καθυστέρηση Είσπραξης Πελάτη',
                message=(
                    f'Ο πελάτης {customer_name} έχει καθυστέρηση είσπραξης {delay_days} ημερών '
                    f'με ληξιπρόθεσμο υπόλοιπο {overdue:.2f}.'
                ),
                entity_type='customer',
                entity_external_id=customer_id,
                entity_name=customer_name,
                period_from=ctx.period_from,
                period_to=ctx.period_to,
                value=overdue,
                baseline_value=open_balance,
                delta_value=float(delay_days),
                delta_pct=((overdue / open_balance) * 100.0) if open_balance > 0 else None,
                metadata_json=_meta(
                    why='Παρατεταμένη καθυστέρηση είσπραξης αυξάνει την πιθανότητα επισφάλειας.',
                    formula='days_since_last_collection >= threshold AND overdue_balance >= min_overdue_balance',
                    drilldown=f'/tenant/customers/{customer_id}/detail',
                    actions=['Προτεραιοποίησε follow-up είσπραξης για τους πελάτες με μεγαλύτερη καθυστέρηση.'],
                ),
            )
        )
    return out
