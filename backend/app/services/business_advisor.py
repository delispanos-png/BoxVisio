from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Awaitable, Callable
from urllib.parse import urlencode

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.kpi_queries import (
    cashflow_summary,
    inventory_snapshot,
    price_control_items,
    purchases_summary,
    receivables_summary,
    sales_by_branch,
    sales_by_category,
    sales_by_fulfillment_point,
    sales_summary,
    suppliers_overview,
)


def _num(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _pct_change(current: float, previous: float) -> float | None:
    if abs(previous) < 0.000001:
        return None
    return ((current - previous) / abs(previous)) * 100.0


def _ratio_pct(part: float, total: float) -> float:
    return (part / total * 100.0) if total else 0.0


def _clamp_score(value: float) -> int:
    return int(max(0, min(100, round(value))))


def _fmt_money(value: float) -> str:
    return f'{value:,.2f} €'.replace(',', 'X').replace('.', ',').replace('X', '.')


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return 'χωρίς βάση σύγκρισης'
    return f'{value:+.2f}%'.replace('.', ',')


def _fmt_date(value: date) -> str:
    return value.strftime('%d/%m/%Y')


def _filter_part(label: str, values: list[str] | None) -> str:
    clean = [str(value).strip() for value in (values or []) if str(value).strip()]
    if not clean:
        return ''
    if len(clean) <= 3:
        return f'{label}: {", ".join(clean)}'
    return f'{label}: {", ".join(clean[:3])} +{len(clean) - 3} ακόμη'


def _filters_label(
    *,
    branches: list[str] | None,
    warehouses: list[str] | None,
    brands: list[str] | None,
    categories: list[str] | None,
    groups: list[str] | None,
) -> str:
    parts = [
        _filter_part('φυσικά σημεία', branches),
        _filter_part('αποθήκες', warehouses),
        _filter_part('brands', brands),
        _filter_part('κατηγορίες', categories),
        _filter_part('ομάδες', groups),
    ]
    active = [part for part in parts if part]
    if not active:
        return 'Φίλτρα: όλη η επιχείρηση, χωρίς επιπλέον περιορισμό.'
    return f'Φίλτρα: {" | ".join(active)}.'


def _same_day_previous_year(value: date) -> date:
    try:
        return value.replace(year=value.year - 1)
    except ValueError:
        # 29/02 -> 28/02 on non-leap previous years.
        return value.replace(year=value.year - 1, day=28)


def _has_fulfillment_rules(config: dict | None) -> bool:
    if not isinstance(config, dict):
        return False
    keys = (
        'pickup_warehouses',
        'store_warehouses',
        'pure_eshop_warehouses',
        'three_pl_warehouses',
        'shipping_method_labels',
        'sales_series_channel_labels',
    )
    return any(bool(config.get(key)) for key in keys)


def _tenant_url(path: str, date_from: date, date_to: date, **params: object) -> str:
    query: list[tuple[str, str]] = [('from', date_from.isoformat()), ('to', date_to.isoformat())]
    for key, value in params.items():
        if value is None or value == '':
            continue
        if isinstance(value, (list, tuple, set)):
            query.extend((key, str(item)) for item in value if item not in (None, ''))
        else:
            query.append((key, str(value)))
    return f'{path}?{urlencode(query, doseq=True)}'


def _item(
    *,
    kind: str,
    module: str,
    title: str,
    text: str,
    impact: float = 0.0,
    severity: str = 'info',
    action: str | None = None,
    href: str | None = None,
    context: str | None = None,
) -> dict[str, Any]:
    return {
        'kind': kind,
        'module': module,
        'title': title,
        'text': text,
        'context': context or '',
        'impact': round(float(impact or 0), 2),
        'severity': severity,
        'action': action or '',
        'href': href or '',
    }


async def _safe(
    label: str,
    producer: Callable[[], Awaitable[dict[str, Any]]],
    warnings: list[dict[str, str]],
    fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        return await producer()
    except Exception as exc:  # pragma: no cover - defensive dashboard composition
        warnings.append({'module': label, 'message': str(exc)})
        return fallback or {}


def _score_status(score: int) -> tuple[str, str]:
    if score >= 82:
        return 'excellent', 'Ισχυρή εικόνα'
    if score >= 68:
        return 'healthy', 'Καλή εικόνα με σημεία προσοχής'
    if score >= 50:
        return 'watch', 'Θέλει διορθωτικές κινήσεις'
    return 'critical', 'Υψηλή πίεση'


def _confidence_level(warnings: list[dict[str, str]], active_modules: int) -> dict[str, Any]:
    if warnings:
        return {
            'level': 'medium',
            'label': 'Μερική βεβαιότητα',
            'text': 'Η ανάγνωση έγινε με διαθέσιμα στοιχεία, αλλά κάποια κυκλώματα χρειάζονται τεχνικό έλεγχο.',
        }
    if active_modules >= 7:
        return {
            'level': 'high',
            'label': 'Υψηλή βεβαιότητα',
            'text': 'Η αναφορά συνδυάζει τα βασικά εμπορικά, οικονομικά και αποθηκευτικά κυκλώματα.',
        }
    return {
        'level': 'medium',
        'label': 'Καλή βεβαιότητα',
        'text': 'Η αναφορά βασίζεται στα διαθέσιμα ενεργά κυκλώματα της εγκατάστασης.',
    }


def _lever(
    *,
    title: str,
    module: str,
    value: float,
    unit: str,
    text: str,
    action: str,
    severity: str,
    href: str,
) -> dict[str, Any]:
    return {
        'title': title,
        'module': module,
        'value': round(float(value or 0), 2),
        'unit': unit,
        'text': text,
        'action': action,
        'severity': severity,
        'href': href,
    }


def _plan_item(day_range: str, title: str, owner: str, text: str, href: str = '') -> dict[str, str]:
    return {
        'range': day_range,
        'title': title,
        'owner': owner,
        'text': text,
        'href': href,
    }


def _dimension_key(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = str(row.get(key) or '').strip()
        if value:
            return value
    return 'N/A'


def _dimension_key_variants(row: dict[str, Any], *, name_key: str, code_key: str) -> list[str]:
    variants: list[str] = []
    for raw_value in (row.get(code_key), row.get(name_key)):
        value = str(raw_value or '').strip()
        if not value:
            continue
        variants.append(value)
        if ',' in value:
            variants.extend(part.strip() for part in value.split(',') if part.strip())

    seen: set[str] = set()
    clean: list[str] = []
    for value in variants:
        key = ' '.join(value.lower().split())
        if key and key not in seen:
            seen.add(key)
            clean.append(key)
    return clean or ['n/a']


def _build_dimension_diagnosis(
    *,
    title: str,
    mode: str,
    current_rows: list[dict[str, Any]],
    previous_rows: list[dict[str, Any]],
    name_key: str,
    code_key: str,
    total_sales_net: float,
    href: str,
) -> dict[str, Any]:
    previous_map: dict[str, dict[str, Any]] = {}
    for row in previous_rows:
        for key in _dimension_key_variants(row, name_key=name_key, code_key=code_key):
            previous_map.setdefault(key, row)
    rows: list[dict[str, Any]] = []
    for row in current_rows:
        previous = next(
            (previous_map[key] for key in _dimension_key_variants(row, name_key=name_key, code_key=code_key) if key in previous_map),
            {},
        )
        current_net = _num(row.get('net_value'))
        previous_net = _num(previous.get('net_value'))
        delta_value = current_net - previous_net
        delta_pct = _pct_change(current_net, previous_net)
        margin_pct = _num(row.get('margin_pct'))
        share_pct = _ratio_pct(current_net, total_sales_net)
        rows.append(
            {
                'name': str(row.get(name_key) or row.get(code_key) or 'N/A'),
                'code': str(row.get(code_key) or ''),
                'net_value': round(current_net, 2),
                'previous_net_value': round(previous_net, 2),
                'delta_value': round(delta_value, 2),
                'delta_pct': delta_pct,
                'share_pct': round(share_pct, 2),
                'margin_pct': round(margin_pct, 2),
            }
        )

    rows = [row for row in rows if float(row.get('net_value') or 0) != 0 or float(row.get('previous_net_value') or 0) != 0]
    known_rows = [
        row
        for row in rows
        if str(row.get('name') or '').strip().upper() not in {'', 'N/A', 'NONE', 'NULL'}
        or str(row.get('code') or '').strip().upper() not in {'', 'N/A', 'NONE', 'NULL'}
    ]
    rows.sort(key=lambda row: float(row.get('net_value') or 0), reverse=True)
    missing_grouping = bool(rows) and not known_rows
    if not rows or missing_grouping:
        dimension_label = 'κατηγορία' if mode == 'categories' else 'φυσικό σημείο'
        dimension_genitive = 'κατηγορίας' if mode == 'categories' else 'φυσικού σημείου'
        missing_data = [
            f'Κωδικός ή όνομα {dimension_genitive} στα παραστατικά, ώστε να γίνει ομαδοποίηση.',
        ]
        if not current_rows:
            missing_data.insert(0, f'Τζίρος ανά {dimension_label} για την επιλεγμένη περίοδο.')
        if not previous_rows:
            missing_data.insert(1 if not current_rows else 0, f'Τζίρος ανά {dimension_label} για την ίδια περίοδο πέρσι.')
        if not rows:
            missing_data.append('Μη μηδενικές αξίες πωλήσεων σε τουλάχιστον μία γραμμή της διάγνωσης.')
        if total_sales_net <= 0:
            missing_data.insert(0, 'Θετικός συνολικός τζίρος στην επιλεγμένη περίοδο.')
        if missing_grouping and total_sales_net > 0:
            missing_data.insert(
                0,
                f'Υπάρχει τζίρος, αλλά όλες οι γραμμές έρχονται χωρίς αναγνωρίσιμη {dimension_label}.',
            )
        return {
            'mode': mode,
            'title': title,
            'summary': (
                'Δεν μπορεί να βγει αξιόπιστη πρόταση, γιατί τα δεδομένα δεν έχουν καθαρή ομαδοποίηση '
                f'ανά {dimension_label} για την επιλεγμένη περίοδο και την ίδια περίοδο πέρσι.'
            ),
            'missing_data': missing_data,
            'action': f'Έλεγξε ότι στα παραστατικά πωλήσεων είναι συμπληρωμένη η {dimension_label} και ότι υπάρχουν πωλήσεις στην περσινή περίοδο σύγκρισης.',
            'best': None,
            'worst': None,
            'rows': [],
            'href': href,
        }

    material_rows = [
        row
        for row in (known_rows or rows)
        if float(row.get('share_pct') or 0) >= 1.0
        or max(abs(float(row.get('net_value') or 0)), abs(float(row.get('previous_net_value') or 0))) >= max(1000.0, total_sales_net * 0.01)
    ]
    decision_rows = material_rows or known_rows or rows
    comparable = [row for row in decision_rows if row.get('delta_pct') is not None]
    best = max(comparable or decision_rows, key=lambda row: float(row.get('delta_value') or row.get('net_value') or 0))
    worst = min(comparable or decision_rows, key=lambda row: float(row.get('delta_value') or row.get('net_value') or 0))
    if mode == 'branches':
        summary = (
            f"Το σημείο που κινείται καλύτερα είναι {best['name']} ({_fmt_pct(best.get('delta_pct'))}). "
            f"Το σημείο που χρειάζεται προσοχή είναι {worst['name']} ({_fmt_pct(worst.get('delta_pct'))})."
        )
        action = 'Σύγκρινε προσωπικό, διαθεσιμότητα, category mix και τοπικές ενέργειες του καλύτερου σημείου με το σημείο που πιέζεται.'
    else:
        summary = (
            f"Η κατηγορία που στηρίζει περισσότερο την εικόνα είναι {best['name']} ({_fmt_pct(best.get('delta_pct'))}). "
            f"Η κατηγορία που θέλει έλεγχο είναι {worst['name']} ({_fmt_pct(worst.get('delta_pct'))})."
        )
        action = 'Έλεγξε τιμή, απόθεμα, προμηθευτή και έκπτωση στις κατηγορίες με αρνητική κίνηση ή χαμηλή συμμετοχή.'

    return {
        'mode': mode,
        'title': title,
        'summary': summary,
        'action': action,
        'best': best,
        'worst': worst,
        'rows': rows[:14],
        'href': href,
    }


async def business_advisor_report(
    db: AsyncSession,
    *,
    date_from: date,
    date_to: date,
    branches: list[str] | None = None,
    warehouses: list[str] | None = None,
    brands: list[str] | None = None,
    categories: list[str] | None = None,
    groups: list[str] | None = None,
    target_margin_pct: float = 35.0,
    price_margin_targets: dict | None = None,
    inventory_coverage_target_days: int = 60,
    fulfillment_config: dict | None = None,
) -> dict[str, Any]:
    days = max(1, (date_to - date_from).days + 1)
    prev_from = _same_day_previous_year(date_from)
    prev_to = _same_day_previous_year(date_to)
    warnings: list[dict[str, str]] = []
    period_context = (
        f'Περίοδος ανάλυσης: {_fmt_date(date_from)} - {_fmt_date(date_to)} ({days} ημέρες). '
        f'Σύγκριση: ίδια περίοδος πέρσι {_fmt_date(prev_from)} - {_fmt_date(prev_to)}. '
        + _filters_label(
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        )
    )

    sales = await _safe(
        'sales',
        lambda: sales_summary(db, date_from, date_to, branches, warehouses, brands, categories, groups),
        warnings,
        {'net_value': 0.0, 'gross_value': 0.0, 'qty': 0.0, 'records': 0},
    )
    previous_sales = await _safe(
        'sales_previous',
        lambda: sales_summary(db, prev_from, prev_to, branches, warehouses, brands, categories, groups),
        warnings,
        {'net_value': 0.0, 'gross_value': 0.0, 'qty': 0.0, 'records': 0},
    )
    purchases = await _safe(
        'purchases',
        lambda: purchases_summary(db, date_from, date_to, branches, warehouses, brands, categories, groups),
        warnings,
        {'net_value': 0.0, 'cost_amount': 0.0, 'qty': 0.0, 'records': 0},
    )
    previous_purchases = await _safe(
        'purchases_previous',
        lambda: purchases_summary(db, prev_from, prev_to, branches, warehouses, brands, categories, groups),
        warnings,
        {'net_value': 0.0, 'cost_amount': 0.0, 'qty': 0.0, 'records': 0},
    )
    inventory = await _safe(
        'inventory',
        lambda: inventory_snapshot(db, date_to, branches, warehouses, brands, categories, groups),
        warnings,
        {'qty_on_hand': 0.0, 'qty_reserved': 0.0, 'value_amount': 0.0, 'cost_amount': 0.0},
    )
    cashflow = await _safe(
        'cashflow',
        lambda: cashflow_summary(db, date_from, date_to, branches),
        warnings,
        {'inflows': 0.0, 'outflows': 0.0, 'net': 0.0, 'entries': 0},
    )
    receivables = await _safe(
        'receivables',
        lambda: receivables_summary(db, date_from, date_to, branches, aggregate_only=True),
        warnings,
        {'summary': {'total_receivables': 0.0, 'overdue_receivables': 0.0, 'overdue_ratio_pct': 0.0}},
    )
    suppliers = await _safe(
        'suppliers',
        lambda: suppliers_overview(db, date_from, date_to, branches, limit=40, aggregate_only=True),
        warnings,
        {'summary': {'open_balance': 0.0, 'overdue_balance': 0.0, 'payments_total': 0.0}},
    )
    pricing = await _safe(
        'price_control',
        lambda: price_control_items(
            db,
            date_from=date_from,
            date_to=date_to,
            categories=categories,
            groups=groups,
            target_margin_pct=target_margin_pct,
            limit=300,
            price_margin_targets=price_margin_targets,
        ),
        warnings,
        {'summary': {'items': 0, 'avg_margin_pct': 0.0, 'target_margin_pct': target_margin_pct}, 'rows': []},
    )
    use_fulfillment_points = _has_fulfillment_rules(fulfillment_config)
    branch_rows = await _safe(
        'sales_by_fulfillment_point' if use_fulfillment_points else 'sales_by_branch',
        lambda: sales_by_fulfillment_point(
            db,
            date_from,
            date_to,
            branches,
            warehouses,
            brands,
            categories,
            groups,
            fulfillment_config,
        )
        if use_fulfillment_points
        else sales_by_branch(db, date_from, date_to, branches, warehouses, brands, categories, groups),
        warnings,
        [],
    )
    previous_branch_rows = await _safe(
        'sales_by_fulfillment_point_previous' if use_fulfillment_points else 'sales_by_branch_previous',
        lambda: sales_by_fulfillment_point(
            db,
            prev_from,
            prev_to,
            branches,
            warehouses,
            brands,
            categories,
            groups,
            fulfillment_config,
        )
        if use_fulfillment_points
        else sales_by_branch(db, prev_from, prev_to, branches, warehouses, brands, categories, groups),
        warnings,
        [],
    )
    category_rows = await _safe(
        'sales_by_category',
        lambda: sales_by_category(db, date_from, date_to, branches, warehouses, brands, categories, groups),
        warnings,
        [],
    )
    previous_category_rows = await _safe(
        'sales_by_category_previous',
        lambda: sales_by_category(db, prev_from, prev_to, branches, warehouses, brands, categories, groups),
        warnings,
        [],
    )

    sales_net = _num(sales.get('net_value'))
    sales_gross = _num(sales.get('gross_value'))
    previous_sales_net = _num(previous_sales.get('net_value'))
    sales_delta_pct = _pct_change(sales_net, previous_sales_net)
    purchases_net = _num(purchases.get('net_value'))
    previous_purchases_net = _num(previous_purchases.get('net_value'))
    purchases_delta_pct = _pct_change(purchases_net, previous_purchases_net)
    purchase_cost = _num(purchases.get('cost_amount')) or purchases_net
    gross_profit = sales_net - purchase_cost
    margin_pct = _ratio_pct(gross_profit, sales_net)
    purchases_to_sales_pct = _ratio_pct(purchases_net, sales_net)

    cash_net = _num(cashflow.get('net'))
    cash_in = _num(cashflow.get('inflows'))
    cash_out = _num(cashflow.get('outflows'))
    receivable_summary = receivables.get('summary') or {}
    receivables_total = _num(receivable_summary.get('total_receivables'))
    overdue_receivables = _num(receivable_summary.get('overdue_receivables'))
    overdue_receivables_pct = _num(receivable_summary.get('overdue_ratio_pct'))
    supplier_summary = suppliers.get('summary') or {}
    supplier_open = _num(supplier_summary.get('open_balance'))
    supplier_overdue = _num(supplier_summary.get('overdue_balance'))
    supplier_overdue_pct = _ratio_pct(supplier_overdue, supplier_open)
    inventory_value = _num(inventory.get('value_amount')) or _num(inventory.get('cost_amount'))
    inventory_qty = _num(inventory.get('qty_on_hand'))
    inventory_to_sales_days = (inventory_value / sales_net * days) if sales_net > 0 else 0.0
    inventory_coverage_target_days = max(1, min(3650, int(inventory_coverage_target_days or 60)))
    inventory_warning_days = inventory_coverage_target_days * 1.25

    pricing_rows = list(pricing.get('rows') or [])
    price_above_target = [row for row in pricing_rows if _num(row.get('price_gap_value')) >= 0]
    price_below_target = [row for row in pricing_rows if _num(row.get('price_gap_value')) < 0]
    price_above_codes = [str(row.get('item_code')) for row in price_above_target if row.get('item_code')]
    price_below_codes = [str(row.get('item_code')) for row in price_below_target if row.get('item_code')]
    avg_price_margin = _num((pricing.get('summary') or {}).get('avg_margin_pct'))
    effective_target_margin_pct = _num((pricing.get('summary') or {}).get('target_margin_pct'), target_margin_pct)
    branch_rows_list = list(branch_rows or []) if isinstance(branch_rows, list) else []
    category_rows_list = list(category_rows or []) if isinstance(category_rows, list) else []
    common_url_filters = {
        'branches': branches or [],
        'warehouses': warehouses or [],
        'brands': brands or [],
        'categories': categories or [],
        'groups': groups or [],
    }
    sales_url = _tenant_url('/tenant/sales', date_from, date_to, **common_url_filters)
    purchases_url = _tenant_url('/tenant/purchases', date_from, date_to, **common_url_filters)
    inventory_url = _tenant_url('/tenant/inventory', date_from, date_to, **common_url_filters)
    cashflow_url = _tenant_url('/tenant/cashflow', date_from, date_to, branches=branches or [])
    finance_url = _tenant_url('/tenant/finance-dashboard', date_from, date_to, branches=branches or [])
    suppliers_url = _tenant_url('/tenant/suppliers', date_from, date_to, branches=branches or [])
    price_control_below_url = _tenant_url(
        '/tenant/price-control',
        date_from,
        date_to,
        categories=categories or [],
        groups=groups or [],
        item_codes=price_below_codes,
        target_margin_pct=effective_target_margin_pct,
        price_position='below',
    )
    price_control_above_url = _tenant_url(
        '/tenant/price-control',
        date_from,
        date_to,
        categories=categories or [],
        groups=groups or [],
        item_codes=price_above_codes,
        target_margin_pct=effective_target_margin_pct,
        price_position='above',
    )
    branch_diagnosis = _build_dimension_diagnosis(
        title='Διάγνωση φυσικών σημείων',
        mode='branches',
        current_rows=branch_rows_list,
        previous_rows=list(previous_branch_rows or []) if isinstance(previous_branch_rows, list) else [],
        name_key='branch',
        code_key='branch_code',
        total_sales_net=sales_net,
        href=sales_url,
    )
    category_diagnosis = _build_dimension_diagnosis(
        title='Διάγνωση κατηγοριών',
        mode='categories',
        current_rows=category_rows_list,
        previous_rows=list(previous_category_rows or []) if isinstance(previous_category_rows, list) else [],
        name_key='category',
        code_key='category_code',
        total_sales_net=sales_net,
        href=sales_url,
    )
    dimension_mode = 'branches' if len([r for r in branch_rows_list if _num(r.get('net_value')) != 0]) > 1 else 'categories'

    positives: list[dict[str, Any]] = []
    risks: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []
    score = 72.0

    if sales_delta_pct is not None and sales_delta_pct >= 5:
        positives.append(
            _item(
                kind='positive',
                module='Πωλήσεις',
                title='Ανοδική πορεία τζίρου',
                text=f'Για αυτή την περίοδο ο καθαρός τζίρος είναι {_fmt_money(sales_net)} και είναι {_fmt_pct(sales_delta_pct)} σε σχέση με την ίδια περίοδο πέρσι. Αυτό δείχνει ότι η ζήτηση βελτιώνεται στο πλαίσιο που βλέπεις.',
                impact=min(12.0, sales_delta_pct / 2),
                severity='success',
                href=sales_url,
                context=period_context,
            )
        )
        score += min(8.0, sales_delta_pct / 3)
    elif sales_delta_pct is not None and sales_delta_pct <= -5:
        risks.append(
            _item(
                kind='risk',
                module='Πωλήσεις',
                title='Πτώση καθαρών πωλήσεων',
                text=f'Για αυτή την περίοδο ο καθαρός τζίρος είναι {_fmt_money(sales_net)} και κινείται {_fmt_pct(sales_delta_pct)} σε σχέση με την ίδια περίοδο πέρσι. Πριν αποφασίσεις ενέργεια, έλεγξε αν η πτώση έρχεται από συγκεκριμένο κατάστημα, brand ή κατηγορία.',
                impact=abs(sales_delta_pct),
                severity='danger' if sales_delta_pct <= -15 else 'warning',
                action='Άνοιξε ανάλυση ανά κατάστημα, brand και κατηγορία και απομόνωσε τις πηγές απώλειας.',
                href=sales_url,
                context=period_context,
            )
        )
        score -= 18.0 if sales_delta_pct <= -15 else 10.0

    if margin_pct >= effective_target_margin_pct:
        positives.append(
            _item(
                kind='positive',
                module='Κερδοφορία',
                title='Το περιθώριο πιάνει τον στόχο',
                text=f'Για αυτή την περίοδο το μικτό περιθώριο είναι {margin_pct:.2f}% με σταθμισμένο στόχο {effective_target_margin_pct:.2f}%. Η τιμολογιακή πολιτική και το κόστος αγορών δεν δείχνουν άμεση πίεση στο επιλεγμένο πλαίσιο.'.replace('.', ','),
                impact=margin_pct - effective_target_margin_pct,
                severity='success',
                context=period_context,
            )
        )
        score += 6.0
    else:
        gap = effective_target_margin_pct - margin_pct
        risks.append(
            _item(
                kind='risk',
                module='Κερδοφορία',
                title='Πίεση στο μικτό περιθώριο',
                text=f'Για αυτή την περίοδο το μικτό περιθώριο είναι {margin_pct:.2f}% και υπολείπεται του σταθμισμένου στόχου κατά {gap:.2f} μονάδες. Αυτό σημαίνει ότι ο τζίρος δεν μετατρέπεται σε αρκετό μικτό κέρδος για τον στόχο που έχεις ορίσει.'.replace('.', ','),
                impact=gap,
                severity='danger' if gap >= 10 else 'warning',
                action='Έλεγξε τιμές κτήσης, εκπτώσεις και προϊόντα κάτω από την τιμή στόχο.',
                href=price_control_below_url,
                context=period_context,
            )
        )
        score -= 16.0 if gap >= 10 else 8.0

    if purchases_to_sales_pct > 80:
        risks.append(
            _item(
                kind='risk',
                module='Αγορές',
                title='Οι αγορές βαραίνουν τον τζίρο',
                text=f'Για αυτή την περίοδο οι αγορές είναι {purchases_to_sales_pct:.2f}% του καθαρού τζίρου. Αν δεν υπάρχει αντίστοιχη αύξηση πωλήσεων ή ανάγκη αποθέματος, υπάρχει κίνδυνος να δεσμεύεται ρευστότητα.'.replace('.', ','),
                impact=purchases_to_sales_pct,
                severity='warning',
                action='Δες προμηθευτές υψηλής συγκέντρωσης και αγορές χωρίς ανάλογη απόδοση πωλήσεων.',
                href=purchases_url,
                context=period_context,
            )
        )
        score -= 8.0
    elif purchases_delta_pct is not None and purchases_delta_pct < 0 and sales_delta_pct is not None and sales_delta_pct >= 0:
        positives.append(
            _item(
                kind='positive',
                module='Αγορές',
                title='Καλύτερη πειθαρχία αγορών',
                text=f'Για αυτή την περίοδο οι αγορές μειώνονται {_fmt_pct(purchases_delta_pct)} ενώ οι πωλήσεις δεν υποχωρούν. Αυτό δείχνει καλύτερο έλεγχο αγορών χωρίς άμεση απώλεια τζίρου.',
                impact=abs(purchases_delta_pct),
                severity='success',
                href=purchases_url,
                context=period_context,
            )
        )

    if cash_net < 0:
        risks.append(
            _item(
                kind='risk',
                module='Ταμειακές ροές',
                title='Αρνητική καθαρή ροή',
                text=f'Για αυτή την περίοδο οι εισροές είναι {_fmt_money(cash_in)}, οι εκροές {_fmt_money(cash_out)} και το καθαρό αποτέλεσμα {_fmt_money(cash_net)}. Αυτό σημαίνει ότι βγήκαν περισσότερα χρήματα από όσα μπήκαν στο ταμείο.',
                impact=abs(cash_net),
                severity='danger',
                action='Προτεραιότητα σε εισπράξεις και έλεγχο πληρωμών προς προμηθευτές.',
                href=cashflow_url,
                context=period_context,
            )
        )
        score -= 12.0
    elif cash_net > 0:
        positives.append(
            _item(
                kind='positive',
                module='Ταμειακές ροές',
                title='Θετική καθαρή ροή',
                text=f'Για αυτή την περίοδο το καθαρό ταμειακό αποτέλεσμα είναι {_fmt_money(cash_net)}. Υπάρχει θετική ροή, άρα οι αποφάσεις μπορούν να δοθούν με λιγότερη πίεση ρευστότητας.',
                impact=cash_net,
                severity='success',
                href=cashflow_url,
                context=period_context,
            )
        )
        score += 4.0

    if overdue_receivables_pct > 20:
        risks.append(
            _item(
                kind='risk',
                module='Πελάτες',
                title='Υψηλές ληξιπρόθεσμες απαιτήσεις',
                text=f'Στο πλαίσιο αυτής της ανάγνωσης οι ληξιπρόθεσμες απαιτήσεις είναι {_fmt_money(overdue_receivables)} ({overdue_receivables_pct:.2f}%). Αυτό δείχνει ποσά που πρέπει να μπουν σε πλάνο είσπραξης πριν επηρεάσουν τη ρευστότητα.'.replace('.', ','),
                impact=overdue_receivables_pct,
                severity='danger' if overdue_receivables_pct > 35 else 'warning',
                action='Φτιάξε λίστα είσπραξης με τους μεγαλύτερους πελάτες και όριο ημερών καθυστέρησης.',
                href=finance_url,
                context=period_context,
            )
        )
        score -= 14.0 if overdue_receivables_pct > 35 else 8.0

    if supplier_overdue_pct > 25:
        risks.append(
            _item(
                kind='risk',
                module='Προμηθευτές',
                title='Πίεση από ληξιπρόθεσμες υποχρεώσεις',
                text=f'Στο πλαίσιο αυτής της ανάγνωσης οι ληξιπρόθεσμες υποχρεώσεις είναι {_fmt_money(supplier_overdue)} ({supplier_overdue_pct:.2f}%). Αυτό επηρεάζει προτεραιότητες πληρωμών και σχέση με προμηθευτές.'.replace('.', ','),
                impact=supplier_overdue_pct,
                severity='warning',
                action='Ιεράρχησε πληρωμές ανά κρίσιμο προμηθευτή και έλεγξε αν υπάρχουν ανοιχτές πιστώσεις.',
                href=suppliers_url,
                context=period_context,
            )
        )
        score -= 8.0

    if inventory_to_sales_days > inventory_warning_days:
        risks.append(
            _item(
                kind='risk',
                module='Απόθεμα',
                title='Υψηλή δέσμευση κεφαλαίου σε απόθεμα',
                text=f'Με βάση το απόθεμα στην ημερομηνία λήξης ({_fmt_date(date_to)}), η αξία αποθέματος είναι {_fmt_money(inventory_value)} και αντιστοιχεί περίπου σε {inventory_to_sales_days:.0f} ημέρες καθαρών πωλήσεων, με στόχο {inventory_coverage_target_days} ημέρες. Αυτό δείχνει κεφάλαιο που μένει δεσμευμένο σε στοκ.',
                impact=inventory_to_sales_days,
                severity='warning',
                action='Εντόπισε αργοκίνητα είδη και brands με υψηλή αξία χωρίς αντίστοιχη κίνηση.',
                href=inventory_url,
                context=period_context,
            )
        )
        score -= 8.0

    if price_below_target:
        risks.append(
            _item(
                kind='risk',
                module='Έλεγχος Τιμών',
                title='Προϊόντα κάτω από την τιμή στόχο',
                text=f'Για αυτή την περίοδο {len(price_below_target)} προϊόντα πωλούνται κάτω από την τιμή που απαιτείται από τους στόχους κατηγορίας/ομάδας. Αυτό μπορεί να ρίχνει το μικτό περιθώριο ακόμη και αν ο τζίρος φαίνεται ικανοποιητικός.',
                impact=len(price_below_target),
                severity='danger' if len(price_below_target) > 20 else 'warning',
                action='Άνοιξε τον Έλεγχο Τιμών και αναπροσάρμοσε έκπτωση ή λιανική στα είδη με μεγαλύτερη διαφορά.',
                href=price_control_below_url,
                context=period_context,
            )
        )
        score -= 10.0
    if price_above_target:
        positives.append(
            _item(
                kind='positive',
                module='Έλεγχος Τιμών',
                title='Προϊόντα με χώρο εμπορικής κίνησης',
                text=f'Για αυτή την περίοδο {len(price_above_target)} προϊόντα βρίσκονται πάνω από την τιμή στόχο. Αυτά μπορούν να στηρίξουν προωθητικές ενέργειες χωρίς να χαθεί αμέσως ο στόχος περιθωρίου.',
                impact=len(price_above_target),
                severity='success',
                href=price_control_above_url,
                context=period_context,
            )
        )

    levers: list[dict[str, Any]] = []
    margin_gap_pct = max(0.0, effective_target_margin_pct - margin_pct)
    if sales_net > 0 and margin_gap_pct > 0:
        margin_gap_value = sales_net * (margin_gap_pct / 100.0)
        levers.append(
            _lever(
                title='Ανάκτηση μικτού περιθωρίου',
                module='Κερδοφορία',
                value=margin_gap_value,
                unit='€ εκτιμώμενο περιθώριο',
                text=f'Αν το περιθώριο φτάσει τον σταθμισμένο στόχο {effective_target_margin_pct:.2f}%, η περίοδος έχει θεωρητικό περιθώριο βελτίωσης περίπου {_fmt_money(margin_gap_value)}.'.replace('.', ','),
                action='Ξεκίνα από προϊόντα κάτω από την τιμή στόχο και από κατηγορίες με μεγάλη πώληση αλλά χαμηλή απόδοση.',
                severity='danger' if margin_gap_pct >= 10 else 'warning',
                href=price_control_below_url,
            )
        )
    if overdue_receivables > 0:
        levers.append(
            _lever(
                title='Απελευθέρωση ταμειακής πίεσης',
                module='Πελάτες',
                value=overdue_receivables,
                unit='€ ληξιπρόθεσμα',
                text=f'Υπάρχουν {_fmt_money(overdue_receivables)} ληξιπρόθεσμες απαιτήσεις που επηρεάζουν άμεσα τη ρευστότητα.',
                action='Βγάλε λίστα είσπραξης ανά πελάτη με προτεραιότητα στα μεγαλύτερα ανοίγματα.',
                severity='danger' if overdue_receivables_pct > 35 else 'warning',
                href=finance_url,
            )
        )
    if inventory_to_sales_days > inventory_coverage_target_days:
        excess_days = max(0.0, inventory_to_sales_days - float(inventory_coverage_target_days))
        estimated_excess_stock = (inventory_value / inventory_to_sales_days * excess_days) if inventory_to_sales_days > 0 else 0.0
        levers.append(
            _lever(
                title='Μείωση δεσμευμένου κεφαλαίου',
                module='Απόθεμα',
                value=estimated_excess_stock,
                unit='€ πιθανό υπερβάλλον απόθεμα',
                text=f'Το απόθεμα αντιστοιχεί σε {inventory_to_sales_days:.0f} ημέρες πωλήσεων. Ο στόχος του tenant είναι {inventory_coverage_target_days} ημέρες, άρα το υπερβάλλον μέρος δείχνει κεφάλαιο που μπορεί να απελευθερωθεί.',
                action='Δούλεψε αργοκίνητα είδη, επιστροφές σε προμηθευτές και στοχευμένες προσφορές.',
                severity='warning',
                href=inventory_url,
            )
        )
    if price_below_target:
        levers.append(
            _lever(
                title='Διόρθωση εμπορικής πολιτικής',
                module='Έλεγχος Τιμών',
                value=len(price_below_target),
                unit='προϊόντα',
                text=f'{len(price_below_target)} προϊόντα έχουν λιανική κάτω από την τιμή στόχο και τραβούν το περιθώριο προς τα κάτω.',
                action='Ομαδοποίησε ανά brand/κατηγορία και αποφάσισε αύξηση λιανικής ή μείωση έκπτωσης.',
                severity='danger' if len(price_below_target) > 20 else 'warning',
                href=price_control_below_url,
            )
        )
    if cash_net < 0:
        levers.append(
            _lever(
                title='Έλεγχος καθαρής ροής',
                module='Ταμειακές ροές',
                value=abs(cash_net),
                unit='€ αρνητικό καθαρό',
                text=f'Η περίοδος έχει αρνητική καθαρή ροή {_fmt_money(cash_net)}.',
                action='Πάγωσε μη κρίσιμες εκροές και συνέδεσε πληρωμές με εισπράξεις επόμενων ημερών.',
                severity='danger',
                href=cashflow_url,
            )
        )
    levers = sorted(levers, key=lambda item: abs(float(item.get('value') or 0)), reverse=True)[:5]

    if not positives:
        positives.append(
            _item(
                kind='positive',
                module='Σύνοψη',
                title='Υπάρχει διαθέσιμη εικόνα για έλεγχο',
                text='Τα βασικά κυκλώματα επιστρέφουν στοιχεία και μπορούν να χρησιμοποιηθούν για επιχειρησιακή απόφαση. Διάβασε τα συμπεράσματα μαζί με την περίοδο και τα φίλτρα που εμφανίζονται στο πλαίσιο κάθε κάρτας.',
                severity='info',
                context=period_context,
            )
        )

    actions.extend(
        sorted(
            [
                item
                for item in risks
                if item.get('action')
            ],
            key=lambda item: float(item.get('impact') or 0),
            reverse=True,
        )[:6]
    )

    score_value = _clamp_score(score)
    status, label = _score_status(score_value)
    leading_risk = risks[0]['title'] if risks else 'χωρίς έντονο αρνητικό σήμα'
    leading_positive = positives[0]['title'] if positives else 'σταθερή εικόνα'
    executive_brief = [
        f'Περίοδος {_fmt_date(date_from)} - {_fmt_date(date_to)} ({days} ημέρες). Η συνολική βαθμολογία είναι {score_value}/100: {label}.',
        f'Κύριο θετικό σήμα: {leading_positive}. Κύριο σημείο προσοχής: {leading_risk}.',
        f'Πωλήσεις περιόδου {_fmt_money(sales_net)} ({_fmt_pct(sales_delta_pct)}), μικτό περιθώριο {margin_pct:.2f}% και καθαρή ταμειακή ροή {_fmt_money(cash_net)}.'.replace('.', ','),
        _filters_label(
            branches=branches,
            warehouses=warehouses,
            brands=brands,
            categories=categories,
            groups=groups,
        ),
    ]
    if warnings:
        executive_brief.append('Υπάρχουν κυκλώματα με μερική ανάγνωση, οπότε η αναφορά κρατά διαγνωστική σημείωση για έλεγχο.')

    if score_value >= 75:
        thesis = 'Η επιχείρηση δείχνει λειτουργικά υγιής. Το επόμενο βήμα δεν είναι άμυνα, είναι βελτιστοποίηση περιθωρίου, αποθέματος και εμπορικής πολιτικής.'
    elif score_value >= 55:
        thesis = 'Η εικόνα είναι διαχειρίσιμη, αλλά υπάρχουν καθαρά σημεία πίεσης. Η διοίκηση πρέπει να εστιάσει σε περιθώριο, ρευστότητα και προϊόντα με λάθος τιμολογιακή συμπεριφορά.'
    else:
        thesis = 'Η επιχείρηση χρειάζεται άμεση παρέμβαση. Η προτεραιότητα είναι ταμείο, κερδοφορία και περιορισμός δεσμευμένου κεφαλαίου πριν επεκταθούν οι εμπορικές κινήσεις.'

    focus_order = [item['module'] for item in sorted(risks, key=lambda item: float(item.get('impact') or 0), reverse=True)]
    focus_order = list(dict.fromkeys(focus_order))[:3]
    if not focus_order:
        focus_order = ['Κερδοφορία', 'Πωλήσεις', 'Απόθεμα']

    action_plan = [
        _plan_item(
            '0-7 ημέρες',
            'Κλείδωμα των άμεσων διαρροών',
            'Διοίκηση / Οικονομικά',
            'Ξεκίνα από το μεγαλύτερο αρνητικό σήμα, δες τα παραστατικά/είδη που το δημιουργούν και όρισε υπεύθυνο διόρθωσης.',
            actions[0].get('href', '') if actions else '',
        ),
        _plan_item(
            '7-30 ημέρες',
            'Διόρθωση εμπορικής πολιτικής',
            'Εμπορική Διεύθυνση',
            'Επανέλεγξε τιμές, εκπτώσεις και αγορές σε κατηγορίες που έχουν τζίρο αλλά χαμηλή απόδοση.',
            price_control_below_url,
        ),
        _plan_item(
            '30+ ημέρες',
            'Σταθερό μοντέλο παρακολούθησης',
            'Διοίκηση',
            'Κάνε εβδομαδιαίο έλεγχο score, κινδύνων και ενεργειών ώστε η απόφαση να γίνεται πριν φανεί το πρόβλημα στα σύνολα.',
            _tenant_url('/tenant/business-advisor', date_from, date_to, target_margin_pct=effective_target_margin_pct),
        ),
    ]

    active_modules = sum(
        1
        for value in [
            sales_net,
            purchases_net,
            inventory_value,
            cash_in + cash_out,
            receivables_total,
            supplier_open,
            len(pricing_rows),
        ]
        if value
    )
    confidence = _confidence_level(warnings, active_modules)

    return {
        'period': {
            'from': date_from.isoformat(),
            'to': date_to.isoformat(),
            'previous_from': prev_from.isoformat(),
            'previous_to': prev_to.isoformat(),
            'days': days,
        },
        'score': {
            'value': score_value,
            'status': status,
            'label': label,
            'summary': executive_brief[0],
        },
        'advisor': {
            'thesis': thesis,
            'focus_order': focus_order,
            'confidence': confidence,
        },
        'executive_brief': executive_brief,
        'positives': sorted(positives, key=lambda item: float(item.get('impact') or 0), reverse=True)[:8],
        'risks': sorted(risks, key=lambda item: float(item.get('impact') or 0), reverse=True)[:8],
        'actions': actions,
        'levers': levers,
        'action_plan': action_plan,
        'dimension_diagnosis': {
            'mode': dimension_mode,
            'primary': branch_diagnosis if dimension_mode == 'branches' else category_diagnosis,
            'secondary': category_diagnosis if dimension_mode == 'branches' else None,
        },
        'modules': {
            'sales': {
                'label': 'Πωλήσεις',
                'net_value': round(sales_net, 2),
                'gross_value': round(sales_gross, 2),
                'qty': round(_num(sales.get('qty')), 2),
                'delta_pct': sales_delta_pct,
            },
            'purchases': {
                'label': 'Αγορές',
                'net_value': round(purchases_net, 2),
                'cost_amount': round(purchase_cost, 2),
                'delta_pct': purchases_delta_pct,
                'purchases_to_sales_pct': round(purchases_to_sales_pct, 2),
            },
            'profitability': {
                'label': 'Κερδοφορία',
                'gross_profit': round(gross_profit, 2),
                'margin_pct': round(margin_pct, 2),
                'target_margin_pct': round(effective_target_margin_pct, 2),
                'default_target_margin_pct': round(target_margin_pct, 2),
            },
            'cashflow': {
                'label': 'Ταμειακές ροές',
                'inflows': round(cash_in, 2),
                'outflows': round(cash_out, 2),
                'net': round(cash_net, 2),
            },
            'receivables': {
                'label': 'Πελάτες',
                'total': round(receivables_total, 2),
                'overdue': round(overdue_receivables, 2),
                'overdue_ratio_pct': round(overdue_receivables_pct, 2),
            },
            'suppliers': {
                'label': 'Προμηθευτές',
                'open_balance': round(supplier_open, 2),
                'overdue_balance': round(supplier_overdue, 2),
                'overdue_ratio_pct': round(supplier_overdue_pct, 2),
            },
            'inventory': {
                'label': 'Απόθεμα',
                'value_amount': round(inventory_value, 2),
                'qty_on_hand': round(inventory_qty, 2),
                'sales_days_equivalent': round(inventory_to_sales_days, 1),
                'target_coverage_days': int(inventory_coverage_target_days),
                'coverage_gap_days': round(inventory_to_sales_days - inventory_coverage_target_days, 1),
            },
            'pricing': {
                'label': 'Έλεγχος Τιμών',
                'items': int((pricing.get('summary') or {}).get('items') or 0),
                'avg_margin_pct': round(avg_price_margin, 2),
                'avg_target_margin_pct': round(effective_target_margin_pct, 2),
                'above_target': len(price_above_target),
                'below_target': len(price_below_target),
            },
        },
        'diagnostics': {'warnings': warnings},
    }
