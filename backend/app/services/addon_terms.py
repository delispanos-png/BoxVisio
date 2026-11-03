"""Commercial terms of add-on modules (Co-Pilot AI today).

Two layers:
- `addon_pricing` (control DB): the catalogue — list price per month, monthly promo
  discount, annual-prepay discount. Edited on /admin/plans.
- per-tenant terms in `subscriptions.feature_flags['_addon_terms'][feature_key]`:
  how this tenant has the add-on — free until a date CloudOn picks, monthly or
  annual billing, an extra discount, a note. Edited on /admin/subscriptions.

A free period that has passed switches the add-on off automatically (see
`expire_addon_free_periods`, run with the other subscription time transitions).
Invoicing itself stays outside the app (WHMCS); this is what the admin and the
tenant see, and what decides whether the module is on.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from app.models.control import AddonPricing, Subscription

TERMS_KEY = '_addon_terms'
BILLING_CHOICES = {
    'none': 'Χωρίς συμφωνία',
    'free': 'Δωρεάν διάθεση',
    'monthly': 'Μηνιαία χρέωση',
    'annual': 'Ετήσια χρέωση',
}


def _money(value: Any) -> Decimal:
    try:
        return Decimal(str(value or 0)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
    except Exception:  # noqa: BLE001
        return Decimal('0.00')


def _pct(value: Any) -> Decimal:
    try:
        v = Decimal(str(value or 0))
    except Exception:  # noqa: BLE001
        return Decimal('0')
    return max(Decimal('0'), min(Decimal('100'), v))


def fmt_money(value: Any, currency: str = 'EUR') -> str:
    v = _money(value)
    text = f'{v:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')
    return f'{text} {"€" if currency == "EUR" else currency}'


def fmt_date(value: Any) -> str:
    d = parse_date(value)
    return d.strftime('%d/%m/%Y') if d else ''


def parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value or '').strip()
    if not raw:
        return None
    for fmt in ('%Y-%m-%d', '%d/%m/%Y'):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


async def get_addon_pricing(db: AsyncSession) -> dict[str, AddonPricing]:
    rows = (await db.execute(select(AddonPricing))).scalars().all()
    return {row.feature_key: row for row in rows}


def pricing_view(row: AddonPricing | None) -> dict[str, Any]:
    """Catalogue numbers as the screens show them."""
    if row is None:
        return {'monthly_price': Decimal('0'), 'monthly_discount_pct': Decimal('0'), 'annual_discount_pct': Decimal('0'),
                'monthly_net': Decimal('0'), 'annual_total': Decimal('0'), 'annual_per_month': Decimal('0'), 'currency': 'EUR',
                'monthly_label': '', 'annual_label': '', 'configured': False}
    price = _money(row.monthly_price)
    md = _pct(row.monthly_discount_pct)
    ad = _pct(row.annual_discount_pct)
    monthly_net = _money(price * (Decimal('100') - md) / Decimal('100'))
    annual_total = _money(price * 12 * (Decimal('100') - ad) / Decimal('100'))
    annual_per_month = _money(annual_total / 12)
    cur = row.currency or 'EUR'
    return {
        'monthly_price': price,
        'monthly_discount_pct': md,
        'annual_discount_pct': ad,
        'monthly_net': monthly_net,
        'annual_total': annual_total,
        'annual_per_month': annual_per_month,
        'currency': cur,
        'monthly_label': fmt_money(monthly_net, cur) + '/μήνα' + (f' (-{md:g}%)' if md else ''),
        'annual_label': fmt_money(annual_total, cur) + '/έτος' + (f' (-{ad:g}%, {fmt_money(annual_per_month, cur)}/μήνα)' if ad else ''),
        'configured': True,
    }


async def upsert_addon_pricing(db: AsyncSession, feature_key: str, *, monthly_price: Any, monthly_discount_pct: Any, annual_discount_pct: Any) -> AddonPricing:
    row = (await db.execute(select(AddonPricing).where(AddonPricing.feature_key == feature_key))).scalar_one_or_none()
    if row is None:
        row = AddonPricing(feature_key=feature_key)
        db.add(row)
    row.monthly_price = _money(monthly_price)
    row.monthly_discount_pct = _pct(monthly_discount_pct)
    row.annual_discount_pct = _pct(annual_discount_pct)
    row.updated_at = datetime.utcnow()
    return row


def get_terms(sub: Subscription | None, feature_key: str) -> dict[str, Any]:
    flags = dict(getattr(sub, 'feature_flags', None) or {})
    all_terms = flags.get(TERMS_KEY) if isinstance(flags.get(TERMS_KEY), dict) else {}
    terms = all_terms.get(feature_key) if isinstance(all_terms.get(feature_key), dict) else {}
    return dict(terms)


def set_terms(sub: Subscription, feature_key: str, *, billing: str, free_until: Any, discount_pct: Any, note: str) -> dict[str, Any]:
    billing = billing if billing in BILLING_CHOICES else 'none'
    until = parse_date(free_until)
    terms = {
        'billing': billing,
        'free_until': until.isoformat() if (until and billing == 'free') else None,
        'discount_pct': float(_pct(discount_pct)) if billing in ('monthly', 'annual') else 0.0,
        'note': (note or '').strip()[:255] or None,
        'status': 'active' if billing != 'none' else 'none',
        'updated_at': datetime.utcnow().isoformat(timespec='seconds'),
    }
    flags = dict(sub.feature_flags or {})
    all_terms = dict(flags.get(TERMS_KEY) or {}) if isinstance(flags.get(TERMS_KEY), dict) else {}
    if billing == 'none':
        all_terms.pop(feature_key, None)
    else:
        all_terms[feature_key] = terms
    flags[TERMS_KEY] = all_terms
    sub.feature_flags = flags
    flag_modified(sub, 'feature_flags')
    return terms


def terms_view(terms: dict[str, Any], pricing: dict[str, Any], *, enabled: bool, today: date | None = None) -> dict[str, Any]:
    """What the add-on costs this tenant and how long it is free, in words."""
    today = today or date.today()
    billing = str(terms.get('billing') or 'none')
    until = parse_date(terms.get('free_until'))
    discount = _pct(terms.get('discount_pct'))
    cur = pricing.get('currency', 'EUR')
    expired = bool(terms.get('status') == 'expired' or (billing == 'free' and until and until < today))
    price_text = ''
    if billing == 'monthly':
        base = _money(pricing['monthly_net'] * (Decimal('100') - discount) / Decimal('100'))
        price_text = fmt_money(base, cur) + '/μήνα' + (f' (έκπτωση {discount:g}%)' if discount else '')
    elif billing == 'annual':
        base = _money(pricing['annual_total'] * (Decimal('100') - discount) / Decimal('100'))
        price_text = fmt_money(base, cur) + '/έτος' + (f' (έκπτωση {discount:g}%)' if discount else '')
    if billing == 'free':
        if expired:
            summary = f'Η δωρεάν περίοδος έληξε στις {until.strftime("%d/%m/%Y")}' if until else 'Η δωρεάν περίοδος έληξε'
            tone = 'danger'
        elif until:
            days_left = (until - today).days
            summary = f'Δωρεάν έως {until.strftime("%d/%m/%Y")} ({days_left} ημ.)'
            tone = 'success' if days_left > 7 else 'warning'
        else:
            summary = 'Δωρεάν (χωρίς ημερομηνία λήξης)'
            tone = 'success'
    elif billing in ('monthly', 'annual'):
        summary = f'{BILLING_CHOICES[billing]}: {price_text}'
        tone = 'primary'
    else:
        summary = 'Χωρίς εμπορική συμφωνία' + (' — ενεργό' if enabled else '')
        tone = 'muted'
    return {
        'billing': billing,
        'billing_label': BILLING_CHOICES.get(billing, billing),
        'free_until': until.isoformat() if until else '',
        'free_until_display': until.strftime('%d/%m/%Y') if until else '',
        'discount_pct': float(discount),
        'note': terms.get('note') or '',
        'expired': expired,
        'summary': summary,
        'tone': tone,
        'price_text': price_text,
        'list_monthly': pricing.get('monthly_label', ''),
        'list_annual': pricing.get('annual_label', ''),
        'enabled': bool(enabled),
    }


def expire_addon_free_periods(sub: Subscription, today: date | None = None) -> list[str]:
    """Switch off add-ons whose free period has passed. Returns the keys changed.
    The caller adds the audit rows and commits."""
    today = today or date.today()
    flags = dict(sub.feature_flags or {})
    all_terms = flags.get(TERMS_KEY)
    if not isinstance(all_terms, dict):
        return []
    changed: list[str] = []
    for key, terms in list(all_terms.items()):
        if not isinstance(terms, dict) or terms.get('billing') != 'free':
            continue
        until = parse_date(terms.get('free_until'))
        if until is None or until >= today:
            continue
        if flags.get(key) or terms.get('status') != 'expired':
            flags[key] = False
            terms = dict(terms)
            terms['status'] = 'expired'
            terms['expired_at'] = datetime.utcnow().isoformat(timespec='seconds')
            all_terms[key] = terms
            changed.append(key)
    if changed:
        flags[TERMS_KEY] = all_terms
        sub.feature_flags = flags
        flag_modified(sub, 'feature_flags')
    return changed


async def tenant_addon_view(db: AsyncSession, sub: Subscription | None, feature_key: str, *, enabled: bool) -> dict[str, Any]:
    pricing = pricing_view((await get_addon_pricing(db)).get(feature_key))
    return terms_view(get_terms(sub, feature_key), pricing, enabled=enabled)
