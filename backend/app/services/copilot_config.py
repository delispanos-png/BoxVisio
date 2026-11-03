"""Per-tenant Co-Pilot configuration helpers.

The tenant supplies its OWN LLM API key (billed to the tenant's own account, never
through us) from its admin panel. The key is stored encrypted with the same Fernet
envelope used for connection secrets and is only ever decrypted in-memory at call time.
Never log or return the plaintext key to a template.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import calendar
import re
from datetime import date, datetime, timedelta
from urllib.parse import parse_qsl, urlencode

from sqlalchemy import update

from sqlalchemy import delete as sa_delete

from app.models.control import CopilotConfig, CopilotSchedule, CopilotUsage, SavedReport
from app.services.connection_secrets import decrypt_json_secret, encrypt_json_secret

DEFAULT_MODEL = 'claude-opus-5'
ALLOWED_MODELS = ('claude-opus-5', 'claude-sonnet-5', 'claude-haiku-4-5')
ALLOWED_DATA_SCOPES = ('aggregates', 'row_level')


async def get_copilot_config(db: AsyncSession, tenant_id: int) -> CopilotConfig | None:
    return (
        await db.execute(select(CopilotConfig).where(CopilotConfig.tenant_id == int(tenant_id)))
    ).scalar_one_or_none()


def copilot_api_key(config: CopilotConfig | None) -> str | None:
    """Decrypt and return the tenant's API key (in-memory only), or None."""
    if config is None or not config.api_key_enc:
        return None
    try:
        payload = decrypt_json_secret(config.api_key_enc)
    except Exception:
        return None
    key = str(payload.get('api_key') or '').strip()
    return key or None


def copilot_is_ready(config: CopilotConfig | None) -> bool:
    """True when the tenant has enabled the Co-Pilot AND provided a key."""
    return bool(config is not None and config.enabled and copilot_api_key(config))


def copilot_settings_view(config: CopilotConfig | None) -> dict[str, Any]:
    """Template-safe view — never exposes the key, only whether one is stored."""
    if config is None:
        return {
            'enabled': False,
            'provider': 'anthropic',
            'model': DEFAULT_MODEL,
            'has_key': False,
            'max_monthly_tokens': 0,
            'data_scope': 'row_level',
            'ready': False,
        }
    return {
        'enabled': bool(config.enabled),
        'provider': config.provider or 'anthropic',
        'model': config.model or DEFAULT_MODEL,
        'has_key': bool(config.api_key_enc),
        'max_monthly_tokens': int(config.max_monthly_tokens or 0),
        'data_scope': config.data_scope or 'row_level',
        'ready': copilot_is_ready(config),
        'daily_report_enabled': bool(config.daily_report_enabled),
        'daily_report_time': config.daily_report_time or '10:00',
        'daily_report_recipients': config.daily_report_recipients or '',
    }


async def upsert_copilot_config(
    db: AsyncSession,
    tenant_id: int,
    *,
    enabled: bool,
    model: str,
    api_key: str | None,
    clear_key: bool = False,
    max_monthly_tokens: int = 0,
    data_scope: str = 'row_level',
    daily_report_enabled: bool = False,
    daily_report_time: str = '10:00',
    daily_report_recipients: str = '',
) -> CopilotConfig:
    """Create or update the tenant's Co-Pilot config.

    - api_key: a new plaintext key to store (encrypted); None/'' leaves the existing key
      untouched so the admin doesn't have to re-enter it on every save.
    - clear_key: wipe the stored key entirely.
    """
    config = await get_copilot_config(db, tenant_id)
    if config is None:
        config = CopilotConfig(tenant_id=int(tenant_id))
        db.add(config)

    config.enabled = bool(enabled)
    config.provider = 'anthropic'
    config.model = model if model in ALLOWED_MODELS else DEFAULT_MODEL
    config.data_scope = data_scope if data_scope in ALLOWED_DATA_SCOPES else 'row_level'
    config.max_monthly_tokens = int(max_monthly_tokens or 0) or None

    config.daily_report_enabled = bool(daily_report_enabled)
    _t = str(daily_report_time or '10:00').strip()
    config.daily_report_time = _t if re.match(r'^([01]?\d|2[0-3]):[0-5]\d$', _t) else '10:00'
    config.daily_report_recipients = (str(daily_report_recipients or '').strip() or None)

    if clear_key:
        config.api_key_enc = None
    elif api_key and str(api_key).strip():
        config.api_key_enc = encrypt_json_secret({'api_key': str(api_key).strip()})
    # else: keep the existing encrypted key

    await db.flush()
    return config


# --- monthly token usage / cap ------------------------------------------------

def _month_key() -> int:
    now = datetime.utcnow()
    return now.year * 100 + now.month


async def month_out_tokens(db: AsyncSession, tenant_id: int) -> int:
    """Output tokens the tenant's Co-Pilot has spent in the current calendar month."""
    row = (
        await db.execute(
            select(CopilotUsage).where(
                CopilotUsage.tenant_id == int(tenant_id),
                CopilotUsage.yyyymm == _month_key(),
            )
        )
    ).scalar_one_or_none()
    return int(row.out_tokens) if row else 0


async def cap_reached(db: AsyncSession, config: CopilotConfig | None) -> bool:
    """True when a monthly cap is set and this month's output tokens already meet it."""
    cap = int((config.max_monthly_tokens or 0) if config else 0)
    if cap <= 0 or config is None:
        return False
    return await month_out_tokens(db, int(config.tenant_id)) >= cap


async def add_usage(db: AsyncSession, tenant_id: int, in_tokens: int, out_tokens: int) -> None:
    """Increment this month's usage counters (upsert)."""
    ym = _month_key()
    existing = (
        await db.execute(
            select(CopilotUsage).where(
                CopilotUsage.tenant_id == int(tenant_id), CopilotUsage.yyyymm == ym
            )
        )
    ).scalar_one_or_none()
    if existing is None:
        db.add(CopilotUsage(tenant_id=int(tenant_id), yyyymm=ym,
                            in_tokens=int(in_tokens or 0), out_tokens=int(out_tokens or 0)))
    else:
        await db.execute(
            update(CopilotUsage)
            .where(CopilotUsage.tenant_id == int(tenant_id), CopilotUsage.yyyymm == ym)
            .values(in_tokens=CopilotUsage.in_tokens + int(in_tokens or 0),
                    out_tokens=CopilotUsage.out_tokens + int(out_tokens or 0))
        )
    await db.commit()


# --- scheduled reports (multiple per tenant) ----------------------------------

_TIME_RE = re.compile(r'^([01]?\d|2[0-3]):[0-5]\d$')


def _norm_time(value: str) -> str:
    t = str(value or '10:00').strip()
    if not _TIME_RE.match(t):
        return '10:00'
    h, m = t.split(':')
    return f'{int(h):02d}:{int(m):02d}'


async def list_schedules(db: AsyncSession, tenant_id: int) -> list[CopilotSchedule]:
    return list(
        (
            await db.execute(
                select(CopilotSchedule)
                .where(CopilotSchedule.tenant_id == int(tenant_id))
                .order_by(CopilotSchedule.send_time, CopilotSchedule.id)
            )
        ).scalars().all()
    )


FREQUENCIES = ('daily', 'weekdays', 'weekly', 'monthly')
WEEKDAY_NAMES = ('Δευτέρα', 'Τρίτη', 'Τετάρτη', 'Πέμπτη', 'Παρασκευή', 'Σάββατο', 'Κυριακή')


def _norm_frequency(value: Any) -> str:
    v = str(value or 'daily').strip().lower()
    return v if v in FREQUENCIES else 'daily'


def _norm_int(value: Any, lo: int, hi: int, default: int) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        return default
    return min(hi, max(lo, n))


def describe_schedule(s: CopilotSchedule) -> str:
    """Human wording of when a routine runs: «Κάθε μέρα 10:00», «Κάθε Δευτέρα 08:30» …"""
    freq = _norm_frequency(s.frequency)
    t = s.send_time or '10:00'
    if freq == 'weekdays':
        return f'Δευτέρα-Παρασκευή {t}'
    if freq == 'weekly':
        return f'Κάθε {WEEKDAY_NAMES[_norm_int(s.weekday, 0, 6, 0)]} {t}'
    if freq == 'monthly':
        d = _norm_int(s.day_of_month, 1, 31, 1)
        return f'Κάθε μήνα στις {d} {t}' if d < 31 else f'Τελευταία μέρα κάθε μήνα {t}'
    return f'Κάθε μέρα {t}'


def schedule_runs_on(s: CopilotSchedule, day: date) -> bool:
    """Does the routine run on this calendar day (time of day not considered)?"""
    freq = _norm_frequency(s.frequency)
    if freq == 'daily':
        return True
    if freq == 'weekdays':
        return day.weekday() < 5
    if freq == 'weekly':
        return day.weekday() == _norm_int(s.weekday, 0, 6, 0)
    # monthly: a day past the month's length means its last day (31 → 28/29/30)
    last = calendar.monthrange(day.year, day.month)[1]
    return day.day == min(_norm_int(s.day_of_month, 1, 31, 1), last)


PERIOD_PRESETS = {
    'yesterday': 'Χθες',
    'today': 'Σήμερα',
    'last_7_days': 'Τελευταίες 7 ημέρες',
    'week_to_date': 'Εβδομάδα μέχρι χθες',
    'previous_week': 'Προηγούμενη εβδομάδα',
    'month_to_date': 'Μήνας μέχρι χθες',
    'previous_month': 'Προηγούμενος μήνας',
    'last_30_days': 'Τελευταίες 30 ημέρες',
    'year_to_date': 'Έτος μέχρι χθες',
    'saved': 'Όπως στην αποθηκευμένη αναφορά',
}
COMPARE_PRESETS = {
    'prev_year': 'Ίδια περίοδος πέρσι',
    'prev_period': 'Προηγούμενη ίση περίοδος',
    'none': 'Χωρίς σύγκριση',
    'saved': 'Όπως στην αποθηκευμένη αναφορά',
}
REPORT_KIND_LABELS = {'analysis': 'Report Builder Πωλήσεων', 'purchases_analysis': 'Report Builder Αγορών'}


def _norm_choice(value: Any, allowed: dict, default: str) -> str:
    v = str(value or '').strip()
    return v if v in allowed else default


def routine_period(preset: str, today: date) -> tuple[date, date]:
    """The rolling period a routine reports on, as of `today` (the run day)."""
    y = today - timedelta(days=1)
    if preset == 'today':
        return today, today
    if preset == 'last_7_days':
        return y - timedelta(days=6), y
    if preset == 'week_to_date':
        start = y - timedelta(days=y.weekday())
        return start, y
    if preset == 'previous_week':
        start = today - timedelta(days=today.weekday() + 7)
        return start, start + timedelta(days=6)
    if preset == 'month_to_date':
        return y.replace(day=1), y
    if preset == 'previous_month':
        first_this = today.replace(day=1)
        last_prev = first_this - timedelta(days=1)
        return last_prev.replace(day=1), last_prev
    if preset == 'last_30_days':
        return y - timedelta(days=29), y
    if preset == 'year_to_date':
        return y.replace(month=1, day=1), y
    return y, y  # yesterday


def _shift_year(d: date) -> date:
    try:
        return d.replace(year=d.year - 1)
    except ValueError:  # 29 Feb
        return d.replace(year=d.year - 1, day=28)


def routine_compare_period(compare: str, a_from: date, a_to: date) -> tuple[date, date] | None:
    if compare == 'prev_year':
        return _shift_year(a_from), _shift_year(a_to)
    if compare == 'prev_period':
        span = (a_to - a_from).days + 1
        return a_from - timedelta(days=span), a_from - timedelta(days=1)
    return None


def routine_report_params(saved_params: str, *, period_preset: str, compare_preset: str, today: date) -> tuple[str, str]:
    """Rewrite a saved Report Builder query string with the routine's rolling period.
    Returns (query_string, human period description)."""
    params = dict(parse_qsl(saved_params or '', keep_blank_values=True))
    # parse_qsl drops repeated keys (metric=…&metric=…); keep them all.
    multi = [(k, v) for k, v in parse_qsl(saved_params or '', keep_blank_values=True)]
    preset = _norm_choice(period_preset, PERIOD_PRESETS, 'yesterday')
    compare = _norm_choice(compare_preset, COMPARE_PRESETS, 'prev_year')
    if preset == 'saved':
        a_from = a_to = None
        desc = f"{params.get('period_from', '')} – {params.get('period_to', '')}"
    else:
        a_from, a_to = routine_period(preset, today)
        desc = f"{a_from.strftime('%d/%m/%Y')} – {a_to.strftime('%d/%m/%Y')}" if a_from != a_to else a_from.strftime('%d/%m/%Y')
    out: list[tuple[str, str]] = []
    drop = {'period_from', 'period_to', 'period_b_from', 'period_b_to', 'mode'} if a_from else {'period_b_from', 'period_b_to', 'mode'}
    for k, v in multi:
        if k not in drop:
            out.append((k, v))
    mode = params.get('mode') or 'analysis'
    if a_from:
        out += [('period_from', a_from.isoformat()), ('period_to', a_to.isoformat())]
    if compare == 'saved':
        out += [('period_b_from', params.get('period_b_from', '')), ('period_b_to', params.get('period_b_to', ''))]
    elif compare == 'none':
        out += [('period_b_from', ''), ('period_b_to', '')]
        if mode == 'comparison':
            mode = 'analysis'
    else:
        base_from = a_from or (date.fromisoformat(params['period_from']) if params.get('period_from') else None)
        base_to = a_to or (date.fromisoformat(params['period_to']) if params.get('period_to') else None)
        b = routine_compare_period(compare, base_from, base_to) if base_from and base_to else None
        out += [('period_b_from', b[0].isoformat() if b else ''), ('period_b_to', b[1].isoformat() if b else '')]
        if b:
            desc += f" (σύγκριση: {b[0].strftime('%d/%m/%Y')} – {b[1].strftime('%d/%m/%Y')})"
    out.append(('mode', mode))
    return urlencode(out), desc


async def list_saved_reports(db: AsyncSession, tenant_id: int) -> list[SavedReport]:
    return list(
        (
            await db.execute(
                select(SavedReport).where(SavedReport.tenant_id == int(tenant_id)).order_by(SavedReport.kind, SavedReport.name)
            )
        ).scalars().all()
    )


def saved_reports_view(reports: list[SavedReport]) -> list[dict]:
    return [
        {'id': r.id, 'name': r.name, 'kind': r.kind, 'kind_label': REPORT_KIND_LABELS.get(r.kind, r.kind)}
        for r in reports
    ]


def schedules_view(schedules: list[CopilotSchedule], reports: list[SavedReport] | None = None) -> list[dict]:
    by_id = {int(r.id): r for r in (reports or [])}
    out = []
    for s in schedules:
        rep = by_id.get(int(s.report_id)) if s.report_id else None
        out.append({
            'id': s.id,
            'enabled': bool(s.enabled),
            'title': s.title,
            'send_time': s.send_time,
            'recipients': s.recipients or '',
            'prompt': s.prompt or '',
            'frequency': _norm_frequency(s.frequency),
            'weekday': _norm_int(s.weekday, 0, 6, 0),
            'day_of_month': _norm_int(s.day_of_month, 1, 31, 1),
            'when': describe_schedule(s),
            'report_id': int(s.report_id) if s.report_id else None,
            'report_name': (rep.name if rep else ('(η αναφορά διαγράφηκε)' if s.report_id else '')),
            'report_kind': (rep.kind if rep else ''),
            'report_kind_label': (REPORT_KIND_LABELS.get(rep.kind, rep.kind) if rep else ''),
            'period_preset': _norm_choice(s.period_preset, PERIOD_PRESETS, 'yesterday'),
            'period_label': PERIOD_PRESETS.get(_norm_choice(s.period_preset, PERIOD_PRESETS, 'yesterday'), ''),
            'compare_preset': _norm_choice(s.compare_preset, COMPARE_PRESETS, 'prev_year'),
            'compare_label': COMPARE_PRESETS.get(_norm_choice(s.compare_preset, COMPARE_PRESETS, 'prev_year'), ''),
            'last_run_at': s.last_run_at.strftime('%d/%m/%Y %H:%M') if s.last_run_at else '',
            'last_status': s.last_status or '',
            'last_error': (s.last_error or '')[:300],
            'created_by': s.created_by or '',
        })
    return out


async def save_schedule(
    db: AsyncSession,
    tenant_id: int,
    sched_id: int | None,
    *,
    title: str,
    send_time: str,
    recipients: str,
    prompt: str,
    enabled: bool = True,
    frequency: str = 'daily',
    weekday: Any = None,
    day_of_month: Any = None,
    created_by: str | None = None,
    report_id: Any = 'unset',
    period_preset: str | None = None,
    compare_preset: str | None = None,
) -> CopilotSchedule | None:
    """Create (sched_id falsy) or update an owned schedule. Returns None if the id was
    given but does not belong to this tenant."""
    sched: CopilotSchedule | None = None
    if sched_id:
        sched = (
            await db.execute(
                select(CopilotSchedule).where(
                    CopilotSchedule.id == int(sched_id), CopilotSchedule.tenant_id == int(tenant_id)
                )
            )
        ).scalar_one_or_none()
        if sched is None:
            return None
    if sched is None:
        sched = CopilotSchedule(tenant_id=int(tenant_id))
        db.add(sched)
    sched.enabled = bool(enabled)
    sched.title = (str(title or 'Report').strip()[:120] or 'Report')
    sched.send_time = _norm_time(send_time)
    sched.recipients = str(recipients or '').strip()
    sched.prompt = str(prompt or '').strip()
    sched.frequency = _norm_frequency(frequency)
    if weekday is not None:
        sched.weekday = _norm_int(weekday, 0, 6, 0)
    if day_of_month is not None:
        sched.day_of_month = _norm_int(day_of_month, 1, 31, 1)
    if created_by and not sched.created_by:
        sched.created_by = str(created_by)[:255]
    if report_id != 'unset':  # the settings forms don't carry it; leave the binding alone there
        try:
            rid = int(report_id) if report_id not in (None, '', 0, '0') else None
        except (TypeError, ValueError):
            rid = None
        if rid is not None:
            # Only a report of this tenant can back a routine.
            owned = (await db.execute(select(SavedReport.id).where(SavedReport.id == rid, SavedReport.tenant_id == int(tenant_id)))).scalar_one_or_none()
            rid = int(owned) if owned else None
        sched.report_id = rid
    if period_preset is not None:
        sched.period_preset = _norm_choice(period_preset, PERIOD_PRESETS, 'yesterday')
    if compare_preset is not None:
        sched.compare_preset = _norm_choice(compare_preset, COMPARE_PRESETS, 'prev_year')
    await db.commit()
    return sched


async def delete_schedule(db: AsyncSession, tenant_id: int, sched_id: int) -> bool:
    res = await db.execute(
        sa_delete(CopilotSchedule).where(
            CopilotSchedule.id == int(sched_id), CopilotSchedule.tenant_id == int(tenant_id)
        )
    )
    await db.commit()
    return bool(res.rowcount)
