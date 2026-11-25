"""Access policy for tenant traffic: AI-browser / automation block and per-user rate limits.

Why: tenant users were driving the BI through the Claude Desktop built-in browser (user agent
«Claude/… MSIX»), paging whole circuits out through the data APIs at >1.000 requests/minute. That
bypasses the Co-Pilot add-on (our billed AI channel) and loads the platform for everyone else.

Two layers:
1. Known AI browsers / automation frameworks are refused outright (403) by user agent, or after the
   page reports `navigator.webdriver` (headless / scripted Chrome).
2. Per-user limits sized well above a human (p99 observed: 12 requests/minute) but far below an
   agent: requests per minute / hour, paginated list pages per minute, document details per
   minute, Excel exports per day. Repeated violations lock the session for a while and notify
   CloudOn. Defaults live in settings; a tenant can be given its own values from the admin
   subscriptions page (subscription_limits rows), cached in Redis.

All state is in Redis; counters are keyed by user id (cookie or bearer session), else by IP.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from fastapi import Request
from redis import Redis
from sqlalchemy import select

from app.core.config import settings
from app.core.security import expected_audience_for_host, safe_decode

logger = logging.getLogger('app.access_policy')

ATHENS = ZoneInfo('Europe/Athens')

# Subscription limit keys (per tenant overrides) and their defaults.
LIMIT_KEYS: dict[str, str] = {
    'rate_per_minute': 'rate_limit_per_minute',
    'rate_per_hour': 'rate_limit_per_hour',
    'list_pages_per_minute': 'rate_limit_list_pages_per_minute',
    'detail_per_minute': 'rate_limit_detail_per_minute',
    'exports_per_day': 'rate_limit_exports_per_day',
}
LIMIT_LABELS: dict[str, str] = {
    'rate_per_minute': 'Αιτήματα / λεπτό',
    'rate_per_hour': 'Αιτήματα / ώρα',
    'list_pages_per_minute': 'Σελίδες λίστας / λεπτό',
    'detail_per_minute': 'Λεπτομέρειες παραστατικών / λεπτό',
    'exports_per_day': 'Εξαγωγές Excel / ημέρα',
}

STRIKES_TO_LOCK = 3
STRIKE_WINDOW_SECONDS = 600
LOCK_SECONDS = 900
AGENT_FLAG_SECONDS = 900
CFG_CACHE_SECONDS = 120
MAX_LIST_LIMIT = 100

# User agents of AI browsers and automation frameworks. The Claude Desktop app's browser pane
# identifies itself as «Claude/<version> … MSIX»; Perplexity Comet and ChatGPT Atlas carry their
# names; the rest are scripted browsers.
AI_BROWSER_UA = re.compile(
    r'(?:\bClaude/\d|\bComet/\d|ChatGPT-Atlas|\bAtlas/\d|HeadlessChrome|Playwright|Puppeteer|Selenium|PhantomJS|'
    r'\bWebDriver\b|Electron/\d.*(?:Claude|OpenAI|ChatGPT)|\bBrowserUse\b|\bOperator/\d)',
    re.IGNORECASE,
)

EXEMPT_PREFIXES = ('/static/', '/favicon', '/health', '/ready', '/metrics', '/login', '/logout', '/admin', '/.well-known')
GUARDED_PREFIXES = ('/tenant/', '/v1/')

LIST_PATH = re.compile(
    r'^(?:/v1/kpi/[^/]+/(?:documents|items)|/v1/kpi/sales/decision-pack|/v1/streams/[^/]+/(?:documents|items)|'
    r'/tenant/exports/(?:analysis|purchases-analysis)|/tenant/items|/tenant/supplier-orders)/?$'
)
DETAIL_PATH = re.compile(r'^/v1/kpi/[^/]+/documents/[^/]+/detail/?$')
EXPORT_PATH = re.compile(r'(?:/download/|/export(?:\.xlsx)?/?$)')
EXPORT_EXEMPT = re.compile(r'^/tenant/copilot/download/')  # Co-Pilot files are the sanctioned AI channel
EXPORT_CONTENT_TYPES = ('spreadsheetml', 'text/csv', 'application/vnd.ms-excel', 'application/octet-stream')


@dataclass
class Subject:
    key: str            # redis key fragment: user:<id> or ip:<addr>
    user_id: int | None
    tenant_id: int | None
    role: str | None
    ip: str
    browser_session: bool  # cookie-authenticated UI session (vs API bearer)


def client_ip(request: Request) -> str:
    forwarded_for = (request.headers.get('x-forwarded-for') or '').split(',')[0].strip()
    if forwarded_for:
        return forwarded_for
    return request.client.host if request.client else 'unknown'


def is_guarded_path(path: str) -> bool:
    if path.startswith(EXEMPT_PREFIXES):
        return False
    return path.startswith(GUARDED_PREFIXES)


def is_ai_browser(user_agent: str) -> bool:
    return bool(user_agent) and bool(AI_BROWSER_UA.search(user_agent))


def resolve_subject(request: Request) -> Subject | None:
    """Who is making the request. None = CloudOn admin (never limited)."""
    ip = client_ip(request)
    token = request.cookies.get('access_token')
    browser_session = bool(token)
    if not token:
        auth_header = request.headers.get('Authorization', '')
        if auth_header.startswith('Bearer '):
            token = auth_header.split(' ', 1)[1]
    if token:
        payload = safe_decode(token, audience=expected_audience_for_host(request.headers.get('host')), token_type='access')
        if payload:
            role = str(payload.get('role') or '')
            if role == 'cloudon_admin':
                return None
            sub = str(payload.get('sub') or '')
            user_id = int(sub) if sub.isdigit() else None
            tenant_id = payload.get('tenant_id')
            key = f'user:{sub}' if sub else f'ip:{ip}'
            return Subject(key=key, user_id=user_id, tenant_id=int(tenant_id) if tenant_id is not None else None,
                           role=role, ip=ip, browser_session=browser_session)
    return Subject(key=f'ip:{ip}', user_id=None, tenant_id=None, role=None, ip=ip, browser_session=False)


# ---------------------------------------------------------------- per-tenant configuration

def default_limits() -> dict[str, int]:
    return {key: int(getattr(settings, attr, 0) or 0) for key, attr in LIMIT_KEYS.items()}


async def _load_tenant_overrides(tenant_id: int) -> dict[str, int]:
    from app.db.control_session import ControlSessionLocal
    from app.models.control import Subscription, SubscriptionLimit

    async with ControlSessionLocal() as db:
        rows = (
            await db.execute(
                select(SubscriptionLimit.limit_key, SubscriptionLimit.limit_value)
                .join(Subscription, Subscription.id == SubscriptionLimit.subscription_id)
                .where(Subscription.tenant_id == tenant_id, SubscriptionLimit.limit_key.in_(list(LIMIT_KEYS)))
            )
        ).all()
    return {str(k): int(v) for k, v in rows if v is not None}


async def tenant_limits(redis: Redis, tenant_id: int | None) -> dict[str, int]:
    limits = default_limits()
    if tenant_id is None:
        return limits
    cache_key = f'rl:cfg:{tenant_id}'
    try:
        cached = redis.get(cache_key)
    except Exception:
        cached = None
    if cached:
        try:
            limits.update({k: int(v) for k, v in json.loads(cached).items()})
            return limits
        except Exception:
            pass
    try:
        overrides = await _load_tenant_overrides(tenant_id)
    except Exception:
        logger.exception('access_policy_cfg_load_failed tenant=%s', tenant_id)
        overrides = {}
    try:
        redis.set(cache_key, json.dumps(overrides), ex=CFG_CACHE_SECONDS)
    except Exception:
        pass
    limits.update(overrides)
    return limits


def invalidate_tenant_limits(redis: Redis, tenant_id: int) -> None:
    try:
        redis.delete(f'rl:cfg:{tenant_id}')
    except Exception:
        pass


# ---------------------------------------------------------------- counters

def _minute() -> str:
    return datetime.now(timezone.utc).strftime('%Y%m%d%H%M')


def _hour() -> str:
    return datetime.now(timezone.utc).strftime('%Y%m%d%H')


def _day() -> str:
    return datetime.now(ATHENS).strftime('%Y%m%d')


def _incr(redis: Redis, key: str, ttl: int) -> int:
    current = int(redis.incr(key))
    if current == 1:
        redis.expire(key, ttl)
    return current


def is_locked(redis: Redis, subject: Subject) -> int:
    """Seconds left on a violation lock, 0 if none."""
    ttl = redis.ttl(f'rl:lock:{subject.key}')
    return max(0, int(ttl)) if ttl and ttl > 0 else 0


def is_flagged_agent(redis: Redis, subject: Subject) -> bool:
    return bool(redis.exists(f'rl:agent:{subject.key}'))


def flag_agent(redis: Redis, subject: Subject) -> None:
    redis.set(f'rl:agent:{subject.key}', '1', ex=AGENT_FLAG_SECONDS)


@dataclass
class Violation:
    rule: str
    limit: int
    current: int
    retry_after: int


def check_request(redis: Redis, subject: Subject, limits: dict[str, int], path: str) -> Violation | None:
    """Count this request against every rule it falls under; return the first rule it breaks."""
    minute, hour = _minute(), _hour()
    checks: list[tuple[str, str, int, int]] = [
        ('rate_per_minute', f'rl:m:{subject.key}:{minute}', limits['rate_per_minute'], 65),
        ('rate_per_hour', f'rl:h:{subject.key}:{hour}', limits['rate_per_hour'], 3660),
    ]
    if LIST_PATH.match(path):
        checks.append(('list_pages_per_minute', f'rl:list:{subject.key}:{minute}', limits['list_pages_per_minute'], 65))
    if DETAIL_PATH.match(path):
        checks.append(('detail_per_minute', f'rl:detail:{subject.key}:{minute}', limits['detail_per_minute'], 65))
    for rule, key, limit, ttl in checks:
        if limit <= 0:
            continue
        current = _incr(redis, key, ttl)
        if current > limit:
            retry_after = int(redis.ttl(key))
            return Violation(rule=rule, limit=limit, current=current, retry_after=max(1, retry_after))
    return None


def is_export_path(path: str) -> bool:
    return bool(EXPORT_PATH.search(path)) and not EXPORT_EXEMPT.match(path)


def export_quota_exceeded(redis: Redis, subject: Subject, limits: dict[str, int]) -> Violation | None:
    limit = limits['exports_per_day']
    if limit <= 0:
        return None
    current = int(redis.get(f'rl:exp:{subject.key}:{_day()}') or 0)
    if current >= limit:
        midnight = (datetime.now(ATHENS) + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        retry = int((midnight - datetime.now(ATHENS)).total_seconds())
        return Violation(rule='exports_per_day', limit=limit, current=current, retry_after=retry)
    return None


def count_export(redis: Redis, subject: Subject) -> int:
    return _incr(redis, f'rl:exp:{subject.key}:{_day()}', 2 * 86400)


def register_strike(redis: Redis, subject: Subject) -> tuple[int, bool]:
    """One strike per subject per minute bucket; STRIKES_TO_LOCK strikes in the window lock the subject.
    Returns (strikes, newly_locked)."""
    bucket_key = f'rl:strikeb:{subject.key}:{_minute()}'
    if not redis.set(bucket_key, '1', nx=True, ex=70):
        return int(redis.get(f'rl:strike:{subject.key}') or 0), False
    strikes = _incr(redis, f'rl:strike:{subject.key}', STRIKE_WINDOW_SECONDS)
    if strikes >= STRIKES_TO_LOCK and redis.set(f'rl:lock:{subject.key}', '1', nx=True, ex=LOCK_SECONDS):
        redis.delete(f'rl:strike:{subject.key}')
        return strikes, True
    return strikes, False


def clamp_list_limit(request: Request) -> None:
    """Browser sessions may not page the data APIs 500 rows at a time: cap `limit` at MAX_LIST_LIMIT."""
    raw = request.scope.get('query_string') or b''
    if b'limit=' not in raw:
        return
    from urllib.parse import parse_qsl, urlencode

    pairs = parse_qsl(raw.decode('utf-8', 'ignore'), keep_blank_values=True)
    changed = False
    out = []
    for k, v in pairs:
        if k == 'limit':
            try:
                if int(v) > MAX_LIST_LIMIT:
                    v = str(MAX_LIST_LIMIT)
                    changed = True
            except ValueError:
                pass
        out.append((k, v))
    if changed:
        request.scope['query_string'] = urlencode(out).encode('utf-8')


# ---------------------------------------------------------------- audit + notification

def _throttled(redis: Redis, key: str, ttl: int) -> bool:
    try:
        return not redis.set(key, '1', nx=True, ex=ttl)
    except Exception:
        return False


async def record_event(
    redis: Redis,
    subject: Subject,
    *,
    action: str,
    path: str,
    user_agent: str,
    extra: dict | None = None,
    throttle_seconds: int = 600,
    notify: bool = False,
) -> None:
    """Audit-log the event (once per subject per throttle window) and optionally email CloudOn."""
    if _throttled(redis, f'rl:evt:{action}:{subject.key}', throttle_seconds):
        return
    payload = {'path': path[:300], 'ip': subject.ip, 'user_agent': user_agent[:255], 'subject': subject.key, **(extra or {})}
    try:
        from app.db.control_session import ControlSessionLocal
        from app.models.control import AuditLog

        async with ControlSessionLocal() as db:
            db.add(
                AuditLog(
                    tenant_id=subject.tenant_id,
                    actor_user_id=subject.user_id,
                    action=action,
                    entity_type='access_policy',
                    entity_id=str(subject.tenant_id) if subject.tenant_id is not None else None,
                    payload=payload,
                )
            )
            await db.commit()
    except Exception:
        logger.exception('access_policy_audit_failed action=%s subject=%s', action, subject.key)
    logger.warning('access_policy %s subject=%s tenant=%s path=%s ua=%s', action, subject.key, subject.tenant_id, path, user_agent[:80])
    if notify and not _throttled(redis, f'rl:mail:{action}:{subject.key}', 3600):
        try:
            from app.services.email_delivery import email_configured, send_email

            if email_configured():
                label = {'agent_blocked': 'Αποκλεισμός AI browser', 'rate_limit_lock': 'Κλείδωμα για υπέρβαση ορίων', 'automation_flagged': 'Σήμανση αυτοματισμού'}.get(action, action)
                body = (
                    f'{label}\n\nTenant: {subject.tenant_id}\nΧρήστης: {subject.key}\nIP: {subject.ip}\n'
                    f'Διαδρομή: {path}\nUser agent: {user_agent}\n'
                    + ''.join(f'{k}: {v}\n' for k, v in (extra or {}).items())
                    + '\nΛεπτομέρειες στο adminpanel → Συνδρομές (chip «AI/agent»).'
                )
                await asyncio.to_thread(send_email, to_email=settings.default_admin_email, subject=f'[BoxVisio] {label} — tenant {subject.tenant_id}', text_body=body)
        except Exception:
            logger.exception('access_policy_mail_failed')


# ---------------------------------------------------------------- behavioural detection (level 3)
# Catches agents that run inside a real browser with a normal user agent (e.g. the Claude for
# Chrome extension). Each request is scored from four windowed signals; the score is recomputed
# from current state (not accumulated), so a human who stops the suspicious pattern drops back.
#   paging      : >= PAGING_RUN consecutive requests to the same list path with increasing offset/page
#   detail_sweep: >= DETAIL_SWEEP distinct document details within 2 minutes (+1 if ids ascend)
#   cadence     : >= CADENCE_MIN requests with no pause >= CADENCE_GAP_S between any two
#   no_human    : >= NO_HUMAN_PAGES consecutive pages left without mouse / wheel / touch / key input
# Score >= SCORE_THRESHOLD → session flagged (403 for AGENT_FLAG_SECONDS), audit + email.
SCORE_THRESHOLD = 5
SIGNAL_WEIGHTS = {'paging': 3, 'detail_sweep': 2, 'detail_ascending': 1, 'cadence': 2, 'no_human': 3}
PAGING_RUN = 4
PAGING_WINDOW_S = 180
DETAIL_SWEEP = 10
DETAIL_WINDOW_S = 120
CADENCE_MIN = 15
CADENCE_GAP_S = 4.0
NO_HUMAN_PAGES = 5
NO_HUMAN_MAX_DWELL_MS = 45000
PAGING_PARAMS = ('offset', 'page', 'skip', 'start')
TIMING_EXEMPT = re.compile(r'/(?:progress|status|poll|heartbeat|ping|agent-guard)(?:/|$)|\.json$')
DETAIL_ID = re.compile(r'/documents/([^/]+)/detail')


def observe(redis: Redis, subject: Subject, path: str, query: dict[str, str]) -> tuple[int, list[str]]:
    """Update the behavioural windows with this request, then score ALL signals from current state
    (so a paging run plus a detail sweep add up even when the last request is neither)."""
    now = time.time()
    signals: list[str] = []
    pg_key, det_key, ts_key = f'ag:pg:{subject.key}', f'ag:det:{subject.key}', f'ag:ts:{subject.key}'

    pipe = redis.pipeline()
    offset = next((query[k] for k in PAGING_PARAMS if k in query), None)
    if offset is not None and offset.lstrip('-').isdigit():
        pipe.rpush(pg_key, f'{path}|{int(offset)}|{int(now)}')
        pipe.ltrim(pg_key, -12, -1)
        pipe.expire(pg_key, PAGING_WINDOW_S + 60)
    m = DETAIL_ID.search(path)
    if m:
        pipe.zadd(det_key, {m.group(1): now})
        pipe.expire(det_key, DETAIL_WINDOW_S + 60)
    pipe.zremrangebyscore(det_key, '-inf', now - DETAIL_WINDOW_S)
    if not TIMING_EXEMPT.search(path):
        pipe.rpush(ts_key, f'{now:.3f}')
        pipe.ltrim(ts_key, -20, -1)
        pipe.expire(ts_key, 300)
    pipe.lrange(pg_key, 0, -1)
    pipe.zrange(det_key, 0, -1)
    pipe.lrange(ts_key, 0, -1)
    pipe.get(f'ag:nohuman:{subject.key}')
    results = pipe.execute()
    paging_items, detail_ids, ts_raw, no_human_raw = results[-4], results[-3], results[-2], results[-1]

    # paging run: trailing requests to the same list path with increasing offsets, close in time
    items = [e.split('|') for e in paging_items]
    run = 1 if items else 0
    for i in range(len(items) - 1, 0, -1):
        cur, prev = items[i], items[i - 1]
        if cur[0] == prev[0] and int(cur[1]) > int(prev[1]) and int(cur[2]) - int(prev[2]) <= PAGING_WINDOW_S:
            run += 1
        else:
            break
    if run >= PAGING_RUN and items and now - int(items[-1][2]) <= PAGING_WINDOW_S:
        signals.append(f'paging:{run}')

    # detail sweep
    if len(detail_ids) >= DETAIL_SWEEP:
        signals.append(f'detail_sweep:{len(detail_ids)}')
        nums = [int(x) for x in detail_ids[-6:] if str(x).isdigit()]
        if len(nums) >= 5 and all(a < b for a, b in zip(nums, nums[1:])):
            signals.append('detail_ascending')

    # cadence
    ts = [float(x) for x in ts_raw]
    if len(ts) >= CADENCE_MIN and now - ts[-1] < 60:
        gaps = [b - a for a, b in zip(ts, ts[1:])]
        if gaps and max(gaps) < CADENCE_GAP_S:
            signals.append(f'cadence:{len(ts)}in{int(ts[-1] - ts[0])}s')

    # pages without human input (reported by the page beacon)
    try:
        no_human = int(no_human_raw or 0)
    except (TypeError, ValueError):
        no_human = 0
    if no_human >= NO_HUMAN_PAGES:
        signals.append(f'no_human:{no_human}')

    score = sum(SIGNAL_WEIGHTS.get(sig.split(':', 1)[0], 0) for sig in signals)
    return score, signals


def report_page_signal(redis: Redis, subject: Subject, *, human: bool, dwell_ms: int) -> int:
    """Page beacon: human input seen → reset; left quickly without any input → count it."""
    key = f'ag:nohuman:{subject.key}'
    if human:
        redis.delete(key)
        return 0
    if dwell_ms > NO_HUMAN_MAX_DWELL_MS:
        return int(redis.get(key) or 0)
    return _incr(redis, key, 1800)
