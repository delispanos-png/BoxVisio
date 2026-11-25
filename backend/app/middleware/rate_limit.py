"""Tenant access guard: AI-browser block + per-user rate limits (see services/access_policy.py).

Fails open: any internal error lets the request through and is logged, so a Redis hiccup never
takes the product down.
"""
from __future__ import annotations

import logging

from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse
from redis import Redis

from app.core.config import settings
from app.services import access_policy as ap

logger = logging.getLogger('app.access_guard')

_MESSAGES = {
    'agent': (
        'Η πρόσβαση μέσω AI browser δεν επιτρέπεται',
        'Ο browser που χρησιμοποιείτε είναι εργαλείο τεχνητής νοημοσύνης ή αυτοματοποίησης. Οι όροι χρήσης του '
        'BoxVisio δεν επιτρέπουν την πρόσβαση στα δεδομένα μέσω αυτοματοποιημένων εργαλείων. Για ανάλυση με AI '
        'χρησιμοποιήστε τον Co-Pilot του BoxVisio από τον κανονικό σας browser.',
    ),
    'locked': (
        'Προσωρινό κλείδωμα πρόσβασης',
        'Ξεπεράσατε επανειλημμένα το όριο αιτημάτων. Η πρόσβαση ανοίγει ξανά σε {minutes} λεπτά. Αν χρειάζεστε '
        'μαζικά δεδομένα ή αυτοματοποιημένες αναφορές, χρησιμοποιήστε τον Co-Pilot και τις Ρουτίνες του.',
    ),
    'limit': (
        'Πολλά αιτήματα',
        'Ξεπεράσατε το όριο «{label}» ({limit}). Δοκιμάστε ξανά σε {retry} δευτερόλεπτα. Επαναλαμβανόμενες '
        'υπερβάσεις κλειδώνουν προσωρινά την πρόσβαση.',
    ),
    'exports': (
        'Όριο εξαγωγών',
        'Ξεπεράσατε το ημερήσιο όριο εξαγωγών ({limit}). Το όριο μηδενίζει τα μεσάνυχτα. Για προγραμματισμένες '
        'αναφορές χρησιμοποιήστε τις Ρουτίνες του Co-Pilot.',
    ),
}


def _wants_json(request: Request) -> bool:
    if request.url.path.startswith('/v1/'):
        return True
    accept = (request.headers.get('accept') or '').lower()
    return 'application/json' in accept and 'text/html' not in accept or request.headers.get('x-requested-with') == 'XMLHttpRequest'


def _refuse(request: Request, status: int, kind: str, *, retry_after: int = 0, **fmt) -> HTMLResponse | JSONResponse:
    title, text = _MESSAGES[kind]
    text = text.format(**fmt)
    headers = {'Retry-After': str(retry_after)} if retry_after else {}
    if _wants_json(request):
        return JSONResponse(status_code=status, content={'detail': text, 'code': kind, 'retry_after': retry_after}, headers=headers)
    from app.api.ui import templates

    return templates.TemplateResponse(
        'auth/access_limited.html',
        {'request': request, 'title': title, 'text': text, 'kind': kind, 'retry_after': retry_after},
        status_code=status,
        headers=headers,
    )


async def rate_limit_middleware(request: Request, call_next):
    path = request.url.path
    if not ap.is_guarded_path(path):
        return await call_next(request)

    export = False
    subject = None
    redis = None
    try:
        subject = ap.resolve_subject(request)
        if subject is None:  # CloudOn admin
            return await call_next(request)
        redis = getattr(request.app.state, 'redis', None) or Redis.from_url(settings.redis_url, decode_responses=True)
        ua = request.headers.get('user-agent') or ''

        if ap.is_ai_browser(ua):
            await ap.record_event(redis, subject, action='agent_blocked', path=path, user_agent=ua, notify=True)
            return _refuse(request, 403, 'agent')
        if ap.is_flagged_agent(redis, subject):
            await ap.record_event(redis, subject, action='automation_flagged', path=path, user_agent=ua, notify=True)
            return _refuse(request, 403, 'agent')

        lock_left = ap.is_locked(redis, subject)
        if lock_left:
            return _refuse(request, 429, 'locked', retry_after=lock_left, minutes=max(1, lock_left // 60))

        limits = await ap.tenant_limits(redis, subject.tenant_id)
        violation = ap.check_request(redis, subject, limits, path)
        if violation:
            strikes, newly_locked = ap.register_strike(redis, subject)
            if newly_locked:
                await ap.record_event(
                    redis, subject, action='rate_limit_lock', path=path, user_agent=ua, notify=True, throttle_seconds=60,
                    extra={'rule': violation.rule, 'limit': violation.limit, 'current': violation.current, 'strikes': strikes},
                )
                return _refuse(request, 429, 'locked', retry_after=ap.LOCK_SECONDS, minutes=ap.LOCK_SECONDS // 60)
            await ap.record_event(
                redis, subject, action='rate_limit_exceeded', path=path, user_agent=ua, throttle_seconds=300,
                extra={'rule': violation.rule, 'limit': violation.limit, 'current': violation.current, 'strikes': strikes},
            )
            return _refuse(
                request, 429, 'limit', retry_after=violation.retry_after,
                label=ap.LIMIT_LABELS.get(violation.rule, violation.rule), limit=violation.limit, retry=violation.retry_after,
            )

        export = ap.is_export_path(path)
        if export:
            quota = ap.export_quota_exceeded(redis, subject, limits)
            if quota:
                await ap.record_event(
                    redis, subject, action='export_quota_exceeded', path=path, user_agent=ua, throttle_seconds=3600,
                    extra={'limit': quota.limit, 'current': quota.current},
                )
                return _refuse(request, 429, 'exports', retry_after=quota.retry_after, limit=quota.limit)

        if subject.browser_session and path.startswith('/v1/') and ap.LIST_PATH.match(path):
            ap.clamp_list_limit(request)
    except Exception:
        logger.exception('access_guard_failed path=%s', path)
        return await call_next(request)

    response = await call_next(request)
    if export and subject is not None and redis is not None and response.status_code == 200:
        content_type = (response.headers.get('content-type') or '').lower()
        if any(marker in content_type for marker in ap.EXPORT_CONTENT_TYPES):
            try:
                ap.count_export(redis, subject)
            except Exception:
                pass
    return response
