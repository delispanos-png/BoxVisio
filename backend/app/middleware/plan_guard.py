from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import expected_audience_for_host, safe_decode
from app.models.control import Tenant
from app.services.subscriptions import apply_subscription_time_transitions, get_or_create_subscription, is_feature_enabled
from app.services.subscription_features import feature_key_for_path, feature_label, feature_minimum_plan


def required_feature_for_path(path: str) -> str | None:
    return feature_key_for_path(path)


def _wants_html(request: Request) -> bool:
    accept = (request.headers.get('accept') or '').lower()
    return request.url.path.startswith('/tenant/') and 'text/html' in accept


def _upgrade_required_response(request: Request, tenant: Tenant, feature: str) -> HTMLResponse | JSONResponse:
    label = feature_label(feature)
    minimum_plan = feature_minimum_plan(feature)
    current_plan = tenant.plan.value.capitalize() if getattr(tenant, 'plan', None) else 'τρέχον'
    if not _wants_html(request):
        return JSONResponse(
            status_code=403,
            content={
                'detail': f'Η δυνατότητα "{label}" δεν είναι διαθέσιμη στο πακέτο {current_plan}.',
                'feature': feature,
                'feature_label': label,
                'current_plan': current_plan,
                'required_plan': minimum_plan,
            },
        )
    html = f"""<!doctype html>
<html lang="el">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Αναβάθμιση πακέτου</title>
    <link rel="stylesheet" href="/static/vendor/bootstrap/css/bootstrap.min.css">
    <link rel="stylesheet" href="/static/css/boxvisio-unified.css?v=20260528logo1">
  <style>
    body {{ background:#f4f6fb; color:#111827; }}
    .upgrade-wrap {{ min-height:100vh; display:flex; align-items:center; justify-content:center; padding:32px; }}
    .upgrade-card {{ max-width:760px; width:100%; background:#fff; border:1px solid #dbe3f1; border-radius:16px; box-shadow:0 24px 70px rgba(15,23,42,.10); padding:34px; }}
    .upgrade-pill {{ display:inline-flex; align-items:center; gap:8px; padding:8px 12px; border-radius:999px; background:#ede9fe; color:#4f46e5; font-weight:800; font-size:12px; text-transform:uppercase; letter-spacing:.04em; }}
    .upgrade-title {{ font-size:30px; font-weight:900; margin:18px 0 10px; }}
    .upgrade-text {{ color:#53627a; font-size:16px; line-height:1.65; }}
    .upgrade-feature {{ margin:22px 0; padding:18px; border:1px solid #dbe3f1; border-radius:12px; background:#f8fafc; }}
    .upgrade-feature strong {{ display:block; font-size:13px; color:#6b7890; text-transform:uppercase; letter-spacing:.04em; }}
    .upgrade-feature span {{ display:block; font-size:22px; font-weight:900; margin-top:4px; }}
    .upgrade-actions {{ display:flex; gap:12px; flex-wrap:wrap; margin-top:22px; }}
  </style>
</head>
<body>
  <main class="upgrade-wrap">
    <section class="upgrade-card">
      <div class="upgrade-pill">Δυνατότητα πακέτου {minimum_plan}</div>
      <h1 class="upgrade-title">Δεν είναι διαθέσιμο στο πακέτο σου</h1>
      <p class="upgrade-text">
        Η δυνατότητα που επέλεξες υπάρχει στο BoxVisio BI, αλλά δεν περιλαμβάνεται στο πακέτο
        <strong>{current_plan}</strong>. Για να τη χρησιμοποιήσεις, χρειάζεται αναβάθμιση στο πακέτο
        <strong>{minimum_plan}</strong> ή σε μεγαλύτερο.
      </p>
      <div class="upgrade-feature">
        <strong>Δυνατότητα</strong>
        <span>{label}</span>
      </div>
      <div class="upgrade-actions">
        <a class="btn btn-primary" href="/tenant/dashboard">Επιστροφή στο Dashboard</a>
        <a class="btn btn-outline-primary" href="/tenant/manual">Δες τη Βοήθεια</a>
      </div>
    </section>
  </main>
</body>
</html>"""
    return HTMLResponse(status_code=403, content=html)


async def plan_guard_middleware(request: Request, call_next):
    feature = required_feature_for_path(request.url.path)
    if not feature:
        return await call_next(request)

    auth_header = request.headers.get('Authorization', '')
    token = None
    if auth_header.startswith('Bearer '):
        token = auth_header.split(' ', 1)[1]
    else:
        token = request.cookies.get('access_token')
    if not token:
        return await call_next(request)
    expected_aud = expected_audience_for_host(request.headers.get('host'))
    payload = safe_decode(token, audience=expected_aud, token_type='access')
    if not payload:
        return await call_next(request)

    tenant_id = payload.get('tenant_id')
    if tenant_id is None:
        return await call_next(request)

    session_maker = request.app.state.control_sessionmaker
    async with session_maker() as db:  # type: AsyncSession
        tenant = (await db.execute(select(Tenant).where(Tenant.id == tenant_id))).scalar_one_or_none()
        if not tenant:
            return JSONResponse(status_code=400, content={'detail': 'Tenant not found'})

        subscription = await get_or_create_subscription(db, tenant)
        if await apply_subscription_time_transitions(db, tenant, subscription):
            await db.flush()
        if not await is_feature_enabled(db, tenant, subscription, feature):
            return _upgrade_required_response(request, tenant, feature)
        await db.commit()

    return await call_next(request)
