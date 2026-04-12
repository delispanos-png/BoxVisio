from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from redis import Redis
from sqlalchemy import select, text

from app.api import admin, auth, ingest, kpi, ui, whmcs
from app.core.config import settings
from app.core.logging import configure_logging
from app.db.control_session import ControlSessionLocal
from app.models.control import Tenant
from app.middleware.error_handler import error_handler_middleware
from app.middleware.admin_audit import admin_audit_middleware
from app.middleware.csrf import csrf_middleware
from app.middleware.host_access_guard import host_access_guard_middleware
from app.middleware.plan_guard import plan_guard_middleware
from app.middleware.rate_limit import rate_limit_middleware
from app.middleware.rbac_guard import rbac_guard_middleware
from app.middleware.kpi_performance import kpi_performance_middleware
from app.middleware.request_logging import request_logging_middleware
from app.middleware.secure_headers import secure_headers_middleware
from app.middleware.subscription_guard import subscription_guard_middleware
from app.middleware.ui_auth_redirect import ui_auth_redirect_middleware
from app.observability.metrics import (
    db_pool_checked_in,
    db_pool_checked_out,
    db_pool_overflow,
    db_pool_size,
    ingest_dlq_depth,
    ingest_queue_depth,
    render_metrics,
    tenant_engine_cache_size,
)
from app.observability.sentry import init_sentry
from app.db.control_session import get_control_pool_stats
from app.db.tenant_manager import get_tenant_pool_stats
from app.services.ingestion.queueing import tenant_dlq_name, tenant_queue_name

configure_logging()
init_sentry()

app = FastAPI(title=settings.project_name, version=settings.app_version)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)
app.middleware('http')(secure_headers_middleware)
app.middleware('http')(rate_limit_middleware)
app.middleware('http')(host_access_guard_middleware)
app.middleware('http')(ui_auth_redirect_middleware)
app.middleware('http')(rbac_guard_middleware)
app.middleware('http')(csrf_middleware)
app.middleware('http')(plan_guard_middleware)
app.middleware('http')(subscription_guard_middleware)
app.middleware('http')(admin_audit_middleware)
app.middleware('http')(request_logging_middleware)
app.middleware('http')(kpi_performance_middleware)
app.middleware('http')(error_handler_middleware)

STATIC_DIR = Path(__file__).resolve().parent / 'static'
app.mount('/static', StaticFiles(directory=str(STATIC_DIR)), name='static')

app.include_router(auth.router)
app.include_router(admin.router)
app.include_router(ingest.router)
app.include_router(kpi.router)
app.include_router(whmcs.router)
app.include_router(ui.router)

app.state.control_sessionmaker = ControlSessionLocal


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    path = request.url.path
    # UI safety net: never render raw validation JSON for browser form pages.
    if path.startswith('/admin/') or path.startswith('/tenant/') or path in {'/login', '/logout'}:
        referer = request.headers.get('referer')
        if referer:
            return RedirectResponse(url=referer, status_code=303)
        if path.startswith('/admin/'):
            return RedirectResponse(url='/admin/dashboard', status_code=303)
        return RedirectResponse(url='/login', status_code=303)

    request_id = getattr(request.state, 'request_id', None)
    payload = {'detail': exc.errors()}
    if request_id:
        payload['request_id'] = request_id
    return JSONResponse(status_code=422, content=payload)


@app.api_route('/health', methods=['GET', 'HEAD'])
async def health():
    return {'status': 'ok', 'service': settings.project_name}


@app.api_route('/ready', methods=['GET', 'HEAD'])
async def ready():
    async with ControlSessionLocal() as db:
        await db.execute(text('SELECT 1'))
    Redis.from_url(settings.redis_url).ping()
    return {'status': 'ready', 'service': settings.project_name}


@app.get('/metrics')
async def metrics():
    control_stats = get_control_pool_stats()
    db_pool_checked_in.labels(pool='control').set(control_stats['checked_in'])
    db_pool_checked_out.labels(pool='control').set(control_stats['checked_out'])
    db_pool_size.labels(pool='control').set(control_stats['size'])
    db_pool_overflow.labels(pool='control').set(control_stats['overflow'])

    tenant_stats = get_tenant_pool_stats()
    tenant_engine_cache_size.set(tenant_stats.get('_cache', {}).get('size', 0))
    for tenant_key, stats in tenant_stats.items():
        if tenant_key == '_cache':
            continue
        pool_name = f'tenant:{tenant_key}'
        db_pool_checked_in.labels(pool=pool_name).set(stats['checked_in'])
        db_pool_checked_out.labels(pool=pool_name).set(stats['checked_out'])
        db_pool_size.labels(pool=pool_name).set(stats['size'])
        db_pool_overflow.labels(pool=pool_name).set(stats['overflow'])

    redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
    async with ControlSessionLocal() as db:
        tenant_slugs = list((await db.execute(select(Tenant.slug).where(Tenant.status != 'terminated'))).scalars().all())
    for slug in tenant_slugs:
        ingest_queue_depth.labels(tenant=slug).set(redis_client.llen(tenant_queue_name(slug)))
        ingest_dlq_depth.labels(tenant=slug).set(redis_client.llen(tenant_dlq_name(slug)))

    payload, content_type = render_metrics()
    return Response(content=payload, media_type=content_type)
