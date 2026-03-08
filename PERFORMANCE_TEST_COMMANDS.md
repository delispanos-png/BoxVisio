# PERFORMANCE_TEST_COMMANDS.md

## 1) Restart services after code changes
```bash
docker compose restart api worker
```

## 2) Run full benchmark (legacy fan-out vs consolidated)
```bash
docker compose exec -T api /opt/cloudon-bi/.venv/bin/python /opt/cloudon-bi/scripts/benchmark_dashboard_perf.py
```

## 3) Clear KPI cache manually
```bash
docker compose exec -T api /opt/cloudon-bi/.venv/bin/python - <<'PY'
import asyncio
from app.services.kpi_cache import invalidate_tenant_cache
async def main():
    print(await invalidate_tenant_cache())
asyncio.run(main())
PY
```

## 4) Spot-check consolidated endpoints (with token script)
```bash
docker compose exec -T api /opt/cloudon-bi/.venv/bin/python - <<'PY'
import asyncio, time
from datetime import date, timedelta
import httpx
from sqlalchemy import select
from app.core.security import create_access_token
from app.db.control_session import ControlSessionLocal
from app.models.control import User, RoleName

async def main():
    async with ControlSessionLocal() as db:
        u=(await db.execute(select(User).where(User.role==RoleName.tenant_admin, User.is_active.is_(True)).order_by(User.id.asc()))).scalars().first()
        t=create_access_token(subject=str(u.id), tenant_id=u.tenant_id, role=u.role.value, audience='tenant')
    h={'Authorization':f'Bearer {t}','Host':'bi.boxvisio.com'}
    frm=(date.today()-timedelta(days=30)).isoformat(); to=date.today().isoformat()
    urls=[
      f'/v1/dashboard/executive-summary?from={frm}&to={to}',
      f'/v1/dashboard/finance-summary?from={frm}&to={to}&supplier_limit=50&account_limit=50',
      f'/v1/streams/inventory/summary?as_of={to}',
      f'/v1/streams/cash/summary?from={frm}&to={to}',
    ]
    async with httpx.AsyncClient(base_url='http://127.0.0.1:8000', headers=h, timeout=120.0) as c:
        for url in urls:
            t0=time.perf_counter(); r=await c.get(url); ms=(time.perf_counter()-t0)*1000
            print(url, r.status_code, f'{ms:.1f}ms', 'cache=', r.headers.get('X-KPI-Cache'), 'api=', r.headers.get('X-KPI-API-Time-Ms'), 'db=', r.headers.get('X-KPI-DB-Time-Ms'))

asyncio.run(main())
PY
```

## 5) Review KPI performance logs
```bash
docker compose logs --since=10m api | rg "kpi_request_perf"
```
