#!/usr/bin/env python
import argparse
import os
from pathlib import Path
import subprocess
import sys

import psycopg

sys.path.append('/app')
sys.path.append('/opt/cloudon-bi/backend')

from app.core.config import settings  # noqa: E402


def _latest_tenant_head(alembic_ini: Path) -> str:
    """Resolve the latest *tenant* migration head dynamically.

    The alembic version directory holds both control and tenant migrations, so a plain
    ``upgrade head`` is ambiguous (two heads). Tenant revisions are always suffixed
    ``_tenant``, so we pick the single tenant head straight from alembic's own head list.
    This never goes stale when new tenant migrations are added — no manual pin to forget.
    """
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    cfg = Config(str(alembic_ini))
    # alembic.ini uses a relative script_location; make it absolute so head detection works
    # regardless of the current working directory.
    cfg.set_main_option('script_location', str(alembic_ini.parent / 'alembic'))
    script = ScriptDirectory.from_config(cfg)
    tenant_heads = [h for h in script.get_heads() if str(h).endswith('_tenant')]
    if len(tenant_heads) != 1:
        raise RuntimeError(
            f'expected exactly one tenant migration head, found {tenant_heads} '
            f'(all heads: {script.get_heads()})'
        )
    return tenant_heads[0]


def _tenant_db_name(tenant_slug: str) -> str:
    control_dsn = (
        f"host={settings.tenant_db_host} port={settings.tenant_db_port} "
        f"dbname={settings.control_database_url_sync.rsplit('/', 1)[-1]} "
        f"user={settings.tenant_db_superuser} password={settings.tenant_db_superpass}"
    )
    with psycopg.connect(control_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT db_name FROM tenants WHERE slug = %s",
                (tenant_slug,),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f'tenant not found: {tenant_slug}')
            return row[0]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    args = parser.parse_args()

    db_name = _tenant_db_name(args.tenant)
    url = settings.tenant_database_url_template_sync.format(
        user=settings.tenant_db_superuser,
        password=settings.tenant_db_superpass,
        db_name=db_name,
    )

    env = os.environ.copy()
    env['MIGRATION_TARGET'] = 'tenant'
    env['TENANT_MIGRATION_URL'] = url
    backend_root = Path(__file__).resolve().parents[1] / 'backend'
    alembic_ini = backend_root / 'alembic.ini'
    tenant_head = _latest_tenant_head(alembic_ini)
    print(f'[run_tenant_migrations] upgrading {args.tenant} -> tenant head {tenant_head}')
    subprocess.run(
        [sys.executable, '-m', 'alembic', '-c', str(alembic_ini), 'upgrade', tenant_head],
        env=env,
        check=True,
        cwd=str(backend_root),
    )


if __name__ == '__main__':
    main()
