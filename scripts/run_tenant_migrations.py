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

# NOTE: alembic config currently exposes both control + tenant heads.
# Keep tenant migrations pinned to the latest tenant revision to avoid
# ambiguous "head" errors.
TENANT_MIGRATION_HEAD = '20260304_0016_tenant'


def _tenant_creds(tenant_slug: str) -> tuple[str, str, str]:
    control_dsn = (
        f"host={settings.tenant_db_host} port={settings.tenant_db_port} "
        f"dbname={settings.control_database_url_sync.rsplit('/', 1)[-1]} "
        f"user={settings.tenant_db_superuser} password={settings.tenant_db_superpass}"
    )
    with psycopg.connect(control_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT db_name, db_user, db_password FROM tenants WHERE slug = %s",
                (tenant_slug,),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f'tenant not found: {tenant_slug}')
            return row[0], row[1], row[2]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    args = parser.parse_args()

    db_name, db_user, db_password = _tenant_creds(args.tenant)
    url = settings.tenant_database_url_template_sync.format(
        user=db_user,
        password=db_password,
        db_name=db_name,
    )

    env = os.environ.copy()
    env['MIGRATION_TARGET'] = 'tenant'
    env['TENANT_MIGRATION_URL'] = url
    backend_root = Path(__file__).resolve().parents[1] / 'backend'
    alembic_ini = backend_root / 'alembic.ini'
    subprocess.run(
        [sys.executable, '-m', 'alembic', '-c', str(alembic_ini), 'upgrade', TENANT_MIGRATION_HEAD],
        env=env,
        check=True,
        cwd=str(backend_root),
    )


if __name__ == '__main__':
    main()
