#!/usr/bin/env python
import argparse
import sys

import psycopg

sys.path.append('/app')
sys.path.append('/opt/cloudon-bi/backend')

from app.core.config import settings  # noqa: E402


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

    admin_dsn = (
        f"host={settings.tenant_db_host} port={settings.tenant_db_port} "
        f"dbname=postgres user={settings.tenant_db_superuser} password={settings.tenant_db_superpass}"
    )

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '{db_user}') "
                f"THEN CREATE ROLE {db_user} LOGIN PASSWORD '{db_password}'; "
                f"ELSE ALTER ROLE {db_user} WITH PASSWORD '{db_password}'; END IF; END $$;"
            )
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE {db_name} OWNER {db_user}')


if __name__ == '__main__':
    main()
