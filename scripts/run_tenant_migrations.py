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


def _tenant_db(tenant_slug: str) -> tuple[str, str]:
    """(db_name, db_user) for a tenant slug."""
    control_dsn = (
        f"host={settings.tenant_db_host} port={settings.tenant_db_port} "
        f"dbname={settings.control_database_url_sync.rsplit('/', 1)[-1]} "
        f"user={settings.tenant_db_superuser} password={settings.tenant_db_superpass}"
    )
    with psycopg.connect(control_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT db_name, db_user FROM tenants WHERE slug = %s",
                (tenant_slug,),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError(f'tenant not found: {tenant_slug}')
            return row[0], row[1]


def _grant_tenant_privileges(db_name: str, db_user: str) -> list[str]:
    """Hand every object in `public` to the tenant role, and keep doing so.

    This script runs alembic as the SUPERUSER, so anything a migration creates is
    owned by postgres and the tenant role cannot touch it — the tenant's own
    queries then fail with "permission denied for table ...". (The provisioning
    wizard does not have this problem: it migrates as the tenant user, which is
    why freshly provisioned tenants look fine and only migrated-in-place ones
    break.)

    ALTER DEFAULT PRIVILEGES covers every future migration run, so this is a
    one-time repair per tenant rather than a step to remember.

    Returns the list of tables that were still unreadable afterwards (should be
    empty; non-empty means something needs a human).
    """
    dsn = (
        f"host={settings.tenant_db_host} port={settings.tenant_db_port} dbname={db_name} "
        f"user={settings.tenant_db_superuser} password={settings.tenant_db_superpass}"
    )
    ident = psycopg.sql.Identifier
    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            statements = [
                psycopg.sql.SQL('GRANT USAGE, CREATE ON SCHEMA public TO {}').format(ident(db_user)),
                psycopg.sql.SQL('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO {}').format(ident(db_user)),
                psycopg.sql.SQL('GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO {}').format(ident(db_user)),
                psycopg.sql.SQL('GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO {}').format(ident(db_user)),
                psycopg.sql.SQL(
                    'ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA public GRANT ALL ON TABLES TO {}'
                ).format(ident(settings.tenant_db_superuser), ident(db_user)),
                psycopg.sql.SQL(
                    'ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA public GRANT ALL ON SEQUENCES TO {}'
                ).format(ident(settings.tenant_db_superuser), ident(db_user)),
            ]
            for statement in statements:
                cur.execute(statement)

            cur.execute(
                """
                SELECT c.relname
                  FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                 WHERE n.nspname = 'public'
                   AND c.relkind IN ('r', 'p', 'v', 'm')
                   AND NOT has_table_privilege(%s, c.oid, 'SELECT')
                 ORDER BY 1
                """,
                (db_user,),
            )
            return [r[0] for r in cur.fetchall()]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--tenant', required=True)
    parser.add_argument(
        '--grants-only',
        action='store_true',
        help='skip alembic, only repair tenant-role privileges',
    )
    args = parser.parse_args()

    db_name, db_user = _tenant_db(args.tenant)

    if not args.grants_only:
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

    # Always reconcile privileges, migration or not: alembic ran as the superuser,
    # so anything it just created is invisible to the tenant role until now.
    unreadable = _grant_tenant_privileges(db_name, db_user)
    if unreadable:
        print(
            f'[run_tenant_migrations] WARNING {args.tenant}: {len(unreadable)} objects still '
            f'unreadable by {db_user}: {", ".join(unreadable[:10])}'
        )
        raise SystemExit(1)
    print(f'[run_tenant_migrations] privileges reconciled for {db_user} on {db_name}')


if __name__ == '__main__':
    main()
