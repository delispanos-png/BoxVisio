#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import psycopg


SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parents[1]
BACKEND_ROOT = PROJECT_ROOT / "backend"
sys.path.insert(0, str(BACKEND_ROOT))

from app.core.config import settings  # noqa: E402


def _control_dsn() -> str:
    db_name = settings.control_database_url_sync.rsplit("/", 1)[-1]
    return (
        f"host={settings.tenant_db_host} port={settings.tenant_db_port} "
        f"dbname={db_name} user={settings.tenant_db_superuser} password={settings.tenant_db_superpass}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Force SQL connector as default on tenants that already have SQL SoftOne connection. "
            "Disables active external_api connectors on those tenants."
        )
    )
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing")
    parser.add_argument(
        "--keep-external-active",
        action="store_true",
        help="Do not deactivate external_api connectors; still normalize SQL connectors as active default",
    )
    args = parser.parse_args()

    sql_aliases = ("sql_connector", "pharmacyone_sql")
    changed_sql = 0
    changed_external = 0
    touched_tenants: list[int] = []

    with psycopg.connect(_control_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT tenant_id
                FROM tenant_connections
                WHERE connector_type = ANY(%s)
                  AND COALESCE(source_type, 'sql') = 'sql'
                ORDER BY tenant_id
                """,
                (list(sql_aliases),),
            )
            tenant_ids = [int(row[0]) for row in cur.fetchall()]

            for tenant_id in tenant_ids:
                # Normalize SQL connectors as active + sql source_type.
                if not args.dry_run:
                    cur.execute(
                        """
                        UPDATE tenant_connections
                        SET is_active = TRUE,
                            source_type = 'sql',
                            updated_at = NOW()
                        WHERE tenant_id = %s
                          AND connector_type = ANY(%s)
                        """,
                        (tenant_id, list(sql_aliases)),
                    )
                    changed_sql += int(cur.rowcount or 0)
                else:
                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM tenant_connections
                        WHERE tenant_id = %s
                          AND connector_type = ANY(%s)
                        """,
                        (tenant_id, list(sql_aliases)),
                    )
                    changed_sql += int(cur.fetchone()[0] or 0)

                # Optional: disable external_api to make SQL deterministic default.
                if not args.keep_external_active:
                    if not args.dry_run:
                        cur.execute(
                            """
                            UPDATE tenant_connections
                            SET is_active = FALSE,
                                updated_at = NOW()
                            WHERE tenant_id = %s
                              AND connector_type = 'external_api'
                              AND is_active = TRUE
                            """,
                            (tenant_id,),
                        )
                        changed_external += int(cur.rowcount or 0)
                    else:
                        cur.execute(
                            """
                            SELECT COUNT(*)
                            FROM tenant_connections
                            WHERE tenant_id = %s
                              AND connector_type = 'external_api'
                              AND is_active = TRUE
                            """,
                            (tenant_id,),
                        )
                        changed_external += int(cur.fetchone()[0] or 0)

                touched_tenants.append(tenant_id)

        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()

    print(
        json.dumps(
            {
                "dry_run": bool(args.dry_run),
                "tenants_with_sql_connection": len(touched_tenants),
                "sql_rows_updated": changed_sql,
                "external_api_rows_deactivated": changed_external,
                "tenants_sample": touched_tenants[:20],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
