#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import psycopg

sys.path.append('/opt/cloudon-bi/backend')

from app.core.config import settings  # noqa: E402
from app.services.querypacks import load_querypack  # noqa: E402


def _control_dsn() -> str:
    db_name = settings.control_database_url_sync.rsplit('/', 1)[-1]
    return (
        f"host={settings.tenant_db_host} port={settings.tenant_db_port} "
        f"dbname={db_name} user={settings.tenant_db_superuser} password={settings.tenant_db_superpass}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description='Apply default SQL querypack to all existing SQL tenant connections.')
    parser.add_argument('--dry-run', action='store_true', help='Show what would change without writing.')
    parser.add_argument(
        '--enable-operating-expenses',
        action='store_true',
        help='Keep operating_expenses enabled by default for all updated SQL connections.',
    )
    parser.add_argument(
        '--reset-sync-state',
        action='store_true',
        help='Set sync_status=never and last_sync_at=NULL after update.',
    )
    args = parser.parse_args()

    pack = load_querypack('erp_sql', 'default')
    supported_streams = [
        'sales_documents',
        'purchase_documents',
        'inventory_documents',
        'cash_transactions',
        'supplier_balances',
        'customer_balances',
        'operating_expenses',
    ]
    enabled_streams = [s for s in supported_streams if s != 'operating_expenses']
    if args.enable_operating_expenses:
        enabled_streams = list(supported_streams)

    stream_query_mapping = {
        'sales_documents': pack.sales_sql,
        'purchase_documents': pack.purchases_sql,
        'inventory_documents': pack.inventory_sql or '',
        'cash_transactions': pack.cashflow_sql or '',
        'supplier_balances': pack.supplier_balances_sql or '',
        'customer_balances': pack.customer_balances_sql or '',
        'operating_expenses': pack.expenses_sql or '',
    }

    updated = 0
    with psycopg.connect(_control_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, tenant_id, connector_type
                FROM tenant_connections
                WHERE connector_type IN ('sql_connector', 'sql', 'erp_sql')
                ORDER BY tenant_id, id
                """
            )
            rows = cur.fetchall()
            print(f'found_sql_connections={len(rows)}')
            for row in rows:
                conn_id, tenant_id, connector_type = row
                if args.dry_run:
                    print(f'dry_run connection_id={conn_id} tenant_id={tenant_id} connector={connector_type}')
                    continue
                cur.execute(
                    """
                    UPDATE tenant_connections
                    SET
                      sales_query_template = %s,
                      purchases_query_template = %s,
                      inventory_query_template = %s,
                      cashflow_query_template = %s,
                      supplier_balances_query_template = %s,
                      customer_balances_query_template = %s,
                      supported_streams = %s::jsonb,
                      enabled_streams = %s::jsonb,
                      stream_query_mapping = %s::jsonb,
                      source_type = 'sql',
                      updated_at = NOW()
                    WHERE id = %s
                    """,
                    (
                        pack.sales_sql,
                        pack.purchases_sql,
                        pack.inventory_sql or '',
                        pack.cashflow_sql or '',
                        pack.supplier_balances_sql or '',
                        pack.customer_balances_sql or '',
                        json.dumps(supported_streams),
                        json.dumps(enabled_streams),
                        json.dumps(stream_query_mapping),
                        conn_id,
                    ),
                )
                if args.reset_sync_state:
                    cur.execute(
                        """
                        UPDATE tenant_connections
                        SET sync_status = 'never', last_sync_at = NULL, updated_at = NOW()
                        WHERE id = %s
                        """,
                        (conn_id,),
                    )
                updated += 1
        if args.dry_run:
            conn.rollback()
        else:
            conn.commit()

    print(f'updated_connections={updated}')


if __name__ == '__main__':
    main()

