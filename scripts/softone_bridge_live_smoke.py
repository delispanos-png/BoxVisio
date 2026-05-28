#!/usr/bin/env python3
"""Smoke-test the live SoftOne JavaScript bridge through saved tenant API settings."""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict
from typing import Any

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models.control import Tenant, TenantConnection
from app.services.ingestion.base import ConnectorContext
from app.services.ingestion.external_api_connector import ExternalApiIngestConnector


STREAMS = [
    "sales_documents",
    "purchase_documents",
    "inventory_documents",
    "item_master",
    "cash_transactions",
    "supplier_balances",
    "customer_balances",
    "operating_expenses",
    "supplier_orders",
]


def _context(tenant: Tenant, conn: TenantConnection) -> ConnectorContext:
    return ConnectorContext(
        tenant_slug=tenant.slug,
        incremental_column=conn.incremental_column,
        id_column=conn.id_column,
        date_column=conn.date_column,
        branch_column=conn.branch_column,
        item_column=conn.item_column,
        amount_column=conn.amount_column,
        cost_column=conn.cost_column,
        qty_column=conn.qty_column,
        source_type=conn.source_type,
        supported_streams=list(conn.supported_streams or []),
        enabled_streams=list(conn.enabled_streams or []),
        connection_parameters=dict(conn.connection_parameters or {}),
        stream_query_mapping=dict(conn.stream_query_mapping or {}),
        stream_field_mapping=dict(conn.stream_field_mapping or {}),
        stream_file_mapping=dict(conn.stream_file_mapping or {}),
        stream_api_endpoint=dict(conn.stream_api_endpoint or {}),
    )


def _public_connection_info(conn: TenantConnection) -> dict[str, Any]:
    params = conn.connection_parameters if isinstance(conn.connection_parameters, dict) else {}
    base_url = str(params.get("base_url") or "")
    redacted_url = base_url
    if "clientID=" in redacted_url:
        redacted_url = redacted_url.split("clientID=", 1)[0] + "clientID=<redacted>"
    return {
        "connection_id": conn.id,
        "connector_type": conn.connector_type,
        "source_type": conn.source_type,
        "base_url": redacted_url,
        "auth_type": str(params.get("auth_type") or ""),
    }


def _records_count(response: dict[str, Any]) -> int:
    records = response.get("records")
    if isinstance(records, list):
        return len(records)
    count = response.get("count")
    try:
        return int(count)
    except Exception:
        return 0


def _error_text(exc: BaseException) -> str:
    text = str(exc).replace("\n", " ").strip()
    return text[:500]


def _choose_connections(
    session: Session,
    tenant_slug: str | None,
    connection_id: int | None,
) -> list[tuple[Tenant, TenantConnection]]:
    stmt = (
        select(Tenant, TenantConnection)
        .join(TenantConnection, TenantConnection.tenant_id == Tenant.id)
        .order_by(TenantConnection.last_sync_at.desc().nullslast(), TenantConnection.updated_at.desc())
    )
    if tenant_slug:
        stmt = stmt.where(Tenant.slug == tenant_slug)
    if connection_id:
        stmt = stmt.where(TenantConnection.id == connection_id)
    else:
        stmt = stmt.where(TenantConnection.is_active.is_(True))
    pairs = list(session.execute(stmt).all())
    if connection_id:
        return pairs
    return [
        (tenant, conn)
        for tenant, conn in pairs
        if conn.connector_type == "external_api"
        or conn.source_type == "api"
        or bool((conn.connection_parameters or {}).get("base_url"))
    ]


def list_connections(args: argparse.Namespace) -> int:
    engine = create_engine(settings.control_database_url_sync, future=True)
    with Session(engine) as session:
        pairs = _choose_connections(session, args.tenant, args.connection_id)
        rows = [
            {
                "tenant": tenant.slug,
                **_public_connection_info(conn),
                "active": bool(conn.is_active),
            }
            for tenant, conn in pairs
        ]
    print(json.dumps({"connections": rows}, ensure_ascii=False, indent=2))
    return 0


def run(args: argparse.Namespace) -> int:
    engine = create_engine(settings.control_database_url_sync, future=True)
    connector = ExternalApiIngestConnector()
    selected_streams = args.streams.split(",") if args.streams else STREAMS
    selected_streams = [s.strip() for s in selected_streams if s.strip()]
    payload = {
        "limit": args.limit,
        "balanceLimit": args.balance_limit,
    }
    if args.from_date:
        payload["fromDate"] = args.from_date
    if args.to_date:
        payload["toDate"] = args.to_date

    results: list[dict[str, Any]] = []
    with Session(engine) as session:
        pairs = _choose_connections(session, args.tenant, args.connection_id)
        if args.first and pairs:
            pairs = pairs[:1]
        if not pairs:
            print(json.dumps({"success": False, "error": "No active external_api SoftOne connections found."}, ensure_ascii=False))
            return 2

        for tenant, conn in pairs:
            ctx = _context(tenant, conn)
            conn_info = _public_connection_info(conn)
            tenant_result: dict[str, Any] = {
                "tenant": tenant.slug,
                "connection": conn_info,
                "checks": [],
            }

            health_body: dict[str, Any] = {}
            connector._inject_softone_auth(body=health_body, context=ctx, payload={})
            health_endpoint = f"{str(ctx.connection_parameters.get('base_url') or '').rstrip('/')}/HealthCheckBIBridge"
            started = time.monotonic()
            try:
                health = connector._call_endpoint(endpoint=health_endpoint, context=ctx, body=health_body)
                tenant_result["checks"].append(
                    {
                        "name": "HealthCheckBIBridge",
                        "ok": bool(health.get("success", True)),
                        "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
                        "version": health.get("version"),
                    }
                )
            except Exception as exc:
                tenant_result["checks"].append(
                    {
                        "name": "HealthCheckBIBridge",
                        "ok": False,
                        "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
                        "error": _error_text(exc),
                    }
                )

            for stream in selected_streams:
                body = connector._build_request_body(stream=stream, context=ctx, payload=payload)
                endpoint = connector._resolve_stream_endpoint(stream=stream, context=ctx)
                if not endpoint:
                    raise RuntimeError("Could not resolve stream endpoint")
                started = time.monotonic()
                try:
                    response = connector._call_endpoint(endpoint=endpoint, context=ctx, body=body)
                    tenant_result["checks"].append(
                        {
                            "name": stream,
                            "ok": bool(response.get("success", True)),
                            "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
                            "count": _records_count(response),
                            "stream_code": response.get("stream_code"),
                        }
                    )
                except Exception as exc:
                    tenant_result["checks"].append(
                        {
                            "name": stream,
                            "ok": False,
                            "elapsed_ms": round((time.monotonic() - started) * 1000, 1),
                            "error": _error_text(exc),
                        }
                    )
            results.append(tenant_result)

    success = all(check.get("ok") for tenant_result in results for check in tenant_result["checks"])
    print(json.dumps({"success": success, "results": results}, ensure_ascii=False, indent=2))
    return 0 if success else 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke-test live SoftOne BI bridge endpoints.")
    parser.add_argument("--tenant", help="Tenant slug. Defaults to all active external_api tenants.")
    parser.add_argument("--connection-id", type=int, help="Specific tenant_connections.id to test, even if inactive.")
    parser.add_argument("--list-connections", action="store_true", help="List redacted candidate API connections and exit.")
    parser.add_argument("--first", action="store_true", help="Only test the most recently synced/updated matching connection.")
    parser.add_argument("--streams", help="Comma-separated stream list. Defaults to all streams.")
    parser.add_argument("--limit", type=int, default=1, help="Per-stream TOP limit for document streams.")
    parser.add_argument("--balance-limit", type=int, default=100, help="Balance stream limit.")
    parser.add_argument("--from-date", help="Optional YYYY-MM-DD fromDate.")
    parser.add_argument("--to-date", help="Optional YYYY-MM-DD toDate.")
    args = parser.parse_args()
    if args.list_connections:
        return list_connections(args)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
