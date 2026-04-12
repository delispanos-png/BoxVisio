from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.control import TenantConnection
from app.services.ingestion.base import (
    ALL_OPERATIONAL_STREAMS,
    STREAM_TO_ENTITY,
    OperationalIngestStream,
    normalize_stream_name,
    normalize_stream_values,
)

SQL_CONNECTOR_ALIASES = {'sql_connector', 'pharmacyone_sql'}


def _normalize_source_type(connector_type: str, source_type: str | None) -> str:
    raw = str(source_type or '').strip().lower()
    if raw in {'sql', 'api', 'file'}:
        return raw
    connector = str(connector_type or '').strip().lower()
    if 'api' in connector:
        return 'api'
    if 'file' in connector or 'csv' in connector or 'excel' in connector or 'sftp' in connector:
        return 'file'
    return 'sql'


def _default_streams_for_connection(connector_type: str, source_type: str) -> list[OperationalIngestStream]:
    if source_type == 'api' or connector_type == 'external_api':
        return list(ALL_OPERATIONAL_STREAMS)
    if source_type == 'file' or connector_type == 'file_import':
        return ['sales_documents', 'purchase_documents', 'inventory_documents']
    return list(ALL_OPERATIONAL_STREAMS)


def _resolve_enabled_streams(conn: TenantConnection) -> list[OperationalIngestStream]:
    connector_type = str(conn.connector_type or '').strip().lower()
    source_type = _normalize_source_type(connector_type, conn.source_type)
    configured = conn.enabled_streams if isinstance(conn.enabled_streams, list) and conn.enabled_streams else conn.supported_streams
    normalized = normalize_stream_values([str(v) for v in configured] if isinstance(configured, list) else [])
    if normalized:
        return normalized
    return _default_streams_for_connection(connector_type, source_type)


def _api_connection_supports_stream(conn: TenantConnection, stream: OperationalIngestStream) -> bool:
    connector_type = str(conn.connector_type or '').strip().lower()
    source_type = _normalize_source_type(connector_type, conn.source_type)
    if source_type != 'api':
        return True

    endpoints = conn.stream_api_endpoint if isinstance(conn.stream_api_endpoint, dict) else {}
    if endpoints:
        for key, value in endpoints.items():
            if not isinstance(value, str) or not value.strip():
                continue
            token = str(key or '').strip()
            if not token:
                continue
            normalized = normalize_stream_name(token)
            if normalized == stream:
                return True
            lowered = token.lower().replace('-', '_').replace(' ', '_')
            if lowered in {'all', 'default', '*'}:
                return True

    params = conn.connection_parameters if isinstance(conn.connection_parameters, dict) else {}
    base_url = str(params.get('base_url') or '').strip()
    return bool(base_url)


def _candidate_sort_key(entry: dict[str, Any]) -> tuple[int, int]:
    priority = {'api': 0, 'sql': 1, 'file': 2}
    return (priority.get(str(entry.get('source_type') or ''), 9), int(entry.get('id') or 0))


def _connector_matches_preference(connector_type: str, preferred_connector: str | None) -> bool:
    preferred = str(preferred_connector or '').strip().lower()
    if not preferred:
        return True
    connector = str(connector_type or '').strip().lower()
    if preferred in SQL_CONNECTOR_ALIASES:
        return connector in SQL_CONNECTOR_ALIASES
    return connector == preferred


def _default_sql_jobs(tenant_slug: str) -> list[dict[str, Any]]:
    return [
        {
            'connector': 'sql_connector',
            'stream': stream,
            'entity': STREAM_TO_ENTITY[stream],
            'tenant_slug': tenant_slug,
            'payload': {},
            'attempt': 0,
        }
        for stream in ALL_OPERATIONAL_STREAMS
    ]


async def plan_tenant_sync_jobs(
    control_db: AsyncSession,
    *,
    tenant_id: int,
    tenant_slug: str,
    preferred_connector: str | None = None,
) -> list[dict[str, Any]]:
    rows = (
        await control_db.execute(
            select(TenantConnection).where(TenantConnection.tenant_id == tenant_id).order_by(TenantConnection.id.asc())
        )
    ).scalars().all()

    if not rows:
        return _default_sql_jobs(tenant_slug)
    active_rows = [row for row in rows if bool(getattr(row, 'is_active', True))]
    if not active_rows:
        return []
    if preferred_connector:
        preferred_rows = [
            row
            for row in active_rows
            if _connector_matches_preference(str(row.connector_type or ''), preferred_connector)
        ]
        if not preferred_rows:
            return []
        active_rows = preferred_rows

    candidates: list[dict[str, Any]] = []
    for conn in active_rows:
        connector_type = str(conn.connector_type or '').strip().lower() or 'sql_connector'
        source_type = _normalize_source_type(connector_type, conn.source_type)
        enabled_streams = _resolve_enabled_streams(conn)
        if not enabled_streams:
            continue
        candidates.append(
            {
                'id': conn.id,
                'connector_type': connector_type,
                'source_type': source_type,
                'enabled_streams': enabled_streams,
                'connection': conn,
            }
        )

    if not candidates:
        return _default_sql_jobs(tenant_slug)

    jobs: list[dict[str, Any]] = []
    for stream in ALL_OPERATIONAL_STREAMS:
        stream_candidates = [c for c in candidates if stream in c['enabled_streams']]
        if not stream_candidates:
            continue
        stream_candidates = [c for c in stream_candidates if _api_connection_supports_stream(c['connection'], stream)]
        if not stream_candidates:
            continue
        stream_candidates.sort(key=_candidate_sort_key)
        selected = stream_candidates[0]

        payload: dict[str, Any] = {}
        connection = selected['connection']
        params = connection.connection_parameters if isinstance(connection.connection_parameters, dict) else {}
        sync_defaults = params.get('sync_defaults')
        if isinstance(sync_defaults, dict):
            payload = dict(sync_defaults)

        jobs.append(
            {
                'connector': selected['connector_type'],
                'stream': stream,
                'entity': STREAM_TO_ENTITY[stream],
                'tenant_slug': tenant_slug,
                'payload': payload,
                'attempt': 0,
            }
        )

    if not jobs:
        return _default_sql_jobs(tenant_slug)

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for job in jobs:
        key = (str(job['connector']), str(job['stream']))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(job)
    return deduped
