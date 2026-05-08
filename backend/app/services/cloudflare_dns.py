from __future__ import annotations

import json
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from app.core.config import settings


API_BASE = 'https://api.cloudflare.com/client/v4'


def cloudflare_configured() -> bool:
    return bool(
        settings.tenant_subdomain_auto_dns_enabled
        and settings.cloudflare_api_token
        and settings.cloudflare_zone_id
        and settings.tenant_domain_root
        and settings.cloudflare_dns_target
    )


def tenant_hostname(slug: str) -> str:
    root = settings.tenant_domain_root.strip('.')
    return f'{slug}.{root}'


def _request(method: str, path: str, payload: dict | None = None) -> dict:
    body = None if payload is None else json.dumps(payload).encode('utf-8')
    req = Request(
        f'{API_BASE}{path}',
        data=body,
        method=method,
        headers={
            'Authorization': f'Bearer {settings.cloudflare_api_token}',
            'Content-Type': 'application/json',
        },
    )
    with urlopen(req, timeout=20) as response:
        return json.loads(response.read().decode('utf-8'))


def ensure_tenant_dns_record(slug: str) -> dict[str, object]:
    if not cloudflare_configured():
        return {'status': 'skipped', 'reason': 'cloudflare_not_configured'}

    name = tenant_hostname(slug)
    zone_id = settings.cloudflare_zone_id
    query = urlencode({'name': name, 'type': settings.cloudflare_dns_record_type.upper()})
    listed = _request('GET', f'/zones/{zone_id}/dns_records?{query}')
    records = listed.get('result') or []
    payload = {
        'type': settings.cloudflare_dns_record_type.upper(),
        'name': name,
        'content': settings.cloudflare_dns_target,
        'ttl': int(settings.cloudflare_dns_ttl or 1),
        'proxied': bool(settings.cloudflare_dns_proxied),
        'comment': 'Managed by CloudOn BI tenant provisioning',
    }
    if records:
        record_id = records[0].get('id')
        updated = _request('PUT', f'/zones/{zone_id}/dns_records/{record_id}', payload)
        return {'status': 'updated', 'hostname': name, 'cloudflare': updated}

    created = _request('POST', f'/zones/{zone_id}/dns_records', payload)
    return {'status': 'created', 'hostname': name, 'cloudflare': created}
