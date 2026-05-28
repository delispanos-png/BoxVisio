#!/usr/bin/env python3
"""Production smoke checks for the Replenishment / Availability dashboard.

The script intentionally uses only the Python standard library so it can run
inside the API container even when Playwright/curl are not installed.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import timedelta
from typing import Any
from urllib.parse import quote
from urllib.request import Request, urlopen

from app.core.security import create_access_token


REQUIRED_HTML_MARKERS = (
    'Χάρτης πίεσης διαθεσιμότητας',
    'repl-filter-form',
    'replAvailabilityModal',
    'data-repl-drill',
)


def _fetch(base_url: str, path: str, token: str, host: str, timeout: int) -> tuple[int, dict[str, str], bytes, float]:
    request = Request(
        f'{base_url.rstrip("/")}{path}',
        headers={
            'Host': host,
            'Cookie': f'access_token={token}',
        },
    )
    started = time.perf_counter()
    with urlopen(request, timeout=timeout) as response:
        body = response.read()
        elapsed_ms = (time.perf_counter() - started) * 1000
        return response.status, dict(response.headers), body, elapsed_ms


def run_smoke(args: argparse.Namespace) -> dict[str, Any]:
    token = create_access_token(
        subject=str(args.user_id),
        tenant_id=int(args.tenant_id),
        role=args.role,
        audience='tenant',
        expires_delta=timedelta(minutes=20),
    )

    page_status, _page_headers, page_body, page_ms = _fetch(
        args.base_url,
        '/tenant/replenishment',
        token,
        args.host,
        args.timeout,
    )
    html = page_body.decode('utf-8', errors='ignore')
    marker_results = {marker: marker in html for marker in REQUIRED_HTML_MARKERS}

    drill_path = (
        '/v1/kpi/replenishment/availability-drilldown'
        f'?dimension=store&value={quote(args.store)}&kind=shortage'
    )
    drill_results: list[dict[str, Any]] = []
    for _ in range(2):
        status, headers, body, elapsed_ms = _fetch(args.base_url, drill_path, token, args.host, args.timeout)
        payload = json.loads(body.decode('utf-8'))
        rows = payload.get('rows') if isinstance(payload, dict) else []
        first = rows[0] if rows else {}
        drill_results.append(
            {
                'status': status,
                'elapsed_ms': round(elapsed_ms, 2),
                'cache': headers.get('x-kpi-cache') or headers.get('X-KPI-Cache'),
                'rows': len(rows),
                'first_item_code': first.get('item_code'),
                'first_shortage_qty': first.get('shortage_qty'),
            }
        )

    passed = (
        page_status == 200
        and all(marker_results.values())
        and all(item['status'] == 200 for item in drill_results)
        and drill_results[-1]['rows'] > 0
        and str(drill_results[-1].get('cache') or '').upper() == 'HIT'
    )
    return {
        'passed': passed,
        'page': {
            'status': page_status,
            'elapsed_ms': round(page_ms, 2),
            'bytes': len(page_body),
            'markers': marker_results,
        },
        'drilldown': drill_results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='Smoke test Replenishment / Availability.')
    parser.add_argument('--base-url', default='http://localhost:8000')
    parser.add_argument('--host', default='bi.boxvisio.com')
    parser.add_argument('--tenant-id', type=int, default=3)
    parser.add_argument('--user-id', type=int, default=5)
    parser.add_argument('--role', default='tenant_admin')
    parser.add_argument('--store', default='Εδρα')
    parser.add_argument('--timeout', type=int, default=60)
    args = parser.parse_args()

    result = run_smoke(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
