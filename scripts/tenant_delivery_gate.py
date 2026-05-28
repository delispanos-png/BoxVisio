#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = SCRIPT_PATH.parents[1]


def _today() -> date:
    return datetime.now(UTC).date()


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    return value


def _project_path(raw_path: str) -> Path:
    path = Path(raw_path)
    return path if path.is_absolute() else PROJECT_ROOT / path


async def _run_step(name: str, command: list[str]) -> dict[str, Any]:
    started = datetime.now(UTC)
    process = await asyncio.create_subprocess_exec(
        *command,
        cwd=str(PROJECT_ROOT),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_raw, stderr_raw = await process.communicate()
    elapsed_ms = round((datetime.now(UTC) - started).total_seconds() * 1000.0, 2)
    stdout = stdout_raw.decode('utf-8', errors='replace')
    stderr = stderr_raw.decode('utf-8', errors='replace')
    status = 'PASS' if process.returncode == 0 else ('WARN' if process.returncode == 1 else 'FAIL')
    return {
        'name': name,
        'status': status,
        'returncode': process.returncode,
        'elapsed_ms': elapsed_ms,
        'command': command,
        'stdout_tail': stdout[-4000:],
        'stderr_tail': stderr[-4000:],
    }


def _overall(steps: list[dict[str, Any]]) -> str:
    if any(step.get('status') == 'FAIL' for step in steps):
        return 'FAIL'
    if any(step.get('status') == 'WARN' for step in steps):
        return 'WARN'
    return 'PASS'


async def run(args: argparse.Namespace) -> dict[str, Any]:
    py = sys.executable
    from_date = date.fromisoformat(args.from_date)
    to_date = date.fromisoformat(args.to_date)
    steps: list[dict[str, Any]] = []

    if not args.skip_enrichment:
        steps.append(
            await _run_step(
                'dimension_enrichment',
                [
                    py,
                    str(PROJECT_ROOT / 'scripts/enrich_tenant_dimensions.py'),
                    '--tenant',
                    args.tenant,
                    '--from-date',
                    from_date.isoformat(),
                    '--to-date',
                    to_date.isoformat(),
                    '--fill-no-sales-abc',
                ],
            )
        )

    if not args.skip_warmup:
        steps.append(
            await _run_step(
                'cache_warmup',
                [
                    py,
                    str(PROJECT_ROOT / 'scripts/warm_tenant_caches.py'),
                    '--tenant',
                    args.tenant,
                    '--from-date',
                    from_date.isoformat(),
                    '--to-date',
                    to_date.isoformat(),
                    '--base-url',
                    args.base_url,
                    '--host',
                    args.host,
                    '--rounds',
                    str(args.warmup_rounds),
                    '--slow-ms',
                    str(args.slow_ms),
                ],
            )
        )

    steps.append(
        await _run_step(
            'production_readiness',
            [
                py,
                str(PROJECT_ROOT / 'scripts/production_readiness_check.py'),
                '--tenant',
                args.tenant,
                '--from-date',
                from_date.isoformat(),
                '--to-date',
                to_date.isoformat(),
                '--base-url',
                args.base_url,
                '--slow-ms',
                str(args.slow_ms),
            ],
        )
    )

    report = {
        'generated_at': datetime.now(UTC).isoformat(timespec='seconds').replace('+00:00', 'Z'),
        'overall_status': _overall(steps),
        'tenant': args.tenant,
        'window': {'from': from_date, 'to': to_date},
        'steps': steps,
        'release_note': (
            'PASS means the tenant passed enrichment, cache warm-up and production readiness. '
            'WARN means delivery can continue only after reviewing the listed gaps. '
            'FAIL means stop delivery.'
        ),
    }
    out_dir = _project_path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
    out_path = out_dir / f"tenant_delivery_gate_{args.tenant}_{stamp}.json"
    out_path.write_text(json.dumps(_json_safe(report), ensure_ascii=False, indent=2), encoding='utf-8')
    report['artifact'] = str(out_path)
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run the tenant delivery gate before go-live/customer handoff.')
    parser.add_argument('--tenant', default='pharmacy295')
    parser.add_argument('--from-date', default='2025-01-01')
    parser.add_argument('--to-date', default=_today().isoformat())
    parser.add_argument('--base-url', default='http://127.0.0.1:8000')
    parser.add_argument('--host', default='bi.boxvisio.com')
    parser.add_argument('--slow-ms', type=int, default=2500)
    parser.add_argument('--warmup-rounds', type=int, default=2)
    parser.add_argument('--out-dir', default='artifacts/delivery_gate')
    parser.add_argument('--skip-enrichment', action='store_true')
    parser.add_argument('--skip-warmup', action='store_true')
    return parser.parse_args()


def main() -> None:
    report = asyncio.run(run(parse_args()))
    print(
        json.dumps(
            _json_safe(
                {
                    'overall_status': report.get('overall_status'),
                    'artifact': report.get('artifact'),
                    'steps': [
                        {
                            'name': step.get('name'),
                            'status': step.get('status'),
                            'returncode': step.get('returncode'),
                            'elapsed_ms': step.get('elapsed_ms'),
                        }
                        for step in report.get('steps', [])
                    ],
                }
            ),
            ensure_ascii=False,
            indent=2,
        )
    )
    if report.get('overall_status') == 'FAIL':
        raise SystemExit(2)
    if report.get('overall_status') == 'WARN':
        raise SystemExit(1)


if __name__ == '__main__':
    main()
