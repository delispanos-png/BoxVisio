#!/usr/bin/env python
"""Stamp the build number into backend/app/build_info.json.

The running app cannot ask git anything: the API container mounts the repo but
has no `git` binary. So the build identity is written to a file at commit time
and simply read at runtime.

Normally invoked by the pre-commit hook (see scripts/install_git_hooks.sh), which
is why the build number is the commit count *plus one* — the commit being made
does not exist yet when the hook runs.

    python scripts/bump_build.py            # for the commit about to be made
    python scripts/bump_build.py --current  # for HEAD as it already is
"""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
VERSION_FILE = REPO_ROOT / 'VERSION'
BUILD_INFO = REPO_ROOT / 'backend' / 'app' / 'build_info.json'


def _git(*args: str) -> str:
    return subprocess.run(
        ['git', *args],
        cwd=str(REPO_ROOT),
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    ).stdout.strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--current',
        action='store_true',
        help='stamp HEAD as it stands, instead of the commit about to be created',
    )
    args = parser.parse_args()

    series = VERSION_FILE.read_text(encoding='utf-8').strip() or '0.0'
    commits = int(_git('rev-list', '--count', 'HEAD'))
    build = commits if args.current else commits + 1

    payload = {
        'series': series,
        'build': build,
        'version': f'{series}.{build}',
        #  HEAD is the parent of the commit being made; good enough to identify
        #  the tree, and the pre-commit hook cannot know its own hash.
        'commit': _git('rev-parse', '--short', 'HEAD'),
        'branch': _git('rev-parse', '--abbrev-ref', 'HEAD'),
        'stamped_at': datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }
    BUILD_INFO.write_text(json.dumps(payload, indent=2) + '\n', encoding='utf-8')
    print(f"[bump_build] {payload['version']} ({payload['branch']} @ {payload['commit']})")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
