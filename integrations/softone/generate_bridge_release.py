#!/usr/bin/env python3
"""Generate a dated SoftOne bridge release from the canonical bridge file.

This keeps the customer-deployable JavaScript snapshots in sync with the
canonical bridge while validating the SQL/querypack mappings we depend on.
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
INTEGRATIONS_DIR = ROOT / "integrations" / "softone"
CANONICAL_BRIDGE = INTEGRATIONS_DIR / "boxvisio_bi_bridge.js"
QUERYPACK_SALES = ROOT / "backend" / "querypacks" / "pharmacyone" / "facts" / "sales_facts.sql"
MANUAL = INTEGRATIONS_DIR / "SOFTONE_BI_BRIDGE_TECHNICAL_MANUAL.md"


SYNC_RULES = [
    {
        "name": "ERP source timestamp",
        "querypack_tokens": ["F.SOTIME", "source_created_at"],
        "bridge_tokens": ["SOURCE_CREATED_AT", "SOTIME", "INSDATE"],
    },
    {
        "name": "Sales channel from document header",
        "querypack_tokens": ["F.CCC88ECHANNEL", "channel_ext_id", "channel_name"],
        "bridge_tokens": ["docChannel", "CHANNEL_EXT_ID", "CHANNEL_NAME", "CCC88ECHANNEL"],
    },
    {
        "name": "Payment method from FINDOC.PAYMENT",
        "querypack_tokens": ["F.PAYMENT", "payment_method", "FROM PAYMENT P"],
        "bridge_tokens": ["PAYMENT_METHOD", "F.PAYMENT", "OUTER APPLY (SELECT TOP 1 P.CODE, P.NAME FROM PAYMENT P"],
    },
    {
        "name": "Item group from ITEM.MTRGROUP",
        "querypack_tokens": ["I.MTRGROUP", "group_external_id", "group_name", "LEFT JOIN MTRGROUP MG"],
        "bridge_tokens": ["GROUP_EXT_ID", "GROUP_NAME", "MTRGROUP", "LEFT JOIN MTRGROUP MG"],
    },
]


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _extract_version(bridge_text: str) -> str:
    match = re.search(r'var\s+BVBI_VERSION\s*=\s*"([^"]+)"', bridge_text)
    if not match:
        raise SystemExit("Could not find BVBI_VERSION in canonical bridge.")
    return match.group(1)


def _validate_alignment(bridge_text: str, querypack_text: str) -> list[str]:
    notes: list[str] = []
    for rule in SYNC_RULES:
        missing_querypack = [token for token in rule["querypack_tokens"] if token not in querypack_text]
        missing_bridge = [token for token in rule["bridge_tokens"] if token not in bridge_text]
        if missing_querypack or missing_bridge:
            detail_parts = []
            if missing_querypack:
                detail_parts.append("querypack missing: " + ", ".join(missing_querypack))
            if missing_bridge:
                detail_parts.append("bridge missing: " + ", ".join(missing_bridge))
            raise SystemExit(f"Bridge sync check failed for '{rule['name']}': " + " | ".join(detail_parts))
        notes.append(rule["name"])
    return notes


def _build_release_text(bridge_text: str, release_date: str, version: str) -> str:
    banner = (
        "/*\n"
        "  Generated release snapshot for customer infrastructure.\n"
        f"  Source: integrations/softone/boxvisio_bi_bridge.js\n"
        f"  Release date: {release_date}\n"
        f"  Bridge version: {version}\n"
        "*/\n\n"
    )
    return banner + bridge_text


def _manual_hint_exists(text: str) -> bool:
    return "generate_bridge_release.py" in text


def _append_manual_hint(text: str) -> str:
    addition = """

## 13) Release Workflow For Customer JS Files

Canonical source:

- `integrations/softone/boxvisio_bi_bridge.js`

After every SQL/querypack change that affects SoftOne sales extraction, run:

```bash
python3 integrations/softone/generate_bridge_release.py
```

What the generator does:

- validates that the canonical bridge still matches the current `sales_facts.sql` mappings
- creates a dated customer snapshot like `boxvisio_bi_bridge_2026-04-22.js`
- keeps the deployable customer JS under the same `integrations/softone/` folder

Validation currently guards these synchronized fields:

- `FINDOC.CCC88ECHANNEL` -> channel
- `FINDOC.PAYMENT` -> payment method
- `FINDOC.SOTIME/INSDATE` -> `source_created_at`
- `ITEM.MTRGROUP` -> item group
""".rstrip()
    return text.rstrip() + "\n" + addition + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate dated SoftOne BI bridge release file.")
    parser.add_argument("--date", help="Release date in YYYY-MM-DD format. Defaults to today.")
    parser.add_argument("--stdout", action="store_true", help="Print generated content instead of writing the release file.")
    args = parser.parse_args()

    if args.date:
        try:
            release_date = dt.date.fromisoformat(args.date).isoformat()
        except ValueError as exc:
            raise SystemExit(f"Invalid --date value: {exc}") from exc
    else:
        release_date = dt.date.today().isoformat()

    bridge_text = _read(CANONICAL_BRIDGE)
    querypack_text = _read(QUERYPACK_SALES)
    version = _extract_version(bridge_text)
    validated_rules = _validate_alignment(bridge_text, querypack_text)
    release_text = _build_release_text(bridge_text, release_date, version)

    if args.stdout:
        sys.stdout.write(release_text)
        return 0

    output_path = INTEGRATIONS_DIR / f"boxvisio_bi_bridge_{release_date}.js"
    _write(output_path, release_text)

    manual_text = _read(MANUAL)
    if not _manual_hint_exists(manual_text):
        _write(MANUAL, _append_manual_hint(manual_text))

    print(f"Generated: {output_path.relative_to(ROOT)}")
    print(f"Bridge version: {version}")
    print("Validated:")
    for rule_name in validated_rules:
        print(f"- {rule_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
