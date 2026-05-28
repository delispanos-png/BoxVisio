#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright


BASE_URL = os.environ.get("QA_BASE_URL", "http://127.0.0.1").rstrip("/")
HOST_HEADER = os.environ.get("QA_HOST", "bi.boxvisio.com")
ACCESS_TOKEN = os.environ["QA_ACCESS_TOKEN"]
OUT_DIR = Path(os.environ.get("QA_OUT_DIR", "artifacts/visual-qa")) / ("central-nav-" + datetime.utcnow().strftime("%Y%m%d-%H%M%S"))
RESOLVE_HOST_TO = os.environ.get("QA_RESOLVE_HOST_TO", "")

VIEWPORTS = [
    ("desktop-wide", 1900, 900),
    ("desktop", 1600, 900),
    ("tablet", 1024, 768),
    ("mobile", 390, 844),
]


def cookie_domain() -> str:
    parsed = urlparse(BASE_URL)
    return parsed.hostname or "127.0.0.1"


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    report: dict[str, object] = {"out_dir": str(OUT_DIR), "issues": [], "viewports": []}
    with sync_playwright() as p:
        launch_args = []
        if RESOLVE_HOST_TO and HOST_HEADER:
            launch_args.append(f"--host-resolver-rules=MAP {HOST_HEADER} {RESOLVE_HOST_TO}")
        browser = p.chromium.launch(args=launch_args)
        headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
        if HOST_HEADER and not RESOLVE_HOST_TO:
            headers["Host"] = HOST_HEADER
        context = browser.new_context(ignore_https_errors=True, extra_http_headers=headers)
        context.add_cookies(
            [
                {
                    "name": "access_token",
                    "value": ACCESS_TOKEN,
                    "domain": cookie_domain(),
                    "path": "/",
                    "httpOnly": True,
                    "secure": BASE_URL.startswith("https://"),
                    "sameSite": "Lax",
                }
            ]
        )
        for viewport_name, width, height in VIEWPORTS:
            page = context.new_page()
            page.set_viewport_size({"width": width, "height": height})
            response = page.goto(f"{BASE_URL}/tenant/dashboard", wait_until="networkidle", timeout=60_000)
            status = response.status if response else 0
            metrics = page.evaluate(
                """
                () => {
                  const grid = document.querySelector('.central-nav-grid');
                  const tiles = Array.from(document.querySelectorAll('.central-nav-grid .bv-nav-tile'));
                  const rows = [...new Set(tiles.map((el) => Math.round(el.getBoundingClientRect().top)))];
                  return {
                    tileCount: tiles.length,
                    rowCount: rows.length,
                    gridColumns: grid ? getComputedStyle(grid).gridTemplateColumns : '',
                    overflow: Math.max(0, document.documentElement.scrollWidth - document.documentElement.clientWidth),
                    tileHeights: tiles.map((el) => Math.round(el.getBoundingClientRect().height)),
                    labels: tiles.map((el) => (el.innerText || '').trim())
                  };
                }
                """
            )
            issues: list[str] = []
            if status >= 400:
                issues.append(f"HTTP {status}")
            if metrics["tileCount"] != 7:
                issues.append(f"expected 7 nav tiles, got {metrics['tileCount']}")
            if viewport_name in {"desktop-wide", "desktop"} and metrics["rowCount"] != 1:
                issues.append(f"desktop nav uses {metrics['rowCount']} rows")
            if metrics["overflow"] > 3:
                issues.append(f"horizontal overflow {metrics['overflow']}px")
            screenshot = OUT_DIR / f"{viewport_name}-dashboard.png"
            page.screenshot(path=str(screenshot), full_page=True)
            entry = {
                "viewport": viewport_name,
                "status": status,
                "metrics": metrics,
                "issues": issues,
                "screenshot": str(screenshot),
            }
            report["viewports"].append(entry)
            for issue in issues:
                report["issues"].append({"viewport": viewport_name, "issue": issue})
            page.close()
        browser.close()
    report_path = OUT_DIR / "report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"out_dir": str(OUT_DIR), "issues": report["issues"], "report": str(report_path)}, ensure_ascii=False, indent=2))
    return 1 if report["issues"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
