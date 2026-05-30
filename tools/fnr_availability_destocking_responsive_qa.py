import json
import os
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright


BASE_URL = os.environ.get("QA_BASE_URL", "https://bi.boxvisio.com").rstrip("/")
HOST_HEADER = os.environ.get("QA_HOST", "")
HOST_RESOLVER_RULES = os.environ.get("QA_HOST_RESOLVER_RULES", "MAP bi.boxvisio.com 127.0.0.1")
ACCESS_TOKEN = os.environ["QA_ACCESS_TOKEN"]
OUT_DIR = Path(os.environ.get("QA_OUT_DIR", "artifacts/fnr-availability-destocking-qa")) / datetime.utcnow().strftime("%Y%m%d-%H%M%S")

VIEWPORTS = [
    ("desktop", 1600, 900),
    ("tablet", 1024, 768),
    ("mobile", 430, 932),
]

PAGES = [
    ("fnr", "/tenant/fnr", ".fnr-workbench", [".fnr-inputs", ".fnr-summary", ".fnr-table"]),
    ("availability", "/tenant/availability", ".av-workbench", [".av-inputs", ".av-summary", ".av-table"]),
    ("destocking", "/tenant/destocking", ".dst-workbench", [".dst-inputs", ".dst-summary", ".dst-table"]),
]


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "-", value.lower()).strip("-")


def cookie_domain() -> str:
    return urlparse(BASE_URL).hostname or "localhost"


def visible_width(page, selector: str) -> float:
    return float(
        page.locator(selector).first.evaluate(
            "(el) => el.getBoundingClientRect().width"
        )
    )


def assert_page(page, page_name: str, viewport_name: str, workbench_selector: str, required: list[str]) -> list[str]:
    issues: list[str] = []
    if page.locator("text=Subscription blocked").count():
        issues.append("subscription blocked message is visible")
    if page.locator("text=κλειδωμένο").count() or page.locator("text=locked").count():
        issues.append("worksheet appears locked")
    for selector in required:
        if page.locator(selector).count() < 1:
            issues.append(f"missing required selector {selector}")
    overflow = page.evaluate("() => Math.max(0, document.documentElement.scrollWidth - document.documentElement.clientWidth)")
    if viewport_name in {"desktop", "tablet"} and overflow > 24:
        issues.append(f"unexpected page horizontal overflow: {overflow}px")
    if viewport_name == "mobile" and overflow > 360:
        issues.append(f"mobile page overflow is excessive: {overflow}px")
    if page.locator(workbench_selector).count():
        width = visible_width(page, workbench_selector)
        viewport_width = page.viewport_size["width"]
        if viewport_name == "desktop" and width > min(1360, viewport_width - 24):
            issues.append(f"filter workbench is too wide: {width}px")
        if viewport_name == "desktop" and width < 900:
            issues.append(f"filter workbench is too narrow: {width}px")
    if page_name in {"availability", "destocking"}:
        for tab in ["Trends", "Correlation", "Recommendations"]:
            if page.get_by_role("button", name=tab).count():
                page.get_by_role("button", name=tab).click()
                page.wait_for_timeout(250)
            else:
                issues.append(f"missing tab {tab}")
    if page_name == "fnr":
        if page.locator(".fnr-table tbody tr").count() < 1:
            issues.append("FNR table has no rows")
    return issues


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    report: dict[str, object] = {"base_url": BASE_URL, "out_dir": str(OUT_DIR), "pages": [], "issues": []}
    with sync_playwright() as p:
        launch_args = []
        if HOST_RESOLVER_RULES:
            launch_args.append(f"--host-resolver-rules={HOST_RESOLVER_RULES}")
        browser = p.chromium.launch(args=launch_args)
        headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
        if HOST_HEADER:
            headers["Host"] = HOST_HEADER
        context = browser.new_context(ignore_https_errors=True, extra_http_headers=headers)
        context.add_cookies([
            {
                "name": "access_token",
                "value": ACCESS_TOKEN,
                "domain": cookie_domain(),
                "path": "/",
                "httpOnly": True,
                "secure": BASE_URL.startswith("https://"),
                "sameSite": "Lax",
            }
        ])
        for viewport_name, width, height in VIEWPORTS:
            page = context.new_page()
            page.set_viewport_size({"width": width, "height": height})
            for page_name, path, workbench_selector, required in PAGES:
                url = f"{BASE_URL}{path}"
                response = page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                status = response.status if response else 0
                entry = {"viewport": viewport_name, "page": page_name, "url": url, "status": status, "issues": []}
                if status >= 400:
                    entry["issues"].append(f"HTTP {status}")
                else:
                    try:
                        page.locator(workbench_selector).first.wait_for(state="visible", timeout=90_000)
                    except Exception as exc:
                        entry["issues"].append(f"workbench did not become visible: {exc}")
                    entry["issues"].extend(assert_page(page, page_name, viewport_name, workbench_selector, required))
                screenshot = OUT_DIR / f"{viewport_name}-{slug(page_name)}.png"
                page.screenshot(path=str(screenshot), full_page=True)
                entry["screenshot"] = str(screenshot)
                report["pages"].append(entry)
                for issue in entry["issues"]:
                    report["issues"].append({**entry, "issue": issue})
            page.close()
        browser.close()
    report_path = OUT_DIR / "report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"out_dir": str(OUT_DIR), "issues": report["issues"], "report": str(report_path)}, ensure_ascii=False))
    return 1 if report["issues"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
