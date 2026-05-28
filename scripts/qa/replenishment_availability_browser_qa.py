import json
import os
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright


BASE_URL = os.environ.get("QA_BASE_URL", "http://api:8000").rstrip("/")
HOST_HEADER = os.environ.get("QA_HOST", "bi.boxvisio.com")
ACCESS_TOKEN = os.environ["QA_ACCESS_TOKEN"]
OUT_DIR = Path(os.environ.get("QA_OUT_DIR", "artifacts/replenishment-availability-qa")) / datetime.utcnow().strftime("%Y%m%d-%H%M%S")
RESOLVE_HOST_TO = os.environ.get("QA_RESOLVE_HOST_TO", "")

VIEWPORTS = [
    ("desktop", 1600, 900),
    ("iphone14-pro-max", 430, 932),
]


def slug(value: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "-", value.lower()).strip("-")


def cookie_domain() -> str:
    parsed = urlparse(BASE_URL)
    return parsed.hostname or "api"


def assert_replenishment(page, viewport_name: str) -> list[str]:
    issues: list[str] = []
    if page.locator("text=Subscription blocked").count():
        issues.append("subscription blocked message is visible")
    if page.locator("text=Replenishment / Availability είναι διαθέσιμο").count():
        issues.append("feature appears locked for QA tenant")
    for selector, label in [
        (".repl-filter-form", "filter form"),
        ("text=Χάρτης πίεσης διαθεσιμότητας", "availability section"),
        ("#replAvailabilityModal", "availability modal"),
        ("[data-repl-drill]", "drilldown buttons"),
    ]:
        if page.locator(selector).count() < 1:
            issues.append(f"missing {label}")

    overflow = page.evaluate(
        """
        () => {
          const doc = document.documentElement;
          return Math.max(0, doc.scrollWidth - doc.clientWidth);
        }
        """
    )
    if overflow > 3:
        issues.append(f"page has horizontal overflow: {overflow}px")

    if page.locator("[data-repl-drill]").count():
        page.locator("[data-repl-drill]").first.click()
        page.wait_for_timeout(700)
        if not page.locator("#replAvailabilityModal.show, #replAvailabilityModal[style*='display: block']").count():
            issues.append("availability drilldown modal did not open")
        try:
            page.wait_for_function(
                """
                () => {
                  const rows = document.querySelector('#replAvailabilityModalRows');
                  return rows && !rows.textContent.includes('Φόρτωση');
                }
                """,
                timeout=12_000,
            )
        except Exception:
            issues.append("availability drilldown stayed in loading state")
        if page.locator("#replAvailabilityModalRows tr").count() < 1:
            issues.append("availability drilldown returned no table rows")
        page.screenshot(path=str(OUT_DIR / f"{viewport_name}-drilldown-modal.png"), full_page=True)
        page.keyboard.press("Escape")
        page.wait_for_timeout(150)

    if viewport_name == "iphone14-pro-max":
        page.locator(".repl-filter-form input[name='search']").fill("mounjaro")
        page.locator(".repl-filter-form button[type='submit']").click()
        page.wait_for_load_state("networkidle", timeout=45_000)
        if "search=mounjaro" not in page.url:
            issues.append("mobile filter submit did not preserve search query")
        overflow_after_filter = page.evaluate(
            """
            () => Math.max(0, document.documentElement.scrollWidth - document.documentElement.clientWidth)
            """
        )
        if overflow_after_filter > 3:
            issues.append(f"mobile filtered page has horizontal overflow: {overflow_after_filter}px")
    return issues


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    report: dict[str, object] = {
        "base_url": BASE_URL,
        "host_header": HOST_HEADER,
        "out_dir": str(OUT_DIR),
        "pages": [],
        "issues": [],
    }
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
            url = f"{BASE_URL}/tenant/replenishment"
            response = page.goto(url, wait_until="networkidle", timeout=45_000)
            status = response.status if response else 0
            entry = {"viewport": viewport_name, "page": "replenishment", "url": url, "status": status}
            if status >= 400:
                entry["issues"] = [f"HTTP {status}"]
            else:
                entry["issues"] = assert_replenishment(page, viewport_name)
            screenshot = OUT_DIR / f"{viewport_name}-{slug('replenishment')}.png"
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
