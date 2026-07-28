"""Capture the screenshots used by the in-app Help manual (/tenant/help).

The manual is served to every tenant, so the shots are taken against the internal
R&D pilot tenant and every piece of personal data is masked in the DOM *before*
the shot is taken (customer names -> pseudonyms, AFM/phone/email -> placeholders).

Usage:
    python scripts/capture_manual_screenshots.py                 # everything
    python scripts/capture_manual_screenshots.py dashboard sales # selected shots
    python scripts/capture_manual_screenshots.py --list

Requires a Playwright install (chromium). The venv used to build the shipped
images lives outside the repo; see docs/HELP_MANUAL.md.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = REPO_ROOT / 'backend' / 'app' / 'static' / 'docs' / 'manual'

#  Chromium will not let you override the Host header, so we resolve the real
#  vhost name to the local nginx instead (see --host-resolver-rules below).
HOST_HEADER = os.environ.get('MANUAL_SHOT_HOST', 'bi.boxvisio.com')
RESOLVE_TO = os.environ.get('MANUAL_SHOT_RESOLVE_TO', '127.0.0.1')
BASE_URL = os.environ.get('MANUAL_SHOT_BASE_URL', f'https://{HOST_HEADER}')
API_CONTAINER = os.environ.get('MANUAL_SHOT_API_CONTAINER', 'cloudon_bi-api-1')
PG_CONTAINER = os.environ.get('MANUAL_SHOT_PG_CONTAINER', 'cloudon_bi-postgres-1')
TENANT_ID = int(os.environ.get('MANUAL_SHOT_TENANT_ID', '1'))
TENANT_DB = os.environ.get('MANUAL_SHOT_TENANT_DB', 'R&DDB')
USER_ID = os.environ.get('MANUAL_SHOT_USER_ID', '8')

VIEWPORT = {'width': 1600, 'height': 1000}
JPEG_QUALITY = 82


@dataclass
class Shot:
    """One screenshot: a page to open and what to capture on it."""

    name: str
    path: str
    #  CSS selector to wait for before shooting (page is considered "ready").
    wait_for: str | None = None
    #  When set, only this element is captured instead of the viewport.
    clip_selector: str | None = None
    #  Extra settle time in ms on top of network idle (charts animate).
    settle_ms: int = 1500
    #  JS evaluated in the page right before masking (open a modal, a tab, ...).
    prepare: str | None = None
    full_page: bool = False
    #  Hide these selectors before shooting (cookie bars, debug chips, ...).
    hide: list[str] = field(default_factory=list)
    #  Drive the page's own filter form to a period that actually has data.
    #  None keeps the page default (right for snapshot-based screens).
    period: tuple[str, str] | None = ('01/01/2026', '31/05/2026')
    #  Waited for *after* prepare() runs — used for modals that the page opens
    #  itself, so we shoot a fully initialised dialog rather than an empty shell.
    wait_for_visible: str | None = None


SHOTS: list[Shot] = [
    # ---- Ξενάγηση / γενικά ------------------------------------------------
    Shot('ui-tour-full', '/tenant/dashboard', wait_for='.bv-metric-card'),
    Shot(
        'ui-sidebar',
        '/tenant/dashboard',
        wait_for='#leftsidebar',
        clip_selector='#leftsidebar',
        prepare="""
          document.querySelectorAll('.bv-menu-group').forEach(g => {
            g.classList.add('open', 'active');
            const sub = g.querySelector('.bv-submenu');
            if (sub) { sub.style.display = 'block'; sub.style.maxHeight = 'none'; }
          });
        """,
        period=None,
    ),
    Shot(
        'ui-topbar',
        '/tenant/dashboard',
        wait_for='.bv-page-head',
        clip_selector='.topbar',
        period=None,
    ),
    Shot(
        'ui-filters',
        '/tenant/sales-documents',
        wait_for='.filter-card',
        clip_selector='.filter-card',
        prepare="""
          const panel = document.getElementById('pageFiltersCollapse');
          if (panel) { panel.hidden = false; panel.style.display = 'block'; }
        """,
        period=None,
    ),
    Shot(
        'ui-date-modal',
        '/tenant/dashboard',
        wait_for='.filter-card',
        clip_selector='#globalDateModal .modal-content',
        wait_for_visible='#globalDateModal.show',
        prepare="""
          const input = document.querySelector('#pageFiltersCollapse input[data-date-format="dmy"]')
            || document.querySelector('input[data-date-format="dmy"]');
          if (input) { input.focus(); input.click(); }
          await new Promise(r => setTimeout(r, 700));
        """,
        period=None,
    ),
    Shot(
        'ui-kpi-help',
        '/tenant/dashboard',
        wait_for='.bv-metric-card',
        clip_selector='#kpiHelpModal .modal-content',
        wait_for_visible='#kpiHelpModal.show',
        prepare="""
          //  Executive cards route to KPI help through their icon only (the card
          //  body is reserved for the breakdown). Gross profit is the best example
          //  to show: it is the KPI whose caveat people most often need.
          const card = document.querySelector('.bv-metric-card[data-executive-card="gross_profit_period"]')
            || document.querySelector('.bv-metric-card[data-executive-card]');
          const icon = card && card.querySelector('.mc-icon');
          if (icon) icon.click(); else if (card) card.click();
          await new Promise(r => setTimeout(r, 900));
        """,
        period=None,
    ),
    # ---- Dashboards -------------------------------------------------------
    Shot('dashboard', '/tenant/dashboard', wait_for='.bv-metric-card'),
    Shot('finance-dashboard', '/tenant/finance-dashboard', wait_for='.bv-metric-card'),
    Shot('insights', '/tenant/insights'),
    Shot('business-advisor', '/tenant/business-advisor', settle_ms=2500),
    # ---- Επιχειρησιακά κυκλώματα -----------------------------------------
    Shot('sales-documents', '/tenant/sales-documents'),
    Shot('purchase-documents', '/tenant/purchase-documents'),
    Shot('supplier-orders', '/tenant/supplier-orders'),
    Shot('warehouse-documents', '/tenant/warehouse-documents'),
    Shot('expense-documents', '/tenant/expense-documents'),
    Shot('operating-expenses', '/tenant/operating-expenses'),
    Shot('cash-transactions', '/tenant/cashflow'),
    Shot('supplier-balances', '/tenant/suppliers'),
    Shot('customer-balances', '/tenant/customers'),
    # ---- Αναλύσεις --------------------------------------------------------
    Shot('sales-analytics', '/tenant/sales', settle_ms=2500),
    Shot('pos', '/tenant/pos', settle_ms=2500),
    Shot('call-center', '/tenant/call-center', settle_ms=2500),
    Shot('eshop-analysis', '/tenant/e-shop-analysis', settle_ms=2500),
    Shot('sellout', '/tenant/exports/sellout'),
    Shot('purchases-analytics', '/tenant/purchases', settle_ms=2500),
    Shot('inventory-analytics', '/tenant/inventory', settle_ms=2500),
    Shot('warehouse-items', '/tenant/items'),
    Shot('replenishment', '/tenant/replenishment', settle_ms=3000),
    Shot('fnr', '/tenant/fnr', settle_ms=3000),
    Shot('availability', '/tenant/availability', settle_ms=3000),
    Shot('destocking', '/tenant/destocking', settle_ms=3000),
    Shot('price-control', '/tenant/price-control', settle_ms=2500),
    Shot('supplier-targets', '/tenant/supplier-targets'),
    Shot('era-exploration-data', '/tenant/era-exploration-data'),
    Shot('iqvia', '/tenant/iqvia'),
    # ---- Συγκρίσεις / εξαγωγές / ρυθμίσεις --------------------------------
    Shot('comparisons', '/tenant/comparisons/period-vs-period', settle_ms=2500),
    Shot('exports', '/tenant/exports/reports'),
    Shot('tenant-users', '/tenant/users'),
    Shot('tenant-settings', '/tenant/settings'),
    Shot('tenant-profile', '/tenant/profile'),
    Shot('tenant-messages', '/tenant/messages'),
]


def mint_token() -> str:
    """Mint a short-lived tenant session token via the API container.

    Avoids needing (or storing) anybody's password: the token is signed with the
    app's own secret and expires on its own.
    """
    code = (
        "import sys; sys.path.insert(0,'/opt/cloudon-bi/backend')\n"
        'from datetime import timedelta\n'
        'from app.core.security import create_access_token\n'
        f"print(create_access_token(subject='{USER_ID}', tenant_id={TENANT_ID}, "
        "role='tenant_admin', expires_delta=timedelta(hours=6)))\n"
    )
    out = subprocess.run(
        ['docker', 'exec', API_CONTAINER, '/opt/cloudon-bi/.venv/bin/python', '-c', code],
        capture_output=True,
        text=True,
        check=True,
    )
    return out.stdout.strip().splitlines()[-1]


def load_person_names() -> list[str]:
    """Customer names from the pilot tenant — these are real people, so they get
    replaced with pseudonyms before any pixel is captured."""
    pg_password = os.environ.get('MANUAL_SHOT_PGPASSWORD', '')
    if not pg_password:
        env_file = REPO_ROOT / '.env'
        for line in env_file.read_text(encoding='utf-8').splitlines():
            if line.startswith('CONTROL_DB_PASSWORD_REAL='):
                pg_password = line.split('=', 1)[1].strip()
            if line.startswith('CONTROL_DATABASE_URL=') and not pg_password:
                pg_password = line.split('://postgres:', 1)[1].split('@', 1)[0]
    out = subprocess.run(
        [
            'docker', 'exec', '-e', f'PGPASSWORD={pg_password}', PG_CONTAINER,
            'psql', '-U', 'postgres', '-d', TENANT_DB, '-tAc',
            'select name from dim_customers where name is not null',
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return [n.strip() for n in out.stdout.splitlines() if n.strip()]


# Injected into every page before the shot. Keep it dependency-free.
# Sets the page's own From/To inputs and submits its filter form — the same path
# the global date picker takes, so pages that ignore it are left untouched.
SET_PERIOD_JS = r"""
(period) => {
  //  Only ever the page's own filter form. An earlier version fell back to
  //  document.querySelector('form'), which on filter-less pages is the sidebar
  //  logout form — submitting it logged the capture session out mid-run.
  const panel = document.getElementById('pageFiltersCollapse');
  const form = (panel && panel.querySelector('form')) || document.querySelector('form[id$="Filters"]');
  if (!form) return 'no-form';
  if (form.action && /\/logout\b/.test(form.action)) return 'refused-logout-form';
  if ((form.method || '').toLowerCase() === 'post') return 'refused-post-form';

  const isFrom = (el) => /(^|[\s_-])(from|a_from|b_from|cffrom)([\s_-]|$)/.test(
    ((el.name || '') + ' ' + (el.id || '')).toLowerCase());
  const isTo = (el) => /(^|[\s_-])(to|a_to|b_to|snapshot|as_of|cfto)([\s_-]|$)/.test(
    ((el.name || '') + ' ' + (el.id || '')).toLowerCase());

  const dateInputs = Array.from(form.querySelectorAll('input[data-date-format="dmy"], input[name], input[id]'))
    .filter((el) => el.hasAttribute('data-date-format') || /(from|to|date|asof|as_of|snapshot)/.test(
      ((el.name || '') + ' ' + (el.id || '')).toLowerCase()));

  const fromEl = dateInputs.find(isFrom) || dateInputs[0];
  const toEl = dateInputs.find(isTo) || dateInputs[1] || dateInputs[0];
  if (!fromEl && !toEl) return 'no-date-inputs';

  const set = (el, value) => {
    if (!el) return;
    el.value = value;
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
  };
  set(fromEl, period[0]);
  set(toEl, period[1]);

  if (typeof form.requestSubmit === 'function') form.requestSubmit();
  else form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
  return 'applied';
}
"""

MASK_JS = r"""
(payload) => {
  const map = new Map(payload.map);
  const norm = (s) => (s || '').replace(/\s+/g, ' ').trim().toUpperCase();

  const AFM = /\b\d{9}\b/g;
  const PHONE = /\b(?:\+30\s?)?(?:69|21|22|23|24|25|26|27|28)\d{8}\b/g;
  const EMAIL = /\b[\w.+-]+@[\w-]+\.[\w.]{2,}\b/g;

  let counter = 0;
  const assigned = new Map();
  const pseudonym = (key) => {
    if (!assigned.has(key)) {
      counter += 1;
      assigned.set(key, 'ΠΕΛΑΤΗΣ ' + String(counter).padStart(3, '0'));
    }
    return assigned.get(key);
  };
  //  A masked street should still read as a street, otherwise the screenshot
  //  looks like a bug rather than like the product.
  let addrCounter = 0;
  const addrAssigned = new Map();
  const addressStub = (key) => {
    if (!addrAssigned.has(key)) {
      addrCounter += 1;
      addrAssigned.set(key, 'ΟΔΟΣ ΠΑΡΑΔΕΙΓΜΑΤΟΣ ' + addrCounter);
    }
    return addrAssigned.get(key);
  };

  const scrub = (raw) => {
    let text = raw;
    const key = norm(text);
    if (map.has(key)) {
      text = pseudonym(key);
    } else {
      // "ΟΝΟΜΑ ΕΠΩΝΥΜΟ (1234)" and similar decorated cells.
      const bare = key.replace(/\s*[\(\[].*$/, '').replace(/\s*[-–]\s*\d+$/, '').trim();
      if (bare && map.has(bare)) {
        text = text.replace(new RegExp(bare.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i'), pseudonym(bare));
      }
    }
    text = text.replace(EMAIL, 'name@example.com');
    text = text.replace(PHONE, '69XXXXXXXX');
    return text;
  };

  const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  const nodes = [];
  while (walker.nextNode()) nodes.push(walker.currentNode);
  for (const node of nodes) {
    const raw = node.nodeValue;
    if (!raw || !raw.trim()) continue;
    const next = scrub(raw);
    if (next !== raw) node.nodeValue = next;
  }

  // Any column that is explicitly a person/customer column gets masked even if
  // the value is not in the dimension table (free-text counterparties).
  document.querySelectorAll('table').forEach((table) => {
    const heads = Array.from(table.querySelectorAll('thead th'));
    const isAddress = (th) => /διεύθυνσ|address/i.test(th.textContent || '');
    const personCols = heads
      .map((th, i) => (/πελάτ|customer|επωνυμ|ονοματ|counterpart|αιτιολογ|διεύθυνσ|address/i.test(th.textContent || '') ? i : -1))
      .filter((i) => i >= 0);
    if (!personCols.length) return;
    table.querySelectorAll('tbody tr').forEach((tr) => {
      const cells = tr.children;
      personCols.forEach((i) => {
        const cell = cells[i];
        if (!cell) return;
        const value = (cell.textContent || '').trim();
        if (!value || value.length < 4) return;
        if (/^[\d\s.,%€\-\/]+$/.test(value)) return;
        cell.textContent = isAddress(heads[i]) ? addressStub(norm(value)) : pseudonym(norm(value));
      });
    });
  });

  document.querySelectorAll('table').forEach((table) => {
    const heads = Array.from(table.querySelectorAll('thead th'));
    const idCols = heads
      .map((th, i) => (/αφμ|α\.?μ\.?κ\.?α|vat *no|tax *id|ταυτότητ|αδτ/i.test(th.textContent || '') ? i : -1))
      .filter((i) => i >= 0);
    if (!idCols.length) return;
    table.querySelectorAll('tbody tr').forEach((tr) => {
      idCols.forEach((i) => {
        const cell = tr.children[i];
        if (cell && (cell.textContent || '').trim()) cell.textContent = '0XXXXXXXX';
      });
    });
  });

  // Titles/tooltips can carry the same data.
  document.querySelectorAll('[title]').forEach((el) => {
    const t = el.getAttribute('title');
    if (t) el.setAttribute('title', scrub(t));
  });

  return counter;
}
"""


async def capture(shots: list[Shot]) -> None:
    from playwright.async_api import async_playwright

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    token = mint_token()
    names = load_person_names()
    payload = {'map': [[n.replace(' ', ' ').strip().upper(), True] for n in names]}
    print(f'masking {len(names)} customer names')

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(args=[
            '--ignore-certificate-errors',
            f'--host-resolver-rules=MAP {HOST_HEADER} {RESOLVE_TO}',
        ])
        context = await browser.new_context(
            viewport=VIEWPORT,
            ignore_https_errors=True,
            locale='el-GR',
            timezone_id='Europe/Athens',
        )
        await context.add_cookies([
            {'name': 'access_token', 'value': token, 'url': BASE_URL},
            {'name': 'lang', 'value': 'el', 'url': BASE_URL},
            {'name': 'theme', 'value': 'light', 'url': BASE_URL},
        ])
        page = await context.new_page()
        page.on('console', lambda m: None)

        for shot in shots:
            target = OUT_DIR / f'{shot.name}.jpg'
            try:
                await page.goto(f'{BASE_URL}{shot.path}', wait_until='domcontentloaded', timeout=60_000)
                try:
                    await page.wait_for_load_state('networkidle', timeout=30_000)
                except Exception:
                    pass
                #  Never ship a shot of the login screen or an error page as if it
                #  were the feature. Bail loudly instead.
                landed = await page.evaluate(
                    '() => ({ shell: !!document.querySelector("#leftsidebar"), url: location.pathname })'
                )
                if not landed['shell']:
                    print(f'  SKIP {shot.name}: not authenticated / no access (landed on {landed["url"]})')
                    continue

                if shot.wait_for:
                    try:
                        await page.wait_for_selector(shot.wait_for, timeout=20_000)
                    except Exception:
                        print(f'  ! {shot.name}: wait_for {shot.wait_for} not found')
                await page.wait_for_timeout(shot.settle_ms)

                if shot.period:
                    outcome = await page.evaluate(SET_PERIOD_JS, list(shot.period))
                    if outcome == 'applied':
                        try:
                            await page.wait_for_load_state('networkidle', timeout=30_000)
                        except Exception:
                            pass
                        await page.wait_for_timeout(shot.settle_ms)

                if shot.prepare:
                    await page.evaluate(f'async () => {{ {shot.prepare} }}')
                    await page.wait_for_timeout(400)

                if shot.wait_for_visible:
                    try:
                        await page.wait_for_selector(shot.wait_for_visible, state='visible', timeout=10_000)
                    except Exception:
                        print(f'  ! {shot.name}: {shot.wait_for_visible} never became visible')
                    await page.wait_for_timeout(500)

                for selector in shot.hide:
                    await page.evaluate(
                        '(sel) => document.querySelectorAll(sel).forEach(e => e.style.visibility = "hidden")',
                        selector,
                    )

                masked = await page.evaluate(MASK_JS, payload)

                element = None
                if shot.clip_selector:
                    element = await page.query_selector(shot.clip_selector)
                    if element is None:
                        print(f'  ! {shot.name}: clip selector {shot.clip_selector} missing, full viewport')

                opts = {'path': str(target), 'type': 'jpeg', 'quality': JPEG_QUALITY}
                if element is not None:
                    await element.screenshot(**opts)
                else:
                    await page.screenshot(full_page=shot.full_page, **opts)

                size_kb = target.stat().st_size // 1024
                print(f'  ok {shot.name:<24} {size_kb:>5} KB  (masked {masked})')
            except Exception as exc:  # keep going, report at the end
                print(f'  FAIL {shot.name}: {type(exc).__name__}: {exc}')

        await browser.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('names', nargs='*', help='only capture these shots')
    parser.add_argument('--list', action='store_true', help='list shot names and exit')
    args = parser.parse_args()

    if args.list:
        for shot in SHOTS:
            print(f'{shot.name:<24} {shot.path}')
        return 0

    selected = [s for s in SHOTS if not args.names or s.name in args.names]
    if not selected:
        print('no matching shots', file=sys.stderr)
        return 1
    asyncio.run(capture(selected))
    manifest = OUT_DIR / 'manifest.json'
    manifest.write_text(
        json.dumps(sorted(p.name for p in OUT_DIR.glob('*.jpg')), ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
