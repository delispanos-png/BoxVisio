# In-app Help section (`/tenant/help`)

The tenant portal ships its own manual. It replaced the single long page that
used to live at `/tenant/manual` (that URL now 301s to the circuits page, so old
bookmarks and the per-page Help buttons keep working — the anchors are the same
circuit ids).

## Where the content lives

Content is data, not markup. Three modules own it:

| File | Owns |
| --- | --- |
| `backend/app/core/kpi_catalog.py` | Every KPI: title, what it shows, formula, data source, which filters move it, and the caveats. |
| `backend/app/core/help_content.py` | `CIRCUITS` (one entry per screen), `TASK_GROUPS` (the "how do I find X" index), `FAQ`. All bilingual. |
| `backend/app/templates/tenant/help/` | Presentation only. |

**The KPI catalog is the single source of truth.** It feeds all three of:

1. the KPI help popup in the app (served as JSON from
   `/tenant/help/kpi-catalog.json`, fetched once and browser-cached),
2. the KPI dictionary page (`/tenant/help/kpis`),
3. the per-circuit KPI tables (`/tenant/help/circuits`).

Before this existed, the popup text lived in a JS dictionary inside
`base_tenant.html` and the manual repeated the same explanations in a Jinja
literal. They had already drifted. Add a KPI in one place now.

### Adding or changing a KPI

Add a `Kpi(...)` entry to `CATALOG` in `kpi_catalog.py`. Nothing else to touch.

- `keys` are the card titles as actually rendered, plus aliases. Matching is
  accent- and case-insensitive.
- `normalize_kpi_key()` in Python and `normalizeKpiKey()` in `base_tenant.html`
  **must stay equivalent** — the catalog ships pre-normalised keys and the
  browser normalises the card title it reads off the DOM. Both use a
  Unicode-aware class; an ASCII-only one silently breaks every Greek title.

To confirm coverage after a change:

```bash
cd backend && python3 -c "
import sys; sys.path.insert(0,'.')
from app.core.kpi_catalog import catalog_for_lang, normalize_kpi_key
entries = catalog_for_lang('el')
seen = {}
for e in entries:
    for k in e['keys']:
        assert k not in seen, (k, seen[k], e['id'])
        seen[k] = e['id']
print(len(entries), 'entries,', len(seen), 'unique keys')
"
```

## Languages

Every user-visible string in `help_content.py` is a `_t(el, en)` map (or `_tl`
for a list). `localize()` flattens an entry to one language and the
`*_for_lang()` helpers are what the routes call — never read `CIRCUITS`
directly in a request path, or English users get Greek.

`kpi_catalog.py` is bilingual the same way, via the `Kpi` dataclass fields.

Template prose uses `{% set en = lang == 'en' %}` and inline conditionals. Note
that markup inside `{{ ... }}` is escaped, so anything containing tags must use
`{% if en %}…{% else %}…{% endif %}` blocks instead of a ternary.

To check nothing was missed:

```bash
cd backend && python3 -c "
import sys, re; sys.path.insert(0,'.')
from app.core.help_content import circuits_for_lang, task_groups_for_lang, faq_for_lang
GREEK = re.compile(r'[\u0370-\u03ff\u1f00-\u1fff]')
def walk(o, path, bad):
    if isinstance(o, str):
        if GREEK.search(o): bad.append((path, o[:60]))
    elif isinstance(o, dict):
        for k, v in o.items(): walk(v, f'{path}.{k}', bad)
    elif isinstance(o, (list, tuple)):
        for i, v in enumerate(o): walk(v, f'{path}[{i}]', bad)
bad = []
for name, data in (('circuits', circuits_for_lang('en')), ('tasks', task_groups_for_lang('en')), ('faq', faq_for_lang('en'))):
    walk(data, name, bad)
print('Greek left in EN:', bad or 'none')
"
```

The rest of the tenant portal is Greek-first; only the Help section is fully
bilingual. The last-sync placeholder in the page header is the one shell string
the Help pages needed, and it is handled in `base_tenant.html`.

## Per-circuit help

Each circuit has help in three shapes, all rendered from one template
(`tenant/help/_circuit_panel.html`) so the markup cannot drift:

| Where | Route |
| --- | --- |
| Combined page (all circuits) | `/tenant/help/circuits` |
| One circuit, standalone | `/tenant/help/circuits/<id>` |
| Fragment for the in-page drawer | `/tenant/help/circuits/<id>/panel` |

The **Help button under every page title** opens that page's circuit in a
slide-over (`bv-help-drawer.js`) rather than navigating, so the user keeps their
filters. The button's `href` is the standalone page, so no-JS, middle-click and
open-in-new-tab still behave correctly. An unknown circuit id redirects to the
combined page.

`_manual_help_anchor_map` in `base_tenant.html` maps `active_page` to a circuit.
Where two routes share an `active_page` (FnR and Replenishment do), the route
passes `manual_help_anchor` in its context to override the map.

## How a card finds its help text

The shared handler in `base_tenant.html` resolves, in order:

1. `data-kpi-id` on the card or trigger — names a catalog entry outright. Use
   this when the visible label is ambiguous across pages ("Προϊόντα" means
   something different on price control than on the market-data screens).
2. `data-kpi-what` / `-formula` / `-source` / `-filters` / `-caveats` on the
   card — per-page overrides, always win over the catalog for the field they set.
3. Title match against the catalog.
4. A generic fallback.

`BV_KPI_CARD_SELECTOR` lists every tile shape that opens help. `pc-metric-card`
(price control) and `fnr-kpi` (FnR summary) were never wired before, so their
`data-kpi-*` attributes did nothing on click.

**Cards that own their click.** Several pages bind their own handler to a card
to open a drill-down (inventory's stock-value card, price control's
above/below-target tiles, the dashboard's executive cards). The convention is:
**the card body opens the analysis, the small icon opens the help.** The
dashboard enforced this in a capture-phase listener that called
`stopImmediatePropagation()` on icon clicks, which made help unreachable there;
it now lets icon clicks through.

## Screenshots

`scripts/capture_manual_screenshots.py` drives a headless Chromium against the
running app and writes JPEGs to `backend/app/static/docs/manual/`. A circuit
whose shot is missing simply renders without an image, so a partial run is safe.

```bash
# needs a playwright venv (kept outside the repo) with chromium installed
python scripts/capture_manual_screenshots.py            # everything
python scripts/capture_manual_screenshots.py dashboard  # one shot
python scripts/capture_manual_screenshots.py --list
```

### Rules this script follows, and why

- **Shot against the internal R&D pilot tenant (id 1), never a customer.** The
  manual is served to every tenant.
- **Personal data is masked in the DOM before the shot is taken.** The pilot
  database holds real people's names. Customer names become pseudonyms,
  addresses become a neutral street, AFM/ΑΜΚΑ columns become `0XXXXXXXX`, and
  phones/emails are replaced by pattern. Masking happens client-side after
  render and before `page.screenshot`.
- **It authenticates with a short-lived token minted through the API
  container**, so no password is needed or stored.
- **It only ever submits the page's own filter form.** An earlier version fell
  back to `document.querySelector('form')`, which on filter-less pages is the
  sidebar logout form — it logged the session out mid-run and captured the login
  screen into ten "screenshots". There is now an explicit refusal plus an
  authenticated-shell check before every shot.
- **The default period is `01/01/2026 – 31/05/2026`**, chosen because the pilot
  dataset ends 2026-05-28; the app default (last 30 days) renders all zeros.
  Snapshot-based and pure-UI shots pass `period=None`.

## Tenant database privileges

`scripts/run_tenant_migrations.py` runs alembic as the **superuser**, so every
table a migration creates is owned by `postgres` and invisible to the tenant
role until privileges are reconciled — the tenant's own queries fail with
"permission denied for table ...". (The provisioning wizard migrates *as the
tenant user*, which is why freshly provisioned tenants never showed this and
only migrated-in-place ones broke.)

The script now always reconciles privileges after migrating, and sets
`ALTER DEFAULT PRIVILEGES` so future runs stay correct. To repair a tenant
without running migrations:

```bash
python scripts/run_tenant_migrations.py --tenant <slug> --grants-only
```

It exits non-zero and names the objects if anything is still unreadable.

### Feature-gated pages

Business Advisor, Call Center, Replenishment, IQVIA and eRA are disabled on the
pilot tenant's subscription and return 403. They were captured by temporarily
enabling those five flags on the pilot subscription and restoring the original
JSON immediately afterwards. If you re-shoot them, do the same — save
`subscriptions.feature_flags` for tenant 1 first, and diff it back.
