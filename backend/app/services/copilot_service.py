"""BoxVisio Co-Pilot — the AI assistant agent.

Hybrid data access: curated helpers over the existing KPI/help layer + a guarded
read-only SQL tool for maximum breadth. The tenant's OWN Anthropic (Claude) key is
used (billed to the tenant), decrypted in-memory only. Read-only SQL runs inside a
READ ONLY transaction with a statement timeout, single-statement + SELECT/WITH-only
guard, and a forced row cap — defence in depth so a prompt-injected query cannot write.
"""
from __future__ import annotations

import json
import re
from typing import Any, AsyncGenerator

from sqlalchemy import text

from app.core.kpi_catalog import catalog_for_lang
from app.db.tenant_manager import get_tenant_db_session
from app.models.control import CopilotConfig, Tenant
from app.services.copilot_config import copilot_api_key

# --- SQL guard -------------------------------------------------------------
_MAX_ROWS = 200
_STMT_TIMEOUT_MS = 15000
_SELECT_START = re.compile(r'^\s*(select|with)\b', re.IGNORECASE)
# Tables the aggregates-only scope may read (no raw fact rows, no customer PII).
_AGG_ONLY_ALLOWED_PREFIXES = ('agg_', 'dim_calendar', 'dim_branches', 'dim_warehouses',
                              'dim_categories', 'dim_groups', 'dim_items', 'dim_brands',
                              'dim_payment_methods', 'dim_document_types')


class SqlGuardError(ValueError):
    pass


def guard_sql(sql: str, *, data_scope: str = 'row_level') -> str:
    """Validate + normalise a model-supplied query. Raises SqlGuardError on anything
    that isn't a single read-only SELECT/CTE. Appends a LIMIT when missing."""
    raw = (sql or '').strip().rstrip(';').strip()
    if not raw:
        raise SqlGuardError('Κενό ερώτημα.')
    # Single statement only — no stacked queries.
    if ';' in raw:
        raise SqlGuardError('Επιτρέπεται μόνο ένα ερώτημα (χωρίς «;»).')
    if not _SELECT_START.match(raw):
        raise SqlGuardError('Επιτρέπονται μόνο ερωτήματα SELECT / WITH (read-only).')
    # Aggregates-only scope: forbid raw fact_* tables and customer/supplier detail.
    if data_scope == 'aggregates':
        lowered = raw.lower()
        for banned in ('fact_', 'dim_customers', 'dim_suppliers', 'dim_accounts',
                       'agg_customer_balances', 'agg_supplier_balances'):
            if banned in lowered:
                raise SqlGuardError(
                    'Σε λειτουργία «μόνο σύνολα» δεν επιτρέπονται αναλυτικές γραμμές '
                    f'({banned}). Χρησιμοποίησε τους πίνακες agg_*.'
                )
    # Force a row cap so a runaway query can't stream millions of rows to the model.
    if not re.search(r'\blimit\s+\d+', raw, re.IGNORECASE):
        raw = f'{raw}\nLIMIT {_MAX_ROWS}'
    return raw


def _jsonable(value: Any) -> Any:
    from datetime import date, datetime
    from decimal import Decimal
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


async def run_sql(tenant: Tenant, sql: str, *, data_scope: str = 'row_level') -> dict[str, Any]:
    """Execute a guarded read-only SELECT against the tenant DB. Returns {columns, rows}
    or {error}. Never writes — enforced by SELECT-only guard + READ ONLY transaction."""
    try:
        safe = guard_sql(sql, data_scope=data_scope)
    except SqlGuardError as exc:
        return {'error': str(exc)}
    try:
        async for db in get_tenant_db_session(
            tenant_key=str(tenant.id), db_name=tenant.db_name,
            db_user=tenant.db_user, db_password=tenant.db_password,
        ):
            # READ ONLY must be the first statement of the transaction.
            await db.execute(text('SET TRANSACTION READ ONLY'))
            await db.execute(text(f"SET LOCAL statement_timeout = '{_STMT_TIMEOUT_MS}'"))
            result = await db.execute(text(safe))
            mappings = result.mappings().fetchmany(_MAX_ROWS)
            rows = [{k: _jsonable(v) for k, v in m.items()} for m in mappings]
            await db.rollback()
            cols = list(rows[0].keys()) if rows else []
            return {'columns': cols, 'row_count': len(rows), 'rows': rows,
                    'truncated': len(rows) >= _MAX_ROWS}
    except Exception as exc:  # noqa: BLE001 — surface DB errors to the model, not to logs
        msg = str(exc).splitlines()[0][:300]
        return {'error': f'Σφάλμα εκτέλεσης: {msg}'}
    return {'error': 'Δεν ήταν διαθέσιμη η βάση του πελάτη.'}


async def describe_schema(tenant: Tenant, table: str | None = None, *, data_scope: str = 'row_level') -> dict[str, Any]:
    """List the tenant's analytics tables (fact_/dim_/agg_) and, for a given table,
    its columns + types — so the model can write correct SQL."""
    try:
        async for db in get_tenant_db_session(
            tenant_key=str(tenant.id), db_name=tenant.db_name,
            db_user=tenant.db_user, db_password=tenant.db_password,
        ):
            await db.execute(text('SET TRANSACTION READ ONLY'))
            if table:
                tbl = re.sub(r'[^a-z0-9_]', '', str(table).lower())
                rows = (await db.execute(text(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name=:t ORDER BY ordinal_position"
                ), {'t': tbl})).mappings().all()
                await db.rollback()
                return {'table': tbl, 'columns': [{'name': r['column_name'], 'type': r['data_type']} for r in rows]}
            names = (await db.execute(text(
                "SELECT tablename FROM pg_tables WHERE schemaname='public' AND "
                "(tablename LIKE 'fact_%' OR tablename LIKE 'dim_%' OR tablename LIKE 'agg_%') "
                "ORDER BY tablename"
            ))).scalars().all()
            await db.rollback()
            if data_scope == 'aggregates':
                names = [n for n in names if n.startswith(_AGG_ONLY_ALLOWED_PREFIXES)]
            return {'tables': list(names)}
    except Exception as exc:  # noqa: BLE001
        return {'error': str(exc).splitlines()[0][:300]}
    return {'error': 'Δεν ήταν διαθέσιμη η βάση του πελάτη.'}


def app_help(query: str | None = None) -> dict[str, Any]:
    """Return KPI definitions + circuit help (the single-source catalog) so the Co-Pilot
    can answer «τι κάνω εδώ / τι σημαίνει αυτό». Filters by keyword when given."""
    items = catalog_for_lang('el')
    q = (query or '').strip().lower()
    if q:
        terms = [t for t in re.split(r'\s+', q) if len(t) > 2]
        def match(it: dict) -> bool:
            hay = f"{it.get('title','')} {it.get('description','')}".lower()
            return any(t in hay for t in terms) if terms else True
        items = [it for it in items if match(it)] or items
    compact = [{'title': it.get('title', ''), 'description': it.get('description', '')}
               for it in items][:40]
    return {'kpis': compact}


# --- Anthropic tool schema -------------------------------------------------
TOOLS = [
    {
        'name': 'run_sql',
        'description': (
            'Εκτέλεσε ΜΟΝΟ read-only SQL (SELECT/WITH) στη βάση του πελάτη (PostgreSQL) '
            'και πάρε πίσω τις γραμμές. Χρησιμοποίησέ το για οποιαδήποτε ερώτηση για δεδομένα '
            '(πωλήσεις, απόθεμα, αγορές, ταμείο, έξοδα, πελάτες, προμηθευτές). Πάντα κάλεσε '
            'πρώτα describe_schema αν δεν ξέρεις τις στήλες. Οι πίνακες agg_* έχουν έτοιμα '
            'ημερήσια/μηνιαία σύνολα (πιο γρήγορα από τα fact_*).'
        ),
        'input_schema': {
            'type': 'object',
            'properties': {'query': {'type': 'string', 'description': 'Ένα SELECT/WITH ερώτημα PostgreSQL.'}},
            'required': ['query'],
        },
    },
    {
        'name': 'describe_schema',
        'description': (
            'Δες τους διαθέσιμους πίνακες ανάλυσης (fact_/dim_/agg_) και, με όρισμα table, '
            'τις στήλες + τύπους ενός πίνακα. Κάλεσέ το πριν γράψεις SQL για άγνωστο πίνακα.'
        ),
        'input_schema': {
            'type': 'object',
            'properties': {'table': {'type': 'string', 'description': 'Προαιρετικό όνομα πίνακα για τις στήλες του.'}},
        },
    },
    {
        'name': 'app_help',
        'description': (
            'Επεξηγήσεις της εφαρμογής και ορισμοί KPI (μοναδική πηγή αλήθειας). '
            'Χρησιμοποίησέ το για «τι κάνω εδώ / τι σημαίνει αυτός ο δείκτης / τι θα έπρεπε να κοιτάξω».'
        ),
        'input_schema': {
            'type': 'object',
            'properties': {'query': {'type': 'string', 'description': 'Προαιρετική λέξη-κλειδί για φιλτράρισμα.'}},
        },
    },
]


_DATA_GUIDE = (
    "\n\nΟΔΗΓΟΣ ΔΕΔΟΜΕΝΩΝ (PostgreSQL):\n"
    "- Πωλήσεις: agg_sales_daily / agg_sales_monthly (έτοιμα σύνολα ανά ημέρα/μήνα· στήλες qty, net_value, gross_value, doc_date, behavior_code) και agg_sales_item_daily ανά είδος. Αναλυτικά: fact_sales.\n"
    "- Αγορές: agg_purchases_daily/monthly, fact_purchases. Απόθεμα: agg_inventory_snapshot_daily, fact_inventory. Ταμείο: agg_cash_daily, fact_cashflows. Έξοδα: agg_expenses_*. Πελάτες/προμηθευτές: dim_customers/dim_suppliers, agg_*_balances_daily.\n"
    "- Διαστάσεις: dim_items, dim_branches, dim_categories, dim_suppliers κ.λπ. (σύνδεση με τα *_ext_id).\n"
    "- Προτίμησε τους πίνακες agg_* (είναι προ-αθροισμένοι, πολύ πιο γρήγοροι) εκτός αν χρειάζεσαι αναλυτική γραμμή.\n"
    "- behavior_code: ο 131 είναι η βασική ΠΩΛΗΣΗ (θετικά ποσά)· οι 181/151/152 είναι ΕΠΙΣΤΡΟΦΕΣ/πιστωτικά (αρνητικά ποσά). Για «καθαρές πωλήσεις» άθροισε net_value σε όλα τα behavior_code (οι επιστροφές αφαιρούνται μόνες τους αφού είναι αρνητικές). Για «μεικτές πωλήσεις» χρησιμοποίησε μόνο τα θετικά (behavior_code=131). Πάντα ανάφερε ποια βάση χρησιμοποίησες.\n"
    "- Ποσά σε €, ημερομηνίες σε dd/mm/yyyy στην απάντηση."
)


def build_system_prompt(tenant: Tenant, *, page_context: str | None = None) -> str:
    company = getattr(tenant, 'display_name', None) or getattr(tenant, 'name', None) or 'η επιχείρηση'
    ctx = f"\nΟ χρήστης αυτή τη στιγμή βλέπει τη σελίδα: «{page_context}». Αν ρωτήσει «τι κάνω εδώ;» εξήγησε αυτή τη σελίδα." if page_context else ''
    return (
        f"Είσαι ο Co-Pilot του BoxVisio BI για {company} — ένας έμπειρος αναλυτής "
        "επιχειρηματικής ευφυΐας για φαρμακείο. Απαντάς στα ελληνικά, με σαφήνεια και "
        "συντομία, με νούμερα και συμπεράσματα που βοηθούν στη λήψη αποφάσεων.\n\n"
        "Έχεις εργαλεία για να διαβάζεις τα ΠΡΑΓΜΑΤΙΚΑ δεδομένα του πελάτη:\n"
        "- run_sql: read-only SQL στη βάση (πωλήσεις, απόθεμα, αγορές, ταμείο, έξοδα κ.λπ.)\n"
        "- describe_schema: πίνακες/στήλες πριν γράψεις SQL\n"
        "- app_help: επεξηγήσεις εφαρμογής & ορισμοί KPI\n\n"
        "Κανόνες: Ποτέ μην επινοείς νούμερα — αν χρειάζεσαι στοιχεία, κάλεσε εργαλείο. "
        "Οι ημερομηνίες εμφανίζονται ως dd/mm/yyyy. Τα ποσά σε €. Όταν δίνεις νούμερο, πες "
        "και την περίοδο. Αν ένα ερώτημα είναι ασαφές, κάνε τη λογική υπόθεση και ανάφερέ την. "
        "Κράτα τις απαντήσεις εστιασμένες."
        f"{ctx}"
        f"{_DATA_GUIDE}"
    )


async def _dispatch_tool(tenant: Tenant, name: str, tool_input: dict, *, data_scope: str) -> dict:
    if name == 'run_sql':
        return await run_sql(tenant, str(tool_input.get('query') or ''), data_scope=data_scope)
    if name == 'describe_schema':
        return await describe_schema(tenant, tool_input.get('table'), data_scope=data_scope)
    if name == 'app_help':
        return app_help(tool_input.get('query'))
    return {'error': f'Άγνωστο εργαλείο: {name}'}


async def stream_answer(
    config: CopilotConfig,
    tenant: Tenant,
    history: list[dict[str, Any]],
    *,
    page_context: str | None = None,
    max_rounds: int = 6,
) -> AsyncGenerator[dict[str, Any], None]:
    """Run the agentic loop and yield SSE-friendly events:
      {'type':'text','text':...} | {'type':'tool','name':...} | {'type':'done','usage':...}
      | {'type':'error','error':...}
    `history` is a list of {'role','content'} where content is a string or a list of blocks.
    """
    import anthropic

    api_key = copilot_api_key(config)
    if not api_key:
        yield {'type': 'error', 'error': 'Δεν έχει οριστεί κλειδί Anthropic στις ρυθμίσεις.'}
        return

    client = anthropic.AsyncAnthropic(api_key=api_key)
    system = build_system_prompt(tenant, page_context=page_context)
    data_scope = config.data_scope or 'row_level'
    messages: list[dict[str, Any]] = list(history)
    out_tokens = 0
    in_tokens = 0

    try:
        for _round in range(max_rounds):
            model = config.model or 'claude-opus-5'
            kwargs: dict[str, Any] = dict(
                model=model, max_tokens=4096, system=system, tools=TOOLS, messages=messages,
            )
            # Low effort = far less thinking time (this is a data-lookup assistant, not a
            # deep-reasoning task) → much faster first token. Stream the thinking summary so
            # the user sees the model working. Haiku doesn't support adaptive thinking/effort.
            if 'haiku' not in model:
                kwargs['thinking'] = {'type': 'adaptive', 'display': 'summarized'}
                kwargs['output_config'] = {'effort': 'low'}
            async with client.messages.stream(**kwargs) as stream:
                async for event in stream:
                    if getattr(event, 'type', '') != 'content_block_delta':
                        continue
                    delta = getattr(event, 'delta', None)
                    dtype = getattr(delta, 'type', '')
                    if dtype == 'thinking_delta':
                        chunk = getattr(delta, 'thinking', '') or ''
                        if chunk:
                            yield {'type': 'thinking', 'text': chunk}
                    elif dtype == 'text_delta':
                        chunk = getattr(delta, 'text', '') or ''
                        if chunk:
                            yield {'type': 'text', 'text': chunk}
                final = await stream.get_final_message()

            out_tokens += int(getattr(final.usage, 'output_tokens', 0) or 0)
            in_tokens += int(getattr(final.usage, 'input_tokens', 0) or 0)
            messages.append({'role': 'assistant', 'content': final.content})

            if final.stop_reason != 'tool_use':
                yield {'type': 'done', 'usage': {'input_tokens': in_tokens, 'output_tokens': out_tokens}}
                return

            tool_results = []
            for block in final.content:
                if getattr(block, 'type', None) == 'tool_use':
                    yield {'type': 'tool', 'name': block.name}
                    result = await _dispatch_tool(tenant, block.name, dict(block.input or {}), data_scope=data_scope)
                    tool_results.append({
                        'type': 'tool_result',
                        'tool_use_id': block.id,
                        'content': json.dumps(result, ensure_ascii=False)[:60000],
                    })
            messages.append({'role': 'user', 'content': tool_results})

        yield {'type': 'done', 'usage': {'input_tokens': in_tokens, 'output_tokens': out_tokens}}
    except Exception as exc:  # noqa: BLE001
        yield {'type': 'error', 'error': str(exc).splitlines()[0][:300]}
