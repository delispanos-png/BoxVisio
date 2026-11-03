"""Routines: scheduled emails built on a saved Report Builder report.

A routine attaches the report as Excel for a rolling period (yesterday, month to
date, …) and, when the tenant's Co-Pilot is configured and the routine carries a
prompt, adds a short Co-Pilot commentary written from the same rows. The Excel is
produced by the exact code behind the Report Builder's download button, so the
attachment matches what the user sees on screen.
"""
from __future__ import annotations

import io
from datetime import date
from types import SimpleNamespace
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.datastructures import QueryParams

from app.models.control import CopilotConfig, CopilotSchedule, SavedReport, Tenant
from app.services.copilot_config import REPORT_KIND_LABELS, copilot_is_ready, routine_report_params

XLSX_MIME = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
_COMMENTARY_ROWS = 60


async def build_routine_report(db: AsyncSession, tenant: Tenant, sched: CopilotSchedule, today: date) -> dict[str, Any]:
    """Return {'filename', 'content', 'report_name', 'kind_label', 'period', 'rows_text'}.
    Raises RuntimeError with a Greek message when the report cannot be built."""
    from app.api.ui import _render_exports_download  # heavy module; imported on demand

    rep = (
        await db.execute(
            select(SavedReport).where(SavedReport.id == int(sched.report_id), SavedReport.tenant_id == int(tenant.id))
        )
    ).scalar_one_or_none()
    if rep is None:
        raise RuntimeError('Η αποθηκευμένη αναφορά της ρουτίνας δεν υπάρχει πια. Διάλεξε άλλη στις Ρουτίνες.')
    if rep.kind not in REPORT_KIND_LABELS:
        raise RuntimeError(f'Μη υποστηριζόμενος τύπος αναφοράς: {rep.kind}')
    qs, period_desc = routine_report_params(
        rep.params, period_preset=sched.period_preset, compare_preset=sched.compare_preset, today=today
    )
    fake_request = SimpleNamespace(query_params=QueryParams(qs))
    response = await _render_exports_download(request=fake_request, tenant=tenant, variant=rep.kind, fmt='xlsx')
    if getattr(response, 'status_code', 200) != 200:
        raise RuntimeError('Η εξαγωγή της αναφοράς απέτυχε.')
    content = bytes(response.body)
    safe_name = ''.join(ch if ch.isalnum() or ch in ' _-' else '_' for ch in rep.name).strip() or 'report'
    filename = f"{safe_name}_{today.strftime('%Y%m%d')}.xlsx"
    return {
        'filename': filename,
        'content': content,
        'report_name': rep.name,
        'kind_label': REPORT_KIND_LABELS.get(rep.kind, rep.kind),
        'period': period_desc,
        'rows_text': _rows_as_text(content),
    }


def _rows_as_text(xlsx: bytes) -> str:
    """The first rows of the sheet as a compact text table for the Co-Pilot commentary."""
    try:
        from openpyxl import load_workbook
    except Exception:  # noqa: BLE001
        return ''
    try:
        wb = load_workbook(io.BytesIO(xlsx), read_only=True, data_only=True)
        ws = wb.active
        lines = []
        for i, row in enumerate(ws.iter_rows(values_only=True)):
            if i >= _COMMENTARY_ROWS + 1:
                lines.append('…')
                break
            lines.append(' | '.join('' if v is None else str(v) for v in row))
        return '\n'.join(lines)
    except Exception:  # noqa: BLE001
        return ''


async def routine_commentary(config: CopilotConfig | None, tenant: Tenant, sched: CopilotSchedule, report: dict[str, Any]) -> tuple[str, int]:
    """Co-Pilot commentary on the attached report (only when configured and asked for)."""
    if not (sched.prompt or '').strip() or not copilot_is_ready(config):
        return '', 0
    from app.services.copilot_service import generate_report

    prompt = (
        f"Στέλνεται αυτόματα με email η αναφορά «{report['report_name']}» ({report['kind_label']}) "
        f"για την περίοδο {report['period']}, συνημμένη ως Excel. Τα δεδομένα της αναφοράς είναι:\n\n"
        f"{report['rows_text'] or '(κενή αναφορά)'}\n\n"
        f"Οδηγίες του χρήστη για το σχόλιο που θα συνοδεύει την αναφορά: {sched.prompt.strip()}\n\n"
        "Γράψε στα ελληνικά ένα σύντομο, πρακτικό σχόλιο με bullets, βασισμένο ΜΟΝΟ στα παραπάνω δεδομένα "
        "(και, αν χρειάζεται, στα εργαλεία σου). Μην επαναλάβεις όλο τον πίνακα· ανάδειξε τα σημαντικά."
    )
    return await generate_report(config, tenant, prompt=prompt)
