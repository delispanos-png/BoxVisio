/* BoxVisio POS dashboard customer patch
 * Version: 2026-04-22
 *
 * SQL connector fields this patch expects through the backend:
 * - SoftOne FINDOC.PAYMENT -> payment_method
 * - SoftOne FINDOC.CCC88ECHANNEL -> channel_ext_id / channel_name
 * - SoftOne ITEM.MTRGROUP -> group_external_id / group_name -> DimGroup / item group breakdown
 *
 * What this patch does:
 * - turns the "Αποδείξεις Περιόδου" KPI icon into a payment-method breakdown modal trigger
 * - turns the "Σύνολο Εισπράξεων" KPI icon into an item-group breakdown modal trigger
 * - renames POS wording from "Κανάλι Πώλησης" to "Ομάδα Ειδών"
 * - hides legacy inline panels when present, so the page stays cleaner
 *
 * Expected backend support:
 * - GET /v1/kpi/pos/by-payment-method
 * - GET /v1/kpi/pos/by-category
 *
 * Notes:
 * - the by-category endpoint must already return item-group labels from SoftOne ITEM.MTRGROUP
 *   if you want OTC / RX / Παραφάρμακο instead of channel/store labels.
 */
(function () {
  'use strict';

  const PATCH_ATTR = 'data-pos-customer-patch-20260422';
  if (document.documentElement.hasAttribute(PATCH_ATTR)) return;
  document.documentElement.setAttribute(PATCH_ATTR, '1');

  const EUR = new Intl.NumberFormat('el-GR', {
    style: 'currency',
    currency: 'EUR',
    maximumFractionDigits: 2,
  });
  const NUM = new Intl.NumberFormat('el-GR', { maximumFractionDigits: 1 });
  const NUM0 = new Intl.NumberFormat('el-GR', { maximumFractionDigits: 0 });
  const COLORS = ['#3b82f6', '#22c55e', '#f97316', '#8b5cf6', '#ec4899', '#06b6d4', '#ef4444', '#eab308'];

  function fmtEur(v) {
    return EUR.format(Number(v || 0));
  }

  function fmtNum(v) {
    return NUM.format(Number(v || 0));
  }

  function fmtInt(v) {
    return NUM0.format(Number(v || 0));
  }

  function posFiltersToParams() {
    const form = document.getElementById('posFilters');
    const p = new URLSearchParams();
    if (!form) return p;
    const fd = new FormData(form);
    const fromRaw = fd.get('from');
    const toRaw = fd.get('to');
    const toIso = window.bvToIsoDate
      ? window.bvToIsoDate.bind(window)
      : function fallback(raw) { return String(raw || '').trim(); };
    p.set('from', toIso(fromRaw));
    p.set('to', toIso(toRaw));
    const branches = document.getElementById('posBranches');
    Array.from(branches?.selectedOptions || []).forEach((opt) => p.append('branches', opt.value));
    return p;
  }

  function ensureStyles() {
    if (document.getElementById('posCustomerPatchStyles')) return;
    const style = document.createElement('style');
    style.id = 'posCustomerPatchStyles';
    style.textContent = `
      .pos-patch-clickable-icon { cursor: pointer; }
      .pos-patch-modal .modal-dialog { max-width: 1180px; }
      .pos-patch-modal .modal-body { padding: 1.25rem 1.5rem; }
      .pos-patch-table th { font-size: 12px; text-transform: uppercase; letter-spacing: .03em; color: #64748b; font-weight: 700; }
      .pos-patch-table td { vertical-align: middle; font-weight: 600; }
      .pos-patch-empty { border: 1px dashed #cbd5e1; border-radius: 12px; padding: 20px; color: #64748b; text-align: center; background: #f8fafc; font-weight: 600; }
      .pos-patch-bar-track { height: 8px; border-radius: 999px; background: #e9eef8; overflow: hidden; margin-top: 4px; }
      .pos-patch-bar-fill { height: 100%; border-radius: 999px; }
      .pos-patch-pm-row { display: flex; align-items: center; justify-content: space-between; gap: 10px; padding: 10px 0; border-bottom: 1px dashed #e2e8f0; }
      .pos-patch-pm-row:last-child { border-bottom: none; }
      .pos-patch-pm-name { font-weight: 700; color: #1f2937; flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .pos-patch-pm-value { font-weight: 800; color: #0f172a; white-space: nowrap; }
      .pos-patch-pm-pct { font-size: 12px; font-weight: 700; color: #64748b; white-space: nowrap; }
    `;
    document.head.appendChild(style);
  }

  function buildModal(id, titleText, subtitleId, bodyHtml) {
    let el = document.getElementById(id);
    if (el) return el;
    const wrapper = document.createElement('div');
    wrapper.innerHTML = `
      <div class="modal fade bv-modal bv-modal--responsive pos-patch-modal" id="${id}" tabindex="-1" aria-hidden="true" data-bv-modal="true" data-bv-close-on-backdrop="true">
        <div class="modal-dialog modal-xl">
          <div class="modal-content">
            <div class="modal-header">
              <div>
                <h6 class="modal-title fw-bold mb-0" style="font-size:1rem;color:#0f172a;">${titleText}</h6>
                <div id="${subtitleId}" style="font-size:.78rem;color:#64748b;"></div>
              </div>
              <button type="button" class="btn btn-outline-primary btn-sm" data-bs-dismiss="modal">
                <i class="fe fe-x me-1"></i> Κλείσιμο
              </button>
            </div>
            <div class="modal-body">${bodyHtml}</div>
          </div>
        </div>
      </div>
    `.trim();
    el = wrapper.firstElementChild;
    document.body.appendChild(el);
    return el;
  }

  function getModalController(el) {
    if (!el) return null;
    if (window.BVModalSystem?.create) return window.BVModalSystem.create(el, { closeOnBackdrop: true, keyboard: true });
    if (window.bootstrap?.Modal) return window.bootstrap.Modal.getOrCreateInstance(el);
    return {
      show() {
        el.style.display = 'flex';
        el.classList.add('show');
        document.body.classList.add('modal-open');
      },
    };
  }

  function updateSubtitle(id) {
    const el = document.getElementById(id);
    if (!el) return;
    const params = posFiltersToParams();
    const from = params.get('from') || '';
    const to = params.get('to') || '';
    const branchCount = params.getAll('branches').length;
    const dateLabel = from && to ? `${from} → ${to}` : 'Τρέχουσα περίοδος';
    el.textContent = branchCount ? `${dateLabel} · ${branchCount} υποκαταστήματα` : dateLabel;
  }

  async function fetchJson(url) {
    const resp = await fetch(url);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return await resp.json();
  }

  function renderBreakdownTable(targetId, rows, nameKey) {
    const tbody = document.getElementById(targetId);
    if (!tbody) return;
    const total = rows.reduce((sum, row) => sum + Number(row.value || row.gross_value || 0), 0);
    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="4" class="text-center text-muted py-4">Δεν υπάρχουν δεδομένα</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map((row, index) => {
      const value = Number(row.value || row.gross_value || 0);
      const pct = total > 0 ? (value / total * 100) : 0;
      const color = COLORS[index % COLORS.length];
      const name = String(row[nameKey] || row.name || row.category || 'N/A');
      return `
        <tr>
          <td class="ps-3 fw-bold">${name}</td>
          <td class="text-end">${fmtEur(value)}</td>
          <td class="text-end">${fmtNum(pct)}%</td>
          <td class="pe-3">
            <div class="pos-patch-bar-track">
              <div class="pos-patch-bar-fill" style="width:${pct.toFixed(1)}%;background:${color}"></div>
            </div>
          </td>
        </tr>
      `;
    }).join('');
  }

  function renderPaymentList(targetId, rows) {
    const wrap = document.getElementById(targetId);
    if (!wrap) return;
    if (!rows.length) {
      wrap.innerHTML = '<div class="pos-patch-empty">Δεν υπάρχουν δεδομένα</div>';
      return;
    }
    const total = rows.reduce((sum, row) => sum + Number(row.value || row.gross_value || 0), 0);
    wrap.innerHTML = rows.map((row, index) => {
      const value = Number(row.value || row.gross_value || 0);
      const receipts = Number(row.receipts || 0);
      const pct = total > 0 ? (value / total * 100) : 0;
      const color = COLORS[index % COLORS.length];
      const name = String(row.payment_method || row.name || 'N/A');
      return `
        <div class="pos-patch-pm-row">
          <div class="pos-patch-pm-name" title="${name}">${name}</div>
          <div class="d-flex flex-column align-items-end" style="min-width:120px">
            <span class="pos-patch-pm-value">${fmtEur(value)}</span>
            <span class="pos-patch-pm-pct">${fmtNum(pct)}% · ${fmtInt(receipts)} απ.</span>
            <div class="pos-patch-bar-track" style="width:110px">
              <div class="pos-patch-bar-fill" style="width:${pct.toFixed(1)}%;background:${color}"></div>
            </div>
          </div>
        </div>
      `;
    }).join('');
  }

  function findMetricCardByLabel(labelText) {
    return Array.from(document.querySelectorAll('.bv-metric-card')).find((card) => {
      const label = card.querySelector('.mc-label');
      return label && label.textContent.trim() === labelText;
    }) || null;
  }

  function hideLegacyPanels() {
    const paymentHeader = Array.from(document.querySelectorAll('.card-header h6')).find((el) => el.textContent.includes('Εισπράξεις ανά Τρόπο Πληρωμής'));
    if (paymentHeader) {
      const card = paymentHeader.closest('.card');
      if (card) card.closest('.col-12, .col-xl-5, .col-xl-4, .col-xl-6')?.setAttribute('hidden', 'hidden');
    }
    const detailHeader = Array.from(document.querySelectorAll('.card-header h6')).find((el) => (
      el.textContent.includes('Αναλυτικά ανά Κανάλι Πώλησης') || el.textContent.includes('Αναλυτικά ανά Κατηγορία')
    ));
    if (detailHeader) {
      const card = detailHeader.closest('.card');
      if (card) card.closest('.row')?.setAttribute('hidden', 'hidden');
    }
  }

  function renameLabels() {
    Array.from(document.querySelectorAll('.card-header h6')).forEach((el) => {
      const txt = el.textContent || '';
      if (txt.includes('Έσοδα ανά Κανάλι Πώλησης')) {
        el.innerHTML = '<i class="fe fe-grid me-1"></i> Έσοδα ανά Ομάδα Ειδών';
      }
      if (txt.includes('Αναλυτικά ανά Κανάλι Πώλησης')) {
        el.innerHTML = '<i class="fe fe-list me-1"></i> Αναλυτικά ανά Ομάδα Ειδών';
      }
    });
  }

  async function openPaymentModal(triggerEl) {
    ensureStyles();
    const modalEl = buildModal(
      'posCustomerPatchPaymentModal',
      'Εισπράξεις ανά Τρόπο Πληρωμής',
      'posCustomerPatchPaymentSubtitle',
      '<div id="posCustomerPatchPaymentBody"><div class="pos-patch-empty">Φόρτωση...</div></div>'
    );
    updateSubtitle('posCustomerPatchPaymentSubtitle');
    const controller = getModalController(modalEl);
    controller?.show(triggerEl);
    const body = document.getElementById('posCustomerPatchPaymentBody');
    if (body) body.innerHTML = '<div class="pos-patch-empty">Φόρτωση...</div>';
    try {
      const rows = await fetchJson(`/v1/kpi/pos/by-payment-method?${posFiltersToParams().toString()}`);
      renderPaymentList('posCustomerPatchPaymentBody', Array.isArray(rows) ? rows : []);
    } catch (err) {
      if (body) body.innerHTML = '<div class="pos-patch-empty">Σφάλμα φόρτωσης δεδομένων.</div>';
      console.error('pos customer patch payment modal error', err);
    }
  }

  async function openGroupModal(triggerEl) {
    ensureStyles();
    const modalEl = buildModal(
      'posCustomerPatchGroupModal',
      'Αναλυτικά ανά Ομάδα Ειδών',
      'posCustomerPatchGroupSubtitle',
      `
        <div class="table-responsive">
          <table class="table table-hover mb-0 pos-patch-table">
            <thead>
              <tr>
                <th class="ps-3">Ομάδα Ειδών</th>
                <th class="text-end">Αξία (€)</th>
                <th class="text-end">Μερίδιο %</th>
                <th class="pe-3" style="min-width:160px">Διανομή</th>
              </tr>
            </thead>
            <tbody id="posCustomerPatchGroupTbody">
              <tr><td colspan="4" class="text-center text-muted py-4">Φόρτωση...</td></tr>
            </tbody>
          </table>
        </div>
      `
    );
    updateSubtitle('posCustomerPatchGroupSubtitle');
    const controller = getModalController(modalEl);
    controller?.show(triggerEl);
    try {
      const rows = await fetchJson(`/v1/kpi/pos/by-category?${posFiltersToParams().toString()}`);
      renderBreakdownTable('posCustomerPatchGroupTbody', Array.isArray(rows) ? rows : [], 'category');
    } catch (err) {
      const tbody = document.getElementById('posCustomerPatchGroupTbody');
      if (tbody) {
        tbody.innerHTML = '<tr><td colspan="4" class="text-center text-muted py-4">Σφάλμα φόρτωσης δεδομένων.</td></tr>';
      }
      console.error('pos customer patch group modal error', err);
    }
  }

  function attachHandlers() {
    const receiptsCard = findMetricCardByLabel('Αποδείξεις Περιόδου');
    const grossCard = findMetricCardByLabel('Σύνολο Εισπράξεων');

    const receiptsIcon = receiptsCard?.querySelector('.mc-icon');
    if (receiptsIcon && !receiptsIcon.dataset.posPatchBound) {
      receiptsIcon.dataset.posPatchBound = '1';
      receiptsIcon.classList.add('pos-patch-clickable-icon');
      receiptsIcon.title = 'Αναλυτικά ανά τρόπο πληρωμής';
      receiptsIcon.addEventListener('click', function (e) {
        e.preventDefault();
        e.stopPropagation();
        e.stopImmediatePropagation();
        openPaymentModal(receiptsIcon);
      });
    }

    const grossIcon = grossCard?.querySelector('.mc-icon');
    if (grossIcon && !grossIcon.dataset.posPatchBound) {
      grossIcon.dataset.posPatchBound = '1';
      grossIcon.classList.add('pos-patch-clickable-icon');
      grossIcon.title = 'Αναλυτικά ανά ομάδα ειδών';
      grossIcon.addEventListener('click', function (e) {
        e.preventDefault();
        e.stopPropagation();
        e.stopImmediatePropagation();
        openGroupModal(grossIcon);
      });
    }
  }

  function onReady() {
    if (!location.pathname.includes('/tenant/pos')) return;
    renameLabels();
    hideLegacyPanels();
    attachHandlers();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', onReady, { once: true });
  } else {
    onReady();
  }
})();
