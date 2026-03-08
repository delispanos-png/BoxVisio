(function () {
  function toNumber(value) {
    const n = Number(value);
    return Number.isFinite(n) ? n : 0;
  }

  function cellText(row, index) {
    const cell = row.children[index];
    return cell ? String(cell.textContent || '').trim() : '';
  }

  function parseMaybeNumber(text) {
    const cleaned = String(text || '')
      .replace(/\./g, '')
      .replace(',', '.')
      .replace(/[^\d\-\.]/g, '');
    if (!cleaned) return null;
    const n = Number(cleaned);
    return Number.isFinite(n) ? n : null;
  }

  function attachAdminTable(table) {
    if (!table || table.dataset.bvAdminTableBound === '1') return;
    const tbody = table.tBodies && table.tBodies[0];
    if (!tbody) return;

    const baseRows = Array.from(tbody.querySelectorAll('tr'));
    if (baseRows.length <= 10) return;

    table.dataset.bvAdminTableBound = '1';

    const wrapper = table.closest('.table-responsive') || table.parentElement;
    const host = wrapper && wrapper.parentElement ? wrapper.parentElement : table.parentElement;
    if (!host) return;

    const tools = document.createElement('div');
    tools.className = 'bv-admin-table-tools';
    tools.innerHTML = `
      <input type="search" class="form-control form-control-sm bv-admin-table-search" placeholder="Αναζήτηση..." aria-label="Αναζήτηση">
      <div class="bv-pager-field bv-pager-field--size">
        <label class="bv-pager-label">Ανά σελίδα</label>
        <select class="form-select form-select-sm">
          <option value="25">25</option>
          <option value="50" selected>50</option>
          <option value="100">100</option>
          <option value="250">250</option>
        </select>
      </div>
    `;

    const footer = document.createElement('div');
    footer.className = 'bv-admin-table-footer';
    footer.innerHTML = `
      <small class="bv-admin-table-range">Εμφάνιση 0 έως 0 από 0</small>
      <div class="bv-pager">
        <div class="bv-pager-nav" role="group" aria-label="Πλοήγηση σελίδων">
          <button type="button" class="btn btn-outline-secondary btn-sm" data-nav="first" aria-label="Πρώτη σελίδα">&laquo;</button>
          <button type="button" class="btn btn-outline-secondary btn-sm" data-nav="prev" aria-label="Προηγούμενη σελίδα">&lsaquo;</button>
          <button type="button" class="btn btn-outline-secondary btn-sm" data-nav="next" aria-label="Επόμενη σελίδα">&rsaquo;</button>
          <button type="button" class="btn btn-outline-secondary btn-sm" data-nav="last" aria-label="Τελευταία σελίδα">&raquo;</button>
        </div>
        <small class="bv-pager-info">1 / 1</small>
      </div>
    `;

    host.insertBefore(tools, wrapper);
    host.appendChild(footer);

    const searchInput = tools.querySelector('.bv-admin-table-search');
    const pageSizeSelect = tools.querySelector('select');
    const rangeEl = footer.querySelector('.bv-admin-table-range');
    const pageInfoEl = footer.querySelector('.bv-pager-info');
    const navButtons = {
      first: footer.querySelector('[data-nav="first"]'),
      prev: footer.querySelector('[data-nav="prev"]'),
      next: footer.querySelector('[data-nav="next"]'),
      last: footer.querySelector('[data-nav="last"]'),
    };

    let rows = baseRows.slice();
    let page = 1;
    let sortIdx = -1;
    let sortDir = 'asc';

    function applySort(inputRows) {
      if (sortIdx < 0) return inputRows.slice();
      const sorted = inputRows.slice().sort((a, b) => {
        const aText = cellText(a, sortIdx);
        const bText = cellText(b, sortIdx);
        const aNum = parseMaybeNumber(aText);
        const bNum = parseMaybeNumber(bText);
        let cmp = 0;
        if (aNum !== null && bNum !== null) cmp = aNum - bNum;
        else cmp = aText.localeCompare(bText, 'el', { sensitivity: 'base' });
        return sortDir === 'asc' ? cmp : -cmp;
      });
      return sorted;
    }

    function render() {
      const searchTerm = String(searchInput.value || '').trim().toLowerCase();
      const size = Math.max(1, toNumber(pageSizeSelect.value) || 50);
      const filtered = searchTerm
        ? rows.filter((row) => String(row.textContent || '').toLowerCase().includes(searchTerm))
        : rows.slice();
      const sorted = applySort(filtered);
      const pages = Math.max(1, Math.ceil(sorted.length / size));
      if (page > pages) page = pages;

      const offset = (page - 1) * size;
      const pageRows = sorted.slice(offset, offset + size);

      tbody.innerHTML = '';
      if (!pageRows.length) {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = Math.max(1, table.tHead?.rows?.[0]?.cells?.length || 1);
        td.className = 'text-center text-muted py-3';
        td.textContent = 'Δεν υπάρχουν διαθέσιμα δεδομένα.';
        tr.appendChild(td);
        tbody.appendChild(tr);
      } else {
        pageRows.forEach((row) => tbody.appendChild(row));
      }

      const from = sorted.length ? offset + 1 : 0;
      const to = Math.min(offset + pageRows.length, sorted.length);
      rangeEl.textContent = `Εμφάνιση ${from} έως ${to} από ${sorted.length}`;
      pageInfoEl.textContent = `${page} / ${pages}`;

      navButtons.first.disabled = page <= 1;
      navButtons.prev.disabled = page <= 1;
      navButtons.next.disabled = page >= pages;
      navButtons.last.disabled = page >= pages;
    }

    const headers = Array.from(table.tHead?.rows?.[0]?.cells || []);
    headers.forEach((th, idx) => {
      if (!th || th.dataset.bvSortable === '0') return;
      th.style.cursor = 'pointer';
      th.title = 'Ταξινόμηση';
      th.addEventListener('click', () => {
        if (sortIdx === idx) {
          sortDir = sortDir === 'asc' ? 'desc' : 'asc';
        } else {
          sortIdx = idx;
          sortDir = 'asc';
        }
        headers.forEach((h) => h.removeAttribute('data-sort'));
        th.setAttribute('data-sort', sortDir);
        render();
      });
    });

    searchInput.addEventListener('input', () => {
      page = 1;
      render();
    });
    pageSizeSelect.addEventListener('change', () => {
      page = 1;
      render();
    });

    navButtons.first.addEventListener('click', () => {
      page = 1;
      render();
    });
    navButtons.prev.addEventListener('click', () => {
      page = Math.max(1, page - 1);
      render();
    });
    navButtons.next.addEventListener('click', () => {
      page += 1;
      render();
    });
    navButtons.last.addEventListener('click', () => {
      const size = Math.max(1, toNumber(pageSizeSelect.value) || 50);
      page = Math.max(1, Math.ceil(rows.length / size));
      render();
    });

    render();
  }

  function initPageBreadCrumb() {
    document.querySelectorAll('.bv-page-head').forEach((head) => {
      const breadcrumb = head.querySelector('.bv-page-breadcrumb');
      if (!breadcrumb) return;
      breadcrumb.setAttribute('title', breadcrumb.textContent || '');
    });
  }

  function init() {
    initPageBreadCrumb();
    document.querySelectorAll('table[data-bv-admin-table="1"]').forEach(attachAdminTable);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
