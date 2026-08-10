/* Cascading item-category filters (Κατ.1 → Κατ.2 → Κατ.3).
   Works with the checkbox dropdown widgets used by FNR / Availability / Destocking
   (`[data-fnr-multi]`, `[data-av-multi]`, `[data-dst-multi]` = "category_N").
   Reads the distinct (c1,c2,c3) combinations from a JSON <script id="bvCatHierarchy">
   so Κατ.2 shows only children of the selected Κατ.1, and Κατ.3 only children of Κατ.2. */
(function () {
  function init() {
    var dataEl = document.getElementById('bvCatHierarchy');
    if (!dataEl) return;
    var HIER;
    try { HIER = JSON.parse(dataEl.textContent || '[]'); } catch (e) { return; }
    if (!Array.isArray(HIER) || !HIER.length) return;

    var l2parents = {}; // c2 -> {c1: 1}
    var l3parents = {}; // c3 -> {c2: 1}
    HIER.forEach(function (r) {
      var c1 = (r[0] || ''), c2 = (r[1] || ''), c3 = (r[2] || '');
      if (c2) { (l2parents[c2] = l2parents[c2] || {})[c1] = 1; }
      if (c3) { (l3parents[c3] = l3parents[c3] || {})[c2] = 1; }
    });

    function box(level) {
      return document.querySelector(
        '[data-fnr-multi="category_' + level + '"],' +
        '[data-av-multi="category_' + level + '"],' +
        '[data-dst-multi="category_' + level + '"]'
      );
    }
    function boxes(level) {
      var b = box(level);
      return b ? Array.prototype.slice.call(b.querySelectorAll('input[type="checkbox"]')) : [];
    }
    function selected(level) {
      return boxes(level).filter(function (c) { return c.checked; }).map(function (c) { return c.value; });
    }
    if (!box(1) || !box(2)) return; // page has no cascading category filters

    var guard = false;
    function filterLevel(level, parents, selectedParents) {
      var pset = {};
      selectedParents.forEach(function (v) { pset[v] = 1; });
      var anyParent = selectedParents.length > 0;
      boxes(level).forEach(function (cb) {
        var par = parents[cb.value] || {};
        var show = !anyParent || Object.keys(par).some(function (p) { return pset[p]; });
        var lbl = cb.closest('label');
        if (lbl) lbl.style.display = show ? '' : 'none';
        if (!show && cb.checked) {
          cb.checked = false;
          cb.dispatchEvent(new Event('change', { bubbles: true }));
        }
      });
    }
    function apply() {
      if (guard) return;
      guard = true;
      try {
        filterLevel(2, l2parents, selected(1));
        filterLevel(3, l3parents, selected(2));
      } finally { guard = false; }
    }

    [1, 2].forEach(function (level) {
      boxes(level).forEach(function (cb) { cb.addEventListener('change', apply); });
    });
    apply();
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init);
  else init();
})();
