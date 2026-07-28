/* Help section behaviour: live filtering of tasks / KPI entries, and the
   mobile "jump to section" select. No dependencies. */
(function () {
  'use strict';

  //  Greek users type without accents as often as with them, and search terms
  //  arrive in either case. Fold both away before matching.
  function fold(value) {
    return (value || '')
      .normalize('NFD')
      .replace(/[̀-ͯ]/g, '')
      .toLowerCase()
      .replace(/\s+/g, ' ')
      .trim();
  }

  function applyFilter(scope, rawTerm) {
    var terms = fold(rawTerm).split(' ').filter(Boolean);
    var items = scope.querySelectorAll('[data-help-item]');
    var matches = 0;

    items.forEach(function (item) {
      if (!item.dataset.helpHaystack) {
        item.dataset.helpHaystack = fold(item.getAttribute('data-help-text') || item.textContent);
      }
      var haystack = item.dataset.helpHaystack;
      var hit = terms.every(function (t) { return haystack.indexOf(t) !== -1; });
      item.hidden = !hit;
      if (hit) matches += 1;
    });

    //  Hide a group heading once everything under it is filtered out.
    scope.querySelectorAll('[data-help-group]').forEach(function (group) {
      var visible = group.querySelector('[data-help-item]:not([hidden])');
      group.hidden = !visible;
    });

    var empty = document.querySelector('[data-help-empty]');
    if (empty) empty.classList.toggle('is-shown', terms.length > 0 && matches === 0);
  }

  function initFilters() {
    document.querySelectorAll('[data-help-filter]').forEach(function (input) {
      var scope = document.querySelector(input.getAttribute('data-help-filter'));
      if (!scope) return;

      var timer = null;
      input.addEventListener('input', function () {
        window.clearTimeout(timer);
        timer = window.setTimeout(function () { applyFilter(scope, input.value); }, 120);
      });

      //  Escape clears, which is what people expect from a search field.
      input.addEventListener('keydown', function (event) {
        if (event.key === 'Escape' && input.value) {
          event.preventDefault();
          input.value = '';
          applyFilter(scope, '');
        }
      });

      //  Deep links such as /tenant/help/kpis?q=margin land pre-filtered.
      var q = new URLSearchParams(window.location.search).get('q');
      if (q) {
        input.value = q;
        applyFilter(scope, q);
      }
    });
  }

  function initJump() {
    document.querySelectorAll('[data-help-jump]').forEach(function (select) {
      select.addEventListener('change', function () {
        if (!select.value) return;
        var target = document.querySelector(select.value);
        if (target) target.scrollIntoView({ behavior: 'smooth', block: 'start' });
      });
    });
  }

  //  A task linked from elsewhere (…/find#some-task) should arrive already open.
  function openHashTarget() {
    if (!window.location.hash) return;
    var target = null;
    try {
      target = document.querySelector(window.location.hash);
    } catch (err) {
      return;
    }
    if (!target) return;
    var details = target.closest ? target.closest('details') : null;
    if (details) details.open = true;
    target.scrollIntoView({ block: 'start' });
  }

  function init() {
    initFilters();
    initJump();
    openHashTarget();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
