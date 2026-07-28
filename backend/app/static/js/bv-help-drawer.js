/* Contextual per-circuit help drawer.

   Every operational page carries a Help button. Sending the user to the manual
   would cost them their filters, so the circuit's help slides in over the page
   instead. The button keeps a real href, so no-JS, middle-click and
   open-in-new-tab all still do the sensible thing. */
(function () {
  'use strict';

  var drawer, panel, body, titleEl, fullLink, lastFocus;
  var cache = {};

  function isOpen() {
    return drawer && !drawer.hidden;
  }

  function focusables() {
    return Array.prototype.filter.call(
      panel.querySelectorAll('a[href], button:not([disabled]), input, select, textarea, [tabindex]:not([tabindex="-1"])'),
      function (el) { return el.offsetParent !== null; }
    );
  }

  function onKeydown(event) {
    if (!isOpen()) return;
    if (event.key === 'Escape') {
      event.preventDefault();
      close();
      return;
    }
    if (event.key !== 'Tab') return;
    //  Keep tabbing inside the drawer while it is modal.
    var items = focusables();
    if (!items.length) return;
    var first = items[0];
    var last = items[items.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  }

  function open(circuitId, label) {
    if (!drawer) return;
    lastFocus = document.activeElement;
    drawer.hidden = false;
    document.body.classList.add('bv-help-drawer-open');
    if (label && titleEl) titleEl.textContent = label;
    if (fullLink) fullLink.href = '/tenant/help/circuits/' + circuitId;

    if (cache[circuitId]) {
      body.innerHTML = cache[circuitId];
    } else {
      fetch('/tenant/help/circuits/' + circuitId + '/panel', { credentials: 'same-origin' })
        .then(function (r) { return r.ok ? r.text() : null; })
        .then(function (html) {
          if (html === null) {
            //  No panel for this page — fall back to the full manual rather than
            //  leaving an empty drawer on screen.
            window.location.href = '/tenant/help/circuits';
            return;
          }
          cache[circuitId] = html;
          body.innerHTML = html;
          body.scrollTop = 0;
        })
        .catch(function () {
          window.location.href = '/tenant/help/circuits/' + circuitId;
        });
    }

    window.setTimeout(function () { panel.focus(); }, 30);
  }

  function close() {
    if (!isOpen()) return;
    drawer.hidden = true;
    document.body.classList.remove('bv-help-drawer-open');
    if (lastFocus && lastFocus.focus) lastFocus.focus();
  }

  function init() {
    drawer = document.getElementById('bvHelpDrawer');
    if (!drawer) return;
    panel = drawer.querySelector('.bv-help-drawer-panel');
    body = document.getElementById('bvHelpDrawerBody');
    titleEl = document.getElementById('bvHelpDrawerTitle');
    fullLink = document.getElementById('bvHelpDrawerFull');

    document.addEventListener('click', function (event) {
      var trigger = event.target.closest('[data-help-panel]');
      if (trigger) {
        //  Let modified clicks (new tab, download, middle click) behave normally.
        if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey || event.button !== 0) return;
        event.preventDefault();
        open(trigger.getAttribute('data-help-panel'), (trigger.dataset.helpPanelLabel || '').trim() || null);
        return;
      }
      if (event.target.closest('[data-help-drawer-close]')) {
        event.preventDefault();
        close();
      }
    });

    document.addEventListener('keydown', onKeydown);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
