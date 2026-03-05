(function () {
  function getCookie(name) {
    const parts = document.cookie.split(';').map(v => v.trim());
    for (const p of parts) {
      if (p.startsWith(name + '=')) return decodeURIComponent(p.substring(name.length + 1));
    }
    return '';
  }

  function setCookie(name, value, maxAge) {
    document.cookie = `${name}=${encodeURIComponent(value)}; Path=/; Max-Age=${maxAge}; SameSite=Lax`;
  }

  function applyTheme(theme) {
    const next = theme === 'dark' ? 'dark' : 'light';
    document.body.setAttribute('data-theme', next);
    document.body.classList.toggle('dark-mode', next === 'dark');
  }

  function initTheme() {
    const fromCookie = getCookie('theme');
    const fromStorage = (() => {
      try { return localStorage.getItem('theme') || ''; } catch (e) { return ''; }
    })();
    applyTheme((fromCookie || fromStorage || 'light').toLowerCase());

    document.querySelectorAll('[data-theme-toggle]').forEach((btn) => {
      btn.addEventListener('click', function (e) {
        e.preventDefault();
        const current = document.body.getAttribute('data-theme') || 'light';
        const next = current === 'dark' ? 'light' : 'dark';
        setCookie('theme', next, 365 * 24 * 60 * 60);
        try { localStorage.setItem('theme', next); } catch (err) {}
        applyTheme(next);
      });
    });
  }

  function initFilterToggle() {
    document.querySelectorAll('[data-filter-toggle]').forEach((btn) => {
      const targetId = btn.getAttribute('data-target') || 'pageFiltersCollapse';
      const panel = document.getElementById(targetId);
      if (!panel) return;

      if (panel.getAttribute('data-open') !== '1') {
        panel.hidden = true;
      }

      btn.addEventListener('click', function () {
        const isHidden = panel.hidden;
        panel.hidden = !isHidden;
        btn.setAttribute('aria-expanded', isHidden ? 'true' : 'false');
      });
    });
  }

  function initSidebarToggle() {
    document.querySelectorAll('[data-sidebar-toggle]').forEach((btn) => {
      btn.addEventListener('click', function (e) {
        e.preventDefault();
        document.body.classList.toggle('sidenav-toggled');
      });
    });
  }

  document.addEventListener('DOMContentLoaded', function () {
    initTheme();
    initFilterToggle();
    initSidebarToggle();
  });
})();
