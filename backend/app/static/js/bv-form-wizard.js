(function () {
  const registry = new WeakMap();

  function resolveElement(ref) {
    if (!ref) return null;
    if (ref instanceof HTMLElement) return ref;
    if (typeof ref === 'string') return document.querySelector(ref);
    return null;
  }

  function normalizeTarget(target) {
    const value = String(target || '').trim();
    if (!value) return '';
    return value.startsWith('#') ? value : `#${value}`;
  }

  function cssEscape(value) {
    if (window.CSS && typeof window.CSS.escape === 'function') {
      return window.CSS.escape(value);
    }
    return String(value).replace(/[^a-zA-Z0-9_-]/g, '\\$&');
  }

  function readStepTarget(stepEl) {
    if (!stepEl) return '';
    const raw = stepEl.getAttribute('data-target') || stepEl.getAttribute('data-bs-target');
    return normalizeTarget(raw);
  }

  function BvFormWizard(root, options) {
    this.root = root;
    this.options = options || {};
    this.scope = root.closest('[data-bv-wizard-scope], .modal-body, .card-body, form') || root.parentElement || document;
    this.steps = Array.from(root.querySelectorAll('[data-bv-wizard-step]'));
    this.targets = this.steps.map((step) => readStepTarget(step));
    this.content = this.resolveContent(
      String(this.options.contentSelector || root.getAttribute('data-bv-wizard-content') || '').trim()
    );
    this.panes = this.targets.map((target) => this.resolvePane(target));
    this.prevButtons = Array.from(this.scope.querySelectorAll('[data-bv-wizard-prev]'));
    this.nextButtons = Array.from(this.scope.querySelectorAll('[data-bv-wizard-next]'));
    this.currentIndex = -1;
    this.bound = {
      onStepClick: this.onStepClick.bind(this),
      onStepKeydown: this.onStepKeydown.bind(this),
      onPrevClick: this.onPrevClick.bind(this),
      onNextClick: this.onNextClick.bind(this),
    };
    this.bind();
    this.initialize();
  }

  BvFormWizard.prototype.resolveContent = function resolveContent(contentSelector) {
    if (contentSelector) {
      const localMatch = this.scope.querySelector(contentSelector);
      if (localMatch) return localMatch;
      const globalMatch = document.querySelector(contentSelector);
      if (globalMatch) return globalMatch;
    }
    const next = this.root.nextElementSibling;
    if (next && next.classList.contains('tab-content')) return next;
    const inScope = this.scope.querySelector('.tab-content');
    if (inScope) return inScope;
    const container = this.root.closest('.modal-content, .card, form, body');
    return container ? container.querySelector('.tab-content') : null;
  };

  BvFormWizard.prototype.resolvePane = function resolvePane(target) {
    const normalized = normalizeTarget(target);
    if (!normalized) return null;
    const paneId = normalized.slice(1);
    const escapedId = cssEscape(paneId);
    if (this.content) {
      const inContent = this.content.querySelector(`#${escapedId}`);
      if (inContent) return inContent;
    }
    return document.getElementById(paneId);
  };

  BvFormWizard.prototype.bind = function bind() {
    this.root.classList.add('bv-form-wizard');
    this.steps.forEach((step, index) => {
      step.setAttribute('role', 'tab');
      step.setAttribute('tabindex', index === 0 ? '0' : '-1');
      step.setAttribute('aria-selected', index === 0 ? 'true' : 'false');
      step.dataset.wizardIndex = String(index);
      step.addEventListener('click', this.bound.onStepClick);
      step.addEventListener('keydown', this.bound.onStepKeydown);
    });
    this.prevButtons.forEach((btn) => btn.addEventListener('click', this.bound.onPrevClick));
    this.nextButtons.forEach((btn) => btn.addEventListener('click', this.bound.onNextClick));
  };

  BvFormWizard.prototype.initialize = function initialize() {
    if (!this.steps.length) return;
    const fromOptions = normalizeTarget(this.options.initialTarget);
    const fromDataAttr = normalizeTarget(this.root.getAttribute('data-bv-wizard-initial'));
    const activeStep = this.steps.find((step) => step.classList.contains('is-active') || step.classList.contains('active'));
    const fromStep = readStepTarget(activeStep);
    const activePane = this.content?.querySelector('.tab-pane.active[id]');
    const fromPane = activePane ? `#${activePane.id}` : '';
    const fallback = this.targets[0];
    const initialTarget = fromOptions || fromDataAttr || fromStep || fromPane || fallback;
    this.show(initialTarget, true);
  };

  BvFormWizard.prototype.findIndex = function findIndex(targetOrIndex) {
    if (!this.steps.length) return -1;
    if (typeof targetOrIndex === 'number' && Number.isFinite(targetOrIndex)) {
      const safeIndex = Math.trunc(targetOrIndex);
      if (safeIndex >= 0 && safeIndex < this.steps.length) return safeIndex;
      return -1;
    }
    const normalizedTarget = normalizeTarget(targetOrIndex);
    if (!normalizedTarget) return -1;
    return this.targets.findIndex((target) => target === normalizedTarget);
  };

  BvFormWizard.prototype.updateActions = function updateActions() {
    const hasMultipleSteps = this.steps.length > 1;
    const atStart = this.currentIndex <= 0;
    const atEnd = this.currentIndex >= this.steps.length - 1;
    this.prevButtons.forEach((btn) => {
      btn.classList.toggle('d-none', !hasMultipleSteps);
      btn.disabled = hasMultipleSteps ? atStart : true;
      btn.setAttribute('aria-disabled', btn.disabled ? 'true' : 'false');
    });
    this.nextButtons.forEach((btn) => {
      btn.classList.toggle('d-none', !hasMultipleSteps);
      btn.disabled = hasMultipleSteps ? atEnd : true;
      btn.setAttribute('aria-disabled', btn.disabled ? 'true' : 'false');
    });
  };

  BvFormWizard.prototype.show = function show(targetOrIndex, silent) {
    const nextIndex = this.findIndex(targetOrIndex);
    if (nextIndex < 0) return false;
    const activeTarget = this.targets[nextIndex];
    this.currentIndex = nextIndex;
    this.steps.forEach((step, index) => {
      const isActive = index === nextIndex;
      const isComplete = index < nextIndex;
      step.classList.toggle('is-active', isActive);
      step.classList.toggle('is-complete', isComplete);
      step.setAttribute('aria-selected', isActive ? 'true' : 'false');
      step.setAttribute('tabindex', isActive ? '0' : '-1');
      if (isActive) step.setAttribute('aria-current', 'step');
      else step.removeAttribute('aria-current');
    });
    this.panes.forEach((pane, index) => {
      if (!pane) return;
      const isActive = index === nextIndex;
      pane.classList.toggle('active', isActive);
      pane.classList.toggle('show', isActive);
      pane.hidden = !isActive;
      pane.setAttribute('aria-hidden', isActive ? 'false' : 'true');
    });
    this.updateActions();
    if (!silent) {
      this.root.dispatchEvent(
        new CustomEvent('bv:wizard-change', {
          bubbles: true,
          detail: {
            index: nextIndex,
            total: this.steps.length,
            target: activeTarget,
          },
        })
      );
    }
    return true;
  };

  BvFormWizard.prototype.next = function next() {
    if (this.currentIndex < 0) return this.show(0);
    return this.show(Math.min(this.currentIndex + 1, this.steps.length - 1));
  };

  BvFormWizard.prototype.prev = function prev() {
    if (this.currentIndex < 0) return this.show(0);
    return this.show(Math.max(this.currentIndex - 1, 0));
  };

  BvFormWizard.prototype.onStepClick = function onStepClick(event) {
    event.preventDefault();
    const stepEl = event.currentTarget;
    const target = readStepTarget(stepEl);
    this.show(target);
  };

  BvFormWizard.prototype.onStepKeydown = function onStepKeydown(event) {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    const stepEl = event.currentTarget;
    const target = readStepTarget(stepEl);
    this.show(target);
  };

  BvFormWizard.prototype.onPrevClick = function onPrevClick(event) {
    event.preventDefault();
    this.prev();
  };

  BvFormWizard.prototype.onNextClick = function onNextClick(event) {
    event.preventDefault();
    this.next();
  };

  function create(rootRef, options) {
    const root = resolveElement(rootRef);
    if (!root) return null;
    const existing = registry.get(root);
    if (existing) return existing;
    const wizard = new BvFormWizard(root, options || {});
    registry.set(root, wizard);
    return wizard;
  }

  function get(rootRef) {
    const root = resolveElement(rootRef);
    if (!root) return null;
    return registry.get(root) || create(root);
  }

  function init(scope) {
    const root = scope && scope.querySelectorAll ? scope : document;
    root.querySelectorAll('[data-bv-form-wizard]').forEach((wizardEl) => create(wizardEl));
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () {
      init(document);
    });
  } else {
    init(document);
  }

  window.BVFormWizard = { create, get, init };
})();
