(function () {
  /*
   * BV Modal Design System
   * Markup contract:
   * - .modal.bv-modal.bv-modal--responsive
   * - data-bv-modal="true"
   * Optional attributes:
   * - data-bv-close-on-backdrop="true|false" (default false)
   * - data-bv-esc-close="true|false" (default true)
   * - data-bv-focus-trap="true|false" (default true)
   * - data-bv-restore-focus="true|false" (default true)
   */
  const registry = new WeakMap();
  const FOCUSABLE_SELECTOR = [
    'a[href]',
    'button:not([disabled])',
    'textarea:not([disabled])',
    'input:not([disabled]):not([type="hidden"])',
    'select:not([disabled])',
    '[tabindex]:not([tabindex="-1"])',
  ].join(',');

  let fallbackOpenCount = 0;
  let bodyOverflowBeforeLock = '';
  let bodyPaddingBeforeLock = '';

  function parseBoolean(value, fallback) {
    if (value === undefined || value === null || value === '') return fallback;
    if (typeof value === 'boolean') return value;
    const normalized = String(value).trim().toLowerCase();
    if (['1', 'true', 'yes', 'on'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'off'].includes(normalized)) return false;
    return fallback;
  }

  function resolveElement(modalRef) {
    if (!modalRef) return null;
    if (modalRef instanceof HTMLElement) return modalRef;
    if (typeof modalRef === 'string') return document.querySelector(modalRef);
    return null;
  }

  function getFocusable(container) {
    if (!container || !container.querySelectorAll) return [];
    const candidates = container.querySelectorAll(FOCUSABLE_SELECTOR);
    return Array.from(candidates).filter((el) => {
      if (!(el instanceof HTMLElement)) return false;
      if (!el.isConnected) return false;
      if (el.hidden) return false;
      const style = window.getComputedStyle(el);
      if (style.visibility === 'hidden' || style.display === 'none') return false;
      return true;
    });
  }

  function lockBodyScroll() {
    if (fallbackOpenCount > 0) {
      fallbackOpenCount += 1;
      return;
    }
    fallbackOpenCount = 1;
    const scrollbarWidth = Math.max(0, window.innerWidth - document.documentElement.clientWidth);
    bodyOverflowBeforeLock = document.body.style.overflow;
    bodyPaddingBeforeLock = document.body.style.paddingRight;
    document.body.style.overflow = 'hidden';
    if (scrollbarWidth > 0) {
      document.body.style.paddingRight = `${scrollbarWidth}px`;
    }
    document.body.classList.add('modal-open');
    document.body.classList.add('bv-modal-lock');
  }

  function unlockBodyScroll() {
    if (fallbackOpenCount <= 0) return;
    fallbackOpenCount -= 1;
    if (fallbackOpenCount > 0) return;
    document.body.style.overflow = bodyOverflowBeforeLock;
    document.body.style.paddingRight = bodyPaddingBeforeLock;
    document.body.classList.remove('modal-open');
    document.body.classList.remove('bv-modal-lock');
  }

  function BVModalController(modalEl, options) {
    this.el = modalEl;
    this.options = options || {};
    this.bootstrapInstance = null;
    this.lastTrigger = null;
    this.isOpen = false;
    this.pointerDownOnBackdrop = false;
    this.bound = {
      onShow: this.onShow.bind(this),
      onBackdropPointerDown: this.onBackdropPointerDown.bind(this),
      onBackdropClick: this.onBackdropClick.bind(this),
      onKeydown: this.onKeydown.bind(this),
      onShown: this.onShown.bind(this),
      onHidden: this.onHidden.bind(this),
    };
    this.init();
  }

  BVModalController.prototype.option = function option(name, fallback) {
    if (this.options[name] !== undefined) return this.options[name];
    if (name === 'closeOnBackdrop') return parseBoolean(this.el.dataset.bvCloseOnBackdrop, fallback);
    if (name === 'keyboard') return parseBoolean(this.el.dataset.bvEscClose, fallback);
    if (name === 'focusTrap') return parseBoolean(this.el.dataset.bvFocusTrap, fallback);
    if (name === 'restoreFocus') return parseBoolean(this.el.dataset.bvRestoreFocus, fallback);
    return fallback;
  };

  BVModalController.prototype.init = function init() {
    this.el.classList.add('bv-modal');
    if (!this.el.hasAttribute('tabindex')) {
      this.el.setAttribute('tabindex', '-1');
    }
    if (!this.el.hasAttribute('role')) {
      this.el.setAttribute('role', 'dialog');
    }
    if (window.bootstrap && window.bootstrap.Modal) {
      const closeOnBackdrop = this.option('closeOnBackdrop', false);
      const keyboard = this.option('keyboard', true);
      const focusTrap = this.option('focusTrap', true);
      this.bootstrapInstance = window.bootstrap.Modal.getOrCreateInstance(this.el, {
        backdrop: closeOnBackdrop ? true : 'static',
        keyboard,
        focus: focusTrap,
      });
      this.el.addEventListener('show.bs.modal', this.bound.onShow);
      this.el.addEventListener('shown.bs.modal', this.bound.onShown);
      this.el.addEventListener('hidden.bs.modal', this.bound.onHidden);
    }
  };

  BVModalController.prototype.captureTrigger = function captureTrigger(triggerEl) {
    if (triggerEl instanceof HTMLElement) {
      this.lastTrigger = triggerEl;
      return;
    }
    if (document.activeElement instanceof HTMLElement) {
      this.lastTrigger = document.activeElement;
      return;
    }
    this.lastTrigger = null;
  };

  BVModalController.prototype.focusInitial = function focusInitial() {
    const target =
      this.el.querySelector('[data-bv-initial-focus]') ||
      getFocusable(this.el)[0] ||
      this.el.querySelector('.modal-body') ||
      this.el;
    if (!(target instanceof HTMLElement)) return;
    window.setTimeout(() => {
      try {
        target.focus({ preventScroll: true });
      } catch (_err) {
        target.focus();
      }
    }, 0);
  };

  BVModalController.prototype.restoreFocus = function restoreFocus() {
    if (!this.option('restoreFocus', true)) return;
    const trigger = this.lastTrigger;
    this.lastTrigger = null;
    if (!(trigger instanceof HTMLElement) || !trigger.isConnected) return;
    if (trigger.matches(':disabled')) return;
    window.setTimeout(() => {
      try {
        trigger.focus({ preventScroll: true });
      } catch (_err) {
        trigger.focus();
      }
    }, 0);
  };

  BVModalController.prototype.enforceFocusTrap = function enforceFocusTrap(event) {
    if (!this.option('focusTrap', true)) return;
    const focusables = getFocusable(this.el);
    if (!focusables.length) {
      event.preventDefault();
      this.el.focus();
      return;
    }
    const first = focusables[0];
    const last = focusables[focusables.length - 1];
    const active = document.activeElement;
    if (event.shiftKey && active === first) {
      event.preventDefault();
      last.focus();
      return;
    }
    if (!event.shiftKey && active === last) {
      event.preventDefault();
      first.focus();
    }
  };

  BVModalController.prototype.show = function show(triggerEl) {
    this.captureTrigger(triggerEl);
    if (this.bootstrapInstance) {
      this.bootstrapInstance.show();
      return;
    }
    if (this.isOpen) return;
    this.isOpen = true;
    lockBodyScroll();
    this.el.style.display = 'block';
    this.el.removeAttribute('aria-hidden');
    this.el.setAttribute('aria-modal', 'true');
    window.requestAnimationFrame(() => {
      this.el.classList.add('show');
    });
    document.addEventListener('keydown', this.bound.onKeydown, true);
    this.el.addEventListener('pointerdown', this.bound.onBackdropPointerDown, true);
    this.el.addEventListener('click', this.bound.onBackdropClick, true);
    this.focusInitial();
  };

  BVModalController.prototype.hide = function hide() {
    if (this.bootstrapInstance) {
      this.bootstrapInstance.hide();
      return;
    }
    if (!this.isOpen) return;
    this.isOpen = false;
    this.el.classList.remove('show');
    this.el.style.display = 'none';
    this.el.setAttribute('aria-hidden', 'true');
    this.el.removeAttribute('aria-modal');
    document.removeEventListener('keydown', this.bound.onKeydown, true);
    this.el.removeEventListener('pointerdown', this.bound.onBackdropPointerDown, true);
    this.el.removeEventListener('click', this.bound.onBackdropClick, true);
    unlockBodyScroll();
    this.restoreFocus();
  };

  BVModalController.prototype.onShow = function onShow(event) {
    this.isOpen = true;
    if (event?.relatedTarget instanceof HTMLElement) {
      this.captureTrigger(event.relatedTarget);
      return;
    }
    if (!this.lastTrigger) {
      this.captureTrigger(null);
    }
  };

  BVModalController.prototype.onShown = function onShown() {
    this.isOpen = true;
    this.focusInitial();
  };

  BVModalController.prototype.onHidden = function onHidden() {
    this.isOpen = false;
    this.restoreFocus();
  };

  BVModalController.prototype.onBackdropPointerDown = function onBackdropPointerDown(event) {
    this.pointerDownOnBackdrop = event.target === this.el;
  };

  BVModalController.prototype.onBackdropClick = function onBackdropClick(event) {
    const closeOnBackdrop = this.option('closeOnBackdrop', false);
    if (!closeOnBackdrop) {
      this.pointerDownOnBackdrop = false;
      return;
    }
    if (this.pointerDownOnBackdrop && event.target === this.el) {
      event.preventDefault();
      this.hide();
    }
    this.pointerDownOnBackdrop = false;
  };

  BVModalController.prototype.onKeydown = function onKeydown(event) {
    if (!this.isOpen) return;
    if (event.key === 'Escape' && this.option('keyboard', true)) {
      event.preventDefault();
      this.hide();
      return;
    }
    if (event.key === 'Tab') {
      this.enforceFocusTrap(event);
    }
  };

  function create(modalRef, options) {
    const el = resolveElement(modalRef);
    if (!el) return null;
    const existing = registry.get(el);
    if (existing) return existing;
    const controller = new BVModalController(el, options || {});
    registry.set(el, controller);
    return controller;
  }

  function get(modalRef) {
    const el = resolveElement(modalRef);
    if (!el) return null;
    return registry.get(el) || create(el);
  }

  function init(root) {
    const scope = root && root.querySelectorAll ? root : document;
    scope.querySelectorAll('.modal[data-bv-modal], .modal.bv-modal').forEach((modalEl) => create(modalEl));
  }

  function show(modalRef, triggerEl, options) {
    const modal = create(modalRef, options || {});
    modal?.show(triggerEl);
    return modal;
  }

  function hide(modalRef) {
    const modal = get(modalRef);
    modal?.hide();
    return modal;
  }

  document.addEventListener('DOMContentLoaded', function () {
    init(document);
  });

  document.addEventListener('click', function (event) {
    const dismissTrigger = event.target.closest('.modal [data-bs-dismiss="modal"]');
    if (dismissTrigger) {
      const owner = dismissTrigger.closest('.modal');
      if (owner && registry.has(owner)) {
        event.preventDefault();
        hide(owner);
        return;
      }
    }

    const openTrigger = event.target.closest('[data-bv-modal-open]');
    if (openTrigger) {
      event.preventDefault();
      const target = openTrigger.getAttribute('data-bv-modal-open');
      show(target, openTrigger);
      return;
    }
    const closeTrigger = event.target.closest('[data-bv-modal-close]');
    if (!closeTrigger) return;
    event.preventDefault();
    const target = closeTrigger.getAttribute('data-bv-modal-close');
    if (target) {
      hide(target);
      return;
    }
    const ownerModal = closeTrigger.closest('.modal');
    if (ownerModal) hide(ownerModal);
  });

  window.BVModalSystem = { create, get, init, show, hide };
})();
