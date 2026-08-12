/* BoxVisio Co-Pilot — chat client (shared by the dedicated page and the floating widget).
   Streams answers from POST /tenant/copilot/ask (SSE over fetch). Text-only history is
   kept client-side and re-sent each turn; tool rounds happen server-side within a turn. */
(function () {
  'use strict';

  function cookie(name) {
    const m = document.cookie.match('(^|;)\\s*' + name + '\\s*=\\s*([^;]+)');
    return m ? decodeURIComponent(m.pop()) : '';
  }

  function el(tag, cls, text) {
    const node = document.createElement(tag);
    if (cls) node.className = cls;
    if (text != null) node.textContent = text;
    return node;
  }

  // Minimal, safe markdown-ish rendering: escape, then bold + line breaks + bullets.
  function renderText(node, raw) {
    const esc = String(raw || '')
      .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    const html = esc
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
      .replace(/`([^`]+)`/g, '<code>$1</code>')
      .replace(/^\s*[-•]\s+(.*)$/gm, '<div class="bvc-li">• $1</div>')
      .replace(/\n/g, '<br>');
    node.innerHTML = html;
  }

  function mount(root, opts) {
    opts = opts || {};
    root.classList.add('bvc-root');
    root.innerHTML = '';

    const log = el('div', 'bvc-log');
    const greeting = el('div', 'bvc-msg bvc-assistant');
    renderText(greeting, opts.greeting || 'Γεια σου! Είμαι ο Co-Pilot. Ρώτησέ με για τις πωλήσεις, το απόθεμα, τι σημαίνει ένας δείκτης, ή «τι κάνω εδώ;».');
    log.appendChild(greeting);

    const form = el('form', 'bvc-input');
    const input = el('textarea', 'bvc-textarea');
    input.rows = 1;
    input.placeholder = 'Ρώτησε κάτι…';
    const send = el('button', 'bvc-send');
    send.type = 'submit';
    send.innerHTML = '<i class="fe fe-send"></i>';
    form.append(input, send);
    root.append(log, form);

    const history = [];
    let busy = false;

    // Suggestion chips (only shown on the full page).
    if (opts.suggestions) {
      const chips = el('div', 'bvc-suggest');
      ['Τι πωλήσεις έχω αυτόν τον μήνα;', 'Ποια είδη έχουν το μεγαλύτερο απόθεμα;', 'Τι θα έπρεπε να κοιτάξω σήμερα;'].forEach((q) => {
        const c = el('button', 'bvc-chip', q);
        c.type = 'button';
        c.addEventListener('click', () => { input.value = q; ask(); });
        chips.appendChild(c);
      });
      log.appendChild(chips);
    }

    function addMsg(role) {
      const m = el('div', 'bvc-msg ' + (role === 'user' ? 'bvc-user' : 'bvc-assistant'));
      log.appendChild(m);
      log.scrollTop = log.scrollHeight;
      return m;
    }

    input.addEventListener('input', () => {
      input.style.height = 'auto';
      input.style.height = Math.min(input.scrollHeight, 140) + 'px';
    });
    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); ask(); }
    });
    form.addEventListener('submit', (e) => { e.preventDefault(); ask(); });

    async function ask() {
      const text = input.value.trim();
      if (!text || busy) return;
      busy = true;
      input.value = '';
      input.style.height = 'auto';
      send.disabled = true;

      const userNode = addMsg('user');
      renderText(userNode, text);
      history.push({ role: 'user', content: text });

      const botNode = addMsg('assistant');
      const status = el('div', 'bvc-status');
      status.innerHTML = '<span class="bvc-dot"></span><span class="bvc-dot"></span><span class="bvc-dot"></span>';
      botNode.appendChild(status);
      const body = el('div', 'bvc-body');
      botNode.appendChild(body);

      let answer = '';
      try {
        const resp = await fetch('/tenant/copilot/ask', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': cookie('csrf_token') },
          body: JSON.stringify({ messages: history, page_context: opts.pageContext || document.title || '' }),
        });
        if (!resp.ok) {
          let msg = 'Σφάλμα (' + resp.status + ').';
          try { const j = await resp.json(); if (j && j.error) msg = j.error; } catch (_) {}
          status.remove();
          renderText(body, '⚠️ ' + msg);
          if (resp.status === 400 || resp.status === 403) {
            const a = el('a', 'bvc-cfg', 'Άνοιγμα ρυθμίσεων');
            a.href = '/tenant/settings'; body.appendChild(document.createElement('br')); body.appendChild(a);
          }
          history.pop();
          return;
        }
        const reader = resp.body.getReader();
        const dec = new TextDecoder();
        let buf = '';
        for (;;) {
          const { value, done } = await reader.read();
          if (done) break;
          buf += dec.decode(value, { stream: true });
          const frames = buf.split('\n\n');
          buf = frames.pop();
          for (const frame of frames) {
            const line = frame.trim();
            if (!line.startsWith('data:')) continue;
            let ev;
            try { ev = JSON.parse(line.slice(5).trim()); } catch (_) { continue; }
            if (ev.type === 'text') {
              status.remove();
              answer += ev.text;
              renderText(body, answer);
            } else if (ev.type === 'tool') {
              status.innerHTML = '<span class="bvc-tool"><i class="fe fe-database"></i> ' +
                (ev.name === 'run_sql' ? 'διαβάζω δεδομένα…' : ev.name === 'describe_schema' ? 'ελέγχω δομή…' : 'επεξηγήσεις…') + '</span>';
            } else if (ev.type === 'error') {
              status.remove();
              renderText(body, '⚠️ ' + (ev.error || 'Σφάλμα.'));
            }
            log.scrollTop = log.scrollHeight;
          }
        }
        status.remove();
        if (answer.trim()) history.push({ role: 'assistant', content: answer });
        else renderText(body, 'Δεν έλαβα απάντηση.');
      } catch (err) {
        status.remove();
        renderText(body, '⚠️ Πρόβλημα σύνδεσης. Δοκίμασε ξανά.');
        history.pop();
      } finally {
        busy = false;
        send.disabled = false;
        input.focus();
        log.scrollTop = log.scrollHeight;
      }
    }

    return { ask, focus: () => input.focus() };
  }

  window.BvCopilot = { mount: mount };
})();
