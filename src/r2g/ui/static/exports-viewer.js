(function (global) {
  'use strict';

  const CARD_ID = 'exports-viewer';
  const TABS = [['mapping', 'Mapping'], ['csi', 'CSI'], ['r2rml', 'R2RML'], ['provenance', 'Provenance']];
  const ISSUE_17 = 'https://github.com/arango-solutions-org/r2g-arango/issues/17';
  const STATUS = {
    missing: 'Not exported',
    current: 'Current',
    stale: 'Out of date',
    unverified: 'Unverified',
    malformed: 'Malformed',
    too_large: 'Too large',
    unreadable: 'Unreadable',
  };
  const REASONS = {
    mapping_changed: 'The mapping changed after this export.',
    schema_changed: 'The schema changed after this export.',
    receipt_missing: 'Its export record is missing.',
    receipt_unreadable: 'Its export record cannot be read.',
    receipt_invalid: 'Its export record is not valid.',
    project_mismatch: 'Its export record belongs to another project.',
    tenant_mismatch: 'Its export record belongs to another tenant.',
    artifact_changed: 'The file no longer matches its export record.',
    not_utf8: 'The file is not UTF-8 text.',
    invalid_csi: 'The file is not valid CSI.',
  };
  const OVERALL = {
    none: 'No exported mapping yet.',
    partial: 'Only one of the two exports is available.',
    attention: 'These exports need attention before you rely on them.',
    complete: 'Both exports are current and were generated from the same inputs.',
  };
  const state = { project: null, tab: 'mapping', data: null, error: null, loading: false, request: 0 };

  const esc = value => global.escHtml(value);

  function injectStyles() {
    if (document.getElementById('exports-viewer-styles')) return;
    const style = document.createElement('style');
    style.id = 'exports-viewer-styles';
    style.textContent = `
      .floating-card[data-fc-id="${CARD_ID}"]:not(.minimized) {
        width:min(1040px, calc(100vw - 60px));
        height:min(740px, calc(100vh - 100px));
        min-width:min(480px, calc(100vw - 20px));
        min-height:360px;
        max-width:calc(100vw - 20px);
        max-height:calc(100vh - 20px);
        resize:both;
      }
      .floating-card[data-fc-id="${CARD_ID}"] .fc-body { display:flex; flex-direction:column; gap:8px; overflow:auto; }
      .exports-tabs { display:flex; gap:4px; flex-wrap:wrap; flex:none; }
      .exports-tabs button { border:1px solid var(--border); background:var(--surface); color:var(--text); border-radius:6px; padding:4px 10px; font-size:12px; cursor:pointer; }
      .exports-tabs button[aria-selected="true"] { background:#f4fef2; color:#006532; border-color:#006532; font-weight:600; }
      .exports-banner { border:1px solid var(--border); border-radius:6px; padding:8px; font-size:12px; color:var(--text); flex:none; }
      .exports-banner code, .exports-table code, .exports-class code { font-family:Courier,monospace; overflow-wrap:anywhere; }
      .exports-pill { display:inline-block; border:1px solid var(--border); border-radius:10px; padding:0 7px; font-size:11px; font-weight:600; white-space:nowrap; }
      .exports-pill.current { color:#006532; background:#f4fef2; border-color:#006532; }
      .exports-pill.malformed, .exports-pill.unreadable { color:var(--red); border-color:var(--red); }
      .exports-pill.stale, .exports-pill.unverified, .exports-pill.too_large { color:var(--yellow); border-color:var(--yellow); }
      .exports-class { border:1px solid var(--border); border-radius:6px; padding:8px; flex:none; }
      .exports-class h4 { margin:0 0 6px; font-size:13px; font-weight:600; color:var(--text); }
      .exports-sides { display:grid; grid-template-columns:repeat(auto-fit, minmax(280px, 1fr)); gap:8px; }
      .exports-sides > div { min-width:0; }
      .exports-side-title { font-size:11px; font-weight:600; color:var(--text-muted); margin:4px 0; }
      .exports-table { width:100%; border-collapse:collapse; font-size:11px; color:var(--text); table-layout:fixed; }
      .exports-table th, .exports-table td { border-bottom:1px solid var(--border); padding:3px 4px; text-align:left; vertical-align:top; overflow-wrap:anywhere; }
      .exports-note { font-size:11px; color:var(--text-muted); margin:4px 0; }
      .exports-diff { font-size:11px; color:var(--yellow); margin:0 0 6px; overflow-wrap:anywhere; }
      .exports-code { width:100%; min-height:220px; flex:1; resize:none; box-sizing:border-box; border:1px solid var(--border); border-radius:6px; background:var(--surface2); color:var(--text); padding:10px; font:12px/1.45 Courier,monospace; white-space:pre; }
    `;
    document.head.appendChild(style);
  }

  function pill(status) {
    return `<span class="exports-pill ${esc(status)}">${esc(STATUS[status] || status)}</span>`;
  }

  function reasonText(reason) {
    if (String(reason).startsWith('invalid_r2rml: ')) {
      return `The file is not R2RML that r2g wrote (${reason.slice('invalid_r2rml: '.length)}).`;
    }
    return REASONS[reason] || reason;
  }

  function table(headers, rows) {
    if (!rows.length) return '<div class="exports-note">None.</div>';
    const head = headers.map(h => `<th scope="col">${esc(h)}</th>`).join('');
    const body = rows.map(row =>
      `<tr>${row.map(cell => `<td><code>${esc(cell ?? '')}</code></td>`).join('')}</tr>`
    ).join('');
    return `<table class="exports-table"><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
  }

  function banner(data) {
    const commands = ['csi', 'r2rml'].filter(kind => data[kind].status === 'missing').map(kind =>
      `<div><code>r2g export-${kind} --project ${esc(data.project)}</code></div>`
    ).join('');
    return `<div class="exports-banner" role="status">
      <b>${esc(OVERALL[data.state] || data.state)}</b>
      <div>CSI ${pill(data.csi.status)} R2RML ${pill(data.r2rml.status)}</div>
      ${commands ? `<div class="exports-note">An administrator publishes the missing export with:</div>${commands}` : ''}
    </div>`;
  }

  function csiSide(entity, relationships) {
    const outgoing = relationships.filter(r => r.from === entity.name);
    return `<div>
      <div class="exports-side-title">CSI entity <code>${esc(entity.name)}</code> in collection <code>${esc(entity.collection)}</code></div>
      ${table(['Property', 'Stored field'], entity.properties.map(p => [p.name, p.field]))}
      ${outgoing.length ? table(['Relationship', 'To', 'Edge collection'],
        outgoing.map(r => [r.type, r.to, r.edge_collection])) : ''}
    </div>`;
  }

  function r2rmlSide(item) {
    const joins = item.joins.map(j => [
      j.predicate, j.parent, j.conditions.map(c => `${c.child} = ${c.parent}`).join(', '),
    ]);
    return `<div>
      <div class="exports-side-title">R2RML table <code>${esc(item.table)}</code> as <code>${esc(item.class)}</code></div>
      ${table(['Column', 'Predicate', 'Datatype'], item.columns.map(c => [c.column, c.predicate, c.datatype]))}
      ${joins.length ? table(['Join predicate', 'Parent', 'On'], joins) : ''}
    </div>`;
  }

  function comparedClasses(data) {
    const { classes, properties } = data.comparison;
    const base = data.r2rml.receipt.options.concept_base || '';
    const tableOf = name => data.r2rml.structure.tables.find(t =>
      (base && t.class.startsWith(base) ? t.class.slice(base.length) : t.class) === name);
    const relationships = data.csi.structure.relationships;
    const entityOf = name => data.csi.structure.entities.find(e => e.name === name);
    const section = (name, label) => {
      const diff = properties.find(p => p.class === name);
      const entity = entityOf(name);
      const item = tableOf(name);
      return `<section class="exports-class">
        <h4><code>${esc(name)}</code>${label ? ` · ${esc(label)}` : ''}</h4>
        ${diff ? `<p class="exports-diff">Property names differ. Only in CSI: <code>${esc(diff.csi_only.join(', ') || 'none')}</code>.
          Only in R2RML: <code>${esc(diff.r2rml_only.join(', ') || 'none')}</code>.
          <a href="${ISSUE_17}" target="_blank" rel="noopener noreferrer">Known issue #17</a></p>` : ''}
        <div class="exports-sides">
          ${entity ? csiSide(entity, relationships) : ''}${item ? r2rmlSide(item) : ''}
        </div>
      </section>`;
    };
    return [
      ...classes.paired.map(name => section(name, '')),
      ...classes.csi_only.map(name => section(name, 'only in CSI')),
      ...classes.r2rml_only.map(name => section(name, 'only in R2RML')),
    ].join('');
  }

  function separateSides(data) {
    const csi = data.csi.structure;
    const r2rml = data.r2rml.structure;
    const sides = [];
    if (csi) {
      sides.push(`<section class="exports-class"><h4>CSI</h4>${csi.entities.map(
        e => csiSide(e, csi.relationships)).join('') || '<div class="exports-note">No entities.</div>'}</section>`);
    }
    if (r2rml) {
      sides.push(`<section class="exports-class"><h4>R2RML</h4>${r2rml.tables.map(r2rmlSide).join('')
        || '<div class="exports-note">No tables.</div>'}</section>`);
    }
    const reason = data.comparison.reason ? `Not compared: ${data.comparison.reason}.` : '';
    return `${data.state === 'none' ? '' : `<div class="exports-note">${esc(reason)}</div>`}${sides.join('')}`;
  }

  function mappingTab(data) {
    const notes = data.r2rml.structure?.notes || [];
    const problems = ['csi', 'r2rml'].flatMap(kind => data[kind].reasons.map(
      reason => `<div class="exports-note">${kind === 'csi' ? 'CSI' : 'R2RML'}: ${esc(reasonText(reason))}</div>`));
    const body = data.comparison.status === 'comparable' ? comparedClasses(data) : separateSides(data);
    const noted = notes.length
      ? `<section class="exports-class"><h4>What R2RML leaves out</h4>${notes.map(
        n => `<div class="exports-note"><code>${esc(n)}</code></div>`).join('')}</section>`
      : '';
    return `${problems.join('')}${body}${noted}`;
  }

  function rawTab(data, kind) {
    const entry = data[kind];
    const reasons = entry.reasons.map(r => `<div class="exports-note">${esc(reasonText(r))}</div>`).join('');
    if (entry.content === null) {
      return `${reasons}<div class="exports-note">${esc(STATUS[entry.status])}: no content to show.</div>`;
    }
    const label = kind === 'csi' ? 'Saved CSI document' : 'Saved R2RML document';
    return `${reasons}<textarea id="exports-viewer-code" class="exports-code" readonly spellcheck="false" aria-label="${label}"></textarea>`;
  }

  function provenanceSide(data, kind) {
    const entry = data[kind];
    const receipt = entry.receipt;
    const reasons = entry.reasons.map(r => `<div class="exports-note">${esc(reasonText(r))}</div>`).join('');
    const facts = receipt ? table(['Field', 'Value'], [
      ['Snapshot ID', receipt.snapshot_id],
      ['Generated', receipt.generated_at],
      ['r2g version', receipt.r2g_version],
      ['Tenant', receipt.tenant],
      ['Artifact SHA-256', receipt.artifact_sha256],
      ['Mapping SHA-256', receipt.mapping_sha256],
      ['Schema SHA-256', receipt.schema_sha256],
    ]) : '<div class="exports-note">No verified export record.</div>';
    const options = receipt ? `<div class="exports-side-title">Export options</div>${table(['Option', 'Value'],
      Object.entries(receipt.options).map(([key, value]) => [key, value === null ? 'none' : String(value)]))}` : '';
    const stamps = entry.bitemporal;
    const bitemporal = stamps && stamps.status === 'unavailable'
      ? `<div class="exports-note">Bitemporal provenance unavailable from this snapshot: <code>${esc(stamps.fields.join(', '))}</code>.</div>`
      : '';
    return `<div><div class="exports-side-title">${kind === 'csi' ? 'CSI' : 'R2RML'} ${pill(entry.status)}</div>
      ${reasons}${facts}${options}${bitemporal}</div>`;
  }

  function provenanceTab(data) {
    const current = data.current;
    return `<section class="exports-class"><h4>Current inputs</h4>${table(['Field', 'Value'], [
      ['Snapshot ID', current.snapshot_id],
      ['Mapping SHA-256', current.mapping_sha256],
      ['Schema SHA-256', current.schema_sha256],
    ])}</section>
    <section class="exports-class"><div class="exports-sides">
      ${provenanceSide(data, 'csi')}${provenanceSide(data, 'r2rml')}
    </div></section>`;
  }

  function renderBody(view) {
    if (view.error) {
      return `<div class="exports-banner" role="alert"><b>Could not load the exported mapping.</b>
        <div>${esc(view.error)}</div></div>`;
    }
    if (!view.data) return '<div class="exports-note" role="status">Loading the exported mapping…</div>';
    const tabs = TABS.map(([id, label]) =>
      `<button type="button" role="tab" data-exports-tab="${id}" aria-selected="${view.tab === id}">${label}</button>`
    ).join('');
    const content = view.tab === 'mapping' ? mappingTab(view.data)
      : view.tab === 'provenance' ? provenanceTab(view.data)
        : rawTab(view.data, view.tab);
    return `<div class="exports-tabs" role="tablist" aria-label="Exported mapping views">${tabs}</div>
      ${banner(view.data)}<div role="tabpanel">${content}</div>`;
  }

  function visibleContent() {
    if (!state.data || !['csi', 'r2rml'].includes(state.tab)) return null;
    return state.data[state.tab].content;
  }

  async function copyVisible() {
    const content = visibleContent();
    if (content === null) return;
    try {
      await navigator.clipboard.writeText(content);
      global.toast('Export copied', 'success');
    } catch (_error) {
      const code = document.getElementById('exports-viewer-code');
      if (code) {
        code.focus();
        code.select();
        global.toast('Press Cmd or Ctrl+C to copy the selected export', 'info');
      }
    }
  }

  function show() {
    const open = global.isFloatingCardOpen(CARD_ID);
    const complete = state.data?.state === 'complete';
    global.openFloatingCard({
      id: CARD_ID,
      title: `Exported mapping: ${state.project}`,
      status: state.loading ? { label: 'Loading', kind: 'running' }
        : state.error ? { label: 'Error', kind: 'failed' }
          : { label: complete ? 'Complete' : 'Review', kind: complete ? 'completed' : 'running' },
      position: open ? undefined : { right: 30, top: 70 },
      body: renderBody(state),
      actions: [
        { label: 'Refresh', onClick: load },
        ...(visibleContent() === null ? [] : [{ label: 'Copy', kind: 'primary', onClick: copyVisible }]),
      ],
    });
    const code = document.getElementById('exports-viewer-code');
    if (code) code.value = visibleContent();
    document.querySelectorAll('[data-exports-tab]').forEach(button => {
      button.addEventListener('click', () => {
        state.tab = button.dataset.exportsTab;
        show();
        document.querySelector(`[data-exports-tab="${state.tab}"]`)?.focus();
      });
    });
  }

  async function load() {
    const request = ++state.request;
    state.loading = true;
    show();
    let data = null;
    let error = null;
    try {
      data = await global.apiFetch(`/api/projects/${encodeURIComponent(state.project)}/exports`);
    } catch (failure) {
      error = global.errorDetail(failure);
    }
    // A newer request owns the card, and a closed card stays closed.
    if (request !== state.request || !global.isFloatingCardOpen(CARD_ID)) return;
    Object.assign(state, { data, error, loading: false });
    show();
  }

  function openExportsViewer(options) {
    const project = options?.project;
    if (!project) {
      global.toast('Open a project first.', 'error');
      return Promise.resolve();
    }
    injectStyles();
    Object.assign(state, { project, tab: 'mapping', data: null, error: null });
    return load();
  }

  global.openExportsViewer = openExportsViewer;
  global.R2GExportsViewer = { open: openExportsViewer, renderBody, visibleContent };
})(window);
