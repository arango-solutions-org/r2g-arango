(function (global) {
  'use strict';

  const CARD_ID = 'arangoimport-viewer';
  const state = {
    mode: 'jsonl',
    scope: 'all',
    mappingId: null,
    view: 'bundle',
    artifactDir: './output',
    result: null,
  };

  function injectStyles() {
    if (document.getElementById('arangoimport-viewer-styles')) return;
    const style = document.createElement('style');
    style.id = 'arangoimport-viewer-styles';
    style.textContent = `
      .floating-card[data-fc-id="${CARD_ID}"]:not(.minimized) {
        width:min(960px, calc(100vw - 60px));
        height:min(720px, calc(100vh - 100px));
        min-width:min(480px, calc(100vw - 20px));
        min-height:360px;
        max-width:calc(100vw - 20px);
        max-height:calc(100vh - 20px);
        resize:both;
      }
      .floating-card[data-fc-id="${CARD_ID}"] .fc-body {
        display:flex;
        flex-direction:column;
      }
      .import-viewer-controls { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:8px; }
      .import-viewer-controls label { font-size:11px; color:#282828; }
      .import-viewer-controls select,
      .import-viewer-controls input { display:block; margin-top:3px; min-width:150px; }
      .import-viewer-controls input { width:260px; }
      .import-viewer-banner { background:#f4fef2; border:1px solid #e5e5e5; border-radius:6px; padding:8px; margin-bottom:8px; font-size:11px; color:#282828; }
      .import-viewer-warning { color:#da1a20; margin-top:5px; }
      .import-viewer-code { width:100%; min-height:180px; flex:1; resize:none; box-sizing:border-box; border:1px solid #e5e5e5; border-radius:6px; background:#f8f8f8; color:#282828; padding:10px; font:12px/1.45 Courier,monospace; white-space:pre; }
      .import-viewer-meta { color:#9a9a9a; font-size:11px; margin-top:5px; }
    `;
    document.head.appendChild(style);
  }

  function draftMapping() {
    if (typeof global.buildMappingPayload !== 'function') {
      throw new Error('The current mapping is not available.');
    }
    return global.buildMappingPayload();
  }

  async function fetchPreview() {
    if (!currentProject) throw new Error('Open a project first.');
    return global.apiFetch(
      `/api/projects/${encodeURIComponent(currentProject)}/arangoimport-preview`,
      {
        method: 'POST',
        body: JSON.stringify({
          mode: state.mode,
          scope: state.scope,
          mapping_id: state.mappingId,
          mapping: draftMapping(),
          overwrite_on_initial: true,
          artifact_dir: state.artifactDir,
        }),
      }
    );
  }

  function joinedCommands(items) {
    return (items || []).map(item =>
      `# ${item.source_table} -> ${item.target_collection}\n${item.command}`
    ).join('\n\n');
  }

  // A bare command lacks what the bundle's preamble supplies: the exported
  // password it reads as @ARANGO_PASSWORD@, and the run_import check that
  // stops on rows arangoimport drops while still exiting 0.
  const BARE_COMMAND_NOTE = [
    '# Bare arangoimport command(s), without the full bundle\'s safeguards:',
    '#  - export ARANGO_PASSWORD first; the command reads it as @ARANGO_PASSWORD@',
    '#  - arangoimport exits 0 even when it drops rows; check its',
    '#    "warnings/errors:" line, or run the full bundle, which checks it',
    '',
  ].join('\n');

  function visibleContent() {
    const result = state.result;
    if (!result) return '';
    if (state.view === 'focused' && result.focused_command) {
      return BARE_COMMAND_NOTE + result.focused_command.command;
    }
    if (state.view === 'documents') return BARE_COMMAND_NOTE + joinedCommands(result.documents);
    if (state.view === 'edges') return BARE_COMMAND_NOTE + joinedCommands(result.edges);
    if (state.view === 'graph') return result.graph_script || '';
    return result.script || '';
  }

  function renderBody() {
    const result = state.result;
    const focusedOption = result && result.focused_command
      ? '<option value="focused">Selected command</option>'
      : '';
    const warnings = (result?.warnings || []).map(
      warning => `<div class="import-viewer-warning">${global.escHtml(warning)}</div>`
    ).join('');
    return `
      <div class="import-viewer-controls">
        <label>Artifact mode
          <select id="import-viewer-mode">
            <option value="jsonl"${state.mode === 'jsonl' ? ' selected' : ''}>JSONL batch</option>
            <option value="csv"${state.mode === 'csv' ? ' selected' : ''}>CSV-direct</option>
          </select>
        </label>
        <label>Show
          <select id="import-viewer-view">
            ${focusedOption}
            <option value="bundle">Full bundle</option>
            <option value="documents">Document commands</option>
            <option value="edges">Edge commands</option>
            <option value="graph">Graph creation</option>
          </select>
        </label>
        <label>Artifact directory
          <input id="import-viewer-artifact-dir" type="text"
            value="${global.escHtml(state.artifactDir)}"
            aria-label="Artifact directory">
        </label>
      </div>
      <div class="import-viewer-banner">
        <b>Generated batch import script</b> — Studio Load still streams data directly
        over HTTP and does not execute this script. Prepare artifacts under
        <code>${global.escHtml(result?.data_dir || '')}</code> before running it.
        ${warnings}
      </div>
      <textarea id="import-viewer-code" class="import-viewer-code" readonly spellcheck="false" aria-label="Generated arangoimport script"></textarea>
      <div class="import-viewer-meta">
        ${global.escHtml(result?.mapping_source || 'draft')} mapping ·
        ${global.escHtml(state.mode === 'jsonl' ? 'import.sh' : 'import_csv.sh')} ·
        password supplied at runtime through <code>ARANGO_PASSWORD</code> ·
        drag the lower-right corner to resize
      </div>`;
  }

  function syncContent() {
    const code = document.getElementById('import-viewer-code');
    if (code) code.value = visibleContent();
  }

  function bindControls() {
    const mode = document.getElementById('import-viewer-mode');
    const view = document.getElementById('import-viewer-view');
    const artifactDir = document.getElementById('import-viewer-artifact-dir');
    if (mode) {
      mode.addEventListener('change', async event => {
        const previousDefault = state.mode === 'jsonl' ? './output' : './dumps';
        state.mode = event.target.value;
        if (!state.artifactDir || state.artifactDir === previousDefault) {
          state.artifactDir = state.mode === 'jsonl' ? './output' : './dumps';
        }
        await loadAndRender();
      });
    }
    if (artifactDir) {
      artifactDir.addEventListener('change', async event => {
        const nextValue = event.target.value.trim();
        if (!nextValue) {
          global.toast('Artifact directory cannot be empty', 'error');
          event.target.value = state.artifactDir;
          return;
        }
        state.artifactDir = nextValue;
        await loadAndRender();
      });
    }
    if (view) {
      if (state.view === 'focused' && state.result?.focused_command) {
        view.value = 'focused';
      } else {
        state.view = 'bundle';
        view.value = 'bundle';
      }
      view.addEventListener('change', event => {
        state.view = event.target.value;
        syncContent();
      });
    }
    syncContent();
  }

  function showResult() {
    global.openFloatingCard({
      id: CARD_ID,
      title: 'Generated arangoimport script',
      status: { label: state.mode === 'jsonl' ? 'JSONL' : 'CSV-direct', kind: 'success' },
      position: { right: 30, top: 70 },
      body: renderBody(),
      actions: [
        { label: 'Copy', onClick: copyVisible },
        { label: 'Download', kind: 'primary', onClick: downloadVisible },
      ],
    });
    bindControls();
  }

  async function loadAndRender() {
    try {
      state.result = await fetchPreview();
      showResult();
    } catch (error) {
      global.toast(`Import preview failed: ${error.message}`, 'error');
    }
  }

  async function copyVisible() {
    const content = visibleContent();
    try {
      await navigator.clipboard.writeText(content);
      global.toast('Import script copied', 'success');
    } catch (_error) {
      const code = document.getElementById('import-viewer-code');
      if (code) {
        code.focus();
        code.select();
        document.execCommand('copy');
        global.toast('Import script copied', 'success');
      }
    }
  }

  function downloadVisible() {
    const blob = new Blob([visibleContent()], { type: 'text/x-shellscript;charset=utf-8' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = state.mode === 'jsonl' ? 'import.sh' : 'import_csv.sh';
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(link.href);
  }

  function openArangoImportViewer(options) {
    const opts = options || {};
    injectStyles();
    state.mode = opts.mode || 'jsonl';
    state.scope = opts.scope || 'all';
    state.mappingId = opts.mappingId || null;
    state.view = state.scope === 'all' ? 'bundle' : 'focused';
    state.artifactDir = opts.artifactDir ||
      (state.mode === 'jsonl' ? './output' : './dumps');
    state.result = null;
    return loadAndRender();
  }

  global.openArangoImportViewer = openArangoImportViewer;
  global.R2GArangoImportViewer = {
    open: openArangoImportViewer,
    visibleContent,
  };
})(window);
