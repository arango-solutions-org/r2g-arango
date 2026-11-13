from __future__ import annotations

from pathlib import Path

STATIC_DIR = Path(__file__).parents[1] / "src" / "r2g" / "ui" / "static"


def test_viewer_is_loaded_and_available_from_context_surfaces():
    index = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    # Relative, so it also loads under a path prefix (see test_control_plane_mode).
    assert '<script src="static/arangoimport-viewer.js"></script>' in index
    assert index.count("View arangoimport script bundle") >= 3
    assert index.count("View arangoimport command") >= 2
    assert "scope: 'collection'" in index
    assert "scope: 'edge'" in index


def test_viewer_supports_modes_views_copy_and_client_download():
    source = (STATIC_DIR / "arangoimport-viewer.js").read_text(encoding="utf-8")
    for expected in (
        'value="jsonl"',
        'value="csv"',
        'value="bundle"',
        'value="documents"',
        'value="edges"',
        'value="graph"',
        "navigator.clipboard.writeText",
        "new Blob(",
        "URL.createObjectURL",
        "resize:both",
        "drag the lower-right corner to resize",
        "Artifact directory",
        "artifact_dir: state.artifactDir",
        "state.mode = opts.mode || 'jsonl'",
        "state.artifactDir = opts.artifactDir",
    ):
        assert expected in source


def test_viewer_explains_streaming_load_and_runtime_password():
    source = (STATIC_DIR / "arangoimport-viewer.js").read_text(encoding="utf-8")
    assert "Studio Load still streams data directly" in source
    assert "ARANGO_PASSWORD" in source
    assert "output_path" not in source


def test_source_picker_lists_all_supported_relational_sources():
    index = (STATIC_DIR / "index.html").read_text(encoding="utf-8")

    for source_type in (
        "postgresql",
        "mysql",
        "sqlserver",
        "snowflake",
        "clickhouse",
    ):
        assert f'<option value="{source_type}">' in index
    assert "Demo tomorrow" not in index


def test_bare_command_views_say_what_the_bundle_adds():
    """Selected command, documents and edges show commands without the preamble.

    A bare command lacks the bundle's exported ARANGO_PASSWORD and its
    run_import check, which stops on rows arangoimport drops while exiting 0.
    Every view that shows bare commands leads with a note saying so; the full
    bundle and the graph script do not need one.
    """
    source = (STATIC_DIR / "arangoimport-viewer.js").read_text(encoding="utf-8")
    note_start = source.index("const BARE_COMMAND_NOTE")
    note = source[note_start:source.index("].join", note_start)]
    assert "export ARANGO_PASSWORD first" in note
    assert "exits 0 even when it drops rows" in note
    assert "warnings/errors:" in note
    for expression in (
        "BARE_COMMAND_NOTE + result.focused_command.command",
        "BARE_COMMAND_NOTE + joinedCommands(result.documents)",
        "BARE_COMMAND_NOTE + joinedCommands(result.edges)",
    ):
        assert expression in source
    assert "return result.script || '';" in source
