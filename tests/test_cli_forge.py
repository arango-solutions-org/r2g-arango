"""CLI tests for ``r2g forge generate`` (Federation Forge walking skeleton).

Pure in-memory + tmp_path — no live database. The end-to-end roundtrip lives
in ``tests/integration/test_forge_roundtrip.py``.
"""

from __future__ import annotations

import json
import sys

import pytest
from typer.testing import CliRunner

from r2g.main import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def _reset_structlog(monkeypatch):
    import structlog

    def _stderr_setup(level: str = "INFO", json_output: bool = False) -> None:
        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(0),
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
            cache_logger_on_first_use=False,
        )

    monkeypatch.setattr("r2g.log.setup_logging", _stderr_setup)
    monkeypatch.setattr("r2g.main.setup_logging", _stderr_setup)
    _stderr_setup()
    yield
    structlog.reset_defaults()


@pytest.fixture
def ontology_file(tmp_path):
    path = tmp_path / "ontology.json"
    path.write_text(
        json.dumps(
            {
                "entities": [
                    {
                        "name": "Account",
                        "properties": [{"name": "accountName", "type": "string"}],
                    },
                    {
                        "name": "Contact",
                        "properties": [{"name": "isPrimary", "type": "boolean"}],
                    },
                ],
                "relationships": [
                    {
                        "type": "contactsToAccounts",
                        "fromEntity": "Contact",
                        "toEntity": "Account",
                    }
                ],
            }
        )
    )
    return path


def test_generate_writes_artifacts(ontology_file, tmp_path):
    out_dir = tmp_path / "out"
    result = runner.invoke(
        app,
        [
            "forge",
            "generate",
            "--ontology",
            str(ontology_file),
            "--seed",
            "421",
            "--out-dir",
            str(out_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Forged" in result.output
    ddl = (out_dir / "forge.sql").read_text()
    assert "CREATE TABLE accounts" in ddl
    assert "FOREIGN KEY (account_id) REFERENCES accounts (id)" in ddl
    load_sql = (out_dir / "forge.load.sql").read_text()
    assert load_sql.count("INSERT INTO contacts") == 10
    rows = json.loads((out_dir / "forge.rows.json").read_text())
    assert set(rows) == {"accounts", "contacts"}


def test_generate_is_deterministic_across_invocations(ontology_file, tmp_path):
    outputs = []
    for run in ("a", "b"):
        out_dir = tmp_path / run
        result = runner.invoke(
            app,
            ["forge", "generate", "--ontology", str(ontology_file), "--seed", "7", "--out-dir", str(out_dir)],
        )
        assert result.exit_code == 0, result.output
        outputs.append(
            tuple((out_dir / name).read_text() for name in ("forge.sql", "forge.load.sql", "forge.rows.json"))
        )
    assert outputs[0] == outputs[1]


def test_refused_ontology_exits_2_with_reason(tmp_path):
    path = tmp_path / "colliding.json"
    path.write_text(
        json.dumps(
            {
                "entities": [
                    {"name": "Account", "properties": [{"name": "label", "type": "string"}]},
                    {"name": "Contact", "properties": [{"name": "label", "type": "string"}]},
                ]
            }
        )
    )
    result = runner.invoke(app, ["forge", "generate", "--ontology", str(path)])
    assert result.exit_code == 2
    assert "Forge refused" in result.output
    assert "collision-free" in result.output


def test_unsupported_dialect_exits_2(ontology_file):
    result = runner.invoke(
        app,
        ["forge", "generate", "--ontology", str(ontology_file), "--dialect", "duckdb"],
    )
    assert result.exit_code == 2
    assert "dialect" in result.output


def test_bad_rows_per_entity_exits_2(ontology_file):
    result = runner.invoke(
        app,
        ["forge", "generate", "--ontology", str(ontology_file), "--rows-per-entity", "0"],
    )
    assert result.exit_code == 2
    assert "rows-per-entity" in result.output


def test_missing_ontology_file_is_a_refusal(tmp_path):
    result = runner.invoke(
        app,
        ["forge", "generate", "--ontology", str(tmp_path / "absent.json")],
    )
    assert result.exit_code == 2
    assert "Forge refused" in result.output
    assert "cannot read ontology file" in result.output


@pytest.mark.parametrize("dialect", ["snowflake", "clickhouse"])
def test_generate_sql_dialects_write_sql_files(ontology_file, tmp_path, dialect):
    out_dir = tmp_path / dialect
    result = runner.invoke(
        app,
        ["forge", "generate", "--ontology", str(ontology_file), "--dialect", dialect, "--out-dir", str(out_dir)],
    )
    assert result.exit_code == 0, result.output
    assert f"dialect {dialect}" in result.output
    assert sorted(p.name for p in out_dir.iterdir()) == ["forge.load.sql", "forge.rows.json", "forge.sql"]
    assert f"-- dialect: {dialect}" in (out_dir / "forge.sql").read_text()


def test_generate_arango_writes_manifest_and_loader_script(ontology_file, tmp_path):
    out_dir = tmp_path / "arango"
    result = runner.invoke(
        app,
        ["forge", "generate", "--ontology", str(ontology_file), "--dialect", "arango", "--out-dir", str(out_dir)],
    )
    assert result.exit_code == 0, result.output
    assert sorted(p.name for p in out_dir.iterdir()) == ["forge.collections.json", "forge.load.py", "forge.rows.json"]
    manifest = json.loads((out_dir / "forge.collections.json").read_text())
    assert [e["name"] for e in manifest["edgeCollections"]] == ["contacts_to_accounts"]
    compile((out_dir / "forge.load.py").read_text(), "forge.load.py", "exec")


def test_rows_file_is_identical_across_dialects(ontology_file, tmp_path):
    rows = {}
    for dialect in ("postgres", "snowflake", "clickhouse", "arango"):
        out_dir = tmp_path / dialect
        result = runner.invoke(
            app,
            ["forge", "generate", "-O", str(ontology_file), "--dialect", dialect, "--seed", "9", "-o", str(out_dir)],
        )
        assert result.exit_code == 0, result.output
        rows[dialect] = (out_dir / "forge.rows.json").read_text()
    assert len(set(rows.values())) == 1


def test_help_lists_the_dialects(ontology_file):
    result = runner.invoke(app, ["forge", "generate", "--help"])
    assert result.exit_code == 0
    for dialect in ("postgres", "snowflake", "clickhouse", "arango"):
        assert dialect in result.output


# ── volume: --scale, --bulk (PLAN F-10/F-11) ─────────────────────────────────


@pytest.fixture
def scale_file(tmp_path):
    path = tmp_path / "scale.json"
    path.write_text(
        json.dumps(
            {
                "rows": {"Account": 3, "Contact": 40},
                "relationships": {"contactsToAccounts": {"distribution": "zipf", "exponent": 1.5}},
            }
        )
    )
    return path


def test_scale_sets_row_counts(ontology_file, scale_file, tmp_path):
    out_dir = tmp_path / "out"
    result = runner.invoke(
        app,
        ["forge", "generate", "-O", str(ontology_file), "--scale", str(scale_file), "-o", str(out_dir)],
    )
    assert result.exit_code == 0, result.output
    rows = json.loads((out_dir / "forge.rows.json").read_text())
    assert (len(rows["accounts"]), len(rows["contacts"])) == (3, 40)


@pytest.mark.parametrize(
    "content, reason",
    [
        ("{not json", "not valid JSON"),
        ('{"rows": {"Acount": 3}}', "does not declare"),
        ('{"relationships": {"contactsToAccounts": {"distribution": "zipf"}}}', "exponent"),
    ],
)
def test_refused_scale_exits_2_with_reason(ontology_file, tmp_path, content, reason):
    path = tmp_path / "scale.json"
    path.write_text(content)
    result = runner.invoke(
        app, ["forge", "generate", "-O", str(ontology_file), "--scale", str(path), "-o", str(tmp_path / "o")]
    )
    assert result.exit_code == 2, result.output
    assert "Forge refused" in result.output and reason in result.output


def test_missing_scale_file_is_a_refusal(ontology_file, tmp_path):
    result = runner.invoke(
        app, ["forge", "generate", "-O", str(ontology_file), "--scale", str(tmp_path / "absent.json")]
    )
    assert result.exit_code == 2
    assert "cannot read scale file" in result.output


@pytest.mark.parametrize("dialect", ["postgres", "arango"])
def test_bulk_writes_chunks_loader_and_manifest(ontology_file, scale_file, tmp_path, dialect):
    out_dir = tmp_path / dialect
    result = runner.invoke(
        app,
        [
            "forge", "generate", "-O", str(ontology_file), "--scale", str(scale_file),
            "--dialect", dialect, "--bulk", "--chunk-rows", "16", "-o", str(out_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Forged" in result.output and "forge.bulk.json" in result.output
    manifest = json.loads((out_dir / "forge.bulk.json").read_text())
    contacts = next(t for t in manifest["targets"] if t["name"] == "contacts")
    assert contacts["rows"] == 40 and [f["rows"] for f in contacts["files"]] == [16, 16, 8]
    assert not (out_dir / "forge.rows.json").exists()
    loader = "forge.bulk.py" if dialect == "arango" else "forge.bulk.sql"
    assert (out_dir / loader).exists()


def test_bad_chunk_rows_exits_2(ontology_file):
    result = runner.invoke(
        app, ["forge", "generate", "-O", str(ontology_file), "--bulk", "--chunk-rows", "0"]
    )
    assert result.exit_code == 2
    assert "chunk-rows" in result.output
