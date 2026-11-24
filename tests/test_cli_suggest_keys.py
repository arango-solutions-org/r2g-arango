"""`r2g source suggest-keys` / `set-key-overlay` -- the propose -> review -> apply loop.

End to end against fakesnow (an in-process Snowflake emulator): r2g's connector,
relational-schema-analyzer's governed sampler and key profiler, the draft file,
the review gate, and the snapshot that applies the reviewed overlay.
"""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

fakesnow = pytest.importorskip("fakesnow")
pytest.importorskip("snowflake.connector")
pytest.importorskip("relational_schema_analyzer.key_profiling")

from r2g.catalog import CatalogManager  # noqa: E402
from r2g.main import app  # noqa: E402

_GEN = "FROM TABLE(GENERATOR(ROWCOUNT => {n}))"
_DDL = [
    "CREATE TABLE CUSTOMERS (CUSTOMER_ID INT, EMAIL VARCHAR)",
    "INSERT INTO CUSTOMERS SELECT seq4() + 1, 'u' || seq4() || '@x.io' " + _GEN.format(n=40),
    "CREATE TABLE ORDERS (ORDER_ID INT, CUSTOMER_ID INT)",
    "INSERT INTO ORDERS SELECT seq4() + 1, MOD(seq4(), 40) + 1 " + _GEN.format(n=60),
]
URL = "snowflake://u:p@acct/DB1/PUBLIC"
runner = CliRunner()


def _log_to_stderr(*_args, **_kwargs) -> None:
    import sys

    import structlog

    structlog.reset_defaults()
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(0),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=False,
    )


@pytest.fixture(autouse=True)
def _reset_structlog(monkeypatch):
    """CliRunner swaps stdout for a stream it closes after each invoke, and the
    CLI's start-up hook binds every logger to stdout and caches it -- so a second
    invoke in one test logs to a closed stream. Here the hook logs to stderr,
    uncached, which keeps several invokes per test safe."""
    monkeypatch.setattr("r2g.main.setup_logging", _log_to_stderr)
    _log_to_stderr()
    yield
    _log_to_stderr()


@pytest.fixture
def warehouse(tmp_path, monkeypatch):
    import snowflake.connector as sc

    monkeypatch.setattr("r2g.main._get_catalog", lambda: CatalogManager(str(tmp_path / "catalog")))
    monkeypatch.chdir(tmp_path)
    with fakesnow.patch():
        conn = sc.connect(database="DB1", schema="PUBLIC")
        cur = conn.cursor()
        for stmt in _DDL:
            cur.execute(stmt)
        cur.close()
        CatalogManager(str(tmp_path / "catalog")).add_source("wh", "snowflake", URL)
        yield tmp_path
        conn.close()


def test_suggest_keys_writes_a_draft_and_applies_nothing(warehouse):
    result = runner.invoke(app, ["source", "suggest-keys", "wh"])
    assert result.exit_code == 0, result.output

    draft = json.loads((warehouse / "wh.keys.draft.json").read_text())
    assert draft["description"].startswith("DRAFT")
    assert draft["tables"]["CUSTOMERS"]["primaryKey"] == ["CUSTOMER_ID"]
    (fk,) = draft["tables"]["ORDERS"]["foreignKeys"]
    assert fk["references"] == {"table": "CUSTOMERS", "columns": ["CUSTOMER_ID"]}
    # Proposing is not applying.
    assert CatalogManager(str(warehouse / "catalog")).get_source("wh").key_overlay is None
    assert "set-key-overlay wh" in result.output and "Cost:" in result.output


def test_an_existing_draft_is_not_overwritten_without_force(warehouse):
    (warehouse / "wh.keys.draft.json").write_text("{}")
    result = runner.invoke(app, ["source", "suggest-keys", "wh"])
    assert result.exit_code == 1 and "--force" in result.output
    assert (warehouse / "wh.keys.draft.json").read_text() == "{}"


def test_the_query_budget_is_reported_when_it_runs_out(warehouse):
    result = runner.invoke(app, ["source", "suggest-keys", "wh", "--max-queries", "1"])
    assert result.exit_code == 0, result.output
    assert "Query budget used up" in result.output


def test_sources_without_a_governed_key_probe_are_refused(warehouse):
    # Snowflake and PostgreSQL samplers can profile keys under a budget; MySQL's cannot.
    CatalogManager(str(warehouse / "catalog")).add_source("my", "mysql", "mysql://h/db")
    result = runner.invoke(app, ["source", "suggest-keys", "my"])
    assert result.exit_code == 2
    assert "Snowflake and PostgreSQL" in result.output


def test_an_unreviewed_draft_is_refused_then_accepted_with_reviewed(warehouse):
    runner.invoke(app, ["source", "suggest-keys", "wh"])
    refused = runner.invoke(app, ["source", "set-key-overlay", "wh", "wh.keys.draft.json"])
    assert refused.exit_code == 1 and "still marked DRAFT" in refused.output
    assert CatalogManager(str(warehouse / "catalog")).get_source("wh").key_overlay is None

    accepted = runner.invoke(app, ["source", "set-key-overlay", "wh", "wh.keys.draft.json", "--reviewed"])
    assert accepted.exit_code == 0, accepted.output
    assert "2 key(s), 1 reference(s)" in accepted.output


def test_an_edited_draft_needs_no_flag(warehouse):
    runner.invoke(app, ["source", "suggest-keys", "wh"])
    path = warehouse / "wh.keys.draft.json"
    draft = json.loads(path.read_text())
    draft["description"] = "Reviewed by data owner, 2026-10-02"
    path.write_text(json.dumps(draft))
    result = runner.invoke(app, ["source", "set-key-overlay", "wh", str(path)])
    assert result.exit_code == 0, result.output


def test_the_full_loop_ends_in_a_snapshot_with_the_reviewed_keys(warehouse):
    runner.invoke(app, ["source", "suggest-keys", "wh"])
    runner.invoke(app, ["source", "set-key-overlay", "wh", "wh.keys.draft.json", "--reviewed"])
    result = runner.invoke(app, ["source", "snapshot", "wh"])
    assert result.exit_code == 0, result.output

    snap = CatalogManager(str(warehouse / "catalog")).get_latest_snapshot("wh")
    tables = snap.schema_data.tables
    assert tables["CUSTOMERS"].primary_key == ["CUSTOMER_ID"]
    (fk,) = tables["ORDERS"].foreign_keys
    assert fk.foreign_table == "CUSTOMERS" and fk.enforced is False


def test_clear_removes_the_overlay(warehouse):
    runner.invoke(app, ["source", "suggest-keys", "wh"])
    runner.invoke(app, ["source", "set-key-overlay", "wh", "wh.keys.draft.json", "--reviewed"])
    result = runner.invoke(app, ["source", "set-key-overlay", "wh", "--clear"])
    assert result.exit_code == 0
    assert CatalogManager(str(warehouse / "catalog")).get_source("wh").key_overlay is None


def test_an_invalid_overlay_file_is_rejected(warehouse):
    (warehouse / "bad.json").write_text(json.dumps({"version": 1, "tables": {"X": {"oops": 1}}}))
    result = runner.invoke(app, ["source", "set-key-overlay", "wh", "bad.json"])
    assert result.exit_code == 1 and "Not a valid key overlay" in result.output


def test_a_sampler_that_cannot_connect_fails_loudly_and_writes_nothing(warehouse, monkeypatch):
    # r2g's connector reads the schema; RSA's sampler opens its own connection. If
    # only the sampler fails (say it parses a key-pair URL differently), every probe
    # declines -- which must not pass for a clean run that found no keys.
    class _Refuses:
        @staticmethod
        def connect(**_params):
            raise RuntimeError("Incorrect username or password was specified.")

    monkeypatch.setattr(
        "relational_schema_analyzer.connectors.snowflake._load_snowflake_connector", lambda: _Refuses
    )
    result = runner.invoke(app, ["source", "suggest-keys", "wh"])
    assert result.exit_code == 1, result.output
    assert "could not connect" in result.output
    assert not (warehouse / "wh.keys.draft.json").exists()


def _mark_views(monkeypatch, *names: str) -> None:
    """fakesnow lists no views, so mark them where r2g's connector returns them."""
    from r2g.connectors.snowflake import SnowflakeConnector

    real = SnowflakeConnector.get_schema

    def get_schema(self):
        schema = real(self)
        for table in schema.tables.values():
            table.is_view = table.name in names
        return schema

    monkeypatch.setattr(SnowflakeConnector, "get_schema", get_schema)


def test_a_view_is_not_evaluated_and_a_base_table_keeps_its_key(warehouse, monkeypatch):
    _mark_views(monkeypatch, "ORDERS")
    result = runner.invoke(app, ["source", "suggest-keys", "wh"])
    assert result.exit_code == 0, result.output

    draft = json.loads((warehouse / "wh.keys.draft.json").read_text())
    assert draft["tables"]["CUSTOMERS"]["primaryKey"] == ["CUSTOMER_ID"]
    assert "primaryKey" not in draft["tables"].get("ORDERS", {})
    assert "view: uniqueness of a view is not a key" in " ".join(result.output.split())


def test_a_view_only_schema_writes_an_empty_draft_and_says_why(warehouse, monkeypatch):
    _mark_views(monkeypatch, "CUSTOMERS", "ORDERS")
    result = runner.invoke(app, ["source", "suggest-keys", "wh"])
    assert result.exit_code == 0, result.output

    draft = json.loads((warehouse / "wh.keys.draft.json").read_text())
    assert not any("primaryKey" in spec for spec in draft["tables"].values())
    output = " ".join(result.output.split())
    assert "CUSTOMERS: view: uniqueness of a view is not a key" in output
    assert "ORDERS: view: uniqueness of a view is not a key" in output


def test_a_table_referring_to_a_view_gets_no_inferred_relationship(warehouse, monkeypatch):
    # Without the view the reference is inferred (see the first test in this file).
    # A view gets no proposed key, so the reference has nothing to point at.
    _mark_views(monkeypatch, "CUSTOMERS")
    result = runner.invoke(app, ["source", "suggest-keys", "wh"])
    assert result.exit_code == 0, result.output

    draft = json.loads((warehouse / "wh.keys.draft.json").read_text())
    assert "primaryKey" not in draft["tables"].get("CUSTOMERS", {})
    assert draft["tables"]["ORDERS"]["primaryKey"] == ["ORDER_ID"]
    assert "foreignKeys" not in draft["tables"]["ORDERS"]
    output = " ".join(result.output.split())
    assert "CUSTOMERS: view: uniqueness of a view is not a key" in output
    assert "ORDERS.CUSTOMER_ID -> CUSTOMERS" not in output


def test_a_reviewed_overlay_restores_the_views_key_and_the_relationship(warehouse, monkeypatch):
    _mark_views(monkeypatch, "CUSTOMERS")
    overlay = {
        "version": 1,
        "description": "Reviewed keys for a view and the table that refers to it",
        "tables": {
            "CUSTOMERS": {"primaryKey": ["CUSTOMER_ID"]},
            "ORDERS": {
                "primaryKey": ["ORDER_ID"],
                "foreignKeys": [
                    {
                        "columns": ["CUSTOMER_ID"],
                        "references": {"table": "CUSTOMERS", "columns": ["CUSTOMER_ID"]},
                    }
                ],
            },
        },
    }
    (warehouse / "reviewed.json").write_text(json.dumps(overlay))
    result = runner.invoke(app, ["source", "set-key-overlay", "wh", "reviewed.json"])
    assert result.exit_code == 0, result.output
    result = runner.invoke(app, ["source", "snapshot", "wh"])
    assert result.exit_code == 0, result.output

    tables = CatalogManager(str(warehouse / "catalog")).get_latest_snapshot("wh").schema_data.tables
    assert tables["CUSTOMERS"].is_view is True
    assert tables["CUSTOMERS"].primary_key == ["CUSTOMER_ID"]
    (fk,) = tables["ORDERS"].foreign_keys
    assert (fk.columns, fk.foreign_table, fk.foreign_columns) == (
        ["CUSTOMER_ID"],
        "CUSTOMERS",
        ["CUSTOMER_ID"],
    )
    assert fk.enforced is False
