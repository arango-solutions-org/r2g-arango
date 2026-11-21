"""Offline sources: a schema entered from a file, with no connection (#78).

A partner can hand over its schema instead of a connection. ``source add
--offline`` registers the source with no connection string, and ``source
snapshot --from-file`` reads RSA ``PhysicalSchema`` JSON through the same key
overlay and classification steps an introspected snapshot goes through.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner

from r2g.catalog import CatalogManager
from r2g.connectors.base import create_source_connector
from r2g.main import app
from r2g.snapshot import (
    MAX_SCHEMA_FILE_BYTES,
    OFFLINE_PARAM,
    SchemaFileError,
    build_source_schema,
    capture_source_snapshot,
    is_offline,
    read_schema_file,
)
from r2g.types import Classification, Column, Schema, Table

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


def _keyless_schema() -> Schema:
    return Schema(tables={
        "ACCOUNTS": Table(name="ACCOUNTS", columns=[
            Column(name="ACCOUNT_ID", data_type="text", is_nullable=False),
            Column(name="NAME", data_type="text"),
        ]),
        "OPPORTUNITIES": Table(name="OPPORTUNITIES", columns=[
            Column(name="OPPORTUNITY_ID", data_type="text", is_nullable=False),
            Column(name="ACCOUNT_ID", data_type="text"),
            Column(name="AMOUNT", data_type="numeric"),
        ]),
    })


_OVERLAY = {
    "version": 1,
    "tables": {
        "ACCOUNTS": {"primaryKey": ["ACCOUNT_ID"]},
        "OPPORTUNITIES": {
            "primaryKey": ["OPPORTUNITY_ID"],
            "foreignKeys": [{
                "columns": ["ACCOUNT_ID"],
                "references": {"table": "ACCOUNTS", "columns": ["ACCOUNT_ID"]},
            }],
        },
    },
}


@pytest.fixture
def schema_file(tmp_path) -> Path:
    path = tmp_path / "schema.json"
    _keyless_schema().save_to_file(str(path))
    return path


@pytest.fixture
def catalog_dir(tmp_path, monkeypatch) -> str:
    directory = str(tmp_path / "catalog")
    monkeypatch.setattr("r2g.main._get_catalog", lambda: CatalogManager(directory))
    return directory


def _add_offline(mgr: CatalogManager, **kw):
    return mgr.add_source(
        "partner", "snowflake", "", source_params={OFFLINE_PARAM: True}, **kw
    )


# --- reading the file -------------------------------------------------------


class TestReadSchemaFile:
    def test_reads_tables_and_fingerprints_the_bytes(self, schema_file):
        loaded = read_schema_file(schema_file)
        assert sorted(loaded.schema.tables) == ["ACCOUNTS", "OPPORTUNITIES"]
        assert loaded.source == str(schema_file.resolve())
        assert loaded.fingerprint == hashlib.sha256(schema_file.read_bytes()).hexdigest()

    def test_missing_file_is_refused(self, tmp_path):
        with pytest.raises(SchemaFileError, match="does not exist"):
            read_schema_file(tmp_path / "absent.json")

    def test_a_directory_is_refused(self, tmp_path):
        with pytest.raises(SchemaFileError, match="not a file"):
            read_schema_file(tmp_path)

    def test_non_json_is_refused_naming_the_file(self, tmp_path):
        bad = tmp_path / "schema.sql"
        bad.write_text("CREATE TABLE ACCOUNTS (ACCOUNT_ID VARCHAR);")
        with pytest.raises(SchemaFileError, match="schema.sql' is not a physical schema") as e:
            read_schema_file(bad)
        assert not str(e.value).endswith(" at ")  # JSON errors have no location

    def test_wrong_shape_is_refused(self, tmp_path):
        bad = tmp_path / "schema.json"
        bad.write_text(json.dumps({"tables": {"T": {"columns": "not a list"}}}))
        with pytest.raises(SchemaFileError, match="is not a physical schema"):
            read_schema_file(bad)

    def test_a_schema_with_no_tables_is_refused(self, tmp_path):
        empty = tmp_path / "schema.json"
        empty.write_text(json.dumps({"tables": {}}))
        with pytest.raises(SchemaFileError, match="has no tables"):
            read_schema_file(empty)

    def test_an_oversized_file_is_refused_before_it_is_read(self, tmp_path, monkeypatch):
        big = tmp_path / "schema.json"
        big.write_text(json.dumps({"tables": {}}))
        monkeypatch.setattr("r2g.snapshot.MAX_SCHEMA_FILE_BYTES", 4)
        with pytest.raises(SchemaFileError, match="the limit is 4"):
            read_schema_file(big)
        assert MAX_SCHEMA_FILE_BYTES == 50 * 1024 * 1024

    def test_unicode_names_survive(self, tmp_path):
        path = tmp_path / "schema.json"
        Schema(tables={"KONTO_ÜBERSICHT": Table(name="KONTO_ÜBERSICHT", columns=[
            Column(name="KÄUFER_ID", data_type="text"),
        ])}).save_to_file(str(path))
        assert "KONTO_ÜBERSICHT" in read_schema_file(path).schema.tables


# --- building and storing the snapshot ----------------------------------------


class TestFileSnapshot:
    def test_the_overlay_and_classifications_apply_as_for_a_live_snapshot(
        self, tmp_path, schema_file
    ):
        mgr = CatalogManager(str(tmp_path / "catalog"))
        _add_offline(
            mgr,
            key_overlay=_OVERLAY,
            key_overlay_source="keys.json",
            classifications={"ACCOUNTS": {"NAME": Classification(tags=["PII.Sensitive"])}},
        )
        snap, result = capture_source_snapshot(
            mgr, "partner", schema_name="SALES", schema_file=read_schema_file(schema_file)
        )
        opp = snap.schema_data.tables["OPPORTUNITIES"]
        assert opp.primary_key == ["OPPORTUNITY_ID"]
        assert [(fk.columns, fk.foreign_table) for fk in opp.foreign_keys] == [
            (["ACCOUNT_ID"], "ACCOUNTS")
        ]
        assert result.key_overlay_summary["foreignKeys"] == 1
        assert result.classifications_applied == 1
        assert snap.pg_schema == "SALES"
        assert snap.schema_file_source == str(schema_file.resolve())
        assert snap.schema_file_fingerprint == hashlib.sha256(schema_file.read_bytes()).hexdigest()

    def test_provenance_survives_a_catalog_reload(self, tmp_path, schema_file):
        catalog_dir = str(tmp_path / "catalog")
        mgr = CatalogManager(catalog_dir)
        _add_offline(mgr)
        snap, _ = capture_source_snapshot(
            mgr, "partner", schema_name="SALES", schema_file=read_schema_file(schema_file)
        )
        reloaded = CatalogManager(catalog_dir).get_latest_snapshot("partner")
        assert reloaded is not None and reloaded.id == snap.id
        assert reloaded.schema_file_source == snap.schema_file_source
        assert reloaded.schema_file_fingerprint == snap.schema_file_fingerprint

    def test_a_file_snapshot_equals_the_live_one_for_the_same_schema(
        self, tmp_path, schema_file
    ):
        """The offline path must not change the schema: a file written from an
        introspected schema gives the same tables and keys as introspection."""

        class _Connector:
            def get_schema(self):
                return _keyless_schema()

        mgr = CatalogManager(str(tmp_path / "catalog"))
        mgr.add_source("live", "snowflake", "snowflake://acct/db/SALES", key_overlay=_OVERLAY)
        live_source = mgr.get_source("live")
        from unittest.mock import patch

        with patch("r2g.connectors.base.create_source_connector", lambda *a, **kw: _Connector()):
            live = build_source_schema(live_source, schema_name="SALES")
        from_file = build_source_schema(
            live_source, schema_name="SALES", schema_file=read_schema_file(schema_file)
        )
        assert from_file.schema.model_dump() == live.schema.model_dump()
        assert from_file.key_overlay_summary == live.key_overlay_summary

    def test_an_offline_source_without_a_file_is_refused(self, tmp_path):
        mgr = CatalogManager(str(tmp_path / "catalog"))
        source = _add_offline(mgr)
        assert is_offline(source)
        with pytest.raises(ValueError, match="offline"):
            build_source_schema(source, schema_name="SALES")

    def test_an_introspected_snapshot_serializes_without_the_file_fields(self, tmp_path):
        mgr = CatalogManager(str(tmp_path / "catalog"))
        mgr.add_source("live", "postgresql", "postgresql://localhost/x")
        snap = mgr.create_snapshot("live", _keyless_schema())
        dumped = snap.model_dump(mode="json")
        assert "schema_file_source" not in dumped
        assert "schema_file_fingerprint" not in dumped


class TestConnectorFactory:
    @pytest.mark.parametrize("conn", ["", "   "])
    def test_an_empty_connection_string_is_refused_with_the_reason(self, conn):
        with pytest.raises(ValueError, match="--offline has no connection"):
            create_source_connector("snowflake", conn)


# --- the CLI -----------------------------------------------------------------


class TestCli:
    def test_add_offline_then_snapshot_from_file(self, catalog_dir, schema_file, tmp_path):
        CatalogManager(catalog_dir)
        overlay = tmp_path / "keys.json"
        overlay.write_text(json.dumps(_OVERLAY))
        added = runner.invoke(app, [
            "source", "add", "--name", "partner", "--type", "snowflake",
            "--offline", "--key-overlay", str(overlay),
        ])
        assert added.exit_code == 0, added.output
        assert "Offline source 'partner' added" in added.output

        snapped = runner.invoke(app, [
            "source", "snapshot", "partner", "--pg-schema", "SALES",
            "--from-file", str(schema_file),
        ])
        assert snapped.exit_code == 0, snapped.output
        assert "2 tables read from file" in snapped.output
        assert "Applied reviewed key overlay" in snapped.output

        mgr = CatalogManager(catalog_dir)
        source = mgr.get_source("partner")
        assert source is not None and source.connection_string == ""
        snap = mgr.get_latest_snapshot("partner")
        assert snap is not None and snap.pg_schema == "SALES"
        assert snap.schema_file_source == str(schema_file.resolve())

    def test_list_marks_an_offline_source(self, catalog_dir):
        _add_offline(CatalogManager(catalog_dir))
        listed = runner.invoke(app, ["source", "list"])
        assert listed.exit_code == 0, listed.output
        assert "(offline)" in listed.output

    def test_offline_with_a_connection_is_refused(self, catalog_dir):
        CatalogManager(catalog_dir)
        result = runner.invoke(app, [
            "source", "add", "--name", "p", "--type", "snowflake",
            "--offline", "--conn", "snowflake://acct/db",
        ])
        assert result.exit_code == 1
        assert "drop --conn" in result.output
        assert CatalogManager(catalog_dir).get_source("p") is None

    def test_no_connection_and_not_offline_is_refused(self, catalog_dir):
        CatalogManager(catalog_dir)
        result = runner.invoke(app, ["source", "add", "--name", "p", "--type", "snowflake"])
        assert result.exit_code == 1
        assert "--offline" in result.output
        assert CatalogManager(catalog_dir).get_source("p") is None

    def test_snapshot_of_an_offline_source_needs_a_file(self, catalog_dir):
        _add_offline(CatalogManager(catalog_dir))
        result = runner.invoke(app, ["source", "snapshot", "partner"])
        assert result.exit_code == 1
        assert "--from-file" in result.output
        assert CatalogManager(catalog_dir).get_latest_snapshot("partner") is None

    def test_a_bad_file_stores_nothing(self, catalog_dir, tmp_path):
        _add_offline(CatalogManager(catalog_dir))
        bad = tmp_path / "schema.json"
        bad.write_text("{not json")
        result = runner.invoke(app, ["source", "snapshot", "partner", "--from-file", str(bad)])
        assert result.exit_code == 1
        assert "is not a physical schema" in result.output
        assert CatalogManager(catalog_dir).get_latest_snapshot("partner") is None

    def test_accepting_inferred_keys_keeps_the_file_provenance(self, catalog_dir, schema_file):
        mgr = CatalogManager(catalog_dir)
        # Primary keys only: the ACCOUNT_ID reference is left for inference to find.
        _add_offline(mgr, key_overlay={"version": 1, "tables": {
            "ACCOUNTS": {"primaryKey": ["ACCOUNT_ID"]},
            "OPPORTUNITIES": {"primaryKey": ["OPPORTUNITY_ID"]},
        }})
        first, _ = capture_source_snapshot(
            mgr, "partner", schema_name="SALES", schema_file=read_schema_file(schema_file)
        )
        result = runner.invoke(app, ["source", "infer-fks", "partner", "--accept"])
        assert result.exit_code == 0, result.output
        assert "Accepted 1 candidate" in result.output
        latest = CatalogManager(catalog_dir).get_latest_snapshot("partner")
        assert latest is not None and latest.id != first.id
        assert latest.schema_file_source == first.schema_file_source
        assert latest.schema_file_fingerprint == first.schema_file_fingerprint


class TestLiveReadsRefuseAnOfflineSource:
    """Every command that would read rows or keys from the source says why it
    cannot, instead of failing inside a database driver."""

    @pytest.fixture
    def offline_with_snapshot(self, catalog_dir, schema_file):
        mgr = CatalogManager(catalog_dir)
        _add_offline(mgr)
        capture_source_snapshot(
            mgr, "partner", schema_name="SALES", schema_file=read_schema_file(schema_file)
        )
        return catalog_dir

    @pytest.mark.parametrize("argv, needs", [
        (["source", "infer-fks", "partner", "--sample"], "--sample"),
        (["source", "analyze-denorm", "partner", "--sample"], "--sample"),
        (["cdc-start", "--source", "partner", "unused-schema.json", "unused-mapping.yaml"],
         "cdc-start"),
    ])
    def test_cli_refuses_with_the_reason(self, offline_with_snapshot, argv, needs):
        result = runner.invoke(app, argv)
        assert result.exit_code == 1, result.output
        assert "is offline" in result.output
        assert needs in result.output

    def test_dump_is_refused_by_the_connector_factory(self, offline_with_snapshot, tmp_path):
        result = runner.invoke(app, ["source", "dump", "partner", "--output-dir", str(tmp_path / "out")])
        assert result.exit_code == 2  # dump's exit code for a refused request (ValueError)
        assert "--offline has no connection" in " ".join(result.output.split())

    @pytest.fixture
    def offline_postgres(self, catalog_dir, tmp_path, monkeypatch):
        """An offline PostgreSQL source with a snapshot naming one table, on a server
        whose environment names a default database (port 1: nothing listens)."""
        for var, value in (("PGHOST", "127.0.0.1"), ("PGPORT", "1"), ("PGUSER", "x")):
            monkeypatch.setenv(var, value)
        mgr = CatalogManager(catalog_dir)
        mgr.add_source("partner-pg", "postgresql", "", source_params={OFFLINE_PARAM: True})
        path = tmp_path / "pg.json"
        Schema(tables={"t": Table(name="t", columns=[Column(name="id", data_type="integer")])}
               ).save_to_file(str(path))
        capture_source_snapshot(
            mgr, "partner-pg", schema_name="public", schema_file=read_schema_file(path)
        )
        return mgr

    def test_studio_preview_refuses_with_the_reason(self, offline_postgres, catalog_dir):
        # An empty connection string would make psycopg connect to the environment's
        # default database and preview its rows (seen in review of #107).
        from fastapi.testclient import TestClient

        from r2g.ui.server import create_app

        client = TestClient(create_app(catalog_dir=catalog_dir))
        r = client.get("/api/sources/partner-pg/preview/t")
        assert r.status_code == 400, r.text
        assert "--offline has no connection" in r.json()["detail"]

    def test_mcp_preview_refuses_with_the_reason(self, offline_postgres):
        from unittest.mock import patch

        from r2g.mcp_server import preview_table

        with patch("r2g.mcp_server._get_catalog", return_value=offline_postgres):
            result = preview_table("partner-pg", "t")
        assert "--offline has no connection" in result["error"]

    def test_a_postgres_connector_built_directly_refuses(self):
        # StreamingPipeline's pg_conn_string shim (the MCP server's stream_project)
        # builds one without the factory.
        from r2g.connectors.postgres import PostgresConnector

        with pytest.raises(ValueError, match="--offline has no connection"):
            PostgresConnector("")

    def test_building_a_snapshot_without_a_file_says_it_is_offline(self, catalog_dir):
        from r2g.snapshot import build_source_schema

        source = _add_offline(CatalogManager(catalog_dir))
        with pytest.raises(ValueError, match="is offline"):
            build_source_schema(source, schema_name="SALES")

    def test_value_sampler_refuses_an_empty_connection(self):
        from r2g.fk_inference import create_value_sampler

        with pytest.raises(ValueError, match="--offline has no connection"):
            create_value_sampler("postgresql", "")
