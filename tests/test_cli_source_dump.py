"""CLI tests for ``r2g source dump`` (Phase 6 P6.3).

The command must dispatch through :func:`create_source_connector`
based on the cataloged source's ``source_type``, call
:meth:`SourceSession.dump_table_to_csv` for every table, and produce
one CSV per table in the output directory. These tests use in-memory
fakes so they do not require a live PostgreSQL / Snowflake server.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from r2g.catalog import CatalogManager
from r2g.main import app
from r2g.types import Column, Schema, Table

runner = CliRunner()


@pytest.fixture(autouse=True)
def _reset_structlog(monkeypatch):
    """Pin structlog to stderr for every test in this module.

    ``typer.testing.CliRunner`` replaces ``sys.stdout`` with a temporary
    stream during ``invoke`` and then closes it. ``r2g.main``'s Typer
    callback calls ``setup_logging`` which configures structlog with a
    ``PrintLoggerFactory(file=sys.stdout)`` and ``cache_logger_on_first_use=True``.
    If we allow that to happen inside a test, loggers cached by
    ``r2g.streaming.pipeline`` (and friends) keep a dead file handle and
    every subsequent test that triggers pipeline logging raises
    ``ValueError: I/O operation on closed file``. We neuter the
    callback's reconfigure by forcing ``setup_logging`` to always bind
    to the real ``sys.stderr``.
    """
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
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(0),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=False,
    )


@pytest.fixture
def catalog_with_source(tmp_path, monkeypatch):
    """A catalog dir containing one PG source with a snapshot."""
    catalog_dir = tmp_path / "catalog"
    mgr = CatalogManager(str(catalog_dir))
    mgr.add_source("pg_src", "postgresql", "postgresql://localhost/test")
    schema = Schema(tables={
        "users": Table(
            name="users",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="name", data_type="text"),
            ],
            primary_key=["id"],
        ),
        "orders": Table(
            name="orders",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="user_id", data_type="integer"),
            ],
            primary_key=["id"],
        ),
    })
    mgr.create_snapshot("pg_src", schema, pg_schema="public")

    # Redirect the CLI's catalog to our temp dir.
    monkeypatch.setattr("r2g.main._get_catalog", lambda: CatalogManager(str(catalog_dir)))
    return catalog_dir


class _FakeSession:
    def __init__(self) -> None:
        self.dumped: list[tuple[str, Path]] = []
        self.closed = False

    def count_rows(self, *a, **kw) -> int:
        return 0

    def stream_rows(self, *a, **kw):
        yield from ()

    def dump_table_to_csv(self, table: str, out_path, *, header: bool = True) -> int:
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(f"id,name\n1,Row in {table}\n", encoding="utf-8")
        self.dumped.append((table, out_path))
        return 1

    def close(self) -> None:
        self.closed = True


class _FakeConnector:
    def __init__(self, *a, **kw) -> None:
        self.connection_string = "fake://x"
        self.schema_name = "public"
        self.sessions: list[_FakeSession] = []

    def get_schema(self) -> Schema:  # pragma: no cover
        return Schema()

    def open_session(self) -> _FakeSession:
        s = _FakeSession()
        self.sessions.append(s)
        return s


class TestSourceDump:
    def test_dumps_all_snapshot_tables(self, catalog_with_source, tmp_path):
        captured: list[_FakeConnector] = []

        def _factory(source_type, connection_string, schema_name="public", **kwargs):
            conn = _FakeConnector()
            captured.append(conn)
            return conn

        out_dir = tmp_path / "out"
        with patch("r2g.connectors.base.create_source_connector", _factory):
            result = runner.invoke(
                app,
                [
                    "source", "dump", "pg_src",
                    "--output-dir", str(out_dir),
                ],
            )
        assert result.exit_code == 0, result.output
        assert "Dumping 2 tables" in result.output
        assert (out_dir / "users.csv").exists()
        assert (out_dir / "orders.csv").exists()
        assert captured, "factory must have been invoked"
        assert captured[0].sessions[0].closed is True

    def test_dumps_filtered_by_tables_flag(self, catalog_with_source, tmp_path):
        def _factory(*a, **kw):
            return _FakeConnector()

        out_dir = tmp_path / "out"
        with patch("r2g.connectors.base.create_source_connector", _factory):
            result = runner.invoke(
                app,
                [
                    "source", "dump", "pg_src",
                    "--output-dir", str(out_dir),
                    "--tables", "users",
                ],
            )
        assert result.exit_code == 0, result.output
        assert (out_dir / "users.csv").exists()
        assert not (out_dir / "orders.csv").exists()

    def test_unknown_source_exits_1(self, catalog_with_source, tmp_path):
        result = runner.invoke(
            app,
            [
                "source", "dump", "nope",
                "--output-dir", str(tmp_path / "out"),
            ],
        )
        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_missing_snowflake_extra_exits_1(self, tmp_path, monkeypatch):
        """Dumping a Snowflake source without the driver installed must exit with a helpful error."""
        catalog_dir = tmp_path / "catalog"
        mgr = CatalogManager(str(catalog_dir))
        mgr.add_source("sf", "snowflake", "snowflake://u:p@acct/DB/PUBLIC")
        schema = Schema(tables={
            "USERS": Table(
                name="USERS",
                columns=[Column(name="ID", data_type="number", is_primary_key=True)],
                primary_key=["ID"],
            ),
        })
        mgr.create_snapshot("sf", schema, pg_schema="PUBLIC")

        monkeypatch.setattr("r2g.main._get_catalog", lambda: CatalogManager(str(catalog_dir)))

        def _raise(*a, **kw):
            raise ImportError(
                "Snowflake support requires snowflake-connector-python. "
                "Install with: pip install 'r2g-arango[snowflake]'"
            )

        with patch("r2g.connectors.base.create_source_connector", _raise):
            result = runner.invoke(
                app,
                [
                    "source", "dump", "sf",
                    "--output-dir", str(tmp_path / "out"),
                ],
            )
        assert result.exit_code == 1
        assert "snowflake" in result.output.lower()

    def test_stream_source_flag_dispatches_through_catalog(self, catalog_with_source, tmp_path):
        """``r2g stream --source <name>`` must resolve the source via the catalog
        and drive the pipeline through :class:`SourceConnector`."""
        from r2g.config import ConfigManager

        schema = Schema(tables={
            "users": Table(
                name="users",
                columns=[Column(name="id", data_type="integer", is_primary_key=True)],
                primary_key=["id"],
            ),
        })
        schema_path = tmp_path / "schema.json"
        schema.save_to_file(str(schema_path))
        config = ConfigManager.generate_default_config(schema)
        config_path = tmp_path / "mapping.yaml"
        ConfigManager.save_config(config, str(config_path))

        with patch(
            "r2g.connectors.base.create_source_connector",
            lambda *a, **kw: _FakeConnector(),
        ), patch("r2g.connectors.arango_writer.ArangoWriter") as mock_writer_cls:
            mock_writer = mock_writer_cls.return_value
            mock_writer.connect.return_value = None
            mock_writer.close.return_value = None
            mock_writer.endpoint = "http://x"
            mock_writer.database_name = "d"
            mock_writer.username = "u"
            mock_writer.password = "p"
            mock_writer.max_retries = 0

            result = runner.invoke(
                app,
                [
                    "stream",
                    "--source", "pg_src",
                    "--schema", str(schema_path),
                    "--config", str(config_path),
                    "--dry-run",
                ],
            )
        assert result.exit_code == 0, result.output
        assert "postgresql (pg_src)" in result.output or "pg_src" in result.output


class TestStreamSchemaFromSnapshot:
    """``r2g stream --source <name>`` without ``--schema`` reads the source's latest
    snapshot, as the Studio's load does, so a catalog project loads from the command line."""

    def _run(self, args, tmp_path):
        from r2g.config import ConfigManager

        schema = Schema(tables={"users": Table(
            name="users", columns=[Column(name="id", data_type="integer", is_primary_key=True)],
            primary_key=["id"])})
        config_path = tmp_path / "mapping.yaml"
        ConfigManager.save_config(ConfigManager.generate_default_config(schema), str(config_path))
        seen = {}

        class _Pipeline:
            def __init__(self, **kw):
                seen["schema"] = kw["schema"]
                self.errors = {}
                self.previews = {}

            def run(self, **kw):
                return {"documents": [], "edges": [], "elapsed": 0.0}

        with patch("r2g.connectors.base.create_source_connector",
                   lambda *a, **kw: _FakeConnector()), \
                patch("r2g.connectors.arango_writer.ArangoWriter"), \
                patch("r2g.streaming.pipeline.StreamingPipeline", _Pipeline):
            result = runner.invoke(app, ["stream", *args, "--config", str(config_path),
                                         "--dry-run"])
        return result, seen

    def test_the_latest_snapshot_is_the_schema(self, catalog_with_source, tmp_path):
        result, seen = self._run(["--source", "pg_src"], tmp_path)
        assert result.exit_code == 0, result.output
        assert set(seen["schema"].tables) == {"users", "orders"}

    def test_a_source_without_a_snapshot_says_how_to_take_one(self, catalog_with_source,
                                                               tmp_path):
        CatalogManager(str(tmp_path / "catalog")).add_source(
            "bare", "postgresql", "postgresql://localhost/x")
        result, seen = self._run(["--source", "bare"], tmp_path)
        assert result.exit_code == 1 and "r2g source snapshot bare" in result.output
        assert not seen

    def test_a_connection_string_still_needs_a_schema(self, tmp_path):
        result, seen = self._run(["--pg-conn", "postgresql://localhost/x"], tmp_path)
        assert result.exit_code == 2 and "--schema is needed" in result.output
