"""Behavioral CLI tests for the long-running commands (stream, cdc-start,
kafka-start, mcp) using typer.testing.CliRunner with the heavy collaborators
(ArangoWriter, listeners, consumers) mocked at their module seams.

These complement tests/test_cli.py, which covers the file-based commands.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import click
import pytest
from typer.testing import CliRunner

from r2g.main import app
from r2g.types import Column, ForeignKey, Schema, Table

runner = CliRunner()


def plain_output(result) -> str:
    """Strip Rich/Typer ANSI styling so assertions are version-stable."""
    return click.unstyle(result.output)


@pytest.fixture(autouse=True)
def _reset_structlog():
    """Ensure structlog outputs to real stderr after CliRunner restores stdout."""
    yield
    import structlog

    structlog.reset_defaults()
    structlog.configure(
        logger_factory=structlog.PrintLoggerFactory(),
    )


@pytest.fixture
def schema_file(tmp_path) -> str:
    users = Table(
        name="users",
        columns=[
            Column(name="id", data_type="integer", is_primary_key=True),
            Column(name="name", data_type="text"),
        ],
        primary_key=["id"],
        foreign_keys=[],
    )
    orders = Table(
        name="orders",
        columns=[
            Column(name="id", data_type="integer", is_primary_key=True),
            Column(name="user_id", data_type="integer"),
        ],
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(column="user_id", foreign_table="users", foreign_column="id"),
        ],
    )
    schema = Schema(tables={"users": users, "orders": orders})
    path = tmp_path / "schema.json"
    schema.save_to_file(str(path))
    return str(path)


@pytest.fixture
def config_file(tmp_path, schema_file) -> str:
    from r2g.config import ConfigManager

    schema = Schema.load_from_file(schema_file)
    config = ConfigManager.generate_default_config(schema)
    path = tmp_path / "mapping.yaml"
    ConfigManager.save_config(config, path)
    return str(path)


class TestVersion:
    def test_version_flag(self):
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "r2g" in plain_output(result)

    def test_version_is_eager(self):
        # --version short-circuits even when no command is given
        result = runner.invoke(app, ["--version", "--verbose"])
        assert result.exit_code == 0


def _fake_pipeline(results, previews=None):
    pipeline = MagicMock()
    pipeline.run.return_value = results
    pipeline.previews = previews or {}
    return pipeline


STREAM_ARGS = [
    "stream",
    "--pg-conn", "postgresql://u:p@localhost/db",
    "--endpoint", "http://localhost:8529",
    "--database", "testdb",
]


class TestStream:
    def test_requires_source_or_pg_conn(self, schema_file, config_file, tmp_path, monkeypatch):
        monkeypatch.delenv("PG_CONN", raising=False)
        # point --env-file away from the repo .env so it can't supply PG_CONN
        result = runner.invoke(
            app,
            [
                "--env-file", str(tmp_path / "absent.env"),
                "stream", "--schema", schema_file, "--config", config_file,
            ],
        )
        assert result.exit_code == 2
        assert "--source" in plain_output(result)

    def test_stream_success_summary(self, schema_file, config_file):
        results = {
            "documents": [("users", 2), ("orders", 2)],
            "edges": [("user_orders", 2)],
            "elapsed_seconds": 0.5,
            "skipped": [],
            "errors": {},
        }
        with patch("r2g.connectors.arango_writer.ArangoWriter"), \
             patch("r2g.connectors.base.create_source_connector"), \
             patch(
                 "r2g.streaming.pipeline.StreamingPipeline",
                 return_value=_fake_pipeline(results),
             ):
            result = runner.invoke(
                app, STREAM_ARGS + ["--schema", schema_file, "--config", config_file]
            )
        out = plain_output(result)
        assert result.exit_code == 0, out
        assert "Streaming Import Summary" in out
        assert "Stream complete" in out
        assert "user_orders" in out

    def test_stream_dry_run(self, schema_file, config_file):
        results = {
            "documents": [("users", 2)],
            "edges": [],
            "elapsed_seconds": 0.1,
            "skipped": [],
            "errors": {},
        }
        previews = {"users": [{"_key": "1", "name": "Alice"}]}
        with patch("r2g.connectors.arango_writer.ArangoWriter"), \
             patch("r2g.connectors.base.create_source_connector"), \
             patch(
                 "r2g.streaming.pipeline.StreamingPipeline",
                 return_value=_fake_pipeline(results, previews),
             ):
            result = runner.invoke(
                app,
                STREAM_ARGS
                + ["--schema", schema_file, "--config", config_file, "--dry-run"],
            )
        out = plain_output(result)
        assert result.exit_code == 0, out
        assert "Dry Run Preview" in out
        assert "would be" in out
        assert "Alice" in out

    def test_stream_reports_skipped_and_errors(self, schema_file, config_file):
        results = {
            "documents": [("users", 2)],
            "edges": [],
            "elapsed_seconds": 0.1,
            "skipped": ["orders"],
            "errors": {"users": ["unique constraint violated"]},
        }
        with patch("r2g.connectors.arango_writer.ArangoWriter"), \
             patch("r2g.connectors.base.create_source_connector"), \
             patch(
                 "r2g.streaming.pipeline.StreamingPipeline",
                 return_value=_fake_pipeline(results),
             ):
            result = runner.invoke(
                app, STREAM_ARGS + ["--schema", schema_file, "--config", config_file]
            )
        out = plain_output(result)
        assert result.exit_code == 0, out
        assert "Skipped 1 existing collection(s)" in out
        assert "Import Errors" in out
        assert "unique constraint violated" in out

    def test_stream_pipeline_failure_exits_1(self, schema_file, config_file):
        pipeline = MagicMock()
        pipeline.run.side_effect = RuntimeError("source connection refused")
        # patch r2g.main.log: the structlog proxy may hold a stream captured
        # (and closed) by an earlier CliRunner invocation
        with patch("r2g.main.log"), \
             patch("r2g.connectors.arango_writer.ArangoWriter"), \
             patch("r2g.connectors.base.create_source_connector"), \
             patch("r2g.streaming.pipeline.StreamingPipeline", return_value=pipeline):
            result = runner.invoke(
                app, STREAM_ARGS + ["--schema", schema_file, "--config", config_file]
            )
        out = plain_output(result)
        assert result.exit_code == 1
        assert "Streaming pipeline failed" in out


CDC_ARGS = [
    "cdc-start",
    "--pg-conn", "postgresql://u:p@localhost/db",
]


class TestCdcStart:
    def test_invalid_conflict_policy_exits_1(self, schema_file, config_file):
        result = runner.invoke(
            app,
            CDC_ARGS + [schema_file, config_file, "--conflict-policy", "bogus"],
        )
        assert result.exit_code == 1
        assert "Invalid conflict policy" in plain_output(result)

    def test_cdc_session_lifecycle(self, schema_file, config_file):
        listener = MagicMock()
        listener.run.return_value = None  # stops immediately
        with patch("r2g.connectors.arango_writer.ArangoWriter"), \
             patch(
                 "r2g.cdc.pg_listener.PGReplicationListener",
                 return_value=listener,
             ) as listener_cls:
            result = runner.invoke(app, CDC_ARGS + [schema_file, config_file])
        out = plain_output(result)
        assert result.exit_code == 0, out
        assert "CDC listener starting" in out
        assert "CDC listener stopped" in out
        assert "CDC Session Statistics" in out
        listener.setup.assert_called_once()  # --create-slot default
        listener.run.assert_called_once()
        assert listener_cls.call_args.kwargs["slot_name"] == "r2g_slot"

    def test_no_create_slot_skips_setup(self, schema_file, config_file):
        listener = MagicMock()
        with patch("r2g.connectors.arango_writer.ArangoWriter"), \
             patch("r2g.cdc.pg_listener.PGReplicationListener", return_value=listener):
            result = runner.invoke(
                app, CDC_ARGS + [schema_file, config_file, "--no-create-slot"]
            )
        assert result.exit_code == 0, plain_output(result)
        listener.setup.assert_not_called()

    def test_temporal_mode_announced(self, schema_file, config_file):
        listener = MagicMock()
        with patch("r2g.connectors.arango_writer.ArangoWriter"), \
             patch("r2g.cdc.pg_listener.PGReplicationListener", return_value=listener):
            result = runner.invoke(
                app,
                CDC_ARGS + [schema_file, config_file, "--temporal", "--ttl-seconds", "60"],
            )
        out = plain_output(result)
        assert result.exit_code == 0, out
        assert "Temporal mode enabled" in out
        assert "ttl=60s" in out

    def test_writer_failure_exits_1(self, schema_file, config_file):
        with patch("r2g.main.log"), \
             patch("r2g.connectors.arango_writer.ArangoWriter") as writer_cls:
            writer_cls.return_value.ensure_database.side_effect = RuntimeError("conn refused")
            result = runner.invoke(app, CDC_ARGS + [schema_file, config_file])
        assert result.exit_code == 1
        assert "CDC start failed" in plain_output(result)


class TestCdcStartFromCatalogSource:
    """``cdc-start --source``: PostgreSQL via its slot, Snowflake via streams (P6.16)."""

    @pytest.fixture
    def catalog(self, tmp_path, monkeypatch):
        from r2g.catalog import CatalogManager

        mgr = CatalogManager(str(tmp_path / "catalog"))
        mgr.add_source("sf", "snowflake", "snowflake://svc:pw@acct/CRM/CORE?warehouse=WH")
        mgr.add_source("pg", "postgresql", "postgresql://u:p@pg/db")
        mgr.add_source("ms", "mssql", "mssql://u:p@ms/db")
        monkeypatch.setattr("r2g.main._get_catalog", lambda: mgr)
        return mgr

    def test_both_or_neither_connection_is_refused(self, schema_file, config_file, catalog):
        both = runner.invoke(app, CDC_ARGS + ["--source", "sf", schema_file, config_file])
        neither = runner.invoke(app, ["cdc-start", schema_file, config_file], env={"PG_CONN": ""})
        for result in (both, neither):
            assert result.exit_code == 1
            assert "exactly one of --pg-conn and --source" in plain_output(result)

    def test_unsupported_source_type_is_refused(self, schema_file, config_file, catalog):
        result = runner.invoke(app, ["cdc-start", "--source", "ms", schema_file, config_file])
        assert result.exit_code == 1
        assert "source 'ms' is sqlserver" in plain_output(result)

    def test_snowflake_refuses_last_write_wins(self, schema_file, config_file, catalog):
        result = runner.invoke(app, [
            "cdc-start", "--source", "sf", schema_file, config_file,
            "--conflict-policy", "last_write_wins",
        ])
        assert result.exit_code == 1
        assert "Snowflake stream gives none" in plain_output(result)

    def test_snowflake_runs_the_stream_listener(self, schema_file, config_file, catalog):
        with patch("r2g.connectors.arango_writer.ArangoWriter"), \
             patch("r2g.cdc.handler.CDCHandler") as handler_cls, \
             patch("r2g.main._run_snowflake_cdc") as run_sf, \
             patch("r2g.cdc.pg_listener.PGReplicationListener") as pg_cls:
            handler_cls.return_value.stats.as_dict.return_value = {}
            handler_cls.return_value.resolver.log.summary.return_value = {"total_conflicts": 0}
            result = runner.invoke(app, [
                "cdc-start", "--source", "sf", schema_file, config_file,
                "--once", "--stream-schema", "R2G_CDC",
            ])
        assert result.exit_code == 0, plain_output(result)
        pg_cls.assert_not_called()
        assert handler_cls.call_args.kwargs["skip_noop_updates"] is True
        args, kwargs = run_sf.call_args
        assert args[0] == "snowflake://svc:pw@acct/CRM/CORE?warehouse=WH"
        assert kwargs["once"] is True
        assert kwargs["stream_schema"] == "R2G_CDC"
        assert kwargs["poll_interval"] == 60.0
        assert kwargs["recover_streams"] is True

    def test_snowflake_recovery_can_be_turned_off(self, schema_file, config_file, catalog):
        with patch("r2g.connectors.arango_writer.ArangoWriter"), \
             patch("r2g.cdc.handler.CDCHandler") as handler_cls, \
             patch("r2g.main._run_snowflake_cdc") as run_sf:
            handler_cls.return_value.stats.as_dict.return_value = {}
            handler_cls.return_value.resolver.log.summary.return_value = {"total_conflicts": 0}
            result = runner.invoke(app, [
                "cdc-start", "--source", "sf", schema_file, config_file,
                "--once", "--no-recover-streams",
            ])
        assert result.exit_code == 0, plain_output(result)
        assert run_sf.call_args.kwargs["recover_streams"] is False

    def test_source_wins_over_pg_conn_from_the_environment(self, schema_file, config_file, catalog):
        with patch("r2g.connectors.arango_writer.ArangoWriter"), \
             patch("r2g.cdc.pg_listener.PGReplicationListener") as pg_cls:
            result = runner.invoke(
                app, ["cdc-start", "--source", "pg", schema_file, config_file],
                env={"PG_CONN": "postgresql://from-env/db"},
            )
        assert result.exit_code == 0, plain_output(result)
        assert pg_cls.call_args.kwargs["pg_conn_string"] == "postgresql://u:p@pg/db"

    def test_postgres_source_uses_its_connection_string(self, schema_file, config_file, catalog):
        with patch("r2g.connectors.arango_writer.ArangoWriter"), \
             patch("r2g.cdc.pg_listener.PGReplicationListener") as pg_cls:
            result = runner.invoke(app, ["cdc-start", "--source", "pg", schema_file, config_file])
        assert result.exit_code == 0, plain_output(result)
        kwargs = pg_cls.call_args.kwargs
        assert kwargs["pg_conn_string"] == "postgresql://u:p@pg/db"
        assert kwargs["poll_interval"] == 1.0


class TestKafkaStart:
    def test_invalid_conflict_policy_exits_1(self, schema_file, config_file):
        with patch("r2g.cdc.kafka_consumer.KafkaConsumer"):
            result = runner.invoke(
                app,
                [
                    "kafka-start", schema_file, config_file,
                    "--topics", "cdc.users",
                    "--conflict-policy", "bogus",
                ],
            )
        assert result.exit_code == 1
        assert "Invalid conflict policy" in plain_output(result)

    def test_empty_topics_exits_1(self, schema_file, config_file):
        with patch("r2g.connectors.arango_writer.ArangoWriter"), \
             patch("r2g.cdc.kafka_consumer.KafkaConsumer"):
            result = runner.invoke(
                app,
                ["kafka-start", schema_file, config_file, "--topics", " , "],
            )
        assert result.exit_code == 1
        assert "No topics specified" in plain_output(result)

    def test_kafka_session_lifecycle(self, schema_file, config_file):
        consumer = MagicMock()
        consumer.run.return_value = None
        with patch("r2g.connectors.arango_writer.ArangoWriter"), \
             patch(
                 "r2g.cdc.kafka_consumer.KafkaConsumer",
                 return_value=consumer,
             ) as consumer_cls:
            result = runner.invoke(
                app,
                [
                    "kafka-start", schema_file, config_file,
                    "--topics", "cdc.users,cdc.orders",
                    "--brokers", "kafka:9092",
                ],
            )
        out = plain_output(result)
        assert result.exit_code == 0, out
        assert "Kafka consumer starting" in out
        assert "Kafka CDC Session Statistics" in out
        consumer.run.assert_called_once()
        assert consumer_cls.call_args.kwargs["topics"] == ["cdc.users", "cdc.orders"]
        assert consumer_cls.call_args.kwargs["brokers"] == "kafka:9092"


class TestMcpCommand:
    def test_stdio_transport(self):
        fake_mcp = MagicMock()
        with patch("r2g.mcp_server.mcp", fake_mcp):
            result = runner.invoke(app, ["mcp"])
        assert result.exit_code == 0, plain_output(result)
        fake_mcp.run.assert_called_once_with(transport="stdio")

    def test_sse_loopback_no_auth(self, monkeypatch):
        monkeypatch.delenv("R2G_API_TOKEN", raising=False)
        fake_mcp = MagicMock()
        fake_uvicorn = MagicMock()
        with patch("r2g.mcp_server.mcp", fake_mcp), \
             patch.dict("sys.modules", {"uvicorn": fake_uvicorn}):
            result = runner.invoke(
                app, ["mcp", "--transport", "sse", "--host", "127.0.0.1", "--port", "9999"]
            )
        out = plain_output(result)
        assert result.exit_code == 0, out
        # loopback bind without a configured token -> no auth, served via uvicorn
        assert fake_mcp.settings.host == "127.0.0.1"
        assert fake_mcp.settings.port == 9999
        fake_mcp.sse_app.assert_called_once()
        fake_uvicorn.run.assert_called_once()
        assert "Bearer auth enabled" not in out

    def test_sse_nonloopback_generates_token(self, monkeypatch):
        monkeypatch.delenv("R2G_API_TOKEN", raising=False)
        fake_mcp = MagicMock()
        fake_uvicorn = MagicMock()
        with patch("r2g.mcp_server.mcp", fake_mcp), \
             patch.dict("sys.modules", {"uvicorn": fake_uvicorn}):
            result = runner.invoke(
                app, ["mcp", "--transport", "sse", "--host", "0.0.0.0"]
            )
        out = plain_output(result)
        assert result.exit_code == 0, out
        assert "Bearer auth enabled" in out
        assert "non-loopback bind" in out
        fake_uvicorn.run.assert_called_once()

    def test_sse_uses_configured_token(self, monkeypatch):
        monkeypatch.setenv("R2G_API_TOKEN", "supersecret")
        fake_mcp = MagicMock()
        fake_uvicorn = MagicMock()
        with patch("r2g.mcp_server.mcp", fake_mcp), \
             patch.dict("sys.modules", {"uvicorn": fake_uvicorn}):
            result = runner.invoke(
                app, ["mcp", "--transport", "sse", "--host", "127.0.0.1"]
            )
        out = plain_output(result)
        assert result.exit_code == 0, out
        assert "using R2G_API_TOKEN" in out
        # the raw token is not echoed when supplied via env
        assert "supersecret" not in out


class TestBearerGuard:
    def _run(self, guard, headers):
        """Drive the ASGI guard once and capture the response status."""
        import asyncio

        scope = {"type": "http", "headers": headers}
        sent = []

        async def receive():
            return {"type": "http.request"}

        async def send(message):
            sent.append(message)

        asyncio.run(guard(scope, receive, send))
        return sent

    def test_rejects_missing_token(self):
        from r2g.main import _bearer_guard

        inner = MagicMock()
        guard = _bearer_guard(inner, "tok")
        sent = self._run(guard, headers=[])
        start = next(m for m in sent if m["type"] == "http.response.start")
        assert start["status"] == 401
        inner.assert_not_called()

    def test_accepts_valid_token(self):
        from r2g.main import _bearer_guard

        called = {}

        async def inner(scope, receive, send):
            called["ok"] = True

        guard = _bearer_guard(inner, "tok")
        self._run(guard, headers=[(b"authorization", b"Bearer tok")])
        assert called.get("ok") is True

    def test_rejects_wrong_token(self):
        from r2g.main import _bearer_guard

        inner = MagicMock()
        guard = _bearer_guard(inner, "tok")
        sent = self._run(guard, headers=[(b"authorization", b"Bearer nope")])
        start = next(m for m in sent if m["type"] == "http.response.start")
        assert start["status"] == 401
        inner.assert_not_called()
