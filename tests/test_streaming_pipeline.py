"""StreamingPipeline unit tests.

These tests exercise the pipeline against a ``FakeSourceConnector``
rather than a live PostgreSQL server. The fake satisfies the
:class:`r2g.connectors.base.SourceConnector` / ``SourceSession``
protocols and is the same shape the test suite at large uses for
connector-facing code (see ``tests/test_snowflake_connector.py``).

Why a fake rather than a psycopg mock
-------------------------------------

Pre-Phase-6 the pipeline called ``psycopg.connect`` directly, so the
tests had to ``patch("r2g.streaming.pipeline.psycopg")``. Phase 6
slice 3 moved the read path behind :class:`SourceConnector`, which
means (a) psycopg is no longer reachable from the pipeline module's
namespace and (b) the pipeline is no longer PG-specific — Snowflake
drives it too. The fake below exercises the exact integration surface
the pipeline now uses.
"""

from __future__ import annotations

from typing import Any, Iterator, Optional
from unittest.mock import MagicMock

import pytest

from r2g.connectors.arango_writer import ArangoWriter, ImportBatchError
from r2g.streaming.pipeline import StreamingPipeline
from r2g.types import (
    CollectionMapping,
    Column,
    EdgeDefinition,
    ForeignKey,
    MappingConfig,
    Schema,
    Table,
)

# ── Fake connector / session ────────────────────────────────────────


class FakeSession:
    """Minimal :class:`SourceSession` impl backed by in-memory table data."""

    def __init__(self, tables: dict[str, list[dict[str, Any]]]) -> None:
        self._tables = tables
        self.closed = False
        self.since_calls: list[tuple[str, Optional[str], Optional[str]]] = []

    def count_rows(
        self,
        table: str,
        *,
        since_column: Optional[str] = None,
        since_value: Optional[str] = None,
    ) -> int:
        self.since_calls.append((table, since_column, since_value))
        rows = self._tables.get(table, [])
        if since_column and since_value is not None:
            return sum(
                1 for r in rows if r.get(since_column) and r[since_column] >= since_value
            )
        return len(rows)

    def stream_rows(
        self,
        table: str,
        *,
        batch_size: int = 10_000,
        since_column: Optional[str] = None,
        since_value: Optional[str] = None,
    ) -> Iterator[dict[str, Any]]:
        rows = self._tables.get(table, [])
        if since_column and since_value is not None:
            rows = [r for r in rows if r.get(since_column) and r[since_column] >= since_value]
        yield from rows

    def dump_table_to_csv(self, table: str, out_path: Any, *, header: bool = True) -> int:
        rows = self._tables.get(table, [])
        return len(rows)

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> "FakeSession":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class FakeConnector:
    """Minimal :class:`SourceConnector` used by the streaming tests."""

    def __init__(self, tables: dict[str, list[dict[str, Any]]]) -> None:
        self.connection_string = "fake://local"
        self.schema_name = "public"
        self._tables = tables
        self.sessions_opened: list[FakeSession] = []

    def get_schema(self) -> Schema:  # pragma: no cover - not exercised here
        return Schema()

    def open_session(self) -> FakeSession:
        s = FakeSession(self._tables)
        self.sessions_opened.append(s)
        return s


# ── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def simple_schema() -> Schema:
    users = Table(
        name="users",
        columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="name", data_type="text", is_nullable=False),
            Column(name="email", data_type="text", is_nullable=True),
        ],
        primary_key=["id"],
        foreign_keys=[],
    )
    orders = Table(
        name="orders",
        columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="user_id", data_type="integer", is_nullable=False),
            Column(name="total", data_type="numeric", is_nullable=False),
        ],
        primary_key=["id"],
        foreign_keys=[
            ForeignKey(column="user_id", foreign_table="users", foreign_column="id", constraint_name="fk_user"),
        ],
    )
    return Schema(tables={"users": users, "orders": orders})


@pytest.fixture
def simple_config() -> MappingConfig:
    return MappingConfig(
        collections={
            "users": CollectionMapping(source_table="users", target_collection="users"),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        },
        edges=[
            EdgeDefinition(
                edge_collection="orders_to_users",
                from_collection="orders",
                to_collection="users",
                from_field="user_id",
                to_field="id",
            ),
        ],
    )


@pytest.fixture
def mock_writer():
    writer = MagicMock(spec=ArangoWriter)
    writer.import_batch.return_value = {"created": 0, "errors": 0, "empty": 0, "updated": 0, "ignored": 0}
    return writer


def _pipeline(*, source_connector, arango_writer, schema, config, **kw) -> StreamingPipeline:
    """Construct a StreamingPipeline with sensible defaults for tests."""
    return StreamingPipeline(
        source_connector=source_connector,
        arango_writer=arango_writer,
        schema=schema,
        config=config,
        **kw,
    )


# ── Tests ───────────────────────────────────────────────────────────


class TestStreamingPipelineInit:
    def test_stores_params(self, simple_schema, simple_config, mock_writer):
        pipeline = _pipeline(
            source_connector=FakeConnector({}),
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            batch_size=5000,
        )
        assert pipeline.batch_size == 5000
        assert pipeline.on_duplicate == "replace"

    def test_default_batch_size(self, simple_schema, simple_config, mock_writer):
        pipeline = _pipeline(
            source_connector=FakeConnector({}),
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
        )
        assert pipeline.batch_size == 10_000

    def test_pg_conn_string_backward_compat_builds_connector(
        self, simple_schema, simple_config, mock_writer
    ):
        pipeline = StreamingPipeline(
            pg_conn_string="postgresql://test",
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
        )
        from r2g.connectors.postgres import PostgresConnector

        assert isinstance(pipeline.source_connector, PostgresConnector)

    def test_rejects_missing_source(self, simple_schema, simple_config, mock_writer):
        with pytest.raises(ValueError, match="source_connector or pg_conn_string"):
            StreamingPipeline(
                arango_writer=mock_writer,
                schema=simple_schema,
                config=simple_config,
            )


class TestStreamDocuments:
    def test_creates_collections_and_imports(self, simple_schema, simple_config, mock_writer):
        connector = FakeConnector({
            "users": [
                {"id": 1, "name": "Alice", "email": "a@b.com"},
                {"id": 2, "name": "Bob", "email": None},
            ],
            "orders": [
                {"id": 10, "user_id": 1, "total": 99.99},
            ],
        })
        pipeline = _pipeline(
            source_connector=connector,
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            batch_size=100,
        )
        with connector.open_session() as sess:
            results = pipeline._stream_documents(sess)

        assert len(results) == 2
        assert mock_writer.ensure_collection.call_count == 2
        assert mock_writer.import_batch.call_count == 2


class TestStreamEdges:
    def test_creates_edge_collections_and_imports(self, simple_schema, simple_config, mock_writer):
        connector = FakeConnector({
            "orders": [
                {"id": 1, "user_id": 10, "total": 99.99},
                {"id": 2, "user_id": 20, "total": 50.0},
            ],
            "users": [{"id": 10, "name": "x", "email": None}, {"id": 20, "name": "y", "email": None}],
        })
        pipeline = _pipeline(
            source_connector=connector,
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            batch_size=100,
        )
        with connector.open_session() as sess:
            results = pipeline._stream_edges(sess)

        assert len(results) == 1
        assert results[0][0] == "orders_to_users"
        mock_writer.ensure_collection.assert_called_with("orders_to_users", edge=True)


class TestRunPipeline:
    def test_full_run_calls_connect_and_close(self, simple_schema, simple_config, mock_writer):
        connector = FakeConnector({"users": [], "orders": []})
        pipeline = _pipeline(
            source_connector=connector,
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
        )
        results = pipeline.run()
        mock_writer.connect.assert_called_once()
        mock_writer.close.assert_called_once()
        assert "documents" in results
        assert "edges" in results
        # The single-worker path must open exactly one session and close it.
        assert len(connector.sessions_opened) == 1
        assert connector.sessions_opened[0].closed is True

    def test_aql_delegation_merges_server_results(self, simple_schema, mock_writer):
        # A field expression outside the local subset is delegated to ArangoDB;
        # the pipeline must run the AQL query and merge results into the docs.
        from r2g.types import FieldExpression

        config = MappingConfig(
            collections={
                "users": CollectionMapping(
                    source_table="users",
                    target_collection="users",
                    field_expressions=[
                        FieldExpression(
                            target="slug",
                            sources=["name"],
                            expression="REGEX_REPLACE(@name, '\\\\s+', '-')",
                        )
                    ],
                ),
            },
            edges=[],
        )
        connector = FakeConnector({
            "users": [
                {"id": 1, "name": "Ada Lovelace", "email": "a@b.com"},
                {"id": 2, "name": "Alan Turing", "email": "t@b.com"},
            ],
        })
        # Server returns one computed object per row, in order (nothing for the
        # up-front check, which runs the expression over no rows).
        mock_writer.execute_aql.side_effect = lambda q, b: [] if b == {"rows": []} else [
            {"slug": "Ada-Lovelace"},
            {"slug": "Alan-Turing"},
        ]
        pipeline = _pipeline(
            source_connector=connector,
            arango_writer=mock_writer,
            schema=Schema(tables={"users": simple_schema.tables["users"]}),
            config=config,
        )
        pipeline.run()

        check, batch = mock_writer.execute_aql.call_args_list
        assert check[0][1] == {"rows": []}
        # The bound rows project only the referenced column.
        _query, bind = mock_writer.execute_aql.call_args[0]
        assert bind == {"rows": [{"name": "Ada Lovelace"}, {"name": "Alan Turing"}]}
        # Imported docs carry the server-computed field.
        imported = mock_writer.import_batch.call_args[0][1]
        slugs = {d["_key"]: d.get("slug") for d in imported}
        assert slugs == {"1": "Ada-Lovelace", "2": "Alan-Turing"}

    @staticmethod
    def _delegating(simple_schema):
        from r2g.types import FieldExpression

        config = MappingConfig(
            collections={
                "users": CollectionMapping(
                    source_table="users",
                    target_collection="users",
                    field_expressions=[
                        FieldExpression(
                            target="slug", sources=["name"],
                            expression="REGEX_REPLACE(@name, 'x', 'y')",
                        )
                    ],
                ),
            },
            edges=[],
        )
        rows = [{"id": 1, "name": "Ada", "email": "a@b"}, {"id": 2, "name": "Alan", "email": "t@b"}]
        return config, FakeConnector({"users": rows}), Schema(tables={"users": simple_schema.tables["users"]})

    @pytest.mark.parametrize("answer", [RuntimeError("AQL boom"), [{"slug": "Ady"}]],
                             ids=["server error", "one object for two rows"])
    def test_a_failed_delegation_rejects_the_batch(self, simple_schema, mock_writer, answer):
        """A batch whose server step fails is not loaded without its fields: each of its
        documents goes to the dead-letter queue with the reason (P5c.1.5)."""
        config, connector, schema = self._delegating(simple_schema)

        def server(query, bind):
            if bind == {"rows": []}:
                return []
            if isinstance(answer, Exception):
                raise answer
            return answer

        mock_writer.execute_aql.side_effect = server
        dlq = MagicMock()
        pipeline = _pipeline(source_connector=connector, arango_writer=mock_writer,
                             schema=schema, config=config, dlq=dlq)
        result = pipeline.run()
        mock_writer.import_batch.assert_not_called()
        rejected = [c.args for c in dlq.record_failure.call_args_list]
        assert [(coll, doc["_key"]) for coll, doc, _ in rejected] == [("users", "1"), ("users", "2")]
        assert all("batch not loaded" in why for _, _, why in rejected)
        assert any("batch not loaded" in e for e in result["errors"]["users"])

    def test_an_expression_the_server_cannot_evaluate_stops_the_load_first(
            self, simple_schema, mock_writer):
        """Checked once over no rows before anything is written, naming collection and field."""
        from r2g.streaming.pipeline import DelegationError

        config, connector, schema = self._delegating(simple_schema)
        mock_writer.execute_aql.side_effect = RuntimeError("[ERR 1540] usage of unknown function")
        pipeline = _pipeline(source_connector=connector, arango_writer=mock_writer,
                             schema=schema, config=config)
        with pytest.raises(DelegationError, match="Collection 'users', field 'slug'.*1540"):
            pipeline.run()
        mock_writer.ensure_collection.assert_not_called()
        mock_writer.import_batch.assert_not_called()

    def test_a_dry_run_asks_the_server_nothing(self, simple_schema, mock_writer):
        config, connector, schema = self._delegating(simple_schema)
        pipeline = _pipeline(source_connector=connector, arango_writer=mock_writer,
                             schema=schema, config=config, dry_run=True)
        pipeline.run()
        mock_writer.execute_aql.assert_not_called()

    def test_run_with_graph_creates_graph(self, simple_schema, simple_config, mock_writer):
        connector = FakeConnector({"users": [], "orders": []})
        pipeline = _pipeline(
            source_connector=connector,
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
        )
        pipeline.run(graph_name="test_graph")
        mock_writer.create_named_graph.assert_called_once()
        args = mock_writer.create_named_graph.call_args
        assert args[0][0] == "test_graph"

    def test_pg_session_backed_run_sets_repeatable_read(
        self, simple_schema, simple_config, mock_writer, monkeypatch
    ):
        """The PG session must SET TRANSACTION ISOLATION LEVEL REPEATABLE READ.

        We patch ``psycopg.connect`` so the assertion observes the SQL
        the session issues, without needing a live PG server.
        """
        import psycopg

        observed: list[str] = []

        fake_conn = MagicMock()

        def fake_execute(sql, *args, **kwargs):
            observed.append(str(sql))

        fake_conn.execute.side_effect = fake_execute
        fake_cursor = MagicMock()
        fake_cursor.__enter__.return_value = fake_cursor
        fake_cursor.__exit__.return_value = False
        fake_cursor.__iter__ = MagicMock(return_value=iter([]))
        fake_cursor.fetchone.return_value = (0,)
        fake_conn.cursor.return_value = fake_cursor

        monkeypatch.setattr(psycopg, "connect", lambda *a, **kw: fake_conn)

        pipeline = StreamingPipeline(
            pg_conn_string="postgresql://test",
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
        )
        pipeline.run()

        assert any("REPEATABLE READ" in sql for sql in observed), observed


class TestDryRun:
    def test_dry_run_skips_arango_writes(self, simple_schema, simple_config, mock_writer):
        connector = FakeConnector({
            "users": [{"id": 1, "name": "Alice", "email": "a@b.com"}],
            "orders": [{"id": 10, "user_id": 1, "total": 99.99}],
        })
        pipeline = _pipeline(
            source_connector=connector,
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            dry_run=True,
        )
        results = pipeline.run(graph_name="test_graph")

        mock_writer.connect.assert_called_once()
        mock_writer.close.assert_called_once()
        mock_writer.ensure_collection.assert_not_called()
        mock_writer.import_batch.assert_not_called()
        mock_writer.create_named_graph.assert_not_called()

        assert sum(c for _, c in results["documents"]) >= 1
        assert sum(c for _, c in results["edges"]) >= 1

    def test_dry_run_captures_previews(self, simple_schema, simple_config, mock_writer):
        connector = FakeConnector({
            "users": [
                {"id": 1, "name": "Alice", "email": "a@b.com"},
                {"id": 2, "name": "Bob", "email": None},
            ],
            "orders": [{"id": 10, "user_id": 1, "total": 99.99}],
        })
        pipeline = _pipeline(
            source_connector=connector,
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            dry_run=True,
        )
        pipeline.run()

        assert "users" in pipeline.previews
        assert len(pipeline.previews["users"]) == 2
        assert pipeline.previews["users"][0]["_key"] == "1"
        assert "orders_to_users" in pipeline.previews
        assert len(pipeline.previews["orders_to_users"]) == 1


class TestTableFiltering:
    def test_include_tables_filters_documents(self, simple_schema, simple_config, mock_writer):
        connector = FakeConnector({
            "users": [{"id": 1, "name": "Alice", "email": "a@b.com"}],
            "orders": [],
        })
        pipeline = _pipeline(
            source_connector=connector,
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            include_tables={"users"},
        )
        results = pipeline.run()

        doc_names = [name for name, _ in results["documents"]]
        assert "users" in doc_names
        assert "orders" not in doc_names
        assert len(results["edges"]) == 0

    def test_exclude_tables_filters_documents(self, simple_schema, simple_config, mock_writer):
        connector = FakeConnector({
            "users": [{"id": 1, "name": "Alice", "email": "a@b.com"}],
            "orders": [],
        })
        pipeline = _pipeline(
            source_connector=connector,
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            exclude_tables={"orders"},
        )
        results = pipeline.run()

        doc_names = [name for name, _ in results["documents"]]
        assert "users" in doc_names
        assert "orders" not in doc_names
        assert len(results["edges"]) == 0


class TestImportErrorSurfacing:
    def test_batch_errors_captured_in_results(self, simple_schema, simple_config):
        mock_writer = MagicMock(spec=ArangoWriter)
        mock_writer.import_batch.side_effect = ImportBatchError(
            collection="users",
            error_count=2,
            total_count=5,
            details=["doc 1: unique constraint", "doc 3: invalid key"],
        )
        connector = FakeConnector({
            "users": [{"id": 1, "name": "Alice", "email": "a@b.com"}],
            "orders": [{"id": 10, "user_id": 1, "total": 99.99}],
        })
        pipeline = _pipeline(
            source_connector=connector,
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
        )
        results = pipeline.run()
        assert "errors" in results
        assert "users" in results["errors"]
        assert len(results["errors"]["users"]) == 2

    def test_no_errors_key_when_clean(self, simple_schema, simple_config, mock_writer):
        pipeline = _pipeline(
            source_connector=FakeConnector({"users": [], "orders": []}),
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
        )
        results = pipeline.run()
        assert "errors" not in results


class TestSkipExisting:
    def _writer_with_populated_collection(self, count: int) -> MagicMock:
        mock_writer = MagicMock(spec=ArangoWriter)
        mock_writer.import_batch.return_value = {
            "created": 0, "errors": 0, "empty": 0, "updated": 0, "ignored": 0,
        }
        mock_db = MagicMock()
        mock_db.has_collection.return_value = True
        mock_coll = MagicMock()
        mock_coll.count.return_value = count
        mock_db.collection.return_value = mock_coll
        mock_writer.db = mock_db
        return mock_writer

    def test_skips_populated_collections(self, simple_schema, simple_config):
        mock_writer = self._writer_with_populated_collection(100)
        pipeline = _pipeline(
            source_connector=FakeConnector({"users": [], "orders": []}),
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            skip_existing=True,
        )
        results = pipeline.run()
        assert "skipped" in results
        assert len(results["skipped"]) > 0
        mock_writer.import_batch.assert_not_called()

    def test_no_skip_when_empty_collection(self, simple_schema, simple_config):
        mock_writer = self._writer_with_populated_collection(0)
        pipeline = _pipeline(
            source_connector=FakeConnector({"users": [], "orders": []}),
            arango_writer=mock_writer,
            schema=simple_schema,
            config=simple_config,
            skip_existing=True,
        )
        results = pipeline.run()
        assert "skipped" not in results
        mock_writer.ensure_collection.assert_called()


class TestSinceFiltering:
    def test_resolve_since_column_explicit(self, simple_schema, simple_config):
        pipeline = _pipeline(
            source_connector=FakeConnector({}),
            arango_writer=MagicMock(spec=ArangoWriter),
            schema=simple_schema,
            config=simple_config,
            since="2026-01-01",
            since_column="name",
        )
        assert pipeline._resolve_since_column("users") == "name"

    def test_resolve_since_column_not_found(self, simple_schema, simple_config):
        pipeline = _pipeline(
            source_connector=FakeConnector({}),
            arango_writer=MagicMock(spec=ArangoWriter),
            schema=simple_schema,
            config=simple_config,
            since="2026-01-01",
            since_column="nonexistent",
        )
        assert pipeline._resolve_since_column("users") is None

    def test_resolve_since_column_autodetect(self, simple_config):
        schema_with_ts = Schema(tables={
            "events": Table(
                name="events",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="updated_at", data_type="timestamp"),
                    Column(name="payload", data_type="text"),
                ],
                primary_key=["id"],
            ),
        })
        pipeline = _pipeline(
            source_connector=FakeConnector({}),
            arango_writer=MagicMock(spec=ArangoWriter),
            schema=schema_with_ts,
            config=simple_config,
            since="2026-01-01",
        )
        assert pipeline._resolve_since_column("events") == "updated_at"

    def test_resolve_since_column_none_when_no_since(self, simple_schema, simple_config):
        pipeline = _pipeline(
            source_connector=FakeConnector({}),
            arango_writer=MagicMock(spec=ArangoWriter),
            schema=simple_schema,
            config=simple_config,
        )
        assert pipeline._resolve_since_column("users") is None

    def test_resolve_since_column_autodetect_created_at(self, simple_config):
        schema_with_ts = Schema(tables={
            "logs": Table(
                name="logs",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="created_at", data_type="timestamp"),
                    Column(name="msg", data_type="text"),
                ],
                primary_key=["id"],
            ),
        })
        pipeline = _pipeline(
            source_connector=FakeConnector({}),
            arango_writer=MagicMock(spec=ArangoWriter),
            schema=schema_with_ts,
            config=simple_config,
            since="2026-01-01",
        )
        assert pipeline._resolve_since_column("logs") == "created_at"

    def test_resolve_since_column_no_match(self, simple_schema, simple_config):
        """When --since is provided but the table has no timestamp column, returns None."""
        pipeline = _pipeline(
            source_connector=FakeConnector({}),
            arango_writer=MagicMock(spec=ArangoWriter),
            schema=simple_schema,
            config=simple_config,
            since="2026-01-01",
        )
        assert pipeline._resolve_since_column("users") is None

    def test_since_propagates_to_session(self, simple_config):
        """Pipeline should pass since_column/since_value through to the session."""
        schema = Schema(tables={
            "logs": Table(
                name="logs",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="created_at", data_type="timestamp"),
                ],
                primary_key=["id"],
            ),
        })
        config = MappingConfig(
            collections={
                "logs": CollectionMapping(source_table="logs", target_collection="logs"),
            },
        )
        connector = FakeConnector({"logs": [
            {"id": 1, "created_at": "2026-02-01"},
            {"id": 2, "created_at": "2025-12-01"},
        ]})
        pipeline = _pipeline(
            source_connector=connector,
            arango_writer=MagicMock(spec=ArangoWriter),
            schema=schema,
            config=config,
            since="2026-01-01",
            dry_run=True,
        )
        pipeline.run()
        # The session's count_rows must have been called with since plumbing.
        assert connector.sessions_opened, "session should have been opened"
        calls = connector.sessions_opened[0].since_calls
        assert any(c[1] == "created_at" and c[2] == "2026-01-01" for c in calls), calls

    def test_pkless_table_warning(self, simple_config):
        """Tables with no PK should log a warning but not crash."""
        pkless_schema = Schema(tables={
            "logs": Table(
                name="logs",
                columns=[
                    Column(name="message", data_type="text"),
                    Column(name="created_at", data_type="timestamp"),
                ],
                primary_key=[],
            ),
        })
        pkless_config = MappingConfig(
            collections={
                "logs": CollectionMapping(source_table="logs", target_collection="logs"),
            },
        )
        connector = FakeConnector({
            "logs": [{"message": "hello", "created_at": "2026-01-01"}],
        })
        mock_writer = MagicMock(spec=ArangoWriter)
        mock_writer.ensure_collection = MagicMock()
        mock_writer.collection_count = MagicMock(return_value=0)
        mock_writer.import_batch = MagicMock(return_value={
            "created": 1, "errors": 0, "empty": 0, "updated": 0, "ignored": 0,
        })
        pipeline = _pipeline(
            source_connector=connector,
            arango_writer=mock_writer,
            schema=pkless_schema,
            config=pkless_config,
            dry_run=True,
        )
        results = pipeline.run()
        assert results is not None
