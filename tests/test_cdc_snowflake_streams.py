"""Tests for Snowflake change capture through standard streams (PRD P6.16)."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from arango.exceptions import DocumentInsertError

from r2g.cdc.handler import CDCHandler
from r2g.cdc.models import ChangeEvent, ChangeOperation
from r2g.cdc.snowflake_streams import (
    BASIS_DT_DATA_TIMESTAMP,
    BASIS_SERVER_TIME,
    FRESHNESS_COLLECTION,
    KIND_DYNAMIC_TABLE,
    KIND_TABLE,
    KIND_VIEW,
    STREAM_STATE_COLLECTION,
    BatchNotAppliedError,
    FreshnessRecorder,
    SnowflakeStreamError,
    SnowflakeStreamListener,
    StreamStaleError,
    StreamTarget,
    StreamWatermark,
    _state_key,
    rows_to_events,
)
from r2g.connectors.arango_writer import ArangoWriter
from r2g.types import (
    CollectionMapping,
    Column,
    EdgeDefinition,
    ForeignKey,
    MappingConfig,
    Schema,
    Table,
)

T0 = datetime(2026, 11, 14, 12, 0, tzinfo=timezone.utc)
URL = "snowflake://svc:pw@acct/CRM/CORE?warehouse=WH"


@pytest.fixture
def schema() -> Schema:
    return Schema(tables={
        "ACCOUNT": Table(
            name="ACCOUNT",
            columns=[
                Column(name="ID", data_type="NUMBER", is_primary_key=True),
                Column(name="NAME", data_type="TEXT"),
                Column(name="NOTES", data_type="TEXT"),
            ],
            primary_key=["ID"],
        ),
        "CASES": Table(
            name="CASES",
            columns=[
                Column(name="ID", data_type="NUMBER", is_primary_key=True),
                Column(name="ACCOUNT_ID", data_type="NUMBER"),
                Column(name="STATUS", data_type="TEXT"),
            ],
            primary_key=["ID"],
            foreign_keys=[ForeignKey(column="ACCOUNT_ID", foreign_table="ACCOUNT", foreign_column="ID")],
        ),
    })


@pytest.fixture
def mapping() -> MappingConfig:
    return MappingConfig(
        collections={
            "ACCOUNT": CollectionMapping(
                source_table="ACCOUNT", target_collection="Account",
                exclude_fields=["NOTES"],
            ),
            "CASES": CollectionMapping(source_table="CASES", target_collection="Case"),
        },
        edges=[EdgeDefinition(
            edge_collection="caseOf", from_collection="CASES", to_collection="ACCOUNT",
            from_field="ACCOUNT_ID", to_field="ID",
        )],
    )


@pytest.fixture
def writer():
    w = MagicMock(spec=ArangoWriter)
    w.insert_document = MagicMock(return_value={"_key": "1"})
    w.replace_document = MagicMock(return_value={"_key": "1"})
    w.delete_document = MagicMock(return_value=True)
    return w


# ── fake Snowflake ──────────────────────────────────────────────────────────


class FakeCursor:
    def __init__(self, conn: "FakeConn") -> None:
        self.conn = conn
        self.description: list[tuple[str]] | None = None
        self._rows: list[tuple] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.conn.log.append(sql)
        cols, rows = self.conn.respond(sql, params)
        self.description = [(c,) for c in cols] if cols else None
        self._rows = list(rows)

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows

    def fetchmany(self, n):
        rows, self._rows = self._rows[:n], self._rows[n:]
        return rows

    def close(self):
        pass


class FakeConn:
    """Answers the listener's queries from a small in-memory model."""

    def __init__(self, objects: dict[str, tuple[str, str]], stream_rows: dict[str, list[dict]]):
        self.objects = objects  # name -> (TABLE_TYPE, IS_DYNAMIC)
        self.stream_rows = stream_rows  # stream name -> pending rows
        self.streams: dict[str, str] = {}  # stream name -> table_name
        self.log: list[str] = []
        self.now = T0
        self.dt_info: dict[str, dict] = {}
        self.stale: set[str] = set()
        self.dropped: set[str] = set()
        self.read_errors: dict[str, Exception] = {}  # stream name -> what reading it raises
        self._tx_reads: dict[str, list[dict]] | None = None
        self._consumed: set[str] = set()

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        pass

    def _stream_of(self, sql: str) -> str:
        return sql.split('"CORE".')[-1].split('"')[1] if '"CORE".' in sql else ""

    def respond(self, sql: str, params: Any):
        s = sql.strip()
        if s.startswith("SELECT TABLE_NAME"):
            names = params[1:]
            return (["TABLE_NAME", "TABLE_TYPE", "IS_DYNAMIC"],
                    [(n, *self.objects[n]) for n in names if n in self.objects])
        if s.startswith("SHOW DYNAMIC TABLES"):
            rows = [{"name": n, **info} for n, info in self.dt_info.items()]
            cols = sorted({k for r in rows for k in r}) or ["name"]
            return cols, [tuple(r.get(c) for c in cols) for r in rows]
        if s.startswith("SHOW STREAMS"):
            return (["name", "table_name", "stale"],
                    [(n, t, "false") for n, t in self.streams.items()])
        if s.startswith("CREATE STREAM"):
            name = s.split('"CORE".')[1].split('"')[1]
            obj = s.split(" ON ")[1].split('"')[-2]
            self.streams[name] = f"CRM.CORE.{obj}"
            return [], []
        if s.startswith("CREATE TEMPORARY TABLE"):
            return [], []
        if s == "SELECT CURRENT_TIMESTAMP()":
            return ["CURRENT_TIMESTAMP()"], [(self.now,)]
        if s.startswith("SELECT SYSTEM$STREAM_HAS_DATA"):
            name = params[0].split(".")[-1].strip('"')
            if name in self.read_errors:
                raise self.read_errors[name]
            if name in self.stale:
                raise RuntimeError(f"Stream {name} is stale")
            if name in self.dropped:
                # Observed verbatim from Snowflake after CREATE OR REPLACE of the source.
                err = RuntimeError(
                    "091901 (01000): Base table 'CRM.CORE.CASES' dropped, cannot read "
                    f"from stream '{name}' in line {{3}} position {{4}}."
                )
                err.errno = 91901  # type: ignore[attr-defined]
                raise err
            return ["HAS"], [(bool(self.stream_rows.get(name)),)]
        if s == "BEGIN":
            self._tx_reads = {}
            return [], []
        if s.startswith("SELECT * FROM"):
            name = self._stream_of(s)
            rows = list(self.stream_rows.get(name, []))
            assert self._tx_reads is not None, "stream read outside a transaction"
            self._tx_reads[name] = rows
            cols = list(rows[0]) if rows else ["ID"]
            return cols, [tuple(r.get(c) for c in cols) for r in rows]
        if s.startswith("INSERT INTO"):
            assert self._tx_reads is not None, "offset advanced outside a transaction"
            self._consumed.add(self._stream_of(s.split(" FROM ")[1]))
            return [], []
        if s == "COMMIT":
            for name in self._consumed:
                self.stream_rows[name] = []
            self._consumed, self._tx_reads = set(), None
            return [], []
        if s == "ROLLBACK":
            self._consumed, self._tx_reads = set(), None
            return [], []
        raise AssertionError(f"unexpected SQL: {s}")


def _row(action: str, update: bool, row_id: str, **cols) -> dict:
    return {**cols, "METADATA$ACTION": action, "METADATA$ISUPDATE": update, "METADATA$ROW_ID": row_id}


#: What ArangoDB accepts as a document key (checked against 3.12): these
#: characters only, 1 to 254 of them. A space, ``/`` or a non-ASCII character
#: is refused with error 1221, "illegal document key".
_LEGAL_ARANGO_KEY = re.compile(r"[A-Za-z0-9_\-:.@()+,=;$!*'%]{1,254}")


def _illegal_key_error() -> DocumentInsertError:
    err = DocumentInsertError.__new__(DocumentInsertError)
    err.error_code = 1221
    return err


class FakeDB:
    def __init__(self):
        self.docs: dict[str, dict[str, dict]] = {}
        self.created: list[tuple[str, bool]] = []

    def has_collection(self, name):
        return name in self.docs

    def create_collection(self, name, system=False):
        self.created.append((name, system))
        self.docs[name] = {}

    def collection(self, name):
        db = self

        class _C:
            def insert_many(self, docs, overwrite, raise_on_document_error):
                assert overwrite and raise_on_document_error
                for d in docs:
                    if not _LEGAL_ARANGO_KEY.fullmatch(d["_key"]):
                        raise _illegal_key_error()
                    db.docs[name][d["_key"]] = d

        return _C()


def _listener(schema, mapping, writer, conn, db=None, **kw):
    handler = CDCHandler(writer, schema, mapping, skip_noop_updates=True)
    lst = SnowflakeStreamListener(URL, handler, schema, mapping, connect=lambda **_: conn, **kw)
    lst.recorder.db = db or FakeDB()
    return lst


def _one_object(name: str, collection: str) -> tuple[Schema, MappingConfig]:
    """A schema and mapping with a single keyed object called *name*."""
    schema = Schema(tables={name: Table(
        name=name,
        columns=[Column(name="ID", data_type="NUMBER", is_primary_key=True)],
        primary_key=["ID"],
    )})
    mapping = MappingConfig(collections={name: CollectionMapping(
        source_table=name, target_collection=collection,
    )})
    return schema, mapping


# ── rows_to_events ──────────────────────────────────────────────────────────


class TestRowsToEvents:
    def test_update_pair_becomes_one_update_with_both_rows(self):
        events = rows_to_events([
            _row("DELETE", True, "r1", ID=1, NAME="Old"),
            _row("INSERT", True, "r1", ID=1, NAME="New"),
        ], "ACCOUNT", ["ID"])
        assert len(events) == 1
        e = events[0]
        assert e.operation == ChangeOperation.UPDATE
        assert e.old_row == {"ID": 1, "NAME": "Old"}
        assert e.new_row == {"ID": 1, "NAME": "New"}

    def test_insert_and_delete_stay_separate(self):
        events = rows_to_events([
            _row("INSERT", False, "r1", ID=1, NAME="A"),
            _row("DELETE", False, "r2", ID=2, NAME="B"),
        ], "ACCOUNT", ["ID"])
        assert [e.operation for e in events] == [ChangeOperation.INSERT, ChangeOperation.DELETE]
        assert events[1].old_row == {"ID": 2, "NAME": "B"}

    def test_pairs_by_key_even_when_isupdate_is_false(self):
        # A reinitialised dynamic table reports updates as DELETE + INSERT
        # pairs with METADATA$ISUPDATE = FALSE.
        events = rows_to_events([
            _row("INSERT", False, "x9", ID=7, NAME="New"),
            _row("DELETE", False, "x8", ID=7, NAME="Old"),
        ], "ACCOUNT", ["ID"])
        assert [e.operation for e in events] == [ChangeOperation.UPDATE]

    def test_a_key_change_is_a_delete_and_an_insert(self):
        events = rows_to_events([
            _row("DELETE", True, "r1", ID=1, NAME="A"),
            _row("INSERT", True, "r1", ID=2, NAME="A"),
        ], "ACCOUNT", ["ID"])
        assert sorted(e.operation.value for e in events) == ["DELETE", "INSERT"]

    def test_object_without_key_pairs_by_row_id(self):
        events = rows_to_events([
            _row("DELETE", True, "r1", NAME="Old"),
            _row("INSERT", True, "r1", NAME="New"),
        ], "V", [])
        assert [e.operation for e in events] == [ChangeOperation.UPDATE]

    def test_metadata_columns_are_stripped(self):
        (e,) = rows_to_events([_row("INSERT", False, "r1", ID=1)], "ACCOUNT", ["ID"])
        assert e.new_row == {"ID": 1}

    def test_unknown_action_is_refused(self):
        with pytest.raises(SnowflakeStreamError):
            rows_to_events([{"METADATA$ACTION": "MERGE", "ID": 1}], "ACCOUNT", ["ID"])


# ── no-op updates ───────────────────────────────────────────────────────────


class TestNoopUpdates:
    def _update(self, old, new):
        return ChangeEvent(operation=ChangeOperation.UPDATE, table_name="ACCOUNT",
                           old_row=old, new_row=new)

    def test_change_to_unmapped_column_writes_nothing(self, schema, mapping, writer):
        handler = CDCHandler(writer, schema, mapping, skip_noop_updates=True)
        deltas = handler.handle_event(self._update(
            {"ID": 1, "NAME": "A", "NOTES": "x"}, {"ID": 1, "NAME": "A", "NOTES": "y"},
        ))
        assert deltas == []
        assert handler.stats.noop_updates_skipped == 1
        writer.replace_document.assert_not_called()

    def test_change_to_mapped_column_is_written(self, schema, mapping, writer):
        handler = CDCHandler(writer, schema, mapping, skip_noop_updates=True)
        handler.handle_event(self._update({"ID": 1, "NAME": "A"}, {"ID": 1, "NAME": "B"}))
        assert handler.stats.noop_updates_skipped == 0
        writer.replace_document.assert_called_once()

    def test_edge_endpoint_change_is_not_a_noop(self, schema, mapping, writer):
        handler = CDCHandler(writer, schema, mapping, skip_noop_updates=True)
        evt = ChangeEvent(operation=ChangeOperation.UPDATE, table_name="CASES",
                          old_row={"ID": 5, "ACCOUNT_ID": 1, "STATUS": "open"},
                          new_row={"ID": 5, "ACCOUNT_ID": 2, "STATUS": "open"})
        assert not handler.is_noop_update(evt)

    def test_off_by_default(self, schema, mapping, writer):
        handler = CDCHandler(writer, schema, mapping)
        handler.handle_event(self._update({"ID": 1, "NOTES": "x"}, {"ID": 1, "NOTES": "y"}))
        writer.replace_document.assert_called_once()

    def test_update_without_old_row_is_never_a_noop(self, schema, mapping, writer):
        handler = CDCHandler(writer, schema, mapping, skip_noop_updates=True)
        assert not handler.is_noop_update(self._update(None, {"ID": 1}))

    def test_transaction_path_skips_too(self, schema, mapping, writer):
        handler = CDCHandler(writer, schema, mapping, skip_noop_updates=True)
        batch = handler.handle_transaction([
            self._update({"ID": 1, "NAME": "A", "NOTES": "x"}, {"ID": 1, "NAME": "A", "NOTES": "y"}),
        ])
        assert batch.deltas == []
        assert handler.stats.noop_updates_skipped == 1


# ── discovery and setup ─────────────────────────────────────────────────────


class TestDiscovery:
    def test_kinds(self, schema, mapping, writer):
        conn = FakeConn({"ACCOUNT": ("BASE TABLE", "YES"), "CASES": ("VIEW", "NO")}, {})
        conn.dt_info = {"ACCOUNT": {"refresh_mode": "INCREMENTAL"}}
        lst = _listener(schema, mapping, writer, conn)
        targets = lst.discover()
        assert targets == [
            StreamTarget("ACCOUNT", KIND_DYNAMIC_TABLE, "R2G_ACCOUNT"),
            StreamTarget("CASES", KIND_VIEW, "R2G_CASES"),
        ]

    def test_full_refresh_dynamic_table_is_refused(self, schema, mapping, writer):
        conn = FakeConn({"ACCOUNT": ("BASE TABLE", "YES"), "CASES": ("BASE TABLE", "NO")}, {})
        conn.dt_info = {"ACCOUNT": {"refresh_mode": "FULL"}}
        with pytest.raises(SnowflakeStreamError, match="incrementally"):
            _listener(schema, mapping, writer, conn).discover()

    def test_like_wildcards_do_not_pick_another_dynamic_table(self, schema, mapping, writer):
        conn = FakeConn({"ACCOUNT": ("BASE TABLE", "YES"), "CASES": ("BASE TABLE", "NO")}, {})
        conn.dt_info = {"ACCOUNTX": {"refresh_mode": "INCREMENTAL"}}
        with pytest.raises(SnowflakeStreamError, match="did not list ACCOUNT"):
            _listener(schema, mapping, writer, conn).discover()

    def test_materialized_view_is_refused(self, schema, mapping, writer):
        conn = FakeConn({"ACCOUNT": ("MATERIALIZED VIEW", "NO"), "CASES": ("BASE TABLE", "NO")}, {})
        with pytest.raises(SnowflakeStreamError, match="materialized view"):
            _listener(schema, mapping, writer, conn).discover()

    def test_missing_object_is_named(self, schema, mapping, writer):
        conn = FakeConn({"CASES": ("BASE TABLE", "NO")}, {})
        with pytest.raises(SnowflakeStreamError, match="CRM.CORE.ACCOUNT"):
            _listener(schema, mapping, writer, conn).discover()

    def test_setup_creates_missing_streams_on_the_right_kind(self, schema, mapping, writer):
        conn = FakeConn({"ACCOUNT": ("BASE TABLE", "NO"), "CASES": ("VIEW", "NO")}, {})
        conn.streams = {"R2G_ACCOUNT": "CRM.CORE.ACCOUNT"}
        outcome = _listener(schema, mapping, writer, conn).setup()
        assert outcome == {"R2G_ACCOUNT": "exists", "R2G_CASES": "created"}
        (create,) = [s for s in conn.log if s.startswith("CREATE STREAM")]
        assert ' ON VIEW "CRM"."CORE"."CASES"' in create

    def test_setup_refuses_a_stream_reading_another_object(self, schema, mapping, writer):
        conn = FakeConn({"ACCOUNT": ("BASE TABLE", "NO"), "CASES": ("BASE TABLE", "NO")}, {})
        conn.streams = {"R2G_ACCOUNT": "CRM.CORE.SOMETHING_ELSE"}
        with pytest.raises(SnowflakeStreamError, match="already exists but reads"):
            _listener(schema, mapping, writer, conn).setup()

    def test_stream_schema_defaults_to_source_schema(self, schema, mapping, writer):
        conn = FakeConn({}, {})
        lst = _listener(schema, mapping, writer, conn)
        assert (lst.database, lst.source_schema, lst.stream_schema) == ("CRM", "CORE", "CORE")
        assert lst._connect_params["schema"] == "CORE"


# ── consuming ───────────────────────────────────────────────────────────────


def _tables_conn(account_rows=None, case_rows=None) -> FakeConn:
    conn = FakeConn(
        {"ACCOUNT": ("BASE TABLE", "NO"), "CASES": ("BASE TABLE", "NO")},
        {"R2G_ACCOUNT": account_rows or [], "R2G_CASES": case_rows or []},
    )
    conn.streams = {"R2G_ACCOUNT": "CRM.CORE.ACCOUNT", "R2G_CASES": "CRM.CORE.CASES"}
    return conn


class TestConsume:
    def test_changes_reach_arango_and_offset_advances_inside_the_transaction(
        self, schema, mapping, writer,
    ):
        conn = _tables_conn(account_rows=[
            _row("INSERT", False, "a", ID=1, NAME="Acme"),
            _row("DELETE", True, "b", ID=2, NAME="Old"),
            _row("INSERT", True, "b", ID=2, NAME="New"),
            _row("DELETE", False, "c", ID=3, NAME="Gone"),
        ])
        lst = _listener(schema, mapping, writer, conn)
        lst.poll_once()

        writer.insert_document.assert_called_once()
        assert writer.insert_document.call_args.args[1]["NAME"] == "Acme"
        assert writer.replace_document.call_args.args[1]["NAME"] == "New"
        writer.delete_document.assert_called_once_with("Account", "3")

        tx = conn.log[conn.log.index("BEGIN"):conn.log.index("COMMIT") + 1]
        assert tx[1].startswith('SELECT * FROM "CRM"."CORE"."R2G_ACCOUNT"')
        assert tx[-2] == 'INSERT INTO "R2G_OFFSET_SINK" SELECT 1 FROM "CRM"."CORE"."R2G_ACCOUNT" WHERE 0 = 1'
        assert conn.stream_rows["R2G_ACCOUNT"] == []
        # The sink exists before any transaction opens (DDL would commit it).
        sink = 'CREATE TEMPORARY TABLE IF NOT EXISTS "R2G_OFFSET_SINK" (CONSUMED INTEGER)'
        assert conn.log.index(sink) < conn.log.index("BEGIN")

    def test_unmapped_column_update_writes_nothing_but_still_consumes(self, schema, mapping, writer):
        conn = _tables_conn(account_rows=[
            _row("DELETE", True, "a", ID=1, NAME="Acme", NOTES="x"),
            _row("INSERT", True, "a", ID=1, NAME="Acme", NOTES="y"),
        ])
        lst = _listener(schema, mapping, writer, conn)
        result = lst.poll_once()
        writer.replace_document.assert_not_called()
        assert lst.handler.stats.noop_updates_skipped == 1
        assert conn.stream_rows["R2G_ACCOUNT"] == []
        assert result.watermarks["R2G_ACCOUNT"].events == 1

    def test_failed_delta_rolls_back_and_keeps_the_changes(self, schema, mapping, writer):
        writer.insert_document.side_effect = RuntimeError("arango down")
        conn = _tables_conn(account_rows=[_row("INSERT", False, "a", ID=1, NAME="Acme")])
        lst = _listener(schema, mapping, writer, conn)
        with pytest.raises(BatchNotAppliedError):
            lst.poll_once()
        assert "ROLLBACK" in conn.log and "COMMIT" not in conn.log
        assert not any(s.startswith("INSERT INTO") for s in conn.log)
        assert len(conn.stream_rows["R2G_ACCOUNT"]) == 1

    def test_empty_stream_is_not_read_but_its_watermark_advances(self, schema, mapping, writer):
        conn = _tables_conn()
        lst = _listener(schema, mapping, writer, conn)
        result = lst.poll_once()
        assert "BEGIN" not in conn.log
        assert result.watermarks["R2G_ACCOUNT"] == StreamWatermark(T0, BASIS_SERVER_TIME)

    def test_stale_stream_is_reported_by_name(self, schema, mapping, writer):
        conn = _tables_conn()
        conn.stale = {"R2G_CASES"}
        lst = _listener(schema, mapping, writer, conn)
        with pytest.raises(StreamStaleError, match="R2G_CASES"):
            lst.poll_once()

    def test_stream_whose_source_was_recreated_is_reported_as_unreadable(
        self, schema, mapping, writer,
    ):
        conn = _tables_conn()
        conn.dropped = {"R2G_CASES"}
        db = FakeDB()
        lst = _listener(schema, mapping, writer, conn, db=db)
        with pytest.raises(StreamStaleError, match="R2G_CASES"):
            lst.poll_once()
        # The readable stream's progress is still recorded; the broken one's
        # collections are not stamped.
        assert set(db.docs[FRESHNESS_COLLECTION]) == {"Account"}

    @pytest.mark.parametrize("named", [
        "'CRM.CORE.R2G_STALE_LEADS'",
        '"CRM"."CORE"."R2G_STALE_LEADS"',
        "CRM.CORE.R2G_STALE_LEADS",
        "R2G_STALE_LEADS",
    ])
    def test_error_naming_an_object_called_stale_is_not_an_unreadable_stream(self, writer, named):
        # Snowflake's messages carry object names; a name is not a diagnosis.
        schema, mapping = _one_object("STALE_LEADS", "Lead")
        conn = FakeConn({"STALE_LEADS": ("BASE TABLE", "NO")}, {})
        conn.read_errors["R2G_STALE_LEADS"] = RuntimeError(
            f"002003 (42S02): SQL compilation error:\nObject {named} does not exist or not authorized."
        )
        with pytest.raises(RuntimeError, match="does not exist or not authorized") as caught:
            _listener(schema, mapping, writer, conn).poll_once()
        assert not isinstance(caught.value, StreamStaleError)

    @pytest.mark.parametrize("message", [
        "Stream R2G_STALE_LEADS is stale",
        "Stream 'CRM.CORE.R2G_STALE_LEADS' is stale",
        "Base table 'CRM.CORE.STALE_LEADS' dropped, cannot read from stream 'R2G_STALE_LEADS'",
    ])
    def test_object_called_stale_whose_stream_is_unreadable_is_still_reported(
        self, writer, message,
    ):
        schema, mapping = _one_object("STALE_LEADS", "Lead")
        conn = FakeConn({"STALE_LEADS": ("BASE TABLE", "NO")}, {})
        conn.read_errors["R2G_STALE_LEADS"] = RuntimeError(message)
        with pytest.raises(StreamStaleError, match="R2G_STALE_LEADS"):
            _listener(schema, mapping, writer, conn).poll_once()

    @pytest.mark.parametrize(("message", "unreadable"), [
        ("Stream 'CRM.CORE.R2G_STALE' is stale", True),
        ("Stream R2G_STALE is stale", True),
        ("Object 'CRM.CORE.STALE' does not exist or not authorized.", False),
    ])
    def test_object_named_exactly_stale(self, writer, message, unreadable):
        schema, mapping = _one_object("STALE", "Thing")
        conn = FakeConn({"STALE": ("BASE TABLE", "NO")}, {})
        conn.read_errors["R2G_STALE"] = RuntimeError(message)
        with pytest.raises(RuntimeError) as caught:
            _listener(schema, mapping, writer, conn).poll_once()
        assert isinstance(caught.value, StreamStaleError) is unreadable

    def test_object_named_like_a_word_of_the_wording_does_not_hide_it(self, writer):
        schema, mapping = _one_object("STREAM", "Thing")
        conn = FakeConn({"STREAM": ("BASE TABLE", "NO")}, {})
        conn.read_errors["R2G_STREAM"] = RuntimeError(
            "Base table CRM.CORE.STREAM dropped, cannot read from stream R2G_STREAM"
        )
        with pytest.raises(StreamStaleError, match="R2G_STREAM"):
            _listener(schema, mapping, writer, conn).poll_once()

    def test_known_error_number_decides_whatever_the_wording(self, writer):
        schema, mapping = _one_object("ACCOUNT", "Account")
        conn = FakeConn({"ACCOUNT": ("BASE TABLE", "NO")}, {})
        err = RuntimeError("091901 (01000): wording r2g has never seen")
        err.errno = 91901  # type: ignore[attr-defined]
        conn.read_errors["R2G_ACCOUNT"] = err
        with pytest.raises(StreamStaleError, match="R2G_ACCOUNT"):
            _listener(schema, mapping, writer, conn).poll_once()

    def test_watermark_is_read_before_the_stream(self, schema, mapping, writer):
        conn = _tables_conn(account_rows=[_row("INSERT", False, "a", ID=1, NAME="Acme")])
        _listener(schema, mapping, writer, conn).poll_once()
        before_tx = conn.log[:conn.log.index("BEGIN")]
        clock = max(i for i, s in enumerate(before_tx) if s == "SELECT CURRENT_TIMESTAMP()")
        has_data = max(i for i, s in enumerate(before_tx) if s.startswith("SELECT SYSTEM$STREAM_HAS_DATA"))
        assert clock < has_data

    def test_dynamic_table_claims_its_data_timestamp_when_older(self, schema, mapping, writer):
        conn = _tables_conn()
        conn.objects["ACCOUNT"] = ("BASE TABLE", "YES")
        data_ts = T0 - timedelta(minutes=3)
        conn.dt_info = {"ACCOUNT": {"refresh_mode": "INCREMENTAL", "data_timestamp": data_ts}}
        result = _listener(schema, mapping, writer, conn).poll_once()
        assert result.watermarks["R2G_ACCOUNT"] == StreamWatermark(data_ts, BASIS_DT_DATA_TIMESTAMP)

    def test_dynamic_table_without_readable_data_timestamp_uses_server_time(self, schema, mapping, writer):
        conn = _tables_conn()
        conn.objects["ACCOUNT"] = ("BASE TABLE", "YES")
        conn.dt_info = {"ACCOUNT": {"refresh_mode": "INCREMENTAL", "data_timestamp": None}}
        result = _listener(schema, mapping, writer, conn).poll_once()
        assert result.watermarks["R2G_ACCOUNT"].basis == BASIS_SERVER_TIME


# ── freshness ───────────────────────────────────────────────────────────────


class TestFreshness:
    def test_each_collection_gets_the_oldest_watermark_of_its_streams(self):
        db = FakeDB()
        rec = FreshnessRecorder(db, {"ACCOUNT": {"nodes"}, "CASES": {"nodes", "edges"}})
        targets = [StreamTarget("ACCOUNT", KIND_TABLE, "R2G_ACCOUNT"),
                   StreamTarget("CASES", KIND_TABLE, "R2G_CASES")]
        stamps = rec.record(targets, {
            "R2G_ACCOUNT": StreamWatermark(T0, BASIS_SERVER_TIME),
            "R2G_CASES": StreamWatermark(T0 - timedelta(minutes=5), BASIS_SERVER_TIME),
        })
        assert stamps == {"nodes": "2026-11-14T11:55:00+00:00", "edges": "2026-11-14T11:55:00+00:00"}
        assert db.docs[FRESHNESS_COLLECTION]["nodes"]["ingested_at"] == "2026-11-14T11:55:00+00:00"
        assert (FRESHNESS_COLLECTION, True) in db.created
        assert db.docs[STREAM_STATE_COLLECTION]["R2G_ACCOUNT"]["applied_through"] == "2026-11-14T12:00:00+00:00"

    def test_a_collection_with_a_failed_stream_is_not_stamped(self):
        db = FakeDB()
        rec = FreshnessRecorder(db, {"ACCOUNT": {"Account"}, "CASES": {"Case", "caseOf"}})
        targets = [StreamTarget("ACCOUNT", KIND_TABLE, "R2G_ACCOUNT"),
                   StreamTarget("CASES", KIND_TABLE, "R2G_CASES")]
        stamps = rec.record(targets, {"R2G_ACCOUNT": StreamWatermark(T0, BASIS_SERVER_TIME)})
        assert stamps == {"Account": "2026-11-14T12:00:00+00:00"}

    def test_poll_stamps_mapped_collections_with_utc_iso(self, schema, mapping, writer):
        conn = _tables_conn()
        db = FakeDB()
        result = _listener(schema, mapping, writer, conn, db=db).poll_once()
        assert result.collections_stamped == {
            "Account": "2026-11-14T12:00:00+00:00",
            "Case": "2026-11-14T12:00:00+00:00",
            "caseOf": "2026-11-14T12:00:00+00:00",
        }

    def test_stamp_is_readable_by_the_fabric_parser(self):
        # contextual-data-fabric parses ingested_at with fromisoformat and
        # refuses a stamp without a time zone.
        db = FakeDB()
        rec = FreshnessRecorder(db, {"ACCOUNT": {"Account"}})
        rec.record([StreamTarget("ACCOUNT", KIND_TABLE, "R2G_ACCOUNT")],
                   {"R2G_ACCOUNT": StreamWatermark(T0.astimezone(timezone(timedelta(hours=-8))), BASIS_SERVER_TIME)})
        stamp = db.docs[FRESHNESS_COLLECTION]["Account"]["ingested_at"]
        assert datetime.fromisoformat(stamp) == T0
        assert datetime.fromisoformat(stamp).tzinfo is not None


class TestStreamStateKey:
    """A stream is named after its object, and a quoted Snowflake identifier
    may hold characters an ArangoDB key may not."""

    @pytest.mark.parametrize("stream", [
        "R2G_ACCOUNT",
        "R2G_Order Items",
        "R2G_A/B",
        "R2G_KÜNDEN",
        "R2G_顧客",
        "R2G_A$B",
        "R2G_" + "X" * 400,
        "R2G_" + "顧" * 200,
    ])
    def test_key_is_legal_in_arangodb(self, stream):
        assert _LEGAL_ARANGO_KEY.fullmatch(_state_key(stream))

    def test_a_plain_name_is_its_own_key(self):
        assert _state_key("R2G_ACCOUNT") == "R2G_ACCOUNT"

    @pytest.mark.parametrize(("one", "other"), [
        ("R2G_A$B", "R2G_A_B"),
        ("R2G_A B", "R2G_A%20B"),
        ("R2G_" + "X" * 400, "R2G_" + "X" * 401),
        ("R2G_" + "顧" * 200, "R2G_" + "顧" * 199 + "客"),
    ])
    def test_two_streams_never_share_a_key(self, one, other):
        assert _state_key(one) != _state_key(other)

    def test_object_whose_name_needs_quoting_is_recorded_and_stamped(self, writer):
        # The state write comes after Snowflake has committed, so a key
        # ArangoDB refuses would fail every cycle and stamp nothing.
        schema, mapping = _one_object("Order Items", "OrderItem")
        conn = FakeConn({"Order Items": ("BASE TABLE", "NO")}, {})
        db = FakeDB()
        result = _listener(schema, mapping, writer, conn, db=db).poll_once()
        assert result.collections_stamped == {"OrderItem": "2026-11-14T12:00:00+00:00"}
        (doc,) = db.docs[STREAM_STATE_COLLECTION].values()
        assert doc["stream"] == "R2G_Order Items"
        assert doc["_key"] == "R2G_Order%20Items"


class TestCollectionsForTable:
    def test_lpg_layout_reports_shared_collections(self, schema, mapping, writer):
        lpg = mapping.model_copy(update={"graph_layout": "lpg"})
        handler = CDCHandler(writer, schema, lpg)
        assert handler.transformer.collections_for_table("CASES") == {
            lpg.lpg.node_collection, lpg.lpg.edge_collection,
        }

    def test_unmapped_table(self, schema, mapping, writer):
        assert CDCHandler(writer, schema, mapping).transformer.collections_for_table("NOPE") == set()
