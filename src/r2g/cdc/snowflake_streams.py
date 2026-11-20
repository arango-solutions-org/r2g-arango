"""Snowflake change capture through standard streams (PRD P6.16, issue #65).

One standard stream per mapped Snowflake object (table, view or dynamic
table) feeds the existing :class:`~r2g.cdc.handler.CDCHandler`. Each poll
cycle, per stream:

1. Read the server clock (and, for a dynamic table, its data timestamp)
   **before** anything else. That time is a lower bound on what the read
   below will contain, so it is what the watermark claims.
2. ``SYSTEM$STREAM_HAS_DATA``: an empty stream is not read. The call also
   keeps the stream from going stale, and the watermark still advances,
   because "no changes up to T" is as current as "changes up to T".
3. Otherwise, inside one ``BEGIN … COMMIT``: select the stream, apply the
   changes to ArangoDB, then advance the offset with
   ``INSERT INTO <sink> SELECT 1 FROM <stream> WHERE 0 = 1``. A ``SELECT``
   alone never advances a stream; only DML that reads it inside a committed
   transaction does, and it moves the offset to the transaction's start.
   Any delta that fails rolls the transaction back, so the offset stays put
   and the same changes are read again next cycle (at-least-once; the
   default ``source_wins`` policy makes the replay idempotent).

The offset is advanced into a session-scoped temporary table created before
any transaction opens. It must not be created with ``CREATE … AS SELECT``
from the stream: that statement is DDL, which commits on its own and would
consume the stream outside the transaction.

Snowflake reports an UPDATE as a DELETE row (old values) and an INSERT row
(new values). Rows are paired by primary key, falling back to
``METADATA$ROW_ID`` for an object without one, so an update reaches the
handler as one UPDATE with both rows. Pairing by key rather than by
``METADATA$ISUPDATE`` also covers a reinitialised dynamic table, whose
update pairs carry ``ISUPDATE = FALSE``. With both rows in hand the handler
drops an update whose mapped output is unchanged (``skip_noop_updates``),
so an edit to a column no mapping uses writes nothing.

Watermarks
----------

After each cycle r2g writes, per stream, the source time its changes are
applied through (``_r2g_cdc_streams``), and per ArangoDB collection the
oldest watermark among the streams that write to it
(:data:`FRESHNESS_COLLECTION`, one document per collection keyed by its
name, holding ``ingested_at``). That collection and field are the ones the
contextual-data-fabric's ArangoDB executor reads to cite how fresh an
answer is, so a copy kept by this listener is cited without any change on
the fabric's side.
"""

from __future__ import annotations

import hashlib
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

from r2g.cdc.handler import CDCHandler
from r2g.cdc.models import ChangeEvent, ChangeOperation
from r2g.connectors.snowflake import (
    _load_snowflake_connector,
    _parse_snowflake_url,
    _safe_connection_error,
)
from r2g.keys import sanitize_key_component
from r2g.log import get_logger
from r2g.types import MappingConfig, Schema

logger = get_logger(__name__)

#: Per-collection freshness: ``{_key: <collection>, ingested_at: <ISO time>}``.
#: Read by contextual-data-fabric (``cdf.adapters.arango.INGEST_COLLECTION``).
#: A system collection, so schema analyzers skip it and it never becomes a
#: concept in a reverse-engineered CSI.
FRESHNESS_COLLECTION = "_cdf_ingest"

#: Per-stream watermark and the counts of its last cycle.
STREAM_STATE_COLLECTION = "_r2g_cdc_streams"

#: The longest ``_key`` ArangoDB accepts, in bytes.
ARANGO_MAX_KEY_LENGTH = 254

#: Session-scoped table that consumes stream offsets (see module docstring).
OFFSET_SINK = "R2G_OFFSET_SINK"

DEFAULT_STREAM_PREFIX = "R2G_"
DEFAULT_POLL_SECONDS = 60.0

KIND_TABLE = "TABLE"
KIND_VIEW = "VIEW"
KIND_DYNAMIC_TABLE = "DYNAMIC TABLE"

_METADATA_ACTION = "METADATA$ACTION"
_METADATA_ROW_ID = "METADATA$ROW_ID"
_METADATA_PREFIX = "METADATA$"

#: "Base table '…' dropped, cannot read from stream '…'" — the source was
#: dropped or recreated (``CREATE OR REPLACE``) under the stream.
_ERR_STREAM_SOURCE_DROPPED = 91901

BASIS_SERVER_TIME = "server_time_before_read"
BASIS_DT_DATA_TIMESTAMP = "dynamic_table_data_timestamp"


class SnowflakeStreamError(RuntimeError):
    """A stream cannot be set up or read as configured."""


class StreamStaleError(SnowflakeStreamError):
    """The stream can no longer be read from its offset.

    Either it is stale (its offset fell outside the source's retention) or
    its source was dropped or recreated, which Snowflake reports as error
    091901, "Base table … dropped, cannot read from stream". Both lose the
    changes since the last cycle, and both are recovered the same way:
    recreate the stream and resync the object's collections.
    """

    def __init__(self, stream: str, detail: str = "") -> None:
        self.stream = stream
        super().__init__(
            f"Snowflake stream {stream} can no longer be read: it is stale, or "
            "its source was dropped or recreated, so the changes since the last "
            "cycle are lost. Recreate the stream and reload the object's "
            "collections (automatic recovery is not implemented yet)."
            + (f" Snowflake said: {detail}" if detail else "")
        )


class BatchNotAppliedError(SnowflakeStreamError):
    """Some deltas of a stream's batch failed; the offset was not advanced."""


def quote_ident(name: str) -> str:
    """Quote a Snowflake identifier exactly as stored (case preserved)."""
    return '"' + name.replace('"', '""') + '"'


def stream_name_for(source_object: str, prefix: str = DEFAULT_STREAM_PREFIX) -> str:
    return f"{prefix}{source_object}"


@dataclass(frozen=True)
class StreamTarget:
    """One mapped Snowflake object and the stream r2g reads it through."""

    source_object: str
    kind: str
    stream_name: str


@dataclass
class StreamWatermark:
    applied_through: datetime
    basis: str
    events: int = 0
    rows_read: int = 0


@dataclass
class CycleResult:
    """What one poll cycle did, per stream."""

    watermarks: dict[str, StreamWatermark] = field(default_factory=dict)
    collections_stamped: dict[str, str] = field(default_factory=dict)


# ── Stream rows → change events ─────────────────────────────────────────────


def rows_to_events(
    rows: Iterable[dict[str, Any]],
    table: str,
    primary_key: list[str],
    *,
    schema_name: str = "PUBLIC",
) -> list[ChangeEvent]:
    """Turn a standard stream's rows into INSERT / UPDATE / DELETE events.

    A DELETE row and an INSERT row with the same primary key (or, for an
    object without one, the same ``METADATA$ROW_ID``) become one UPDATE
    carrying both rows. A standard stream returns net changes, so a key
    appears at most once per action within a read; should it appear again,
    the repeat starts a new event rather than overwriting the first.
    """
    entries: list[dict[str, dict[str, Any]]] = []
    open_entry: dict[Any, int] = {}

    for raw in rows:
        action = str(raw.get(_METADATA_ACTION, "")).upper()
        if action not in ("INSERT", "DELETE"):
            raise SnowflakeStreamError(
                f"Unexpected {_METADATA_ACTION} {action!r} reading the stream on {table}"
            )
        data = {k: v for k, v in raw.items() if not k.startswith(_METADATA_PREFIX)}
        pk_values = tuple(data.get(c) for c in primary_key)
        if primary_key and all(v is not None for v in pk_values):
            pair_key: Any = ("pk", pk_values)
        else:
            pair_key = ("row_id", raw.get(_METADATA_ROW_ID))

        idx = open_entry.get(pair_key)
        if idx is None or action in entries[idx]:
            open_entry[pair_key] = len(entries)
            entries.append({action: data})
        else:
            entries[idx][action] = data

    events: list[ChangeEvent] = []
    for entry in entries:
        old, new = entry.get("DELETE"), entry.get("INSERT")
        if old is not None and new is not None:
            op = ChangeOperation.UPDATE
        elif new is not None:
            op = ChangeOperation.INSERT
        else:
            op = ChangeOperation.DELETE
        events.append(ChangeEvent(
            operation=op,
            schema_name=schema_name,
            table_name=table,
            new_row=new,
            old_row=old,
        ))
    return events


# ── Freshness ───────────────────────────────────────────────────────────────


def _iso_utc(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat()


class FreshnessRecorder:
    """Writes per-stream watermarks and per-collection freshness to ArangoDB."""

    def __init__(self, db: Any, collections_by_object: dict[str, set[str]]) -> None:
        self.db = db
        self.collections_by_object = collections_by_object

    def _ensure(self, name: str) -> Any:
        if not self.db.has_collection(name):
            try:
                self.db.create_collection(name, system=True)
            except Exception:
                # Another writer may have created it in between.
                if not self.db.has_collection(name):
                    raise
        return self.db.collection(name)

    def record(
        self,
        targets: list[StreamTarget],
        watermarks: dict[str, StreamWatermark],
    ) -> dict[str, str]:
        """Persist this cycle's watermarks; return the collection stamps written.

        A collection is stamped only when every stream writing to it has a
        watermark this cycle, and with the oldest of them: the collection is
        current only as far as its least current source.
        """
        now = _iso_utc(datetime.now(timezone.utc))
        stream_docs = []
        for t in targets:
            wm = watermarks.get(t.stream_name)
            if wm is None:
                continue
            stream_docs.append({
                "_key": _state_key(t.stream_name),
                "stream": t.stream_name,
                "source_object": t.source_object,
                "object_kind": t.kind,
                "applied_through": _iso_utc(wm.applied_through),
                "basis": wm.basis,
                "last_cycle_events": wm.events,
                "last_cycle_rows_read": wm.rows_read,
                "collections": sorted(self.collections_by_object.get(t.source_object, ())),
                "updated_at": now,
            })
        if stream_docs:
            self._ensure(STREAM_STATE_COLLECTION).insert_many(
                stream_docs, overwrite=True, raise_on_document_error=True,
            )

        feeders: dict[str, list[StreamTarget]] = {}
        for t in targets:
            for coll in self.collections_by_object.get(t.source_object, ()):
                feeders.setdefault(coll, []).append(t)

        stamps: dict[str, str] = {}
        for coll, ts in sorted(feeders.items()):
            marks = [watermarks.get(t.stream_name) for t in ts]
            if any(m is None for m in marks):
                continue
            oldest = min(marks, key=lambda m: m.applied_through)  # type: ignore[union-attr]
            stamps[coll] = _iso_utc(oldest.applied_through)  # type: ignore[union-attr]
        if stamps:
            self._ensure(FRESHNESS_COLLECTION).insert_many(
                [
                    {
                        "_key": coll,
                        "ingested_at": stamp,
                        "written_by": "r2g cdc-start (Snowflake streams)",
                        "streams": sorted(t.stream_name for t in feeders[coll]),
                        "updated_at": now,
                    }
                    for coll, stamp in stamps.items()
                ],
                overwrite=True,
                raise_on_document_error=True,
            )
        return stamps


def _state_key(stream_name: str) -> str:
    """The ``_key`` of a stream's state document: legal in ArangoDB, one per stream.

    A stream is named after its source object, and Snowflake allows any
    character in a quoted identifier (``"Order Items"``); ArangoDB does not
    in a key. Disallowed bytes are percent-encoded, which is one-to-one, so
    two streams never share a document. A name whose encoding is too long
    for a key keeps a readable start and ends in a digest of the whole name.
    """
    key = sanitize_key_component(stream_name)
    if len(key) <= ARANGO_MAX_KEY_LENGTH:
        return key
    digest = hashlib.sha256(stream_name.encode("utf-8")).hexdigest()[:32]
    return f"{key[: ARANGO_MAX_KEY_LENGTH - len(digest) - 1]}:{digest}"


# ── Listener ────────────────────────────────────────────────────────────────


class SnowflakeStreamListener:
    """Keeps an ArangoDB copy current from Snowflake standard streams.

    ``schema`` and ``mapping`` are the ones the copy was loaded with; one
    stream is read per mapped object present in ``schema``. Streams live in
    ``stream_schema`` (default: the source schema), which is also where the
    session's temporary offset sink is created, so the role needs
    ``CREATE STREAM`` there and ``SELECT`` on the source objects.
    """

    def __init__(
        self,
        connection_string: str,
        handler: CDCHandler,
        schema: Schema,
        mapping: MappingConfig,
        *,
        source_schema: str | None = None,
        stream_schema: str | None = None,
        stream_prefix: str = DEFAULT_STREAM_PREFIX,
        poll_interval: float = DEFAULT_POLL_SECONDS,
        fetch_size: int = 10_000,
        recorder: FreshnessRecorder | None = None,
        connect: Callable[..., Any] | None = None,
    ) -> None:
        params = _parse_snowflake_url(connection_string)
        url_schema = params.pop("_url_schema", None)
        params.pop("_include_views", None)
        self.database = str(params["database"]).upper()
        self.source_schema = (source_schema or url_schema or "PUBLIC").upper()
        self.stream_schema = (stream_schema or self.source_schema).upper()
        params["database"] = self.database
        params["schema"] = self.stream_schema
        self._connect_params = params
        self._connect = connect

        self.handler = handler
        self.schema = schema
        self.mapping = mapping
        self.stream_prefix = stream_prefix
        self.poll_interval = poll_interval
        self.fetch_size = max(1, fetch_size)

        self.objects = sorted({
            cm.source_table
            for cm in mapping.collections.values()
            if cm.source_table in schema.tables
        })
        collections_by_object = {
            obj: handler.transformer.collections_for_table(obj) for obj in self.objects
        }
        self.recorder = recorder or FreshnessRecorder(handler.writer.db, collections_by_object)

        self.targets: list[StreamTarget] = []
        self._conn: Any = None
        self._sink_ready = False
        self._running = False

    # -- connection ---------------------------------------------------------

    @property
    def connection(self) -> Any:
        if self._conn is None:
            connect = self._connect or _load_snowflake_connector().connect
            try:
                self._conn = connect(**self._connect_params)
            except Exception as err:
                raise RuntimeError(
                    "Failed to connect to Snowflake: "
                    f"{_safe_connection_error(err, self._connect_params)}"
                ) from err
            self._sink_ready = False
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def _execute(self, sql: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
        cur = self.connection.cursor()
        try:
            cur.execute(sql, params) if params is not None else cur.execute(sql)
            if not cur.description:
                return []
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            cur.close()

    def _scalar(self, sql: str, params: tuple[Any, ...] | None = None) -> Any:
        rows = self._execute(sql, params)
        return next(iter(rows[0].values())) if rows else None

    # -- naming -------------------------------------------------------------

    def _source_fqn(self, obj: str) -> str:
        return ".".join(quote_ident(p) for p in (self.database, self.source_schema, obj))

    def _stream_fqn(self, stream: str) -> str:
        return ".".join(quote_ident(p) for p in (self.database, self.stream_schema, stream))

    # -- discovery ----------------------------------------------------------

    def discover(self) -> list[StreamTarget]:
        """Resolve each mapped object's kind and refuse ones a stream cannot read."""
        if not self.objects:
            raise SnowflakeStreamError(
                "The mapping has no collection whose source table is in the schema; "
                "there is nothing to read changes from."
            )
        placeholders = ", ".join("%s" for _ in self.objects)
        rows = self._execute(
            f"SELECT TABLE_NAME, TABLE_TYPE, IS_DYNAMIC "  # noqa: S608
            f"FROM {quote_ident(self.database)}.INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN ({placeholders})",
            (self.source_schema, *self.objects),
        )
        found = {r["TABLE_NAME"]: r for r in rows}
        targets = []
        for obj in self.objects:
            row = found.get(obj)
            if row is None:
                raise SnowflakeStreamError(
                    f"{self.database}.{self.source_schema}.{obj} is mapped but does not exist "
                    "or is not visible to this role."
                )
            table_type = str(row.get("TABLE_TYPE") or "").upper()
            if str(row.get("IS_DYNAMIC") or "").upper() == "YES":
                kind = KIND_DYNAMIC_TABLE
                self._require_incremental(obj)
            elif table_type == "VIEW":
                kind = KIND_VIEW
            elif table_type == "BASE TABLE":
                kind = KIND_TABLE
            else:
                raise SnowflakeStreamError(
                    f"{obj} is a {table_type.lower()}; Snowflake streams cannot read it."
                )
            targets.append(StreamTarget(obj, kind, stream_name_for(obj, self.stream_prefix)))
        self.targets = targets
        return targets

    def _dynamic_table_info(self, obj: str) -> dict[str, Any]:
        like = obj.replace("\\", "\\\\").replace("'", "''")
        rows = self._execute(
            f"SHOW DYNAMIC TABLES LIKE '{like}' IN SCHEMA "
            f"{quote_ident(self.database)}.{quote_ident(self.source_schema)}"
        )
        # LIKE treats _ and % as wildcards and ignores case; keep the exact name.
        for row in rows:
            if row.get("name") == obj:
                return row
        raise SnowflakeStreamError(f"SHOW DYNAMIC TABLES did not list {obj}.")

    def _require_incremental(self, obj: str) -> None:
        mode = str(self._dynamic_table_info(obj).get("refresh_mode") or "").upper()
        if mode != "INCREMENTAL":
            raise SnowflakeStreamError(
                f"Dynamic table {obj} refreshes in {mode or 'an unknown'} mode. Snowflake "
                "supports streams on a dynamic table only when it refreshes incrementally."
            )

    # -- setup / status -----------------------------------------------------

    def status(self) -> dict[str, dict[str, Any]]:
        """``SHOW STREAMS`` rows for r2g's streams in the stream schema, by name."""
        rows = self._execute(
            f"SHOW STREAMS IN SCHEMA "
            f"{quote_ident(self.database)}.{quote_ident(self.stream_schema)}"
        )
        wanted = {stream_name_for(o, self.stream_prefix) for o in self.objects}
        return {r["name"]: r for r in rows if r.get("name") in wanted}

    def setup(self, *, create: bool = True) -> dict[str, str]:
        """Make sure every target has its stream; return what was done per stream.

        An existing stream of the expected name must read the expected
        object; one that reads something else is an error, never reused.
        """
        targets = self.targets or self.discover()
        existing = self.status()
        outcome: dict[str, str] = {}
        for t in targets:
            row = existing.get(t.stream_name)
            if row is not None:
                reads = str(row.get("table_name") or "")
                expected = f"{self.database}.{self.source_schema}.{t.source_object}"
                if reads.replace('"', "").upper() != expected.upper():
                    raise SnowflakeStreamError(
                        f"Stream {t.stream_name} already exists but reads {reads}, "
                        f"not {expected}. Drop it or choose another --stream-prefix."
                    )
                outcome[t.stream_name] = "exists"
                continue
            if not create:
                raise SnowflakeStreamError(
                    f"Stream {t.stream_name} does not exist (run without --no-create-streams)."
                )
            self._execute(
                f"CREATE STREAM IF NOT EXISTS {self._stream_fqn(t.stream_name)} "
                f"ON {t.kind} {self._source_fqn(t.source_object)} "
                f"COMMENT = 'r2g change capture'"
            )
            outcome[t.stream_name] = "created"
            logger.info("snowflake_stream_created", stream=t.stream_name, kind=t.kind)
        return outcome

    # -- one cycle ----------------------------------------------------------

    def _ensure_sink(self) -> None:
        if self._sink_ready:
            return
        self._execute(
            f"CREATE TEMPORARY TABLE IF NOT EXISTS {quote_ident(OFFSET_SINK)} (CONSUMED INTEGER)"
        )
        self._sink_ready = True

    def _lower_bound(self, target: StreamTarget) -> tuple[datetime, str]:
        """A time no later than anything the next read of *target* contains."""
        now = self._scalar("SELECT CURRENT_TIMESTAMP()")
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise SnowflakeStreamError(f"CURRENT_TIMESTAMP() returned {now!r}, not a zoned time.")
        if target.kind == KIND_DYNAMIC_TABLE:
            # A dynamic table holds its sources as of its data timestamp, which
            # trails the clock by up to its target lag. Read before the stream,
            # so a refresh landing in between only makes the claim conservative.
            data_ts = self._dynamic_table_info(target.source_object).get("data_timestamp")
            if isinstance(data_ts, datetime) and data_ts.tzinfo is not None:
                if data_ts < now:
                    return data_ts, BASIS_DT_DATA_TIMESTAMP
            else:
                logger.warning(
                    "snowflake_dt_data_timestamp_unreadable",
                    object=target.source_object, value=repr(data_ts),
                )
        return now, BASIS_SERVER_TIME

    def _raise_if_stale(self, target: StreamTarget, err: Exception) -> None:
        """Re-raise a failed stream read as :class:`StreamStaleError` when it is one.

        Detection is by the read failing, never by ``SHOW STREAMS``: its
        ``stale`` column means "may be stale" and is ``true`` for a stream
        that still reads normally (observed on a table with zero retention).
        """
        message = str(err).lower()
        if (
            getattr(err, "errno", None) == _ERR_STREAM_SOURCE_DROPPED
            or "stale" in message
            or "cannot read from stream" in message
        ):
            raise StreamStaleError(target.stream_name, str(err)) from err

    def consume(self, target: StreamTarget) -> StreamWatermark:
        """Apply one stream's pending changes; return its new watermark."""
        through, basis = self._lower_bound(target)
        stream = self._stream_fqn(target.stream_name)
        try:
            has_data = self._scalar("SELECT SYSTEM$STREAM_HAS_DATA(%s)", (stream,))
        except Exception as err:
            self._raise_if_stale(target, err)
            raise
        if not has_data:
            return StreamWatermark(through, basis)

        self._ensure_sink()
        table_def = self.schema.tables[target.source_object]
        conn = self.connection
        cur = conn.cursor()
        try:
            cur.execute("BEGIN")
            try:
                cur.execute(f"SELECT * FROM {stream}")  # noqa: S608
            except Exception as err:
                self._raise_if_stale(target, err)
                raise
            cols = [d[0] for d in (cur.description or [])]
            rows: list[dict[str, Any]] = []
            while True:
                chunk = cur.fetchmany(self.fetch_size)
                if not chunk:
                    break
                rows.extend(dict(zip(cols, r)) for r in chunk)

            events = rows_to_events(
                rows, target.source_object, list(table_def.primary_key),
                schema_name=self.source_schema,
            )
            failed_before = self.handler.stats.deltas_failed
            for event in events:
                self.handler.handle_event(event)
            failed = self.handler.stats.deltas_failed - failed_before
            if failed:
                raise BatchNotAppliedError(
                    f"{failed} change(s) from {target.stream_name} failed to apply; "
                    "the stream offset was not advanced and they will be read again."
                )

            cur.execute(
                f"INSERT INTO {quote_ident(OFFSET_SINK)} "  # noqa: S608
                f"SELECT 1 FROM {stream} WHERE 0 = 1"
            )
            cur.execute("COMMIT")
        except BaseException:
            try:
                cur.execute("ROLLBACK")
            except Exception as rb_err:  # noqa: BLE001
                logger.warning("snowflake_rollback_failed", error=str(rb_err))
            raise
        finally:
            cur.close()

        logger.info(
            "snowflake_stream_consumed",
            stream=target.stream_name, rows=len(rows), events=len(events),
            applied_through=_iso_utc(through),
        )
        return StreamWatermark(through, basis, events=len(events), rows_read=len(rows))

    def poll_once(self) -> CycleResult:
        """Consume every stream once, then record watermarks and freshness."""
        targets = self.targets or self.discover()
        result = CycleResult()
        errors: list[Exception] = []
        for t in targets:
            try:
                result.watermarks[t.stream_name] = self.consume(t)
            except (StreamStaleError, BatchNotAppliedError) as err:
                logger.error("snowflake_stream_cycle_failed", stream=t.stream_name, error=str(err))
                errors.append(err)
        result.collections_stamped = self.recorder.record(targets, result.watermarks)
        if errors:
            # Other streams' progress is recorded above; report the first failure.
            raise errors[0]
        return result

    # -- loop ---------------------------------------------------------------

    def run(self, *, once: bool = False) -> None:
        """Poll until stopped (SIGINT/SIGTERM or :meth:`stop`), or one cycle with ``once``."""
        if once:
            try:
                self.poll_once()
            finally:
                self.close()
            return

        self._running = True
        prev_int = signal.getsignal(signal.SIGINT)
        prev_term = signal.getsignal(signal.SIGTERM)

        def _stop(signum: int, _frame: Any) -> None:
            logger.info("snowflake_cdc_signal", signal=signum)
            self._running = False

        signal.signal(signal.SIGINT, _stop)
        signal.signal(signal.SIGTERM, _stop)
        try:
            while self._running:
                started = time.monotonic()
                try:
                    self.poll_once()
                except StreamStaleError:
                    raise
                except BatchNotAppliedError:
                    pass  # logged; retried next cycle
                except Exception as err:
                    logger.error("snowflake_cdc_cycle_error", error=str(err))
                    self.close()  # reconnect next cycle
                remaining = self.poll_interval - (time.monotonic() - started)
                while self._running and remaining > 0:
                    time.sleep(min(1.0, remaining))
                    remaining -= 1.0
        finally:
            signal.signal(signal.SIGINT, prev_int)
            signal.signal(signal.SIGTERM, prev_term)
            self.close()

    def stop(self) -> None:
        self._running = False
