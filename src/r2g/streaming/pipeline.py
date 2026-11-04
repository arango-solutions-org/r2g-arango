"""Streaming pipeline: reads from any supported source and writes directly to ArangoDB.

The read path is source-agnostic: any object satisfying the
:class:`r2g.connectors.base.SourceConnector` Protocol can drive the
pipeline (PostgreSQL via :class:`PostgresConnector`, Snowflake via
:class:`SnowflakeConnector`). Each worker opens its own
:class:`r2g.connectors.session.SourceSession` with consistent-snapshot
semantics (PG ``REPEATABLE READ`` / Snowflake ``BEGIN``) so parallelism
does not break read consistency.
"""

from __future__ import annotations

import datetime
import decimal
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import TYPE_CHECKING, Any, Callable, Optional, cast

from r2g.connectors.arango_writer import ArangoWriter, ImportBatchError

if TYPE_CHECKING:
    from r2g.dlq import DeadLetterQueue
from r2g.connectors.base import SourceConnector
from r2g.connectors.session import SourceSession
from r2g.log import get_logger
from r2g.topo_sort import topological_sort_tables
from r2g.transformers.edge_transformer import EdgeTransformer
from r2g.transformers.node_transformer import NodeTransformer
from r2g.types import MappingConfig, Schema

logger = get_logger(__name__)

ProgressFn = Callable[[str, str, int, int | None], None]
EventFn = Callable[[dict[str, Any]], None]


def _json_safe(value: Any) -> Any:
    """Coerce a raw source value into something AQL/JSON can carry.

    Used when projecting source rows for server-side expression delegation:
    dates become ISO strings (AQL date functions accept these), decimals
    become floats, and any other exotic type falls back to ``str``.
    """
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", "replace")
    return str(value)


class DelegationError(ValueError):
    """ArangoDB cannot evaluate an expression the streaming path would delegate to it."""


class StreamingPipeline:
    """Orchestrates streaming data from a relational source to ArangoDB.

    The pipeline is driven by a :class:`SourceConnector`; each worker
    (1 by default, ``workers`` in parallel mode) calls
    :meth:`SourceConnector.open_session` for its own consistent
    snapshot.

    When ``dry_run=True`` the pipeline connects to both the source and
    ArangoDB (validating credentials), reads and transforms all rows,
    but skips writes and graph creation.  Returns counts and sample
    documents for pre-flight validation.

    An optional *on_progress* callback receives ``(event, name, current, total)``
    where *event* is ``"start"`` / ``"progress"`` / ``"done"``; *name* is the
    collection name; *current* is rows processed so far; *total* is the
    estimated row count (may be ``None`` if unavailable).

    Backward compatibility
    ----------------------

    Previous callers passed ``pg_conn_string=...`` (PostgreSQL only).
    That keyword still works — the pipeline wraps it in a
    :class:`PostgresConnector` automatically. New callers should pass
    ``source_connector=...`` directly, which supports any source type.
    """

    PREVIEW_LIMIT = 3

    def __init__(
        self,
        *,
        arango_writer: ArangoWriter,
        schema: Schema,
        config: MappingConfig,
        source_connector: Optional[SourceConnector] = None,
        pg_conn_string: Optional[str] = None,
        batch_size: int = 10_000,
        on_duplicate: str = "replace",
        pg_schema: str = "public",
        dry_run: bool = False,
        drop_collections: bool = False,
        workers: int = 1,
        include_tables: set[str] | None = None,
        exclude_tables: set[str] | None = None,
        skip_existing: bool = False,
        since: str | None = None,
        since_column: str | None = None,
        dlq: "DeadLetterQueue | None" = None,
    ) -> None:
        if source_connector is None and pg_conn_string is None:
            raise ValueError(
                "StreamingPipeline requires either source_connector or pg_conn_string"
            )

        if source_connector is None:
            # Backward-compat shim: legacy callers pass only pg_conn_string.
            from r2g.connectors.postgres import PostgresConnector

            source_connector = PostgresConnector(
                pg_conn_string,  # type: ignore[arg-type]
                schema_name=pg_schema,
            )

        self.source_connector: SourceConnector = source_connector
        self.pg_conn_string = pg_conn_string  # retained for logging / compat
        self.writer = arango_writer
        self.schema = schema
        self.config = config
        self.batch_size = batch_size
        self.on_duplicate = on_duplicate
        self.pg_schema = pg_schema
        self.dry_run = dry_run
        self.drop_collections = drop_collections
        self.workers = max(1, workers)
        self.include_tables: set[str] | None = include_tables
        self.exclude_tables: set[str] | None = exclude_tables
        self.skip_existing = skip_existing
        self.since = since
        self.since_column = since_column
        self.dlq = dlq
        self.skipped: list[str] = []
        self.previews: dict[str, list[dict[str, Any]]] = {}
        self.errors: dict[str, list[str]] = {}
        self._on_progress: ProgressFn | None = None
        self._on_event: EventFn | None = None
        self._lock = __import__("threading").Lock()

    # ── Filtering / skip-existing ──────────────────────────────────

    @property
    def _lpg(self) -> Any:
        """The :class:`LpgLayout` when this project opted in, else ``None``."""
        return self.config.lpg if self.config.graph_layout == "lpg" else None

    def _labels_for_source_table(self, source_table: str) -> list[str] | None:
        """The full LPG label set for the collection mapping a table lands in."""
        for cm in self.config.collections.values():
            if cm.source_table == source_table:
                return cm.labels
        return None

    def _prepare_lpg_collections(self) -> None:
        """Create (and optionally drop) the two shared LPG collections **once**.

        The pg path prepares a collection per table, which is safe because each
        table owns its own. In the LPG layout one node collection and one edge
        collection are shared by every type, so doing this per table would make
        each table's ``--drop-collections`` erase the tables loaded before it,
        and would make ``--skip-existing`` skip every table after the first
        wrote a row. Both fail silently — you get a partial graph, not an error
        — so the preparation is hoisted here and skipped in the per-table path.

        Indexes are provisioned at the same time: the edge collection is created
        with ``edge=True`` (as a document collection it succeeds quietly, then
        graph creation fails ERR 1942/1944 and traversals return nothing).
        """
        from r2g.lpg import edge_indexes, vertex_indexes

        lpg = self.config.lpg
        if self.drop_collections:
            # Edges first: they reference the nodes.
            self.writer.drop_collection(lpg.edge_collection)
            self.writer.drop_collection(lpg.node_collection)
        self.writer.ensure_indexes(lpg.node_collection, vertex_indexes(lpg), edge=False)
        self.writer.ensure_indexes(lpg.edge_collection, edge_indexes(lpg), edge=True)
        logger.info(
            "lpg_collections_prepared",
            node_collection=lpg.node_collection,
            edge_collection=lpg.edge_collection,
        )

    def _should_skip_collection(self, writer: ArangoWriter, collection_name: str) -> bool:
        if not self.skip_existing:
            return False
        try:
            db = writer.db
            if db.has_collection(collection_name):
                # cast: the sync database returns the count directly
                # (python-arango's Result union covers async/batch execution).
                count = cast(int, db.collection(collection_name).count())
                if count > 0:
                    logger.info(
                        "skip_existing_collection",
                        collection=collection_name,
                        count=count,
                    )
                    with self._lock:
                        self.skipped.append(collection_name)
                    return True
        except Exception:
            pass
        return False

    def _should_include_table(self, table_name: str) -> bool:
        if self.include_tables is not None and table_name not in self.include_tables:
            return False
        if self.exclude_tables is not None and table_name in self.exclude_tables:
            return False
        return True

    def _resolve_since_column(self, table_name: str) -> str | None:
        if self.since is None:
            return None
        table = self.schema.tables.get(table_name)
        if table is None:
            return None
        col_names = {c.name for c in table.columns}
        if self.since_column:
            return self.since_column if self.since_column in col_names else None
        for candidate in ("updated_at", "modified_at", "last_modified", "created_at"):
            if candidate in col_names:
                return candidate
        return None

    # ── Write path ─────────────────────────────────────────────────

    def _record_dlq(self, collection: str, details: list[str]) -> None:
        """Persist per-record failure details to the dead-letter queue.

        No-op when no DLQ is attached. Writing must never abort the load,
        so any failure to record is swallowed and logged.
        """
        if self.dlq is None or not details:
            return
        try:
            for detail in details:
                self.dlq.record_failure(collection, {}, str(detail))
        except Exception:  # noqa: BLE001
            logger.warning("dlq_record_failed", collection=collection)

    def _flush_batch(
        self,
        writer: ArangoWriter,
        collection: str,
        batch: list[dict[str, Any]],
    ) -> None:
        try:
            writer.import_batch(collection, batch, self.on_duplicate)
        except ImportBatchError as exc:
            with self._lock:
                errs = self.errors.setdefault(collection, [])
                errs.extend(exc.details[: 50 - len(errs)])
            self._record_dlq(collection, exc.details)
            logger.warning(
                "import_batch_partial_errors",
                collection=collection,
                error_count=exc.error_count,
                total_count=exc.total_count,
            )

    def _apply_delegation(
        self,
        writer: ArangoWriter,
        query: str,
        docs: list[dict[str, Any]],
        raw_rows: list[dict[str, Any]],
        collection: str,
    ) -> bool:
        """Fill server-delegated expression fields for a batch in place.

        Runs the delegated-expression AQL query over the projected source
        rows and merges each returned object into the matching document.
        Returns False when it fails: the batch's documents then go to the
        dead-letter queue with the reason and are not imported, since a
        document is never loaded without a field its mapping defines.
        """
        if not raw_rows:
            return True
        try:
            results = writer.execute_aql(query, {"rows": raw_rows})
            if len(results) != len(docs) or not all(isinstance(r, dict) for r in results):
                raise ValueError(f"expected {len(docs)} objects, got {len(results)} results")
        except Exception as exc:  # noqa: BLE001
            reason = f"AQL delegation failed, batch not loaded: {exc}"
            with self._lock:
                errs = self.errors.setdefault(collection, [])
                if len(errs) < 50:
                    errs.append(reason)
            self._reject_batch(collection, docs, reason)
            logger.warning(
                "aql_delegation_failed",
                collection=collection,
                rows=len(docs),
                error=str(exc),
            )
            return False
        for doc, computed in zip(docs, results):
            doc.update(computed)
        return True

    def _reject_batch(self, collection: str, docs: list[dict[str, Any]], reason: str) -> None:
        """Record each document of a batch that will not be imported in the DLQ."""
        if self.dlq is None:
            return
        try:
            for doc in docs:
                self.dlq.record_failure(collection, doc, reason)
        except Exception:  # noqa: BLE001
            logger.warning("dlq_record_failed", collection=collection)

    def _check_delegations(self) -> None:
        """Refuse the load before anything is written if ArangoDB cannot evaluate an
        expression the streaming path would delegate to it (an unknown function, or
        syntax it rejects), naming the collection and the field. Each expression is
        run once over no rows, which parses it without reading or writing data."""
        for table_name, cm in self._iter_document_jobs():
            transformer = NodeTransformer(
                self.schema.tables[table_name],
                collection_mapping=cm,
                key_separator=self.config.key_separator,
                type_overrides=self.config.type_overrides,
                lpg=self._lpg,
            )
            for fx in transformer.delegated_expressions:
                try:
                    self.writer.execute_aql(transformer.build_delegation_query([fx]), {"rows": []})
                except Exception as exc:  # noqa: BLE001
                    raise DelegationError(
                        f"Collection '{cm.target_collection}', field '{fx.target}': ArangoDB "
                        f"cannot evaluate {fx.expression!r} ({exc}). Nothing was loaded."
                    ) from exc

    def _notify(self, event: str, name: str, current: int, total: int | None) -> None:
        if self._on_progress is not None:
            self._on_progress(event, name, current, total)

    def _emit(self, event_data: dict[str, Any]) -> None:
        if self._on_event is not None:
            self._on_event(event_data)

    def _make_writer(self) -> ArangoWriter:
        return ArangoWriter(
            endpoint=self.writer.endpoint,
            database=self.writer.database_name,
            username=self.writer.username,
            password=self.writer.password,
            max_retries=self.writer.max_retries,
        )

    # ── Per-collection streaming ──────────────────────────────────

    def _stream_one_document(
        self,
        table_name: str,
        cm: Any,
        session: SourceSession,
        writer: Optional[ArangoWriter] = None,
    ) -> tuple[str, int]:
        table_def = self.schema.tables[table_name]
        target = cm.target_collection
        lpg = self._lpg
        # In the LPG layout every type shares one collection, so writes go there
        # while ``target`` stays the *label* — which keeps the per-table
        # progress and result reporting intact.
        write_to = lpg.node_collection if lpg is not None else target
        w = writer or self.writer

        # LPG collections are prepared once in run(); doing it per table would
        # let one table's drop erase the previous tables (see
        # _prepare_lpg_collections).
        if not self.dry_run and lpg is None:
            if self._should_skip_collection(w, target):
                return (target, 0)
            if self.drop_collections:
                w.drop_collection(target)
            w.ensure_collection(target, edge=False)

        if not table_def.primary_key:
            logger.warning(
                "table_has_no_primary_key",
                table=table_name,
                target=target,
                hint="Documents will receive auto-generated _key values; edges referencing this table may fail",
            )

        transformer = NodeTransformer(
            table_def,
            collection_mapping=cm,
            key_separator=self.config.key_separator,
            type_overrides=self.config.type_overrides,
            # ``label`` defaults to cm.target_collection — the per-type name.
            lpg=lpg,
        )

        since_col = self._resolve_since_column(table_name)
        batch: list[dict[str, Any]] = []
        total = 0
        samples: list[dict[str, Any]] = []
        row_estimate = session.count_rows(
            table_name,
            since_column=since_col,
            since_value=self.since if since_col else None,
        )

        delegate = transformer.has_delegated_expressions and not self.dry_run
        delegation_query = transformer.build_delegation_query() if delegate else ""
        ref_cols = transformer.delegated_reference_columns() if delegate else set()
        raw_batch: list[dict[str, Any]] = []
        if delegate:
            logger.info("aql_delegation_enabled", target=target, query=delegation_query)

        logger.info("stream_documents_start", table=table_name, target=target, rows=row_estimate)
        self._notify("start", target, 0, row_estimate)
        self._emit({"event": "start", "collection": target, "type": "document", "estimated_rows": row_estimate})

        for row in session.stream_rows(
            table_name,
            batch_size=self.batch_size,
            since_column=since_col,
            since_value=self.since if since_col else None,
        ):
            doc = transformer.transform_row(row)
            if len(samples) < self.PREVIEW_LIMIT:
                samples.append(doc)
            batch.append(doc)
            if delegate:
                raw_batch.append({c: _json_safe(row.get(c)) for c in ref_cols})
            if len(batch) >= self.batch_size:
                if not self.dry_run:
                    if not delegate or self._apply_delegation(
                            w, delegation_query, batch, raw_batch, target):
                        self._flush_batch(w, write_to, batch)
                total += len(batch)
                batch.clear()
                raw_batch.clear()
                self._notify("progress", target, total, row_estimate)
                self._emit({"event": "progress", "collection": target, "rows": total, "estimated_rows": row_estimate})

        if batch:
            if not self.dry_run:
                if not delegate or self._apply_delegation(
                        w, delegation_query, batch, raw_batch, target):
                    self._flush_batch(w, write_to, batch)
            total += len(batch)

        if self.dry_run:
            with self._lock:
                self.previews[target] = samples

        self._notify("done", target, total, row_estimate)
        self._emit({"event": "done", "collection": target, "rows": total})
        logger.info("stream_documents_done", target=target, count=total)
        return (target, total)

    def _stream_one_edge(
        self,
        edge_def: Any,
        session: SourceSession,
        writer: Optional[ArangoWriter] = None,
    ) -> tuple[str, int]:
        table_name = edge_def.from_collection
        table_def = self.schema.tables[table_name]
        edge_name = edge_def.edge_collection
        lpg = self._lpg
        # One edge collection holds every type; ``edge_name`` stays the type.
        write_to = lpg.edge_collection if lpg is not None else edge_name
        w = writer or self.writer

        if not self.dry_run and lpg is None:
            if self._should_skip_collection(w, edge_name):
                return (edge_name, 0)
            if self.drop_collections:
                w.drop_collection(edge_name)
            w.ensure_collection(edge_name, edge=True)

        from r2g.config import ConfigManager

        target_by_source = ConfigManager.target_by_source_table(self.config)
        transformer = EdgeTransformer(
            edge_def,
            table_def,
            key_separator=self.config.key_separator,
            from_name=target_by_source.get(edge_def.from_collection),
            to_name=target_by_source.get(edge_def.to_collection),
            # The endpoints' FULL label sets, so the copies on the edge match
            # what the node documents carry (P13.13). Passing only the primary
            # label would make filters on any additional label silently miss.
            from_labels=self._labels_for_source_table(edge_def.from_collection),
            to_labels=self._labels_for_source_table(edge_def.to_collection),
            lpg=lpg,
        )

        since_col = self._resolve_since_column(table_name)
        batch: list[dict[str, Any]] = []
        total = 0
        samples: list[dict[str, Any]] = []
        row_estimate = session.count_rows(
            table_name,
            since_column=since_col,
            since_value=self.since if since_col else None,
        )

        logger.info("stream_edges_start", table=table_name, edge=edge_name, rows=row_estimate)
        self._notify("start", edge_name, 0, row_estimate)
        self._emit({"event": "start", "collection": edge_name, "type": "edge", "estimated_rows": row_estimate})

        for row in session.stream_rows(
            table_name,
            batch_size=self.batch_size,
            since_column=since_col,
            since_value=self.since if since_col else None,
        ):
            doc = transformer.transform_row(row)
            if doc is not None:
                if len(samples) < self.PREVIEW_LIMIT:
                    samples.append(doc)
                batch.append(doc)
                if len(batch) >= self.batch_size:
                    if not self.dry_run:
                        self._flush_batch(w, write_to, batch)
                    total += len(batch)
                    batch.clear()
                    self._notify("progress", edge_name, total, row_estimate)
                    self._emit({
                        "event": "progress", "collection": edge_name,
                        "rows": total, "estimated_rows": row_estimate,
                    })

        if batch:
            if not self.dry_run:
                self._flush_batch(w, write_to, batch)
            total += len(batch)

        if self.dry_run:
            with self._lock:
                self.previews[edge_name] = samples

        self._notify("done", edge_name, total, row_estimate)
        self._emit({"event": "done", "collection": edge_name, "rows": total})
        logger.info("stream_edges_done", edge=edge_name, count=total)
        return (edge_name, total)

    # ── Phase orchestration ────────────────────────────────────────

    def _iter_document_jobs(self) -> list[tuple[str, Any]]:
        ordered, cycles = topological_sort_tables(self.schema)
        if cycles:
            for cycle in cycles:
                logger.warning(
                    "circular_fk_dependency",
                    cycle=" -> ".join(cycle),
                    hint="Import order may not satisfy all FK dependencies",
                )
        cm_by_table = {cm.source_table: cm for cm in self.config.collections.values()}
        jobs: list[tuple[str, Any]] = []
        for table_name in ordered:
            cm = cm_by_table.get(table_name)
            if cm is None or cm.collection_type != "document":
                continue
            if cm.source_table not in self.schema.tables:
                logger.warning("stream_skip_unknown_table", table=cm.source_table)
                continue
            if not self._should_include_table(cm.source_table):
                logger.info("stream_skip_filtered_table", table=cm.source_table)
                continue
            jobs.append((cm.source_table, cm))
        return jobs

    def _iter_edge_jobs(self) -> list[tuple[Any]]:
        jobs: list[tuple[Any]] = []
        for edge_def in self.config.edges:
            if edge_def.from_collection not in self.schema.tables:
                logger.warning("stream_edge_skip_unknown_table", table=edge_def.from_collection)
                continue
            if not self._should_include_table(edge_def.from_collection):
                logger.info("stream_edge_skip_filtered_table", table=edge_def.from_collection)
                continue
            jobs.append((edge_def,))
        return jobs

    def _stream_documents(self, session: SourceSession) -> list[tuple[str, int]]:
        return [
            self._stream_one_document(table_name, cm, session=session)
            for (table_name, cm) in self._iter_document_jobs()
        ]

    def _stream_edges(self, session: SourceSession) -> list[tuple[str, int]]:
        return [
            self._stream_one_edge(edge_def, session=session)
            for (edge_def,) in self._iter_edge_jobs()
        ]

    def _run_parallel_phase(
        self,
        jobs: list[tuple[Any, ...]],
        phase_fn: str,
    ) -> list[tuple[str, int]]:
        """Run *jobs* in parallel, each on its own source session + ArangoDB writer."""
        results: list[tuple[str, int]] = []

        def worker(job_args: tuple) -> tuple[str, int]:
            w = self._make_writer()
            if not self.dry_run:
                w.connect()
            session = self.source_connector.open_session()
            try:
                if phase_fn == "doc":
                    result = self._stream_one_document(
                        job_args[0], job_args[1], session=session, writer=w
                    )
                else:
                    result = self._stream_one_edge(
                        job_args[0], session=session, writer=w
                    )
            finally:
                try:
                    session.close()
                except Exception:  # noqa: BLE001
                    pass
            if not self.dry_run:
                w.close()
            return result

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = {pool.submit(worker, j): j for j in jobs}
            for future in as_completed(futures):
                results.append(future.result())

        return results

    def _run_parallel(
        self,
        graph_name: str | None,
    ) -> tuple[list[tuple[str, int]], list[tuple[str, int]]]:
        logger.info("parallel_streaming", workers=self.workers)
        doc_jobs = [(t, cm) for (t, cm) in self._iter_document_jobs()]
        edge_jobs = self._iter_edge_jobs()
        doc_results = self._run_parallel_phase(doc_jobs, "doc")
        edge_results = self._run_parallel_phase(edge_jobs, "edge")
        return doc_results, edge_results

    def run(
        self,
        graph_name: str | None = None,
        on_progress: ProgressFn | None = None,
        on_event: EventFn | None = None,
    ) -> dict[str, Any]:
        """Execute the full streaming pipeline.

        Single-worker mode opens one :class:`SourceSession` and reads
        documents then edges through it. Multi-worker mode opens one
        session per in-flight job (fresh snapshot per worker, matching
        the existing PG behaviour).
        """
        self._on_progress = on_progress
        self._on_event = on_event
        t0 = time.monotonic()
        if not self.dry_run:
            self.writer.ensure_database()
        self.writer.connect()
        if not self.dry_run:
            self._check_delegations()

        # Drop the existing named graph before any collections: ArangoDB will
        # not drop a collection while it is part of a graph (ERR 1942). The
        # graph is recreated from the current edge definitions at the end.
        if not self.dry_run and self.drop_collections and graph_name:
            self.writer.drop_named_graph(graph_name)

        # Hoisted out of the per-table path on purpose — see
        # _prepare_lpg_collections. Covers both the sequential and parallel
        # paths below, and provisions the vertex-centric indexes up front.
        if not self.dry_run and self._lpg is not None:
            self._prepare_lpg_collections()

        if self.workers > 1:
            doc_results, edge_results = self._run_parallel(graph_name)
        else:
            session = self.source_connector.open_session()
            try:
                logger.info("source_snapshot_started", source_type=type(self.source_connector).__name__)
                doc_results = self._stream_documents(session)
                edge_results = self._stream_edges(session)
            finally:
                session.close()

        if not self.dry_run and graph_name:
            from r2g.config import ConfigManager

            edge_defs = ConfigManager.graph_edge_definitions(self.config)
            self.writer.create_named_graph(graph_name, edge_defs)

        self.writer.close()
        elapsed = time.monotonic() - t0

        self._emit({
            "event": "complete",
            "documents": len(doc_results),
            "edges": len(edge_results),
            "total_rows": sum(r[1] for r in doc_results) + sum(r[1] for r in edge_results),
            "elapsed_seconds": elapsed,
            "errors": sum(len(e) for e in self.errors.values()),
        })

        result: dict[str, Any] = {
            "documents": doc_results,
            "edges": edge_results,
            "elapsed_seconds": elapsed,
        }
        if self.errors:
            result["errors"] = self.errors
        if self.skipped:
            result["skipped"] = self.skipped
        return result
