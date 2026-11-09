"""Foreign-key inference for schemas without declared constraints.

Snowflake does not enforce FK constraints and many Snowflake schemas
have none declared at all (PRD P6.6). Even a PostgreSQL dump or a
warehouse-landed table may have lost its referential metadata. This
module provides a *pure-Python* heuristic engine that suggests probable
FKs so the Mapping Studio / CLI can show them as "confirm to accept"
candidates without silently inventing graph topology.

Design
------

The inference engine, options, and the ``InferredForeignKey`` model are shared
with ``relational-schema-analyzer`` (RSA extracted them from r2g). This module is
a thin r2g-facing layer over that shared core:

- :func:`infer_foreign_keys` wraps RSA's engine and re-wraps its results as r2g's
  :class:`InferredForeignKey` (which adds :meth:`~InferredForeignKey.to_edge_definition`,
  the ArangoDB materialization that stays in r2g).
- The concrete value samplers subclass RSA's samplers to add
  :meth:`sample_values` (r2g's Phase-10c value-sampling probe, consumed by
  ``r2g.llm.sampling``). The FK-overlap ``__call__`` and the Phase-11
  denormalization probes are inherited unchanged.
- :func:`create_value_sampler` builds the r2g sampler subclasses for a source.

See ``docs/internal/DESIGN-rsa-compat-layer.md`` (Stage 2, steps 4–5).

The engine returns :class:`InferredForeignKey` objects sorted by ``confidence``
descending. The public API is intentionally small — callers that want to persist
accepted suggestions use the existing ``EdgeDefinition`` model (see
:meth:`InferredForeignKey.to_edge_definition`).
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

# The heuristic engine, options, the InferredForeignKey model, and the concrete
# value samplers are shared with ``relational-schema-analyzer`` (RSA extracted them
# from r2g). r2g keeps only: a thin InferredForeignKey subclass that adds the
# ArangoDB ``to_edge_definition`` conversion, a wrapper that re-wraps RSA's results
# as that subclass, and sampler subclasses that add ``sample_values``. See
# ``docs/internal/DESIGN-rsa-compat-layer.md`` (Stage 2, steps 4–5).
from relational_schema_analyzer.fk_inference import CsvValueSampler as _RsaCsvValueSampler
from relational_schema_analyzer.fk_inference import InferenceMethod as InferenceMethod  # noqa: F401
from relational_schema_analyzer.fk_inference import InferenceOptions as InferenceOptions  # noqa: F401
from relational_schema_analyzer.fk_inference import InferredForeignKey as _RsaInferredForeignKey
from relational_schema_analyzer.fk_inference import MySQLValueSampler as _RsaMySQLValueSampler
from relational_schema_analyzer.fk_inference import PostgresValueSampler as _RsaPostgresValueSampler
from relational_schema_analyzer.fk_inference import Sampler as Sampler  # noqa: F401
from relational_schema_analyzer.fk_inference import SamplerResult as SamplerResult  # noqa: F401
from relational_schema_analyzer.fk_inference import SQLServerValueSampler as _RsaSQLServerValueSampler
from relational_schema_analyzer.fk_inference import infer_foreign_keys as _rsa_infer_foreign_keys
from relational_schema_analyzer.overlay import OVERLAY_CONSTRAINT_PREFIX as OVERLAY_CONSTRAINT_PREFIX

from r2g.log import get_logger
from r2g.naming import edge_collection_name as _edge_name
from r2g.types import EdgeDefinition, ForeignKey, Schema

#: Name prefix for a foreign key a person accepted from inference
#: (``source infer-fks --accept``). Kept beside RSA's overlay prefix because the two
#: mark the same thing: a key asserted on top of the source rather than declared by
#: it. Both are saved ``enforced=False`` -- labelled, not laundered -- so nothing
#: downstream mistakes a guess for a constraint the database guarantees.
INFERRED_CONSTRAINT_PREFIX = "inferred:"


def key_provenance(constraint_name: Optional[str]) -> str:
    """Where a foreign key came from, read from its name.

    ``curated-overlay`` (a reviewed overlay), ``inferred`` (accepted from
    ``infer-fks``), or ``source-declared`` (anything else: the catalog's own).
    """
    name = constraint_name or ""
    if name.startswith(OVERLAY_CONSTRAINT_PREFIX):
        return "curated-overlay"
    if name.startswith(INFERRED_CONSTRAINT_PREFIX):
        return "inferred"
    return "source-declared"

logger = get_logger(__name__)


# ── Public models ────────────────────────────────────────────────────


class InferredForeignKey(_RsaInferredForeignKey):
    """An FK candidate — RSA's shared model plus r2g's ArangoDB edge conversion.

    The inference *engine* (:func:`infer_foreign_keys` and its heuristics) lives in
    ``relational-schema-analyzer``. r2g subclasses RSA's ``InferredForeignKey`` only
    to add :meth:`to_edge_definition`, the ArangoDB-specific materialization that
    stays in r2g (RSA's relational analogue, ``to_foreign_key``, produces a
    declared :class:`~relational_schema_analyzer.types.ForeignKey` instead).
    """

    def to_accepted_foreign_key(self) -> ForeignKey:
        """This candidate as the key ``infer-fks --accept`` writes into a snapshot.

        ``enforced=False``, and named ``inferred:<table>_<cols>_fk`` the way the
        overlay names its keys, so each accepted key has its own name and reads as
        asserted rather than declared. RSA's ``to_foreign_key`` leaves ``enforced`` at
        its default of ``True``, which the serializer then omits -- erasing the only
        record that the key was a guess.
        """
        return ForeignKey(
            columns=list(self.columns),
            foreign_table=self.foreign_table,
            foreign_columns=list(self.foreign_columns),
            constraint_name=f"{INFERRED_CONSTRAINT_PREFIX}{self.table}_{'_'.join(self.columns)}_fk",
            enforced=False,
        )

    def accepted_record(self) -> dict[str, Any]:
        """What the key cannot carry: how it was inferred and how confidently.

        ``ForeignKey`` has no field for either, so the snapshot keeps one of these
        per accepted key (``SchemaSnapshot.inferred_keys``).
        """
        return {
            "table": self.table,
            "columns": list(self.columns),
            "foreign_table": self.foreign_table,
            "foreign_columns": list(self.foreign_columns),
            "method": self.method,
            "confidence": round(float(self.confidence), 3),
        }

    def to_edge_definition(self, edge_collection: Optional[str] = None) -> EdgeDefinition:
        """Convert the suggestion to an :class:`EdgeDefinition` for mapping.

        The default edge-collection name follows R2G's existing
        convention (``<table>_to_<foreign_table>``) so inferred edges
        live alongside declared ones with a consistent namespace.
        """
        name = edge_collection or _edge_name(self.table, self.foreign_table)
        return EdgeDefinition(
            edge_collection=name,
            from_collection=self.table,
            to_collection=self.foreign_table,
            from_fields=list(self.columns),
            to_fields=list(self.foreign_columns),
        )


# ── Entry point ─────────────────────────────────────────────────────


def infer_foreign_keys(
    schema: Schema,
    *,
    options: Optional[InferenceOptions] = None,
    sampler: Optional[Sampler] = None,
) -> list[InferredForeignKey]:
    """Return ranked FK candidates for ``schema``.

    Thin wrapper over the shared
    ``relational_schema_analyzer.fk_inference.infer_foreign_keys`` engine that
    re-wraps each result as r2g's :class:`InferredForeignKey` so callers keep the
    :meth:`to_edge_definition` convenience. r2g's ``Schema`` is an RSA
    ``PhysicalSchema`` subclass, so it is passed straight through; results are
    deduplicated, ranked by confidence, and filtered by ``options.min_confidence``
    exactly as the shared engine specifies.
    """
    return [
        InferredForeignKey.model_validate(ifk.model_dump())
        for ifk in _rsa_infer_foreign_keys(schema, options=options, sampler=sampler)
    ]


# ── Classification gate (Phase 9) ────────────────────────────────────


class GatedSampler:
    """Wraps a value sampler so classified columns are never probed.

    FK value-overlap sampling reads real rows. It emits only a ratio, so values
    never leave the process — but reading a Restricted/PII column at all is a
    policy decision, and r2g already answers it one way at load time (excluded
    unless ``--allow-sensitive``). This makes the sampling path answer it the
    same way instead of having no opinion.

    Wrapping rather than changing the engine keeps the gate in r2g: the
    inference engine itself lives in ``relational-schema-analyzer`` and has no
    concept of classification. Any probe touching an excluded column returns
    ``None``, which every caller already treats as "no measurement available"
    and degrades to name-only scoring.
    """

    def __init__(self, inner: Any, excluded: frozenset[str]) -> None:
        self._inner = inner
        self._excluded = excluded

    def _blocked(self, table: str, *columns: str) -> bool:
        return any(
            c in self._excluded or f"{table}.{c}" in self._excluded for c in columns
        )

    def __call__(self, local_table, local_column, foreign_table, foreign_column):
        # The FK-overlap probe: block if EITHER side is classified, since the
        # overlap statistic is computed from both.
        if self._blocked(local_table, local_column) or self._blocked(
            foreign_table, foreign_column
        ):
            logger.debug(
                "fk_sample_skipped_classified",
                table=local_table, column=local_column,
                foreign_table=foreign_table, foreign_column=foreign_column,
            )
            return None
        return self._inner(local_table, local_column, foreign_table, foreign_column)

    def distinct_ratio(self, table: str, column: str):
        if self._blocked(table, column):
            return None
        return self._inner.distinct_ratio(table, column)

    def group_single_valued(self, table, determinant_columns, dependent_column):
        if self._blocked(table, *determinant_columns, dependent_column):
            return None
        return self._inner.group_single_valued(table, determinant_columns, dependent_column)

    def delimiter_rate(self, table: str, column: str, delimiter: str):
        if self._blocked(table, column):
            return None
        return self._inner.delimiter_rate(table, column, delimiter)

    def sample_values(self, table: str, column: str, limit: int = 5) -> list:
        if self._blocked(table, column):
            return []
        return self._inner.sample_values(table, column, limit)

    def __getattr__(self, name: str) -> Any:
        # Anything not explicitly gated passes through, so the wrapper cannot
        # silently drop a probe added later — but a NEW value probe would then
        # be ungated, which is why every known probe is listed above.
        return getattr(self._inner, name)


def gated_sampler(
    sampler: Any,
    schema: Optional[Schema] = None,
    *,
    allow_sensitive: bool = False,
    threshold: str = "confidential",
) -> Any:
    """Wrap ``sampler`` with the Phase-9 gate unless explicitly allowed."""
    if sampler is None or allow_sensitive or schema is None:
        return sampler
    from r2g.classification import sensitive_columns

    excluded = sensitive_columns(schema, threshold=threshold)
    if not excluded:
        return sampler
    logger.info("fk_sampler_gated", excluded_columns=len(excluded), threshold=threshold)
    return GatedSampler(sampler, excluded)


# ── Concrete value samplers ─────────────────────────────────────────
#
# The FK-overlap ``__call__`` and the Phase-11 denormalization probes
# (``distinct_ratio`` / ``group_single_valued`` / ``delimiter_rate``) are shared
# with RSA and inherited unchanged. r2g adds only ``sample_values`` — a bounded
# "return N distinct non-null values" probe used by ``r2g.llm.sampling`` for
# LLM-assisted ontology derivation, which RSA's samplers do not carry.


class PostgresValueSampler(_RsaPostgresValueSampler):
    """RSA's PostgreSQL FK/denorm sampler plus r2g's ``sample_values`` probe."""

    def sample_values(self, table: str, column: str, limit: int = 5) -> list:
        """Return up to ``limit`` distinct non-null values from a column (or [])."""
        n = max(1, int(limit))
        q = f"""
            SELECT DISTINCT "{column}" AS v
            FROM "{self.schema_name}"."{table}"
            WHERE "{column}" IS NOT NULL
            LIMIT %s
        """  # noqa: S608 - identifiers are quoted; schema is from the catalog
        try:
            conn = self._conn_lazy()
            with conn.cursor() as cur:
                cur.execute(q, (n,))
                return [r[0] for r in cur.fetchall()]
        except Exception as err:  # noqa: BLE001
            logger.warning("sample_values_failed", table=table, column=column, error=str(err))
            try:
                if self._conn is not None:
                    self._conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            return []


class MySQLValueSampler(_RsaMySQLValueSampler):
    """RSA's MySQL / MariaDB FK/denorm sampler plus r2g's ``sample_values`` probe."""

    def sample_values(self, table: str, column: str, limit: int = 5) -> list:
        """Return up to ``limit`` distinct non-null values from a column (or [])."""
        n = max(1, int(limit))
        db = self._qi(self.schema_name)
        q = f"""
            SELECT DISTINCT {self._qi(column)} AS v
            FROM {db}.{self._qi(table)}
            WHERE {self._qi(column)} IS NOT NULL
            LIMIT %s
        """  # noqa: S608 - identifiers are backtick-quoted; db is from the catalog
        try:
            conn = self._conn_lazy()
            with conn.cursor() as cur:
                cur.execute(q, (n,))
                return [r[0] for r in cur.fetchall()]
        except Exception as err:  # noqa: BLE001
            logger.warning("sample_values_failed", table=table, column=column, error=str(err))
            return []


class SQLServerValueSampler(_RsaSQLServerValueSampler):
    """RSA's SQL Server FK/denorm sampler plus r2g's ``sample_values`` probe."""

    def sample_values(self, table: str, column: str, limit: int = 5) -> list:
        """Return up to ``limit`` distinct non-null values from a column (or [])."""
        n = max(1, int(limit))
        s = self._qi(self.schema_name)
        q = f"""
            SELECT DISTINCT TOP (%s) {self._qi(column)} AS v
            FROM {s}.{self._qi(table)}
            WHERE {self._qi(column)} IS NOT NULL
        """  # noqa: S608 - identifiers are bracket-quoted; schema is from the catalog
        try:
            conn = self._conn_lazy()
            cur = conn.cursor()
            try:
                cur.execute(q, (n,))
                return [r[0] for r in cur.fetchall()]
            finally:
                cur.close()
        except Exception as err:  # noqa: BLE001
            logger.warning("sample_values_failed", table=table, column=column, error=str(err))
            return []


class CsvValueSampler(_RsaCsvValueSampler):
    """RSA's CSV FK/denorm sampler plus r2g's ``sample_values`` probe.

    ``sample_values`` leaves out the source's null markers (``\\N`` by default),
    as the CSV source reads them. RSA's own probes read them as values.
    """

    def __init__(self, *args: Any, null_values: Optional[Sequence[str]] = None, **kwargs: Any):
        from r2g.connectors.csv_source import DEFAULT_NULL_VALUES

        super().__init__(*args, **kwargs)
        self.null_values = tuple(DEFAULT_NULL_VALUES if null_values is None else null_values)

    def sample_values(self, table: str, column: str, limit: int = 5) -> list:
        """Return up to ``limit`` distinct non-null values from a column (or [])."""
        import polars as pl

        frame = self._read_columns(table, [column])
        if frame is None or not frame.columns:
            return []
        name = frame.columns[0]
        frame = frame.filter(
            pl.col(name).is_not_null()
            & (pl.col(name) != "")
            & ~pl.col(name).is_in(list(self.null_values))
        )
        series = frame.get_column(name)
        if series.len() == 0:
            return []
        return series.unique(maintain_order=True).head(max(1, int(limit))).to_list()


def create_value_sampler(
    source_type: str | None,
    connection_string: str,
    *,
    pg_schema: str = "public",
    source_params: dict | None = None,
    limit: int = 10_000,
):
    """Build a value-overlap sampler for a source, or ``None`` if unsupported.

    PostgreSQL (incl. ``postgres`` / ``pg`` aliases) → :class:`PostgresValueSampler`,
    MySQL / MariaDB → :class:`MySQLValueSampler`, SQL Server →
    :class:`SQLServerValueSampler`, CSV → :class:`CsvValueSampler`; any other
    type returns ``None`` (the caller should fall back to name-only inference).
    Connector / import errors are allowed to propagate so callers can decide
    whether to log-and-continue.
    """
    from r2g.connectors.base import (
        expand_env_vars,
        is_mysql,
        is_postgresql,
        is_sqlserver,
        normalize_source_type,
    )

    params = source_params or {}
    # Resolve $VAR credential references the same way create_source_connector does.
    connection_string = expand_env_vars(connection_string)
    if is_postgresql(source_type):
        return PostgresValueSampler(connection_string, schema_name=pg_schema, limit=limit)
    if is_mysql(source_type):
        return MySQLValueSampler(connection_string, schema_name=pg_schema, limit=limit)
    if is_sqlserver(source_type):
        return SQLServerValueSampler(connection_string, schema_name=pg_schema, limit=limit)
    if normalize_source_type(source_type) == "csv":
        from r2g.connectors.csv_source import null_values_param

        return CsvValueSampler(
            connection_string,
            delimiter=params.get("delimiter", ","),
            has_header=bool(params.get("has_header", True)),
            limit=limit,
            null_values=null_values_param(params),
        )
    return None
