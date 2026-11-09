"""Abstraction over relational source connectors.

R2G started as a PostgreSQL-only tool; `PostgresConnector` was the single
concrete implementation with a hand-rolled constructor signature. PRD
Phase 6 (Snowflake) calls out an explicit "source abstraction layer"
(P6.5) so the schema-reader, dump, and streaming paths can accept other
relational sources behind a common interface.

Shared vs. local
----------------

The source-agnostic *helpers* were extracted to ``relational-schema-analyzer``
(RSA) and are byte-identical there, so r2g imports the single shared definitions
rather than duplicating them:

- `expand_env_vars`, `normalize_source_type`, `is_postgresql` / `is_mysql` /
  `is_sqlserver`, `serialize_rows` — the source-type/credential helpers used
  across the CLI, UI, and MCP paths.

What stays **local to r2g** (and why):

- `SourceConnector` — the structural `Protocol` every connector satisfies
  (introspect via :meth:`~SourceConnector.get_schema`, bulk-read via
  :meth:`~SourceConnector.open_session`). Kept local because its
  :meth:`~SourceConnector.get_schema` is typed to r2g's ``Schema`` subclass (RSA's
  identical protocol returns the ``PhysicalSchema`` base), which r2g's callers
  (snapshotting, classification annotate, schema diff) rely on.
- `SUPPORTED_SOURCE_TYPES` — r2g's registry includes ``kafka`` (an r2g-only
  connector) and omits RSA's analysis-only ``duckdb`` / ``databricks`` sources.
- `create_source_connector` — the factory dispatches to r2g's concrete
  connectors (including ``kafka``) and their bulk-read sessions, which drive
  r2g's data-migration path and are intentionally kept local (see
  ``docs/internal/DESIGN-rsa-compat-layer.md``, Stage 2 close-out).
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

# Source-agnostic helpers are shared with RSA (byte-identical; verified by the
# connector test suite). Re-exported here so the historical
# ``from r2g.connectors.base import ...`` import path is unchanged.
from relational_schema_analyzer.connectors.base import expand_env_vars as expand_env_vars
from relational_schema_analyzer.connectors.base import is_mysql as is_mysql
from relational_schema_analyzer.connectors.base import is_postgresql as is_postgresql
from relational_schema_analyzer.connectors.base import is_sqlserver as is_sqlserver
from relational_schema_analyzer.connectors.base import normalize_source_type as normalize_source_type
from relational_schema_analyzer.connectors.base import serialize_rows as serialize_rows

from r2g.connectors.session import SourceSession
from r2g.types import Schema


@runtime_checkable
class SourceConnector(Protocol):
    """Structural interface every R2G source connector must satisfy.

    Identical in shape to ``relational_schema_analyzer.connectors.base.SourceConnector``
    but kept local so :meth:`get_schema` is typed to r2g's ``Schema`` subclass
    (whose byte-stable serialization the snapshot/catalog paths depend on) rather
    than RSA's ``PhysicalSchema`` base.
    """

    connection_string: str
    schema_name: str

    def get_schema(self) -> Schema:
        """Introspect the upstream source and return a populated :class:`Schema`."""
        ...

    def open_session(self) -> SourceSession:
        """Open a bulk-read session with consistent-snapshot semantics.

        Each call returns a fresh session; callers are expected to call
        :meth:`SourceSession.close` when done. Parallel workers each
        obtain their own session so they do not contend for a single
        cursor.
        """
        ...


SUPPORTED_SOURCE_TYPES: tuple[str, ...] = (
    "postgresql",
    "mysql",
    "sqlserver",
    "snowflake",
    "clickhouse",
    "csv",
    "kafka",
)


def create_source_connector(
    source_type: str,
    connection_string: str,
    schema_name: str = "public",
    *,
    source_params: dict | None = None,
) -> SourceConnector:
    """Return a connector matching ``source_type``.

    Concrete classes are imported lazily so that users who never touch a
    given source type do not pay for its optional dependency. Unknown /
    unsupported types raise :class:`ValueError`; missing optional deps
    raise :class:`ImportError` with a pip-install hint.

    ``source_params`` carries type-specific configuration (e.g. CSV
    ``delimiter`` / ``has_header`` / ``null_values``, Kafka ``schema_registry_url`` /
    ``topic``). PostgreSQL and Snowflake ignore it.
    """

    key = normalize_source_type(source_type)
    params = source_params or {}
    # Resolve $VAR / ${VAR} credential references (env / r2g secrets) so the
    # catalog never has to store secrets and imported sources connect cleanly.
    connection_string = expand_env_vars(connection_string)
    if key == "postgresql":
        from r2g.connectors.postgres import PostgresConnector

        return PostgresConnector(connection_string, schema_name=schema_name)
    if key == "mysql":
        from r2g.connectors.mysql import MySQLConnector

        return MySQLConnector(connection_string, schema_name=schema_name)
    if key == "sqlserver":
        from r2g.connectors.mssql import SQLServerConnector

        return SQLServerConnector(connection_string, schema_name=schema_name)
    if key == "snowflake":
        from r2g.connectors.snowflake import SnowflakeConnector

        return SnowflakeConnector(connection_string, schema_name=schema_name)
    if key == "clickhouse":
        from r2g.connectors.clickhouse import ClickHouseConnector

        return ClickHouseConnector(connection_string, schema_name=schema_name)
    if key == "csv":
        from r2g.connectors.csv_source import CsvConnector, null_values_param

        return CsvConnector(
            connection_string,
            schema_name=schema_name,
            delimiter=params.get("delimiter", ","),
            has_header=bool(params.get("has_header", True)),
            null_values=null_values_param(params),
        )
    if key == "kafka":
        from r2g.connectors.kafka_source import KafkaConnector

        return KafkaConnector(
            connection_string,
            schema_registry_url=params.get("schema_registry_url", ""),
            topic=params.get("topic", ""),
            subject=params.get("subject"),
            schema_name=schema_name,
        )
    raise ValueError(
        f"Unsupported source type '{source_type}'. "
        f"Expected one of: {', '.join(SUPPORTED_SOURCE_TYPES)}."
    )
