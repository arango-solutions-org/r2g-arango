"""ClickHouse source connector — federation mapping **and** ETL.

Two capabilities, both on the shared ``SourceConnector`` protocol:

- :meth:`ClickHouseConnector.get_schema` introspects ``system.tables`` /
  ``system.columns`` so r2g can produce the conceptual model + R2RML/CSI for a
  ClickHouse source (feeds the fabric's federation leg — see
  ``contextual-data-fabric`` ``cdf.adapters.clickhouse.ClickHouseExecutor``).
- :meth:`ClickHouseConnector.open_session` returns a :class:`ClickHouseSession`
  with ``count_rows`` / ``stream_rows`` / ``dump_table_to_csv``, so r2g can
  **migrate** (ETL) a ClickHouse source into ArangoDB like any other source.

ClickHouse has no separate schema namespace (the *database* is the namespace) and
no enforced FK/PK — the "primary key" is the table's sorting key, surfaced via
``system.columns.is_in_primary_key``; foreign keys come from r2g's FK-inference,
not the engine. The driver (``clickhouse-connect``) is an optional extra and is
imported lazily.
"""

from __future__ import annotations

import csv
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from r2g.connectors.base import quote_backtick
from r2g.log import get_logger
from r2g.types import Column, Schema, Table

logger = get_logger(__name__)

_WRAPPER_RE = re.compile(r"^(?:Nullable|LowCardinality)\((.*)\)$")


def _clean_type(ch_type: str) -> str:
    """Strip ``Nullable(...)`` / ``LowCardinality(...)`` wrappers, lower-case."""
    t = ch_type.strip()
    while True:
        m = _WRAPPER_RE.match(t)
        if not m:
            break
        t = m.group(1).strip()
    return t.lower()


def _database_of(connection_string: str) -> str:
    """The ClickHouse database (namespace) from a ``clickhouse://…/db`` DSN."""
    path = urlparse(connection_string).path.strip("/")
    return path or "default"


def _get_client(connection_string: str) -> Any:
    import clickhouse_connect  # lazy: optional extra

    return clickhouse_connect.get_client(dsn=connection_string)


class ClickHouseConnector:
    """ClickHouse source connector (introspection + ETL session)."""

    def __init__(
        self,
        connection_string: str,
        schema_name: str = "default",
        *,
        client: Any | None = None,
    ) -> None:
        self.connection_string = connection_string
        self._database = _database_of(connection_string)
        # ClickHouse namespace == database; expose it as schema_name for the
        # SourceConnector protocol (r2g's Schema.source_schema, etc.).
        self.schema_name = self._database
        self._client = client  # injected in tests; else built lazily

    def _client_or_build(self) -> Any:
        return self._client if self._client is not None else _get_client(self.connection_string)

    def open_session(self) -> ClickHouseSession:
        return ClickHouseSession(
            self.connection_string, database=self._database, client=self._client
        )

    def get_schema(self) -> Schema:
        client = self._client_or_build()
        logger.info("clickhouse_introspect", database=self._database)
        schema = Schema()
        tables = client.query(
            "SELECT name FROM system.tables WHERE database = {db:String} "
            "AND engine NOT LIKE '%View%' AND NOT is_temporary ORDER BY name",
            parameters={"db": self._database},
        ).result_rows
        for (table_name,) in tables:
            schema.tables[table_name] = self._process_table(client, table_name)
        return schema

    def _process_table(self, client: Any, table_name: str) -> Table:
        rows = client.query(
            "SELECT name, type, is_in_primary_key FROM system.columns "
            "WHERE database = {db:String} AND table = {tbl:String} ORDER BY position",
            parameters={"db": self._database, "tbl": table_name},
        ).result_rows
        columns: list[Column] = []
        pks: list[str] = []
        for name, ch_type, in_primary_key in rows:
            columns.append(
                Column(
                    name=name,
                    data_type=_clean_type(ch_type),
                    is_nullable=str(ch_type).startswith("Nullable("),
                    is_primary_key=bool(in_primary_key),
                )
            )
            if in_primary_key:
                pks.append(name)
        return Table(name=table_name, columns=columns, primary_key=pks, foreign_keys=[])


class ClickHouseSession:
    """Bulk-read session for ETL from ClickHouse into ArangoDB."""

    def __init__(
        self, connection_string: str, *, database: str, client: Any | None = None
    ) -> None:
        self.connection_string = connection_string
        self.schema_name = database
        self._database = database
        self._client = client

    def _client_or_build(self) -> Any:
        if self._client is None:
            self._client = _get_client(self.connection_string)
        return self._client

    def _qualified(self, table: str) -> str:
        return f"{quote_backtick(self._database)}.{quote_backtick(table)}"

    def close(self, *, abort: bool = False) -> None:
        if self._client is None:
            return
        try:
            self._client.close()
        except Exception:  # noqa: BLE001 — best-effort close
            pass
        self._client = None

    def __enter__(self) -> ClickHouseSession:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def count_rows(
        self,
        table: str,
        *,
        since_column: str | None = None,
        since_value: str | None = None,
    ) -> int:
        sql = f"SELECT count() FROM {self._qualified(table)}"  # noqa: S608 — identifier is backtick-quoted
        params: dict[str, Any] = {}
        if since_column and since_value is not None:
            sql += f" WHERE {quote_backtick(since_column)} >= {{since:String}}"
            params["since"] = since_value
        rows = self._client_or_build().query(sql, parameters=params).result_rows
        return int(rows[0][0]) if rows else 0

    def stream_rows(
        self,
        table: str,
        *,
        batch_size: int = 10_000,
        since_column: str | None = None,
        since_value: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Stream rows in server-side blocks (never materialises the table)."""
        qualified = self._qualified(table)
        sql = f"SELECT * FROM {qualified}"  # noqa: S608 — identifier is backtick-quoted
        params: dict[str, Any] = {}
        if since_column and since_value is not None:
            sql += f" WHERE {quote_backtick(since_column)} >= {{since:String}}"
            params["since"] = since_value
        client = self._client_or_build()
        columns = [r[0] for r in client.query(f"DESCRIBE TABLE {qualified}").result_rows]
        with client.query_row_block_stream(sql, parameters=params) as stream:
            for block in stream:
                for row in block:
                    yield dict(zip(columns, row))

    def dump_table_to_csv(self, table: str, out_path: Path, *, header: bool = True) -> int:
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        columns: list[str] = []
        total = 0
        with out_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f, lineterminator="\n")
            for row in self.stream_rows(table):
                if not columns:
                    columns = list(row.keys())
                    if header:
                        writer.writerow(columns)
                writer.writerow(["" if row[c] is None else row[c] for c in columns])
                total += 1
        return total
