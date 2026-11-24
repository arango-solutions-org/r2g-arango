"""Microsoft SQL Server source connector.

Provides:

- :class:`SQLServerConnector` — schema introspection over ``INFORMATION_SCHEMA``
  (tables, columns, primary keys) plus ``sys.foreign_keys`` /
  ``sys.foreign_key_columns`` for declared foreign keys (the clean catalog path
  for composite FKs and their referenced columns).
- :class:`SQLServerSession` — bulk-read session implementing the
  :class:`r2g.connectors.session.SourceSession` Protocol: batched cursor
  streaming and a cursor-based CSV export. Both the streaming pipeline and
  ``source dump`` consume SQL Server through this interface, exactly like the
  other relational connectors.

Schema vs database
------------------

Like PostgreSQL (and unlike MySQL), SQL Server has schemas *inside* a database.
The database lives in the connection string; ``schema_name`` selects the
namespace and defaults to ``dbo``. ``--pg-schema`` (the ``schema_name``
constructor argument) overrides it; the historical ``public`` default is
treated as "use ``dbo``".

Read consistency
----------------

The session reads under SQL Server's default isolation (``READ COMMITTED``).
A true cross-table snapshot would require ``SNAPSHOT`` isolation, which in turn
requires ``ALLOW_SNAPSHOT_ISOLATION`` to be enabled on the database; forcing it
when it is off fails mid-read, so r2g does not force it. Enable snapshot
isolation server-side if you need a point-in-time view across tables.

Connection string format
------------------------

::

    mssql://<user>:<password>@<host>[:<port>]/<database>
    sqlserver://<user>:<password>@<host>[:<port>]/<database>

Default port is 1433.

Missing ``pymssql``
-------------------

``pymssql`` is an optional dependency (``r2g-arango[sqlserver]``). It is never
imported at module-import time; the first introspection / read raises
:class:`ImportError` with a pip-install hint so the UI / MCP server can surface
a clean message.
"""

from __future__ import annotations

import csv
from collections import OrderedDict
from pathlib import Path
from typing import Any, Iterator, Optional
from urllib.parse import parse_qs, unquote, urlparse

from r2g.log import get_logger
from r2g.security import connection_error, secrets_in
from r2g.types import Column, ForeignKey, Schema, Table

logger = get_logger(__name__)

_DEFAULT_SCHEMA_SENTINELS = frozenset({None, "", "public", "PUBLIC"})

# SQL Server `bit` is a boolean (0/1), but the shared type map treats `bit` as a
# PostgreSQL bit-string (→ string). Translate it connector-side so the stored
# data_type carries the correct SQL Server semantics.
_MSSQL_TYPE_OVERRIDES: dict[str, str] = {"bit": "boolean"}


def _load_pymssql() -> Any:
    """Import ``pymssql`` lazily with a helpful error."""
    try:
        import pymssql
    except ImportError as err:
        raise ImportError(
            "SQL Server support requires pymssql. "
            "Install with: pip install 'r2g-arango[sqlserver]'"
        ) from err
    return pymssql


def _quote_ident(name: str) -> str:
    """Bracket-quote a SQL Server identifier, escaping embedded ``]``."""
    return "[" + name.replace("]", "]]") + "]"


def _parse_mssql_url(url: str) -> dict[str, Any]:
    """Parse an ``mssql://`` / ``sqlserver://`` URL into ``pymssql.connect`` kwargs.

    Returns ``server`` / ``port`` / ``user`` / ``password`` / ``database``.
    A ``+driver`` suffix on the scheme (e.g. ``mssql+pymssql``) is tolerated.
    Raises :class:`ValueError` for a malformed URL.
    """
    if not url or "://" not in url:
        raise ValueError(
            "SQL Server connection string must look like "
            "mssql://user:pass@host[:port]/database"
        )
    parsed = urlparse(url)
    scheme = parsed.scheme.lower().split("+", 1)[0]
    if scheme not in ("mssql", "sqlserver"):
        raise ValueError(
            f"Expected an mssql:// or sqlserver:// connection string, got scheme '{parsed.scheme}'"
        )

    user = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    host = parsed.hostname or ""
    if not user or not host:
        raise ValueError(
            "SQL Server connection string must include user and host: "
            "mssql://user:pass@host/database"
        )

    database = (parsed.path or "").lstrip("/").split("/")[0]
    if not database:
        raise ValueError(
            "SQL Server connection string is missing a database path component: "
            "mssql://user:pass@host/<database>"
        )

    # Reserved for future tuning knobs (e.g. tds_version); parsed but unused.
    parse_qs(parsed.query, keep_blank_values=True)

    return {
        "server": host,
        "user": user,
        "password": password,
        "database": database,
        "port": parsed.port or 1433,
    }


class SQLServerConnector:
    """Microsoft SQL Server source connector (introspection + session factory)."""

    def __init__(self, connection_string: str, schema_name: str = "dbo") -> None:
        self.connection_string = connection_string
        self._connect_params = _parse_mssql_url(connection_string)
        self._database = self._connect_params["database"]
        self.schema_name = "dbo" if schema_name in _DEFAULT_SCHEMA_SENTINELS else schema_name

    def _connect(self) -> Any:
        pymssql = _load_pymssql()
        try:
            return pymssql.connect(**self._connect_params)
        except Exception as err:
            raise connection_error(
                "Failed to connect to SQL Server",
                err,
                secrets_in(self._connect_params, self.connection_string),
            ) from None

    def open_session(self) -> "SQLServerSession":
        """Open a bulk-read session for streaming / dumps."""
        return SQLServerSession(
            self.connection_string,
            schema_name=self.schema_name,
            connect_params=dict(self._connect_params),
        )

    def get_schema(self) -> Schema:
        """Introspect the SQL Server schema and return a populated :class:`Schema`."""
        logger.info(
            "mssql_connect",
            server=self._connect_params.get("server"),
            port=self._connect_params.get("port"),
            database=self._database,
            schema=self.schema_name,
        )
        conn = self._connect()
        try:
            return self._introspect(conn)
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

    def _introspect(self, conn: Any) -> Schema:
        schema = Schema()
        cur = conn.cursor(as_dict=True)
        try:
            cur.execute(
                """
                SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
                """,
                (self.schema_name,),
            )
            table_names = [row["TABLE_NAME"] for row in cur.fetchall()]
        finally:
            cur.close()

        for table_name in table_names:
            schema.tables[table_name] = self._process_table(conn, table_name)
        return schema

    def _process_table(self, conn: Any, table_name: str) -> Table:
        cur = conn.cursor(as_dict=True)
        try:
            cur.execute(
                """
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (self.schema_name, table_name),
            )
            columns_data = cur.fetchall()

            cur.execute(
                """
                SELECT kcu.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                 AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                  AND tc.TABLE_SCHEMA = %s AND tc.TABLE_NAME = %s
                ORDER BY kcu.ORDINAL_POSITION
                """,
                (self.schema_name, table_name),
            )
            pks = [row["COLUMN_NAME"] for row in cur.fetchall()]

            # sys.* catalog views give referenced table/column + composite
            # ordering directly (INFORMATION_SCHEMA does not expose the
            # referenced columns cleanly on SQL Server).
            cur.execute(
                """
                SELECT fk.name              AS constraint_name,
                       cpar.name            AS column_name,
                       rt.name              AS foreign_table_name,
                       cref.name            AS foreign_column_name
                FROM sys.foreign_keys fk
                JOIN sys.foreign_key_columns fkc ON fkc.constraint_object_id = fk.object_id
                JOIN sys.tables t   ON t.object_id = fk.parent_object_id
                JOIN sys.schemas s  ON s.schema_id = t.schema_id
                JOIN sys.columns cpar ON cpar.object_id = fk.parent_object_id
                                     AND cpar.column_id = fkc.parent_column_id
                JOIN sys.tables rt  ON rt.object_id = fk.referenced_object_id
                JOIN sys.columns cref ON cref.object_id = fk.referenced_object_id
                                     AND cref.column_id = fkc.referenced_column_id
                WHERE s.name = %s AND t.name = %s
                ORDER BY fk.name, fkc.constraint_column_id
                """,
                (self.schema_name, table_name),
            )
            fk_rows = cur.fetchall()
        finally:
            cur.close()

        columns = []
        for c in columns_data:
            raw_type = (c["DATA_TYPE"] or "").lower()
            columns.append(
                Column(
                    name=c["COLUMN_NAME"],
                    data_type=_MSSQL_TYPE_OVERRIDES.get(raw_type, raw_type),
                    is_nullable=(c["IS_NULLABLE"] == "YES"),
                    is_primary_key=(c["COLUMN_NAME"] in pks),
                )
            )

        grouped: OrderedDict[str, dict[str, Any]] = OrderedDict()
        for fk in fk_rows:
            cname = fk["constraint_name"]
            bucket = grouped.setdefault(
                cname,
                {
                    "columns": [],
                    "foreign_table": fk["foreign_table_name"],
                    "foreign_columns": [],
                    "constraint_name": cname,
                },
            )
            bucket["columns"].append(fk["column_name"])
            bucket["foreign_columns"].append(fk["foreign_column_name"])

        fks = [ForeignKey(**v) for v in grouped.values()]

        return Table(
            name=table_name,
            columns=columns,
            primary_key=pks,
            foreign_keys=fks,
        )


class SQLServerSession:
    """Bulk-read session for :class:`SQLServerConnector`.

    Holds one connection reused across every count / stream / dump. Reads run
    under the server's default isolation (see the module docstring on snapshot
    isolation). Each instance owns its connection; call :meth:`close` when done.
    Parallel workers each open their own session.
    """

    def __init__(
        self,
        connection_string: str,
        *,
        schema_name: str,
        connect_params: dict[str, Any],
    ) -> None:
        self.connection_string = connection_string
        self.schema_name = schema_name
        self._connect_params = dict(connect_params)
        self._conn: Any = None

    @property
    def connection(self) -> Any:
        if self._conn is None:
            pymssql = _load_pymssql()
            try:
                self._conn = pymssql.connect(**self._connect_params)
            except Exception as err:
                raise connection_error(
                    "Failed to connect to SQL Server",
                    err,
                    secrets_in(self._connect_params, self.connection_string),
                ) from None
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def __enter__(self) -> "SQLServerSession":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _qualified(self, table: str) -> str:
        return f"{_quote_ident(self.schema_name)}.{_quote_ident(table)}"

    def count_rows(
        self,
        table: str,
        *,
        since_column: Optional[str] = None,
        since_value: Optional[str] = None,
    ) -> int:
        q = self._qualified(table)
        conn = self.connection
        cur = conn.cursor()
        try:
            if since_column and since_value is not None:
                cur.execute(
                    f"SELECT COUNT(*) FROM {q} WHERE {_quote_ident(since_column)} >= %s",  # noqa: S608
                    (since_value,),
                )
            else:
                cur.execute(f"SELECT COUNT(*) FROM {q}")  # noqa: S608
            row = cur.fetchone()
            return int(row[0]) if row else 0
        finally:
            cur.close()

    def stream_rows(
        self,
        table: str,
        *,
        batch_size: int = 10_000,
        since_column: Optional[str] = None,
        since_value: Optional[str] = None,
    ) -> Iterator[dict[str, Any]]:
        """Stream rows in ``batch_size`` chunks via ``fetchmany``."""
        q = self._qualified(table)
        conn = self.connection
        cur = conn.cursor(as_dict=True)
        try:
            if since_column and since_value is not None:
                cur.execute(
                    f"SELECT * FROM {q} WHERE {_quote_ident(since_column)} >= %s",  # noqa: S608
                    (since_value,),
                )
            else:
                cur.execute(f"SELECT * FROM {q}")  # noqa: S608
            fetch = max(1, batch_size)
            while True:
                rows = cur.fetchmany(fetch)
                if not rows:
                    break
                yield from rows
        finally:
            cur.close()

    def dump_table_to_csv(
        self,
        table: str,
        out_path: Path,
        *,
        header: bool = True,
    ) -> int:
        """Export *table* as CSV through the cursor."""
        q = self._qualified(table)
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        conn = self.connection
        cur = conn.cursor()
        total = 0
        try:
            cur.execute(f"SELECT * FROM {q}")  # noqa: S608
            col_names = [d[0] for d in (cur.description or [])]
            with out_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f, lineterminator="\n")
                if header:
                    writer.writerow(col_names)
                while True:
                    rows = cur.fetchmany(10_000)
                    if not rows:
                        break
                    for row in rows:
                        writer.writerow(["" if v is None else v for v in row])
                        total += 1
        finally:
            cur.close()
        return total


__all__ = ["SQLServerConnector", "SQLServerSession"]
