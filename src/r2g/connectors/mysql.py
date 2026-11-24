"""MySQL / MariaDB source connector.

Provides:

- :class:`MySQLConnector` — schema introspection over ``information_schema``.
- :class:`MySQLSession` — bulk-read session implementing the
  :class:`r2g.connectors.session.SourceSession` Protocol: a consistent-snapshot
  transaction, server-side (unbuffered) cursor streaming, and a cursor-based
  CSV export. Both the streaming pipeline and ``source dump`` consume MySQL
  through this interface, exactly like PostgreSQL and Snowflake.

MariaDB is wire- and ``information_schema``-compatible with MySQL, so the same
connector serves both (``mysql://`` and ``mariadb://`` URLs both work).

Consistent snapshot
-------------------

InnoDB's ``REPEATABLE READ`` plus ``START TRANSACTION WITH CONSISTENT
SNAPSHOT`` gives the session a single point-in-time view across every table
read — the MySQL analog of PostgreSQL's ``SET TRANSACTION ISOLATION LEVEL
REPEATABLE READ`` that :class:`~r2g.streaming.pipeline.StreamingPipeline` has
required since day one.

Connection string format
------------------------

::

    mysql://<user>:<password>@<host>[:<port>]/<database>
    mariadb://<user>:<password>@<host>[:<port>]/<database>

MySQL has no schema namespace separate from the database, so the database in
the URL path *is* the introspection namespace. The connector's ``schema_name``
attribute therefore holds the database name. ``--pg-schema`` (passed as the
``schema_name`` constructor argument) overrides which database to introspect
when it names a real, non-default value; the historical ``public`` default is
treated as "use the database from the URL".

Missing ``pymysql``
-------------------

``pymysql`` is an optional dependency (``r2g-arango[mysql]``). It is never
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


def _load_pymysql() -> Any:
    """Import ``pymysql`` lazily with a helpful error.

    Centralising the import means both :class:`MySQLConnector` and
    :class:`MySQLSession` surface the same message when the optional extra is
    not installed.
    """
    try:
        import pymysql
    except ImportError as err:
        raise ImportError(
            "MySQL support requires pymysql. "
            "Install with: pip install 'r2g-arango[mysql]'"
        ) from err
    return pymysql


def _quote_ident(name: str) -> str:
    """Backtick-quote a MySQL identifier, escaping embedded backticks."""
    return "`" + name.replace("`", "``") + "`"


def _parse_mysql_url(url: str) -> dict[str, Any]:
    """Parse a ``mysql://`` / ``mariadb://`` URL into ``pymysql.connect`` kwargs.

    Returns ``host`` / ``port`` / ``user`` / ``password`` / ``database`` plus
    any recognised query parameters (e.g. ``charset``). Raises
    :class:`ValueError` for a malformed URL.
    """
    if not url or "://" not in url:
        raise ValueError(
            "MySQL connection string must look like "
            "mysql://user:pass@host[:port]/database"
        )
    parsed = urlparse(url)
    if parsed.scheme.lower() not in ("mysql", "mariadb"):
        raise ValueError(
            f"Expected a mysql:// or mariadb:// connection string, got scheme '{parsed.scheme}'"
        )

    user = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    host = parsed.hostname or ""
    if not user or not host:
        raise ValueError(
            "MySQL connection string must include user and host: "
            "mysql://user:pass@host/database"
        )

    database = (parsed.path or "").lstrip("/").split("/")[0]
    if not database:
        raise ValueError(
            "MySQL connection string is missing a database path component: "
            "mysql://user:pass@host/<database>"
        )

    query = {k: v[0] for k, v in parse_qs(parsed.query, keep_blank_values=True).items() if v}

    kwargs: dict[str, Any] = {
        "host": host,
        "user": user,
        "password": password,
        "database": database,
        "port": parsed.port or 3306,
        "charset": query.get("charset", "utf8mb4"),
    }
    return kwargs


class MySQLConnector:
    """MySQL / MariaDB source connector (introspection + session factory)."""

    def __init__(self, connection_string: str, schema_name: str = "") -> None:
        self.connection_string = connection_string
        self._connect_params = _parse_mysql_url(connection_string)
        url_database = self._connect_params["database"]
        # The database in the URL is the namespace; an explicit, non-default
        # schema_name overrides which database to introspect.
        if schema_name in _DEFAULT_SCHEMA_SENTINELS:
            self.schema_name = url_database
        else:
            self.schema_name = schema_name
            self._connect_params["database"] = schema_name

    def _connect(self) -> Any:
        pymysql = _load_pymysql()
        try:
            return pymysql.connect(
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True,
                **self._connect_params,
            )
        except Exception as err:
            raise connection_error(
                "Failed to connect to MySQL",
                err,
                secrets_in(self._connect_params, self.connection_string),
            ) from None

    def open_session(self) -> "MySQLSession":
        """Open a consistent-snapshot read session for streaming / dumps."""
        return MySQLSession(
            self.connection_string,
            schema_name=self.schema_name,
            connect_params=dict(self._connect_params),
        )

    def get_schema(self) -> Schema:
        """Introspect the MySQL schema and return a populated :class:`Schema`."""
        logger.info(
            "mysql_connect",
            host=self._connect_params.get("host"),
            port=self._connect_params.get("port"),
            database=self.schema_name,
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
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT TABLE_NAME
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
                """,
                (self.schema_name,),
            )
            table_names = [row["TABLE_NAME"] for row in cur.fetchall()]

        for table_name in table_names:
            schema.tables[table_name] = self._process_table(conn, table_name)
        return schema

    def _process_table(self, conn: Any, table_name: str) -> Table:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (self.schema_name, table_name),
            )
            columns_data = cur.fetchall()

            cur.execute(
                """
                SELECT COLUMN_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                  AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION
                """,
                (self.schema_name, table_name),
            )
            pks = [row["COLUMN_NAME"] for row in cur.fetchall()]

            cur.execute(
                """
                SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME,
                       CONSTRAINT_NAME
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                  AND REFERENCED_TABLE_NAME IS NOT NULL
                ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
                """,
                (self.schema_name, table_name),
            )
            fk_rows = cur.fetchall()

        columns = [
            Column(
                name=c["COLUMN_NAME"],
                data_type=(c["DATA_TYPE"] or "").lower(),
                is_nullable=(c["IS_NULLABLE"] == "YES"),
                is_primary_key=(c["COLUMN_NAME"] in pks),
            )
            for c in columns_data
        ]

        grouped: OrderedDict[str, dict[str, Any]] = OrderedDict()
        for fk in fk_rows:
            cname = fk["CONSTRAINT_NAME"]
            bucket = grouped.setdefault(
                cname,
                {
                    "columns": [],
                    "foreign_table": fk["REFERENCED_TABLE_NAME"],
                    "foreign_columns": [],
                    "constraint_name": cname,
                },
            )
            bucket["columns"].append(fk["COLUMN_NAME"])
            bucket["foreign_columns"].append(fk["REFERENCED_COLUMN_NAME"])

        fks = [ForeignKey(**v) for v in grouped.values()]

        return Table(
            name=table_name,
            columns=columns,
            primary_key=pks,
            foreign_keys=fks,
        )


class MySQLSession:
    """Bulk-read session for :class:`MySQLConnector`.

    Holds one ``autocommit=False`` connection running a single
    ``REPEATABLE READ`` + ``START TRANSACTION WITH CONSISTENT SNAPSHOT``
    transaction so every count / stream / dump during the session sees the same
    committed snapshot. Each instance owns its connection; call :meth:`close`
    when done. Parallel workers each open their own session.
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
            pymysql = _load_pymysql()
            params = dict(self._connect_params)
            params["autocommit"] = False
            try:
                self._conn = pymysql.connect(**params)
            except Exception as err:
                raise connection_error(
                    "Failed to connect to MySQL",
                    err,
                    secrets_in(self._connect_params, self.connection_string),
                ) from None
            with self._conn.cursor() as cur:
                cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.commit()
            except Exception:  # noqa: BLE001
                pass
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def __enter__(self) -> "MySQLSession":
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
        with conn.cursor() as cur:
            if since_column and since_value is not None:
                cur.execute(
                    f"SELECT COUNT(*) FROM {q} WHERE {_quote_ident(since_column)} >= %s",  # noqa: S608
                    (since_value,),
                )
            else:
                cur.execute(f"SELECT COUNT(*) FROM {q}")  # noqa: S608
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def stream_rows(
        self,
        table: str,
        *,
        batch_size: int = 10_000,
        since_column: Optional[str] = None,
        since_value: Optional[str] = None,
    ) -> Iterator[dict[str, Any]]:
        """Stream rows via an unbuffered (server-side) cursor.

        ``SSDictCursor`` pulls rows from the server incrementally rather than
        buffering the whole result set in the client, so wide / large tables do
        not blow up memory.
        """
        pymysql = _load_pymysql()
        q = self._qualified(table)
        conn = self.connection
        with conn.cursor(pymysql.cursors.SSDictCursor) as cur:
            if since_column and since_value is not None:
                cur.execute(
                    f"SELECT * FROM {q} WHERE {_quote_ident(since_column)} >= %s",  # noqa: S608
                    (since_value,),
                )
            else:
                cur.execute(f"SELECT * FROM {q}")  # noqa: S608
            yield from cur

    def dump_table_to_csv(
        self,
        table: str,
        out_path: Path,
        *,
        header: bool = True,
    ) -> int:
        """Export *table* as CSV via an unbuffered cursor.

        ``SELECT INTO OUTFILE`` would be faster but needs the ``FILE`` privilege
        and writes on the *server*; cursor streaming is portable and works for
        any table the user can ``SELECT``.
        """
        pymysql = _load_pymysql()
        q = self._qualified(table)
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        conn = self.connection
        total = 0
        with conn.cursor(pymysql.cursors.SSCursor) as cur:
            cur.execute(f"SELECT * FROM {q}")  # noqa: S608
            col_names = [d[0] for d in (cur.description or [])]
            with out_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f, lineterminator="\n")
                if header:
                    writer.writerow(col_names)
                for row in cur:
                    writer.writerow(["" if v is None else v for v in row])
                    total += 1
        return total


__all__ = ["MySQLConnector", "MySQLSession"]
