"""Snowflake source connector (Phase 6, slices 1 + 3).

Slice 1 (``get_schema``) reads ``INFORMATION_SCHEMA`` and the
account-level views for declared foreign keys. Slice 3 (P6.3 + P6.4)
adds :meth:`SnowflakeConnector.open_session` returning a
:class:`SnowflakeSession` that implements the
:class:`r2g.connectors.session.SourceSession` Protocol: consistent-snapshot
transaction, cursor-based streaming, and a
``SELECT * FROM … ORDER BY NULL`` CSV export path.

Snowflake does not expose a user-facing ``REPEATABLE READ`` isolation
level, but any multi-statement transaction (``BEGIN`` … ``COMMIT``)
reads a consistent snapshot for its lifetime. The session opens exactly
one such transaction and reuses it across every count / stream / dump
call.

Connection string format
------------------------

R2G stores a single ``connection_string`` per source. For Snowflake we
accept the Snowflake SQLAlchemy URL shape (which is also the format
published by Snowflake themselves)::

    snowflake://<user>:<password>@<account>/<database>[/<schema>]
        ?warehouse=<wh>&role=<role>[&authenticator=<auth>]

For key-pair authentication, leave the password empty and provide the same
connector arguments used by Contextual Data Fabric::

    snowflake://<user>:@<account>/<database>[/<schema>]
        ?warehouse=<wh>&role=<role>&private_key_file=<path>
        [&private_key_file_pwd=<passphrase>]

- ``account`` is the Snowflake account identifier (e.g.
  ``xy12345.us-east-1``); the host portion of the URL.
- ``database`` is required — it identifies the ``INFORMATION_SCHEMA`` we
  introspect.
- ``schema`` is optional inside the URL. If absent the value passed as
  ``schema_name`` to the constructor wins (default ``PUBLIC``).
- ``warehouse``, ``role``, ``authenticator``, ``application``,
  ``private_key_file`` and ``private_key_file_pwd`` are forwarded to
  ``snowflake.connector.connect`` after auth validation.
- ``include_views=true`` lists the schema's views and materialized views
  beside its tables, so a schema whose joinable objects are views (a
  Customer 360 built from ``*_VW`` objects, say) can be snapshotted and
  loaded. Off by default: every r2g connector reads base tables only, and
  a view declares no keys, so its keys come from a reviewed overlay or FK
  inference. Dynamic tables are base tables to ``INFORMATION_SCHEMA`` and
  are always listed. The flag is consumed by r2g, never forwarded.
- Password and key-pair authentication are mutually exclusive. Key-pair auth
  sets ``authenticator="SNOWFLAKE_JWT"`` and lets the Snowflake driver read the
  key file; r2g never reads or persists key bytes.

Example::

    snowflake://svc_r2g:xxx@xy12345.us-east-1/ANALYTICS/CORE
        ?warehouse=ETL_WH&role=R2G_READER

Missing ``snowflake-connector-python``
--------------------------------------

The package is an *optional* dependency (``r2g-arango[snowflake]``). We never
import it at module import time; the first attempt to introspect a
Snowflake source raises :class:`ImportError` with a pip-install hint so
the UI / MCP server can surface a clean message.
"""

from __future__ import annotations

import csv
from collections import OrderedDict
from pathlib import Path
from typing import Any, Iterator, Optional
from urllib.parse import parse_qs, unquote, urlparse

from r2g.log import get_logger
from r2g.security import scrub_dsn_credentials
from r2g.types import Column, ForeignKey, Schema, Table

logger = get_logger(__name__)

_SENSITIVE_CONNECT_FIELDS = ("password", "private_key_file", "private_key_file_pwd")

#: Accepted spellings of the ``include_views`` URL flag. Blank means absent.
_BOOL_WORDS = {
    "": False, "true": True, "false": False, "1": True, "0": False, "yes": True, "no": False,
}

#: What a snapshot lists: base tables always (dynamic tables are base tables to
#: ``INFORMATION_SCHEMA``), views and materialized views only when asked.
_TABLE_TYPES = ("BASE TABLE",)
_TABLE_AND_VIEW_TYPES = ("BASE TABLE", "VIEW", "MATERIALIZED VIEW")


def _load_snowflake_connector() -> Any:
    """Import ``snowflake.connector`` lazily with a helpful error.

    Centralising the import means both :class:`SnowflakeConnector` and
    :class:`SnowflakeSession` surface the same message when the
    optional extra is not installed.
    """
    try:
        import snowflake.connector as _sf
    except ImportError as err:
        raise ImportError(
            "Snowflake support requires snowflake-connector-python. "
            "Install with: pip install 'r2g-arango[snowflake]'"
        ) from err
    return _sf


class SnowflakeConnector:
    """Snowflake source connector (introspection only)."""

    def __init__(self, connection_string: str, schema_name: str = "PUBLIC") -> None:
        self.connection_string = connection_string
        self.schema_name = schema_name.upper() if schema_name else "PUBLIC"
        self._connect_params = _parse_snowflake_url(connection_string)
        url_schema = self._connect_params.pop("_url_schema", None)
        self.include_views = bool(self._connect_params.pop("_include_views", False))
        if url_schema and (schema_name in (None, "", "public", "PUBLIC")):
            self.schema_name = url_schema.upper()
        self._database: str = self._connect_params.get("database", "")
        if not self._database:
            raise ValueError(
                "Snowflake connection string must include a database "
                "(snowflake://user:pass@account/<DATABASE>[/<SCHEMA>])"
            )
        self._database = self._database.upper()

    def open_session(self) -> "SnowflakeSession":
        """Open a Snowflake read session with consistent-snapshot semantics."""
        return SnowflakeSession(
            self.connection_string,
            database=self._database,
            schema_name=self.schema_name,
            connect_params=dict(self._connect_params),
        )

    def get_schema(self) -> Schema:
        """Introspect the Snowflake schema and return a populated :class:`Schema`."""
        snowflake = _load_snowflake_connector()

        connect_kwargs = dict(self._connect_params)
        connect_kwargs.setdefault("schema", self.schema_name)

        logger.info(
            "snowflake_connect",
            account=connect_kwargs.get("account"),
            database=self._database,
            schema=self.schema_name,
            warehouse=connect_kwargs.get("warehouse"),
            role=connect_kwargs.get("role"),
        )

        try:
            conn = snowflake.connect(**connect_kwargs)
        except Exception as err:
            raise RuntimeError(
                f"Failed to connect to Snowflake: {_safe_connection_error(err, connect_kwargs)}"
            ) from err

        try:
            return self._introspect(conn)
        finally:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

    def _introspect(self, conn: Any) -> Schema:
        schema = Schema()
        table_types = _TABLE_AND_VIEW_TYPES if self.include_views else _TABLE_TYPES
        placeholders = ", ".join("%s" for _ in table_types)
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT TABLE_NAME, TABLE_TYPE
                FROM {self._database}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_TYPE IN ({placeholders})
                ORDER BY TABLE_NAME
                """,
                (self.schema_name, *table_types),
            )
            table_rows = cur.fetchall()
        finally:
            cur.close()

        for row in table_rows:
            table_name, table_type = row[0], row[1]
            schema.tables[table_name] = self._process_table(
                conn, table_name, is_view=table_type != "BASE TABLE"
            )

        return schema

    def _process_table(self, conn: Any, table_name: str, *, is_view: bool = False) -> Table:
        columns = self._fetch_columns(conn, table_name)
        pks = [] if is_view else self._fetch_primary_key(conn, table_name)
        fks = [] if is_view else self._fetch_foreign_keys(conn, table_name)

        for col in columns:
            col.is_primary_key = col.name in pks

        return Table(
            name=table_name,
            columns=columns,
            primary_key=pks,
            foreign_keys=fks,
        )

    def _fetch_columns(self, conn: Any, table_name: str) -> list[Column]:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM {self._database}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (self.schema_name, table_name),
            )
            rows = cur.fetchall()
        finally:
            cur.close()
        columns: list[Column] = []
        for name, data_type, is_nullable in rows:
            columns.append(
                Column(
                    name=name,
                    data_type=(data_type or "").lower(),
                    is_nullable=(is_nullable == "YES"),
                    is_primary_key=False,
                )
            )
        return columns

    def _fetch_primary_key(self, conn: Any, table_name: str) -> list[str]:
        """Return primary-key column names, ordered by key position.

        Snowflake exposes declared PKs via ``SHOW PRIMARY KEYS``. We use
        that rather than ``INFORMATION_SCHEMA`` because Snowflake's
        ``TABLE_CONSTRAINTS`` coverage has changed across releases.
        """
        cur = conn.cursor()
        try:
            cur.execute(
                f'SHOW PRIMARY KEYS IN TABLE "{self._database}"."{self.schema_name}"."{table_name}"'
            )
            rows = cur.fetchall()
            columns = [d[0] for d in (cur.description or [])]
        finally:
            cur.close()
        if not rows:
            return []
        col_ix = {c.lower(): i for i, c in enumerate(columns)}
        name_ix = col_ix.get("column_name")
        seq_ix = col_ix.get("key_sequence")
        if name_ix is None:
            return []
        if seq_ix is not None:
            ordered = sorted(rows, key=lambda r: _safe_int(r[seq_ix]))
        else:
            ordered = rows
        return [row[name_ix] for row in ordered]

    def _fetch_foreign_keys(self, conn: Any, table_name: str) -> list[ForeignKey]:
        """Return declared foreign keys via ``SHOW IMPORTED KEYS``.

        Reminder for callers: Snowflake *does not enforce* FK
        constraints (PRD P6.6). They are declarative only, and many
        Snowflake schemas have no FK metadata at all — in which case
        this returns an empty list. FK inference based on naming /
        value overlap is tracked separately as P6.6 and is out of scope
        for this slice.
        """
        cur = conn.cursor()
        try:
            cur.execute(
                f'SHOW IMPORTED KEYS IN TABLE "{self._database}"."{self.schema_name}"."{table_name}"'
            )
            rows = cur.fetchall()
            columns = [d[0] for d in (cur.description or [])]
        finally:
            cur.close()

        if not rows:
            return []

        col_ix = {c.lower(): i for i, c in enumerate(columns)}
        local_ix = col_ix.get("fk_column_name") or col_ix.get("pk_column_name_fk")
        foreign_table_ix = col_ix.get("pk_table_name")
        foreign_col_ix = col_ix.get("pk_column_name")
        constraint_ix = col_ix.get("fk_name")
        key_seq_ix = col_ix.get("key_sequence")
        if local_ix is None or foreign_table_ix is None or foreign_col_ix is None:
            logger.warning(
                "snowflake_fk_columns_unexpected",
                table=table_name,
                columns=columns,
            )
            return []

        grouped: OrderedDict[str, dict[str, Any]] = OrderedDict()
        for row in rows:
            cname = row[constraint_ix] if constraint_ix is not None else row[foreign_table_ix]
            bucket = grouped.setdefault(
                cname,
                {
                    "columns": [],
                    "foreign_columns": [],
                    "foreign_table": row[foreign_table_ix],
                    "constraint_name": cname,
                    "_pairs": [],
                },
            )
            seq = _safe_int(row[key_seq_ix]) if key_seq_ix is not None else len(bucket["_pairs"])
            bucket["_pairs"].append((seq, row[local_ix], row[foreign_col_ix]))

        fks: list[ForeignKey] = []
        for bucket in grouped.values():
            pairs = sorted(bucket["_pairs"], key=lambda p: p[0])
            fks.append(
                ForeignKey(
                    columns=[p[1] for p in pairs],
                    foreign_table=bucket["foreign_table"],
                    foreign_columns=[p[2] for p in pairs],
                    constraint_name=bucket["constraint_name"],
                )
            )
        return fks


def _safe_int(v: Any) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _safe_connection_error(err: Exception, connect_params: dict[str, Any]) -> str:
    """Return a useful driver error without credential or key-path disclosure."""
    message = scrub_dsn_credentials(str(err))
    for field in _SENSITIVE_CONNECT_FIELDS:
        value = connect_params.get(field)
        if value:
            message = message.replace(str(value), "***")
    return message


def _parse_snowflake_url(url: str) -> dict[str, Any]:
    """Parse a Snowflake SQLAlchemy-style URL into connector kwargs.

    Returns the kwargs that ``snowflake.connector.connect`` accepts,
    with one extra private key ``_url_schema`` if the URL path supplied
    a schema (which the connector wrapper consumes before handing the
    dict to the Snowflake driver).
    """

    if not url or "://" not in url:
        raise ValueError(
            "Snowflake connection string must look like "
            "snowflake://user:pass@account/DATABASE[/SCHEMA]?warehouse=...&role=..."
        )
    parsed = urlparse(url)
    if parsed.scheme.lower() != "snowflake":
        raise ValueError(
            f"Expected a snowflake:// connection string, got scheme '{parsed.scheme}'"
        )

    user = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    account = parsed.hostname or ""
    if not user or not account:
        raise ValueError(
            "Snowflake connection string must include user and account: "
            "snowflake://user:pass@account/DATABASE"
        )

    path_parts = [p for p in (parsed.path or "").split("/") if p]
    if not path_parts:
        raise ValueError(
            "Snowflake connection string is missing a database path component"
        )
    database = path_parts[0]
    url_schema = path_parts[1] if len(path_parts) > 1 else None

    query = {k: v[0] for k, v in parse_qs(parsed.query, keep_blank_values=True).items() if v}

    include_views_raw = query.get("include_views", "").strip().lower()
    if include_views_raw not in _BOOL_WORDS:
        raise ValueError(
            "include_views must be true or false "
            f"(got {query.get('include_views', '')!r})"
        )
    include_views = _BOOL_WORDS[include_views_raw]

    private_key_file = query.get("private_key_file", "")
    private_key_file_pwd = query.get("private_key_file_pwd", "")
    if password and private_key_file:
        raise ValueError(
            "Configure exactly one Snowflake authentication method: "
            "password or private_key_file"
        )
    if private_key_file_pwd and not private_key_file:
        raise ValueError("private_key_file_pwd requires private_key_file")
    if not password and not private_key_file:
        raise ValueError(
            "Snowflake authentication requires a password or private_key_file"
        )
    if private_key_file and not Path(private_key_file).is_file():
        raise ValueError("private_key_file must name an existing file")

    kwargs: dict[str, Any] = {
        "user": user,
        "account": account,
        "database": database,
    }
    if private_key_file:
        kwargs["authenticator"] = "SNOWFLAKE_JWT"
        kwargs["private_key_file"] = private_key_file
        if private_key_file_pwd:
            kwargs["private_key_file_pwd"] = private_key_file_pwd
    else:
        kwargs["password"] = password
    for key in ("warehouse", "role", "authenticator", "application"):
        if key in query:
            kwargs[key] = query[key]
    if private_key_file:
        # A caller-supplied authenticator cannot weaken key-pair semantics.
        kwargs["authenticator"] = "SNOWFLAKE_JWT"
    if parsed.port:
        kwargs["port"] = parsed.port
    if url_schema:
        kwargs["_url_schema"] = url_schema
    if include_views:
        kwargs["_include_views"] = True
    return kwargs


class SnowflakeSession:
    """Bulk-read session for :class:`SnowflakeConnector`.

    Opens a single ``snowflake.connector`` connection, starts a
    transaction (``BEGIN``) so that every count / stream / dump during
    the session sees the same committed snapshot, and commits on
    :meth:`close`. A rollback is issued instead if the caller passes
    ``abort=True`` — kept as a hook even though nothing today calls it.

    Snowflake identifiers are case-sensitive when quoted; we always
    quote both the schema and table names with double-quotes and pass
    the *exact* identifier stored on the :class:`Schema` (which in
    practice is upper-case because that's what ``INFORMATION_SCHEMA``
    stores).
    """

    def __init__(
        self,
        connection_string: str,
        *,
        database: str,
        schema_name: str,
        connect_params: dict[str, Any],
    ) -> None:
        self.connection_string = connection_string
        self._database = database
        self.schema_name = schema_name
        self._connect_params = dict(connect_params)
        self._connect_params.setdefault("schema", schema_name)
        self._conn: Any = None
        self._tx_open = False

    @property
    def connection(self) -> Any:
        if self._conn is None:
            snowflake = _load_snowflake_connector()
            try:
                self._conn = snowflake.connect(**self._connect_params)
            except Exception as err:
                raise RuntimeError(
                    "Failed to connect to Snowflake: "
                    f"{_safe_connection_error(err, self._connect_params)}"
                ) from err
            try:
                cur = self._conn.cursor()
                try:
                    cur.execute("BEGIN")
                finally:
                    cur.close()
                self._tx_open = True
            except Exception as err:  # noqa: BLE001
                logger.warning("snowflake_begin_failed", error=str(err))
        return self._conn

    def close(self, *, abort: bool = False) -> None:
        if self._conn is None:
            return
        try:
            if self._tx_open:
                cur = self._conn.cursor()
                try:
                    cur.execute("ROLLBACK" if abort else "COMMIT")
                except Exception as err:  # noqa: BLE001
                    logger.warning(
                        "snowflake_commit_failed",
                        error=str(err),
                        aborted=abort,
                    )
                finally:
                    cur.close()
            self._tx_open = False
        finally:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None

    def __enter__(self) -> "SnowflakeSession":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _qualified(self, table: str) -> str:
        return f'"{self._database}"."{self.schema_name}"."{table}"'

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
                    f'SELECT COUNT(*) FROM {q} WHERE "{since_column}" >= %s',  # noqa: S608
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
        """Stream rows via Snowflake's cursor in ``batch_size`` chunks.

        We use ``cursor.fetchmany(batch_size)`` rather than iterating
        the cursor directly — the Snowflake driver's ``__iter__``
        fetches one row at a time which is an order of magnitude slower
        on wide tables.
        """
        q = self._qualified(table)
        conn = self.connection
        cur = conn.cursor()
        try:
            if since_column and since_value is not None:
                cur.execute(
                    f'SELECT * FROM {q} WHERE "{since_column}" >= %s',  # noqa: S608
                    (since_value,),
                )
            else:
                cur.execute(f"SELECT * FROM {q}")  # noqa: S608
            columns = [d[0] for d in (cur.description or [])]
            fetch = max(1, batch_size)
            while True:
                rows = cur.fetchmany(fetch)
                if not rows:
                    break
                for row in rows:
                    yield dict(zip(columns, row))
        finally:
            cur.close()

    def dump_table_to_csv(
        self,
        table: str,
        out_path: Path,
        *,
        header: bool = True,
    ) -> int:
        """Export *table* as CSV through the Snowflake cursor.

        A future iteration could use ``COPY INTO @<stage>`` for larger
        tables, but that requires the user to have provisioned an
        accessible stage. Cursor streaming is portable, needs no
        Snowflake-side provisioning, and works for any table the user
        can ``SELECT`` from.
        """
        q = self._qualified(table)
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        conn = self.connection
        cur = conn.cursor()
        total = 0
        try:
            cur.execute(f"SELECT * FROM {q}")  # noqa: S608
            columns = [d[0] for d in (cur.description or [])]
            with out_path.open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f, lineterminator="\n")
                if header:
                    writer.writerow(columns)
                while True:
                    rows = cur.fetchmany(10_000)
                    if not rows:
                        break
                    for row in rows:
                        writer.writerow(
                            ["" if v is None else v for v in row]
                        )
                        total += 1
        finally:
            cur.close()
        return total
