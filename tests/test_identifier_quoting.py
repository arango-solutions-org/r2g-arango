"""Identifiers a source reports are quoted, never spliced, into the SQL r2g sends back.

Table and column names reach r2g's queries from introspection, so whoever can name a
table in the source decides them. Each query builder must keep such a name one
identifier: an embedded quote is doubled (or escaped), never allowed to end it.
"""

from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from r2g.connectors.base import quote_backtick, quote_double, table_output_file


@pytest.fixture(autouse=True)
def _reset_structlog():
    """CliRunner closes the stream structlog caches; restore it before and after, so a
    CLI test that ran earlier cannot break the error path this file exercises."""
    import structlog

    def reset():
        structlog.reset_defaults()
        structlog.configure(
            wrapper_class=structlog.make_filtering_bound_logger(0),
            logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        )

    reset()
    yield
    reset()


HOSTILE = 'x"; DROP TABLE victim; COMMIT; --'
HOSTILE_BT = "x` FROM t; DROP TABLE victim; --"


def _render(query: Any) -> str:
    """psycopg ``Composed`` renders without a connection; plain strings pass through."""
    return query.as_string(None) if hasattr(query, "as_string") else str(query)


# ── the quoting helpers ─────────────────────────────────────────────────────────────


def test_quote_double_doubles_embedded_quotes():
    assert quote_double("plain") == '"plain"'
    assert quote_double(HOSTILE) == '"x""; DROP TABLE victim; COMMIT; --"'


def test_quote_backtick_escapes_backtick_and_backslash():
    assert quote_backtick("plain") == "`plain`"
    assert quote_backtick("a`b") == "`a\\`b`"
    # A trailing backslash must not escape the closing quote.
    assert quote_backtick("a\\") == "`a\\\\`"


@pytest.mark.parametrize("name", ["../x", "a/b", "..", ".", "", "a\\b", "a\0b"])
def test_table_output_file_refuses_names_that_leave_the_directory(tmp_path, name):
    with pytest.raises(ValueError, match="cannot be used as a file name"):
        table_output_file(tmp_path, name)


def test_table_output_file_keeps_ordinary_names(tmp_path):
    assert table_output_file(tmp_path, "Order Details") == tmp_path / "Order Details.csv"


# ── PostgreSQL bulk session ─────────────────────────────────────────────────────────


class _PgCursor:
    def __init__(self, sent: list[str]):
        self.sent = sent
        self.itersize = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, query, params=None):
        self.sent.append(_render(query))

    def fetchone(self):
        return (0,)

    def __iter__(self):
        return iter(())

    @contextmanager
    def copy(self, query):
        self.sent.append(_render(query))
        yield iter(())


class _PgConn:
    def __init__(self):
        self.sent: list[str] = []

    def cursor(self, *a, **kw):
        return _PgCursor(self.sent)


def _pg_session():
    from r2g.connectors.postgres import PostgresSession

    s = PostgresSession("postgresql://u@h/db", schema_name="public")
    s._conn = _PgConn()
    return s


def test_postgres_count_rows_quotes_the_table_and_since_column():
    s = _pg_session()
    s.count_rows(HOSTILE)
    s.count_rows("t", since_column=HOSTILE, since_value="2020-01-01")
    assert s._conn.sent == [
        'SELECT count(*) FROM "public"."x""; DROP TABLE victim; COMMIT; --"',
        'SELECT count(*) FROM "public"."t" WHERE "x""; DROP TABLE victim; COMMIT; --" >= %s',
    ]


def test_postgres_stream_rows_and_dump_quote_the_table(tmp_path):
    s = _pg_session()
    list(s.stream_rows(HOSTILE))
    s.dump_table_to_csv(HOSTILE, tmp_path / "out.csv")
    assert s._conn.sent == [
        'SELECT * FROM "public"."x""; DROP TABLE victim; COMMIT; --"',
        'COPY "public"."x""; DROP TABLE victim; COMMIT; --" TO STDOUT WITH CSV HEADER',
    ]


def test_postgres_value_sampler_quotes_table_and_column():
    from r2g.fk_inference import PostgresValueSampler

    sent: list[str] = []

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def execute(self, q, params=None):
            sent.append(" ".join(q.split()))

        def fetchall(self):
            return []

    sampler = PostgresValueSampler("postgresql://u@h/db", schema_name="public")
    sampler._conn_lazy = lambda: type("C", (), {"cursor": lambda self: _Cur()})()
    sampler.sample_values(HOSTILE, HOSTILE)
    q = '"x""; DROP TABLE victim; COMMIT; --"'
    assert sent == [f'SELECT DISTINCT {q} AS v FROM "public".{q} WHERE {q} IS NOT NULL LIMIT %s']


# ── Snowflake ───────────────────────────────────────────────────────────────────────


class _SfCursor:
    def __init__(self, sent):
        self.sent = sent
        self.description = []

    def execute(self, q, params=None):
        self.sent.append(q)

    def fetchone(self):
        return (0,)

    def fetchmany(self, n):
        return []

    def fetchall(self):
        return []

    def close(self):
        pass


def test_snowflake_session_quotes_table_and_since_column():
    from r2g.connectors.snowflake import SnowflakeSession

    s = SnowflakeSession("snowflake://u:p@acct/DB/SCH", database="DB", schema_name="SCH",
                         connect_params={})
    sent: list[str] = []
    s._conn = type("C", (), {"cursor": lambda self: _SfCursor(sent)})()
    s.count_rows(HOSTILE)
    s.count_rows("T", since_column=HOSTILE, since_value="1")
    q = '"x""; DROP TABLE victim; COMMIT; --"'
    assert sent == [
        f'SELECT COUNT(*) FROM "DB"."SCH".{q}',
        f'SELECT COUNT(*) FROM "DB"."SCH"."T" WHERE {q} >= %s',
    ]


def test_snowflake_introspection_quotes_the_table():
    from r2g.connectors.snowflake import SnowflakeConnector

    c = SnowflakeConnector("snowflake://u:p@acct/DB/SCH")
    sent: list[str] = []
    conn = type("C", (), {"cursor": lambda self: _SfCursor(sent)})()
    c._fetch_primary_key(conn, HOSTILE)
    c._fetch_foreign_keys(conn, HOSTILE)
    q = '"x""; DROP TABLE victim; COMMIT; --"'
    assert sent == [
        f'SHOW PRIMARY KEYS IN TABLE "DB"."SCH".{q}',
        f'SHOW IMPORTED KEYS IN TABLE "DB"."SCH".{q}',
    ]


# ── ClickHouse ──────────────────────────────────────────────────────────────────────


def test_clickhouse_session_escapes_backticks():
    from r2g.connectors.clickhouse import ClickHouseSession

    sent: list[str] = []

    class _Result:
        result_rows = [(0,)]

    class _Client:
        def query(self, q, parameters=None):
            sent.append(q)
            return _Result()

    s = ClickHouseSession("clickhouse://h/db", database="db", client=_Client())
    s.count_rows(HOSTILE_BT)
    s.count_rows("t", since_column=HOSTILE_BT, since_value="1")
    q = "`x\\` FROM t; DROP TABLE victim; --`"
    assert sent == [
        f"SELECT count() FROM `db`.{q}",
        f"SELECT count() FROM `db`.`t` WHERE {q} >= {{since:String}}",
    ]


# ── the dump commands write only inside the output directory ───────────────────────


def test_source_dump_skips_a_table_whose_name_leaves_the_directory(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from r2g.catalog import CatalogManager
    from r2g.main import app
    from r2g.types import Column, Schema, Table

    cat = CatalogManager(tmp_path / "cat")
    cat.add_source("s", "postgresql", "postgresql://u@h/db")
    name = "../escaped"
    cat.create_snapshot("s", Schema(tables={name: Table(
        name=name, columns=[Column(name="id", data_type="integer")], primary_key=["id"])}))
    monkeypatch.setattr("r2g.main._get_catalog", lambda: cat)
    # main.py's module-level logger may still hold a stream an earlier CliRunner closed.
    monkeypatch.setattr("r2g.main.log", MagicMock())

    written: list[Path] = []

    class _Session:
        def dump_table_to_csv(self, table, path):
            written.append(Path(path))
            return 0

        def close(self):
            pass

    class _Connector:
        def open_session(self):
            return _Session()

    monkeypatch.setattr("r2g.connectors.base.create_source_connector",
                        lambda *a, **kw: _Connector())
    out = tmp_path / "out"
    result = CliRunner().invoke(app, ["source", "dump", "s", "--output-dir", str(out)])
    assert written == [], result.output
    assert "cannot be used as a file name" in " ".join(result.output.split())
    assert not (tmp_path / "escaped.csv").exists()
