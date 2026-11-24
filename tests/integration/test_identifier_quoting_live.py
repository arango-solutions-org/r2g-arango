"""A table named to end its own identifier stays one identifier on the live engines.

The unit tests in tests/test_identifier_quoting.py check the SQL text; these check
that the engines read it the same way: the hostile table is counted and dumped, and
the table its name tries to drop is still there afterwards.
"""

from __future__ import annotations

import uuid

import pytest

from .conftest import CLICKHOUSE_DSN, PG_CONN, requires_clickhouse, requires_pg


@requires_pg
def test_postgres_session_reads_a_hostile_table_name_as_one_identifier(tmp_path):
    import psycopg
    from psycopg import sql

    from r2g.connectors.postgres import PostgresSession

    schema = f"r2g_quote_{uuid.uuid4().hex[:8]}"
    hostile = f'x"; DROP TABLE {schema}.victim; COMMIT; --'
    with psycopg.connect(PG_CONN, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("CREATE TABLE {} (id int)").format(sql.Identifier(schema, "victim")))
        conn.execute(sql.SQL("CREATE TABLE {} (id int)").format(sql.Identifier(schema, hostile)))
        conn.execute(sql.SQL("INSERT INTO {} VALUES (1), (2)").format(
            sql.Identifier(schema, hostile)))
    try:
        s = PostgresSession(PG_CONN, schema_name=schema)
        try:
            assert s.count_rows(hostile) == 2
            assert s.count_rows(hostile, since_column="id", since_value="2") == 1
            assert [r["id"] for r in s.stream_rows(hostile)] == [1, 2]
            assert s.dump_table_to_csv(hostile, tmp_path / "out.csv") == 2
        finally:
            s.close()
        with psycopg.connect(PG_CONN) as conn:
            assert conn.execute(
                "SELECT to_regclass(%s) IS NOT NULL", (f"{schema}.victim",)).fetchone()[0]
    finally:
        with psycopg.connect(PG_CONN, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@requires_clickhouse
@pytest.mark.parametrize("name", ["x` FROM system.one; --", "ends_with_backslash\\"])
def test_clickhouse_session_reads_a_hostile_table_name_as_one_identifier(name):
    import clickhouse_connect

    from r2g.connectors.base import quote_backtick
    from r2g.connectors.clickhouse import ClickHouseSession, _get_client

    client = _get_client(CLICKHOUSE_DSN)
    db = client.database or "default"
    client.command(f"CREATE TABLE {quote_backtick(db)}.{quote_backtick(name)} "
                   "(id UInt32) ENGINE = Memory")
    try:
        client.command(f"INSERT INTO {quote_backtick(db)}.{quote_backtick(name)} VALUES (1), (2)")
        s = ClickHouseSession(CLICKHOUSE_DSN, database=db, client=client)
        assert s.count_rows(name) == 2
        assert [r["id"] for r in s.stream_rows(name)] == [1, 2]
    finally:
        client.command(f"DROP TABLE IF EXISTS {quote_backtick(db)}.{quote_backtick(name)}")
    assert clickhouse_connect  # imported for the skip guard's dependency, kept explicit
