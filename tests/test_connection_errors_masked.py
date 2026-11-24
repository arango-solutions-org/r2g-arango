"""No r2g connector can print a credential in an error or a traceback (#118).

The MySQL, SQL Server and Postgres connectors put the driver's error text in their message
and chained the driver's exception, which any traceback prints raw. Snowflake masked its
message, but only the password as given, and then chained the raw exception anyway.
ClickHouse and the Postgres CDC listener didn't wrap the connect at all.

Each test uses a fake driver that does the worst thing a driver can: puts the secret, as
given and percent-encoded, into its error. Ported from relational-schema-analyzer's
``tests/test_credentials_masked.py`` (its #57).
"""

from __future__ import annotations

import sys
import traceback
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from r2g.security import SECRET_MASK, connection_error, redact, secrets_in

SECRET = "pa/ss w0rd!"
ENCODED = "pa%2Fss%20w0rd%21"
_CURSORS = SimpleNamespace(DictCursor=object, SSDictCursor=object, SSCursor=object)


class LeakyDriverError(Exception):
    """A driver error that repeats every credential it was given."""


def _leak(*args: Any, **kwargs: Any) -> Any:
    raise LeakyDriverError(f"login failed: args={args!r} kwargs={kwargs!r} raw={SECRET} url={ENCODED}")


def _assert_masked(exc: BaseException) -> None:
    text = "".join(traceback.format_exception(exc))
    assert SECRET not in text and ENCODED not in text, text
    assert SECRET_MASK in str(exc)
    assert "LeakyDriverError" in str(exc)  # the driver's error type survives, for debugging
    assert exc.__cause__ is None and exc.__suppress_context__  # raised ``from None``


class TestHelpers:
    def test_url_password_in_every_encoding(self):
        assert {ENCODED, SECRET} <= set(secrets_in(f"postgresql://u:{ENCODED}@h/db"))

    def test_query_and_keyword_forms(self):
        assert "tok1" in secrets_in("snowflake://acct/db?token=tok1&warehouse=w")
        assert "s3 cr'et" in secrets_in("host=db password='s3 cr\\'et' dbname=x")

    def test_parameter_mapping_and_nothing_to_find(self):
        assert {"p1", "kp"} <= set(secrets_in({"password": "p1", "private_key_file_pwd": "kp", "user": "u"}))
        assert secrets_in(None, "", "/data/demo.csv", {"user": "u"}) == []

    def test_redact_also_scrubs_a_dsn_it_was_not_told_about(self):
        assert redact("at postgresql://bob:hunter2@h/db", []) == "at postgresql://***:***@h/db"

    def test_connection_error_names_the_driver_error(self):
        err = connection_error("Failed to connect", LeakyDriverError(f"x {SECRET}"), [SECRET])
        assert str(err) == f"Failed to connect: LeakyDriverError: x {SECRET_MASK}"


# ── connectors ───────────────────────────────────────────────────────────────


def test_postgres_connector(monkeypatch):
    from r2g.connectors import postgres

    monkeypatch.setattr(postgres.psycopg, "connect", _leak)
    with pytest.raises(RuntimeError, match="Failed to fetch schema from PostgreSQL") as info:
        postgres.PostgresConnector(f"postgresql://u:{ENCODED}@h/db").get_schema()
    _assert_masked(info.value)


def test_postgres_session_and_preview(monkeypatch):
    from r2g.connectors import postgres

    monkeypatch.setattr(postgres.psycopg, "connect", _leak)
    url = f"postgresql://u:{ENCODED}@h/db"
    session = postgres.PostgresConnector(url).open_session()
    with pytest.raises(RuntimeError, match="Failed to connect to PostgreSQL") as info:
        session.connection  # noqa: B018
    _assert_masked(info.value)
    with pytest.raises(RuntimeError, match="Failed to connect to PostgreSQL") as info:
        postgres.preview_table_rows(url, "public", "t", 5)
    _assert_masked(info.value)


@pytest.mark.parametrize(
    ("module", "loader", "cls", "url", "what"),
    [
        ("mysql", "_load_pymysql", "MySQLConnector", f"mysql://u:{ENCODED}@h/db", "MySQL"),
        ("mssql", "_load_pymssql", "SQLServerConnector", f"mssql://u:{ENCODED}@h/db", "SQL Server"),
        (
            "snowflake",
            "_load_snowflake_connector",
            "SnowflakeConnector",
            f"snowflake://u:{ENCODED}@acct/db/sch",
            "Snowflake",
        ),
    ],
)
def test_driver_connectors_and_their_sessions(monkeypatch, module, loader, cls, url, what):
    import importlib

    mod = importlib.import_module(f"r2g.connectors.{module}")
    monkeypatch.setattr(mod, loader, lambda: SimpleNamespace(connect=_leak, cursors=_CURSORS))
    connector = getattr(mod, cls)(url)
    with pytest.raises(RuntimeError, match=f"Failed to connect to {what}") as info:
        connector.get_schema()
    _assert_masked(info.value)
    session = connector.open_session()
    with pytest.raises(RuntimeError, match=f"Failed to connect to {what}") as info:
        session.connection  # noqa: B018
    _assert_masked(info.value)


def test_clickhouse(monkeypatch):
    from r2g.connectors import clickhouse

    monkeypatch.setitem(sys.modules, "clickhouse_connect", SimpleNamespace(get_client=_leak))
    with pytest.raises(RuntimeError, match="Failed to connect to ClickHouse") as info:
        clickhouse._get_client(f"clickhouse://u:{ENCODED}@h:8123/db")
    _assert_masked(info.value)


def test_postgres_cdc_listener(monkeypatch):
    from r2g.cdc import pg_listener

    monkeypatch.setattr(pg_listener.psycopg, "connect", _leak)
    listener = pg_listener.PGReplicationListener(f"postgresql://u:{ENCODED}@h/db", handler=None)
    with pytest.raises(RuntimeError, match="Failed to connect to PostgreSQL") as info:
        listener._connect()
    _assert_masked(info.value)


def test_snowflake_cdc_listener():
    from r2g.cdc.snowflake_streams import SnowflakeStreamListener

    listener = SnowflakeStreamListener(
        f"snowflake://u:{ENCODED}@acct/db/sch",
        handler=MagicMock(),
        schema=MagicMock(),  # stand-ins: only the connection is exercised
        mapping=MagicMock(),
        connect=_leak,
    )
    with pytest.raises(RuntimeError, match="Failed to connect to Snowflake") as info:
        listener.connection  # noqa: B018
    _assert_masked(info.value)
