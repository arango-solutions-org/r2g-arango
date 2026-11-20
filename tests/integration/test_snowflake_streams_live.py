"""Snowflake change capture against a real account (PRD P6.16, issue #65).

Drives :class:`SnowflakeStreamListener` end to end: Snowflake DML on a
dynamic table and on a view, streams read by r2g, deltas written to a real
ArangoDB, watermarks written to ``_cdf_ingest``. Covers the issue's
done-when list that a fake cursor cannot:

* inserts, updates and deletes on a stream over a dynamic table reach
  ArangoDB through the mapping;
* a change to an unmapped column writes nothing;
* a stream that can no longer be read is detected (recovery is not
  implemented yet);
* the per-collection watermark is written, with the dynamic table's data
  timestamp as its basis (``SHOW DYNAMIC TABLES`` column names are checked
  here, not assumed).

Configuration (skipped when absent): the ``SNOWFLAKE_*`` key-pair or password
variables of ``test_forge_roundtrip_snowflake``, plus ``SNOWFLAKE_FORGE_ROLE``
(a role that may create a schema, dynamic table and stream in the database)
and optionally ``R2G_SNOWFLAKE_CDC_DATABASE`` (default ``CDF_FORGE``). One
schema per run, ``R2G_CDC_<pid>_<hex>``, always dropped.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from typing import Any, Iterator
from urllib.parse import quote

import pytest

from r2g.cdc.handler import CDCHandler
from r2g.cdc.snowflake_streams import (
    BASIS_DT_DATA_TIMESTAMP,
    FRESHNESS_COLLECTION,
    KIND_DYNAMIC_TABLE,
    KIND_VIEW,
    STREAM_STATE_COLLECTION,
    SnowflakeStreamListener,
    StreamStaleError,
)
from r2g.connectors.arango_writer import ArangoWriter
from r2g.connectors.snowflake import SnowflakeConnector
from r2g.types import CollectionMapping, EdgeDefinition, MappingConfig

from .conftest import (
    ARANGO_ENDPOINT,
    ARANGO_PASSWORD,
    ARANGO_USER,
    requires_arango,
    requires_snowflake,
)

pytestmark = [
    requires_snowflake,
    requires_arango,
    pytest.mark.skipif(
        not os.getenv("SNOWFLAKE_FORGE_ROLE"),
        reason="SNOWFLAKE_FORGE_ROLE not set (a role that may create a schema, dynamic table and stream)",
    ),
]

DATABASE = os.getenv("R2G_SNOWFLAKE_CDC_DATABASE", "CDF_FORGE").upper()


def _url(schema: str) -> str:
    env = os.environ
    user = quote(env["SNOWFLAKE_USER"], safe="")
    params = {"warehouse": env["SNOWFLAKE_WAREHOUSE"], "role": env["SNOWFLAKE_FORGE_ROLE"]}
    if env.get("SNOWFLAKE_PRIVATE_KEY_FILE"):
        auth = f"{user}:"
        params["private_key_file"] = env["SNOWFLAKE_PRIVATE_KEY_FILE"]
        if env.get("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"):
            params["private_key_file_pwd"] = env["SNOWFLAKE_PRIVATE_KEY_FILE_PWD"]
    else:
        auth = f"{user}:{quote(env['SNOWFLAKE_PASSWORD'], safe='')}"
    query = "&".join(f"{k}={quote(v, safe='')}" for k, v in params.items())
    return f"snowflake://{auth}@{env['SNOWFLAKE_ACCOUNT']}/{DATABASE}/{schema}?{query}"


@pytest.fixture(scope="module")
def sf() -> Iterator[tuple[Any, str]]:
    """A throwaway schema with a dynamic table and a view; yields ``(cursor, schema)``."""
    snowflake = pytest.importorskip("snowflake.connector")
    from r2g.connectors.snowflake import _parse_snowflake_url

    schema = f"R2G_CDC_{os.getpid()}_{uuid.uuid4().hex[:4].upper()}"
    params = _parse_snowflake_url(_url(schema))
    params.pop("_url_schema", None)
    try:
        conn = snowflake.connect(**params)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Snowflake unreachable: {exc}")
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE SCHEMA {DATABASE}.{schema}")
    except BaseException as exc:
        conn.close()
        if "Insufficient privileges" in str(exc):
            pytest.skip(f"role may not CREATE SCHEMA on {DATABASE}")
        raise
    try:
        cur.execute(f"USE SCHEMA {DATABASE}.{schema}")
        cur.execute("CREATE TABLE ACCOUNT_SRC (ID NUMBER(38,0), NAME VARCHAR, NOTES VARCHAR)")
        cur.execute(
            f"CREATE DYNAMIC TABLE ACCOUNT TARGET_LAG = DOWNSTREAM "
            f"WAREHOUSE = {os.environ['SNOWFLAKE_WAREHOUSE']} REFRESH_MODE = INCREMENTAL "
            "AS SELECT ID, NAME, NOTES FROM ACCOUNT_SRC"
        )
        cur.execute("CREATE TABLE CASE_SRC (ID NUMBER(38,0), ACCOUNT_ID NUMBER(38,0), STATUS VARCHAR)")
        cur.execute("ALTER TABLE CASE_SRC SET CHANGE_TRACKING = TRUE")
        cur.execute(
            "CREATE VIEW CASES AS SELECT ID, ACCOUNT_ID, STATUS FROM CASE_SRC WHERE STATUS <> 'closed'"
        )
        yield cur, schema
    finally:
        try:
            cur.execute(f"DROP SCHEMA IF EXISTS {DATABASE}.{schema} CASCADE")
        finally:
            conn.close()


@pytest.fixture(scope="module")
def arango_db() -> Iterator[tuple[str, Any]]:
    from arango import ArangoClient

    name = f"r2g_cdc_sf_{uuid.uuid4().hex[:8]}"
    client = ArangoClient(hosts=ARANGO_ENDPOINT)
    sys_db = client.db("_system", username=ARANGO_USER, password=ARANGO_PASSWORD)
    sys_db.create_database(name)
    try:
        yield name, client.db(name, username=ARANGO_USER, password=ARANGO_PASSWORD)
    finally:
        sys_db.delete_database(name, ignore_missing=True)
        client.close()


@pytest.fixture(scope="module")
def listener(sf, arango_db) -> Iterator[SnowflakeStreamListener]:
    _cur, schema_name = sf
    db_name, _db = arango_db
    url = _url(schema_name) + "&include_views=true"
    schema = SnowflakeConnector(url).get_schema()
    # A dynamic table and a view declare no keys; a reviewed overlay supplies
    # them in practice (P6.8). Set them as the overlay would.
    schema.tables["ACCOUNT"].primary_key = ["ID"]
    schema.tables["CASES"].primary_key = ["ID"]
    mapping = MappingConfig(
        collections={
            "ACCOUNT": CollectionMapping(
                source_table="ACCOUNT", target_collection="Account", exclude_fields=["NOTES"],
            ),
            "CASES": CollectionMapping(source_table="CASES", target_collection="Case"),
        },
        edges=[EdgeDefinition(
            edge_collection="caseOf", from_collection="CASES", to_collection="ACCOUNT",
            from_field="ACCOUNT_ID", to_field="ID",
        )],
    )
    writer = ArangoWriter(
        endpoint=ARANGO_ENDPOINT, database=db_name, username=ARANGO_USER, password=ARANGO_PASSWORD,
    )
    writer.connect()
    handler = CDCHandler(writer, schema, mapping, skip_noop_updates=True)
    lst = SnowflakeStreamListener(url, handler, schema, mapping)
    try:
        yield lst
    finally:
        lst.close()
        writer.close()


def _refresh(cur: Any) -> None:
    cur.execute("ALTER DYNAMIC TABLE ACCOUNT REFRESH")


def _docs(db: Any, coll: str) -> dict[str, dict]:
    return {d["_key"]: d for d in db.collection(coll).all()} if db.has_collection(coll) else {}


class TestSnowflakeStreamsLive:
    """Ordered: each step builds on the previous one's state."""

    def test_1_setup_creates_streams_on_the_right_kinds(self, listener):
        targets = {t.source_object: t.kind for t in listener.discover()}
        assert targets == {"ACCOUNT": KIND_DYNAMIC_TABLE, "CASES": KIND_VIEW}
        assert listener.setup() == {"R2G_ACCOUNT": "created", "R2G_CASES": "created"}
        assert listener.setup() == {"R2G_ACCOUNT": "exists", "R2G_CASES": "exists"}

    def test_2_inserts_reach_arango_and_collections_are_stamped(self, listener, sf, arango_db):
        cur, _ = sf
        _, db = arango_db
        cur.execute("INSERT INTO ACCOUNT_SRC VALUES (1,'Acme','n1'), (2,'Beta','n2'), (3,'Gamma','n3')")
        cur.execute("INSERT INTO CASE_SRC VALUES (10,1,'open'), (11,2,'open')")
        _refresh(cur)
        result = listener.poll_once()
        cur.execute("SELECT CURRENT_TIMESTAMP()")
        after = cur.fetchone()[0]

        accounts = _docs(db, "Account")
        assert {k: d["NAME"] for k, d in accounts.items()} == {"1": "Acme", "2": "Beta", "3": "Gamma"}
        assert all("NOTES" not in d for d in accounts.values())
        assert set(_docs(db, "Case")) == {"10", "11"}
        assert len(_docs(db, "caseOf")) == 2

        account, cases = result.watermarks["R2G_ACCOUNT"], result.watermarks["R2G_CASES"]
        assert account.basis == BASIS_DT_DATA_TIMESTAMP
        stamps = _docs(db, FRESHNESS_COLLECTION)
        assert set(stamps) == {"Account", "Case", "caseOf"}
        for doc in stamps.values():
            stamp = datetime.fromisoformat(doc["ingested_at"])
            assert stamp.tzinfo is not None and stamp <= after
        # Each collection carries the watermark of the stream that writes it.
        assert datetime.fromisoformat(stamps["Account"]["ingested_at"]) == account.applied_through
        assert datetime.fromisoformat(stamps["Case"]["ingested_at"]) == cases.applied_through
        assert datetime.fromisoformat(stamps["caseOf"]["ingested_at"]) == cases.applied_through
        assert set(_docs(db, STREAM_STATE_COLLECTION)) == {"R2G_ACCOUNT", "R2G_CASES"}

    def test_3_updates_and_deletes_reach_arango(self, listener, sf, arango_db):
        cur, _ = sf
        _, db = arango_db
        cur.execute("UPDATE ACCOUNT_SRC SET NAME = 'Acme Corp' WHERE ID = 1")
        cur.execute("DELETE FROM ACCOUNT_SRC WHERE ID = 3")
        # Closing a case drops it from the view's filter: a DELETE in the stream.
        cur.execute("UPDATE CASE_SRC SET STATUS = 'closed' WHERE ID = 11")
        _refresh(cur)
        listener.poll_once()

        accounts = _docs(db, "Account")
        assert {k: d["NAME"] for k, d in accounts.items()} == {"1": "Acme Corp", "2": "Beta"}
        assert set(_docs(db, "Case")) == {"10"}
        assert len(_docs(db, "caseOf")) == 1

    def test_4_unmapped_column_change_writes_nothing(self, listener, sf, arango_db):
        cur, _ = sf
        _, db = arango_db
        rev = _docs(db, "Account")["2"]["_rev"]
        skipped = listener.handler.stats.noop_updates_skipped
        cur.execute("UPDATE ACCOUNT_SRC SET NOTES = 'edited' WHERE ID = 2")
        _refresh(cur)
        result = listener.poll_once()
        assert result.watermarks["R2G_ACCOUNT"].events == 1, "the stream did carry the update"
        assert listener.handler.stats.noop_updates_skipped == skipped + 1
        assert _docs(db, "Account")["2"]["_rev"] == rev

    def test_5_unreadable_stream_is_detected(self, listener, sf, arango_db):
        cur, _ = sf
        _, db = arango_db
        case_mark = _docs(db, STREAM_STATE_COLLECTION)["R2G_CASES"]["applied_through"]
        # Recreating a dynamic table drops its change history: the stream on it
        # fails with 091901 "Base table ... dropped, cannot read from stream".
        # (A truly stale stream cannot be produced on demand; recreating a
        # view's base table, which the docs say breaks the view's streams, was
        # observed to leave the stream readable.)
        cur.execute(
            f"CREATE OR REPLACE DYNAMIC TABLE ACCOUNT TARGET_LAG = DOWNSTREAM "
            f"WAREHOUSE = {os.environ['SNOWFLAKE_WAREHOUSE']} REFRESH_MODE = INCREMENTAL "
            "AS SELECT ID, NAME, NOTES FROM ACCOUNT_SRC"
        )
        with pytest.raises(StreamStaleError, match="R2G_ACCOUNT"):
            listener.poll_once()
        # The other stream still advanced; collections fed by the broken one
        # keep their last stamp rather than claiming a newer one.
        assert _docs(db, STREAM_STATE_COLLECTION)["R2G_CASES"]["applied_through"] > case_mark
        stamps = _docs(db, FRESHNESS_COLLECTION)
        account_mark = _docs(db, STREAM_STATE_COLLECTION)["R2G_ACCOUNT"]["applied_through"]
        assert stamps["Account"]["ingested_at"] == account_mark
