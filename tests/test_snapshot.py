from __future__ import annotations

import json

import pytest

from r2g.catalog import CURRENT_SCHEMA_FORMAT, CatalogManager
from r2g.snapshot import (
    build_source_schema,
    capture_source_snapshot,
    overlay_fingerprint,
    read_schema_file,
)
from r2g.types import Column, Schema, Table


@pytest.fixture
def keyless_schema() -> Schema:
    return Schema(
        tables={
            "ACCOUNTS": Table(
                name="ACCOUNTS",
                columns=[
                    Column(name="ACCOUNT_ID", data_type="number", is_nullable=False),
                    Column(name="NAME", data_type="text"),
                ],
            ),
            "CONTACTS": Table(
                name="CONTACTS",
                columns=[
                    Column(name="CONTACT_ID", data_type="number", is_nullable=False),
                    Column(name="ACCOUNT_ID", data_type="number", is_nullable=False),
                ],
            ),
        }
    )


@pytest.fixture
def key_overlay() -> dict:
    return {
        "version": 1,
        "description": "Reviewed Customer 360 keys",
        "tables": {
            "ACCOUNTS": {"primaryKey": ["ACCOUNT_ID"]},
            "CONTACTS": {
                "primaryKey": ["CONTACT_ID"],
                "foreignKeys": [
                    {
                        "columns": ["ACCOUNT_ID"],
                        "references": {
                            "table": "ACCOUNTS",
                            "columns": ["ACCOUNT_ID"],
                        },
                        "constraintName": "contacts_account",
                    }
                ],
            },
        },
    }


class _Connector:
    def __init__(self, schema: Schema) -> None:
        self.schema = schema

    def get_schema(self) -> Schema:
        return self.schema


def test_build_source_schema_applies_overlay_before_persistence(
    monkeypatch, tmp_path, keyless_schema, key_overlay
):
    mgr = CatalogManager(tmp_path)
    source = mgr.add_source(
        "customer360",
        "snowflake",
        "snowflake://user:password@account/DB/SCHEMA",
        key_overlay=key_overlay,
        key_overlay_source="bundled:customer360",
    )
    monkeypatch.setattr(
        "r2g.connectors.base.create_source_connector",
        lambda *args, **kwargs: _Connector(keyless_schema),
    )

    result = build_source_schema(source, schema_name="R2G_CUSTOMER_360")

    assert result.key_overlay_summary == {
        "tables": 2,
        "primaryKeys": 2,
        "foreignKeys": 1,
        "uniqueConstraints": 0,
    }
    assert keyless_schema.tables["ACCOUNTS"].primary_key == []
    assert result.schema.tables["ACCOUNTS"].primary_key == ["ACCOUNT_ID"]
    (fk,) = result.schema.tables["CONTACTS"].foreign_keys
    assert fk.constraint_name == "overlay:contacts_account"
    assert fk.enforced is False


def test_capture_source_snapshot_retains_overlay_audit_metadata(
    monkeypatch, tmp_path, keyless_schema, key_overlay
):
    mgr = CatalogManager(tmp_path)
    mgr.add_source(
        "customer360",
        "snowflake",
        "snowflake://user:password@account/DB/SCHEMA",
        key_overlay=key_overlay,
        key_overlay_source="bundled:customer360",
    )
    monkeypatch.setattr(
        "r2g.connectors.base.create_source_connector",
        lambda *args, **kwargs: _Connector(keyless_schema),
    )

    snapshot, _ = capture_source_snapshot(
        mgr,
        "customer360",
        schema_name="R2G_CUSTOMER_360",
    )
    reloaded = CatalogManager(tmp_path).get_latest_snapshot("customer360")

    assert reloaded is not None
    assert reloaded.id == snapshot.id
    assert reloaded.key_overlay_source == "bundled:customer360"
    assert reloaded.key_overlay_summary["foreignKeys"] == 1
    assert reloaded.key_overlay_fingerprint == overlay_fingerprint(key_overlay)
    (fk,) = reloaded.schema_data.tables["CONTACTS"].foreign_keys
    assert fk.constraint_name == "overlay:contacts_account"
    assert fk.enforced is False


def test_invalid_overlay_fails_snapshot_loudly(
    monkeypatch, tmp_path, keyless_schema
):
    mgr = CatalogManager(tmp_path)
    source = mgr.add_source(
        "customer360",
        "snowflake",
        "snowflake://user:password@account/DB/SCHEMA",
        key_overlay={
            "version": 1,
            "tables": {"ACCOUNTS": {"primaryKey": ["MISSPELLED"]}},
        },
    )
    monkeypatch.setattr(
        "r2g.connectors.base.create_source_connector",
        lambda *args, **kwargs: _Connector(keyless_schema),
    )

    with pytest.raises(ValueError, match="unknown column"):
        build_source_schema(source, schema_name="R2G_CUSTOMER_360")


def test_overlay_fingerprint_is_canonical_and_detects_same_count_changes(
    key_overlay,
):
    reordered = {
        "tables": key_overlay["tables"],
        "description": key_overlay["description"],
        "version": key_overlay["version"],
    }
    changed = {
        **key_overlay,
        "description": "A different reviewed overlay with the same key counts",
    }

    assert overlay_fingerprint(reordered) == overlay_fingerprint(key_overlay)
    assert overlay_fingerprint(changed) != overlay_fingerprint(key_overlay)



# ── Uniqueness must survive persistence (PLAN-key-identification A1) ─────────
#
# FK inference only targets declared candidate keys -- a primary key or a UNIQUE
# constraint. r2g's Table serializer used to drop unique_constraints, so a UNIQUE
# key present at capture (including one a curator declared in a reviewed overlay)
# vanished when the catalog reloaded the snapshot, taking its inferred FK with it.


@pytest.fixture
def natural_key_schema() -> Schema:
    """ORDERS references ACCOUNTS by a natural key, not by the surrogate PK."""
    return Schema(
        tables={
            "ACCOUNTS": Table(
                name="ACCOUNTS",
                columns=[
                    Column(name="ACCOUNT_ID", data_type="number", is_nullable=False),
                    Column(name="ACCOUNT_CODE", data_type="text", is_nullable=False),
                ],
            ),
            "ORDERS": Table(
                name="ORDERS",
                columns=[
                    Column(name="ORDER_ID", data_type="number", is_nullable=False),
                    Column(name="ACCOUNT_CODE", data_type="text"),
                ],
            ),
        }
    )


def _capture_with_overlay(monkeypatch, tmp_path, schema: Schema, overlay: dict):
    mgr = CatalogManager(tmp_path)
    mgr.add_source(
        "warehouse",
        "snowflake",
        "snowflake://user:password@account/DB/SCHEMA",
        key_overlay=overlay,
        key_overlay_source="test:natural-key",
    )
    monkeypatch.setattr(
        "r2g.connectors.base.create_source_connector",
        lambda *args, **kwargs: _Connector(schema),
    )
    capture_source_snapshot(mgr, "warehouse", schema_name="S")
    reloaded = CatalogManager(tmp_path).get_latest_snapshot("warehouse")
    assert reloaded is not None
    return reloaded


def test_overlay_unique_key_survives_reload_and_still_drives_inference(monkeypatch, tmp_path, natural_key_schema):
    from r2g.fk_inference import infer_foreign_keys

    overlay = {
        "version": 1,
        "tables": {
            "ACCOUNTS": {"primaryKey": ["ACCOUNT_ID"], "uniqueConstraints": [["ACCOUNT_CODE"]]},
            "ORDERS": {"primaryKey": ["ORDER_ID"]},
        },
    }
    reloaded = _capture_with_overlay(monkeypatch, tmp_path, natural_key_schema, overlay)

    assert reloaded.schema_data.tables["ACCOUNTS"].unique_constraints == [["ACCOUNT_CODE"]]
    inferred = {
        (c.table, tuple(c.columns), c.foreign_table, tuple(c.foreign_columns))
        for c in infer_foreign_keys(reloaded.schema_data)
    }
    # The only route to this FK is the UNIQUE natural key: ACCOUNT_CODE is not the PK.
    assert ("ORDERS", ("ACCOUNT_CODE",), "ACCOUNTS", ("ACCOUNT_CODE",)) in inferred


def test_new_snapshots_are_stamped_with_the_current_storage_format(monkeypatch, tmp_path, natural_key_schema):
    from r2g.catalog import CURRENT_SCHEMA_FORMAT

    overlay = {"version": 1, "tables": {"ACCOUNTS": {"primaryKey": ["ACCOUNT_ID"]}}}
    reloaded = _capture_with_overlay(monkeypatch, tmp_path, natural_key_schema, overlay)

    # Format 2 means the snapshot *can* hold UNIQUE keys. It does not promise the
    # capture looked for them: r2g's connectors read none yet (Plan B).
    assert reloaded.schema_format_version == CURRENT_SCHEMA_FORMAT


def test_legacy_snapshot_reads_as_format_1_and_resaves_byte_identically():
    from r2g.catalog import SchemaSnapshot

    # Exactly what a pre-format-2 r2g wrote: no marker, pydantic's own "Z" timestamp.
    legacy = {
        "id": "s1",
        "source_name": "pg",
        "schema_data": {
            "tables": {
                "t": {
                    "name": "t",
                    "columns": [
                        {
                            "name": "id",
                            "data_type": "int",
                            "is_nullable": False,
                            "is_primary_key": True,
                            "classification": None,
                        }
                    ],
                    "primary_key": ["id"],
                    "foreign_keys": [],
                    "is_partitioned": False,
                    "partition_of": None,
                }
            }
        },
        "captured_at": "2026-01-01T00:00:00Z",
        "pg_schema": "public",
        "key_overlay_summary": {},
        "key_overlay_source": None,
        "key_overlay_fingerprint": None,
    }
    snap = SchemaSnapshot.model_validate(legacy)

    # Format 1 could not hold UNIQUE keys, so its empty unique_constraints says nothing.
    assert snap.schema_format_version == 1
    assert snap.model_dump(mode="json") == legacy


def test_table_without_uniqueness_serializes_its_historical_keys_only():
    table = Table(name="T", columns=[Column(name="ID", data_type="int")])
    assert list(table.model_dump()) == [
        "name",
        "columns",
        "primary_key",
        "foreign_keys",
        "is_partitioned",
        "partition_of",
    ]


# ── Second review of #12 ─────────────────────────────────────────────────────


def test_reload_rederives_column_uniqueness_so_ontology_keys_match_capture(monkeypatch, tmp_path, natural_key_schema):
    """RSA's ontology baseline reads only Column.is_unique, which is not persisted."""
    from relational_schema_analyzer.baseline import _is_key_column

    overlay = {
        "version": 1,
        "tables": {
            "ACCOUNTS": {"primaryKey": ["ACCOUNT_ID"], "uniqueConstraints": [["ACCOUNT_CODE"]]},
            "ORDERS": {"primaryKey": ["ORDER_ID"]},
        },
    }
    reloaded = _capture_with_overlay(monkeypatch, tmp_path, natural_key_schema, overlay)
    accounts = reloaded.schema_data.tables["ACCOUNTS"]
    code = next(c for c in accounts.columns if c.name == "ACCOUNT_CODE")
    pk = next(c for c in accounts.columns if c.name == "ACCOUNT_ID")

    assert code.is_unique and pk.is_unique
    assert _is_key_column(code, accounts.primary_key)
    # Derived, not stored: the column's persisted key set is unchanged.
    assert "is_unique" not in code.model_dump()


def test_rederiving_uniqueness_does_not_mark_composite_key_members():
    table = Table(
        name="T",
        primary_key=["A", "B"],
        unique_constraints=[["C", "D"]],
        columns=[Column(name=n, data_type="int") for n in "ABCD"],
    )
    assert not any(c.is_unique for c in table.columns)


def test_snapshot_from_a_newer_format_resaves_as_this_format():
    from r2g.catalog import CURRENT_SCHEMA_FORMAT, SchemaSnapshot

    newer = SchemaSnapshot.model_validate(
        {
            "id": "s9",
            "source_name": "pg",
            "schema_data": {"tables": {}},
            "captured_at": "2026-01-01T00:00:00Z",
            "schema_format_version": CURRENT_SCHEMA_FORMAT + 1,
        }
    )
    # This version's serializers may drop what the newer format stores, so the
    # re-saved marker must not keep claiming the newer format.
    assert newer.model_dump(mode="json")["schema_format_version"] == CURRENT_SCHEMA_FORMAT


@pytest.mark.parametrize(
    ("version", "declared", "lost"),
    [(1, 2, True), (1, 0, False), (2, 2, False)],
)
def test_lost_overlay_unique_keys_detects_only_a_real_loss(version, declared, lost):
    from r2g.catalog import SchemaSnapshot

    snap = SchemaSnapshot.model_validate(
        {
            "id": "s1",
            "source_name": "wh",
            "schema_data": {"tables": {}},
            "captured_at": "2026-01-01T00:00:00Z",
            "key_overlay_summary": {"uniqueConstraints": declared},
            "schema_format_version": version,
        }
    )
    assert snap.lost_overlay_unique_keys is lost


def test_latest_snapshot_warns_when_it_lost_overlay_unique_keys(monkeypatch, tmp_path):
    import r2g.catalog as catalog_mod
    from r2g.catalog import SchemaSnapshot

    mgr = CatalogManager(tmp_path)
    mgr.add_source("wh", "snowflake", "snowflake://user:password@account/DB/SCHEMA")
    catalog = mgr._load()
    catalog.snapshots["old"] = SchemaSnapshot.model_validate(
        {
            "id": "old",
            "source_name": "wh",
            "schema_data": {"tables": {}},
            "captured_at": "2026-01-01T00:00:00Z",
            "key_overlay_summary": {"uniqueConstraints": 1},
        }
    )
    mgr._save(catalog)
    warnings: list[str] = []
    monkeypatch.setattr(catalog_mod.logger, "warning", lambda event, **kw: warnings.append(event))

    assert mgr.get_latest_snapshot("wh").id == "old"
    assert warnings == ["snapshot_lost_overlay_unique_keys"]


# ── Table or view: true, false, and unknown stay distinct ───────────────────


@pytest.mark.parametrize(
    ("recorded", "in_memory", "after_reload"),
    [({}, None, None), ({"is_view": False}, False, None), ({"is_view": True}, True, True)],
    ids=["absent", "false", "true"],
)
def test_offline_schema_file_keeps_view_status(tmp_path, recorded, in_memory, after_reload):
    path = tmp_path / "schema.json"
    table = {"name": "X", "columns": [{"name": "A", "data_type": "text"}], **recorded}
    path.write_text(json.dumps({"tables": {"X": table}}), encoding="utf-8")
    mgr = CatalogManager(tmp_path / "catalog")
    mgr.add_source("wh", "snowflake", "snowflake://user:password@account/DB/SCHEMA")

    _, result = capture_source_snapshot(
        mgr, "wh", schema_name="PUBLIC", schema_file=read_schema_file(path)
    )
    reloaded = CatalogManager(tmp_path / "catalog").get_latest_snapshot("wh")

    assert result.schema.tables["X"].is_view is in_memory
    assert reloaded.schema_data.tables["X"].is_view is after_reload
    persisted = reloaded.model_dump(mode="json")["schema_data"]["tables"]["X"]
    assert ("is_view" in persisted) is (after_reload is True)
    assert reloaded.schema_format_version == CURRENT_SCHEMA_FORMAT == 2


def test_key_overlay_keeps_view_status(monkeypatch, tmp_path):
    columns = [Column(name="ID", data_type="number", is_nullable=False)]
    schema = Schema(
        tables={
            "V": Table(name="V", columns=columns, is_view=True),
            "T": Table(name="T", columns=columns, is_view=False),
            "U": Table(name="U", columns=columns),
        }
    )
    overlay = {
        "version": 1,
        "tables": {name: {"primaryKey": ["ID"]} for name in ("V", "T", "U")},
    }
    mgr = CatalogManager(tmp_path)
    mgr.add_source(
        "wh", "snowflake", "snowflake://user:password@account/DB/SCHEMA", key_overlay=overlay
    )
    monkeypatch.setattr(
        "r2g.connectors.base.create_source_connector",
        lambda *args, **kwargs: _Connector(schema),
    )

    _, result = capture_source_snapshot(mgr, "wh", schema_name="PUBLIC")
    tables = CatalogManager(tmp_path).get_latest_snapshot("wh").schema_data.tables

    assert {n: t.is_view for n, t in result.schema.tables.items()} == {
        "V": True,
        "T": False,
        "U": None,
    }
    assert {n: t.is_view for n, t in tables.items()} == {"V": True, "T": None, "U": None}
    assert all(t.primary_key == ["ID"] for t in tables.values())


def test_a_snapshot_of_known_tables_writes_no_view_key(monkeypatch, tmp_path):
    columns = [Column(name="ID", data_type="number", is_nullable=False)]
    known = Schema(tables={"T": Table(name="T", columns=columns, is_view=False)})
    mgr = CatalogManager(tmp_path)
    mgr.add_source("wh", "snowflake", "snowflake://user:password@account/DB/SCHEMA")
    monkeypatch.setattr(
        "r2g.connectors.base.create_source_connector",
        lambda *args, **kwargs: _Connector(known),
    )

    snapshot, _ = capture_source_snapshot(mgr, "wh", schema_name="PUBLIC")

    assert "is_view" not in snapshot.model_dump(mode="json")["schema_data"]["tables"]["T"]


def test_legacy_snapshot_reloads_as_unknown_and_resaves_byte_identically():
    from r2g.catalog import SchemaSnapshot

    legacy = {
        "id": "old",
        "source_name": "wh",
        "schema_data": {
            "tables": {
                "X": {
                    "name": "X",
                    "columns": [
                        {
                            "name": "A",
                            "data_type": "text",
                            "is_nullable": True,
                            "is_primary_key": False,
                            "classification": None,
                        }
                    ],
                    "primary_key": [],
                    "foreign_keys": [],
                    "is_partitioned": False,
                    "partition_of": None,
                }
            }
        },
        "captured_at": "2026-01-01T00:00:00Z",
        "pg_schema": "public",
        "key_overlay_summary": {},
        "key_overlay_source": None,
        "key_overlay_fingerprint": None,
    }
    snap = SchemaSnapshot.model_validate(legacy)

    assert snap.schema_data.tables["X"].is_view is None
    assert snap.schema_format_version == 1
    saved = snap.model_dump(mode="json")
    assert saved["schema_data"] == legacy["schema_data"]
    assert "schema_format_version" not in saved


def test_fingerprint_changes_only_for_a_known_view():
    from relational_schema_analyzer.metadata import fingerprint_physical_schema

    def fingerprint(**status) -> str:
        table = Table(name="X", columns=[Column(name="A", data_type="text")], **status)
        return fingerprint_physical_schema(Schema(tables={"X": table}))

    assert fingerprint() == fingerprint(is_view=False)
    assert fingerprint() != fingerprint(is_view=True)
