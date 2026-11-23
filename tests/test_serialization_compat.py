"""Serialization compatibility corpus (guards the RSA compat-layer refactor).

This is the byte-stability gate described in
``docs/internal/DESIGN-rsa-compat-layer.md`` §8.1. It freezes the *current*
on-disk JSON shape of the physical types (``Schema`` / ``Table`` / ``Column`` /
``ForeignKey`` / ``Classification``) and of ``MappingConfig`` as committed golden
fixtures, then asserts that:

1. current code reproduces those exact bytes (``model_dump_json(indent=2)``);
2. loading the golden files re-serializes byte-identically (no lossy fields);
3. governance/structural accessors survive the round trip; and
4. the catalog path (``CatalogManager`` → ``catalog.json`` → reload) embeds the
   **same** column shape — including ``classification``.

These tests pass against the pre-refactor code and MUST keep passing after
``r2g.types`` is re-based on ``relational-schema-analyzer`` types. When the shape
is *intentionally* changed (Strategy 2 in the design), regenerate the goldens:

    R2G_REGEN_COMPAT=1 python -m pytest tests/test_serialization_compat.py

and review the fixture diff carefully — a non-empty diff is a persisted-format
change and requires a migration story.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from r2g.catalog import CatalogManager
from r2g.classification import annotate_schema
from r2g.types import (
    Classification,
    CollectionMapping,
    Column,
    EdgeDefinition,
    FieldExpression,
    ForeignKey,
    MappingConfig,
    NamingConvention,
    Schema,
    Table,
)

FIXTURES = Path(__file__).parent / "fixtures" / "serialization_compat"
SCHEMA_GOLDEN = FIXTURES / "reference_schema.json"
MAPPING_GOLDEN = FIXTURES / "reference_mapping.json"

_REGEN = os.environ.get("R2G_REGEN_COMPAT") == "1"


def build_reference_schema() -> Schema:
    """A deliberately broad physical schema exercising every persisted shape.

    Covers: classified columns (full + partial ``Classification``), unclassified
    columns, a single-column FK, a composite FK with a constraint name, a
    composite primary key, and PostgreSQL partition metadata (parent + child).
    Classification is applied via the real :func:`annotate_schema` stamping path
    rather than set directly, so the corpus mirrors ``source snapshot``.
    """
    schema = Schema(
        tables={
            "users": Table(
                name="users",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="email", data_type="text", is_nullable=True),
                    Column(name="ssn", data_type="text", is_nullable=True),
                    Column(name="created_at", data_type="timestamp", is_nullable=True),
                ],
                primary_key=["id"],
            ),
            "orders": Table(
                name="orders",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="user_id", data_type="integer"),
                    Column(name="total", data_type="numeric(10,2)", is_nullable=True),
                ],
                primary_key=["id"],
                foreign_keys=[
                    ForeignKey(column="user_id", foreign_table="users", foreign_column="id"),
                ],
            ),
            "order_items": Table(
                name="order_items",
                columns=[
                    Column(name="order_id", data_type="integer", is_primary_key=True),
                    Column(name="product_id", data_type="integer", is_primary_key=True),
                    Column(name="qty", data_type="integer"),
                ],
                primary_key=["order_id", "product_id"],
            ),
            "shipments": Table(
                name="shipments",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="order_id", data_type="integer"),
                    Column(name="product_id", data_type="integer"),
                ],
                primary_key=["id"],
                foreign_keys=[
                    ForeignKey(
                        columns=["order_id", "product_id"],
                        foreign_table="order_items",
                        foreign_columns=["order_id", "product_id"],
                        constraint_name="fk_shipments_order_items",
                    ),
                ],
            ),
            "events": Table(
                name="events",
                columns=[
                    Column(name="id", data_type="bigint", is_primary_key=True),
                    Column(name="occurred_at", data_type="timestamp"),
                ],
                primary_key=["id"],
                is_partitioned=True,
            ),
            "events_2026": Table(
                name="events_2026",
                columns=[
                    Column(name="id", data_type="bigint", is_primary_key=True),
                    Column(name="occurred_at", data_type="timestamp"),
                ],
                primary_key=["id"],
                partition_of="events",
            ),
        }
    )
    # Stamp governance classification the way `source snapshot` does.
    annotate_schema(
        schema,
        {
            "users": {
                "email": Classification(
                    tags=["PII.Sensitive"],
                    tier="Tier.Tier1",
                    glossary_terms=["Customer.EmailAddress"],
                    source="openmetadata",
                ),
                "ssn": Classification(
                    tags=["PII.Sensitive", "PII.Direct"],
                    tier="Tier.Tier0",
                ),
            },
        },
    )
    return schema


def build_reference_mapping() -> MappingConfig:
    """A broad mapping config exercising every persisted ArangoDB-model shape."""
    return MappingConfig(
        source_schema="commerce",
        key_separator="-",
        collections={
            "users": CollectionMapping(
                source_table="users",
                target_collection="User",
                field_mappings={"id": "user_id", "email": "emailAddress"},
                exclude_fields=["ssn"],
                field_expressions=[
                    FieldExpression(
                        target="fullName",
                        sources=["first_name", "last_name"],
                        expression="CONCAT(first_name, ' ', last_name)",
                        engine="aql",
                        description="fan-in of name parts",
                    ),
                ],
            ),
            "orders": CollectionMapping(
                source_table="orders",
                target_collection="Order",
                include_fields=["id", "user_id", "total"],
            ),
        },
        edges=[
            EdgeDefinition(
                edge_collection="placed",
                from_collection="User",
                to_collection="Order",
                from_field="id",
                to_field="user_id",
            ),
            EdgeDefinition(
                edge_collection="shipped_item",
                from_collection="Shipment",
                to_collection="OrderItem",
                from_fields=["order_id", "product_id"],
                to_fields=["order_id", "product_id"],
            ),
        ],
        type_overrides={"total": "string"},
        naming_convention=NamingConvention(
            collections="pascal", properties="camel", edges="snake"
        ),
    )


def _dump(model) -> str:
    return model.model_dump_json(indent=2)


def _maybe_regen(path: Path, text: str) -> None:
    if _REGEN:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")


class TestSchemaByteStability:
    def test_current_code_reproduces_golden(self):
        text = _dump(build_reference_schema())
        _maybe_regen(SCHEMA_GOLDEN, text)
        assert text == SCHEMA_GOLDEN.read_text(encoding="utf-8").rstrip("\n"), (
            "Schema serialization drifted from the frozen corpus. If intentional, "
            "regenerate with R2G_REGEN_COMPAT=1 and provide a migration story."
        )

    def test_golden_load_roundtrips_byte_identically(self):
        golden = SCHEMA_GOLDEN.read_text(encoding="utf-8").rstrip("\n")
        loaded = Schema.model_validate_json(golden)
        assert _dump(loaded) == golden

    def test_governance_and_structure_accessors_survive(self):
        loaded = Schema.model_validate_json(
            SCHEMA_GOLDEN.read_text(encoding="utf-8")
        )
        users = loaded.tables["users"]
        by_name = {c.name: c for c in users.columns}

        # Classified columns.
        email = by_name["email"].classification
        assert email is not None
        assert email.tags == ["PII.Sensitive"]
        assert email.tier == "Tier.Tier1"
        assert email.glossary_terms == ["Customer.EmailAddress"]
        assert email.source == "openmetadata"

        ssn = by_name["ssn"].classification
        assert ssn is not None
        assert ssn.tags == ["PII.Sensitive", "PII.Direct"]
        assert ssn.tier == "Tier.Tier0"
        assert ssn.source == "catalog"  # default

        # Unclassified columns carry no classification.
        assert by_name["id"].classification is None
        assert by_name["created_at"].classification is None

        # Composite PK + composite FK + constraint name.
        assert loaded.tables["order_items"].primary_key == ["order_id", "product_id"]
        ship_fk = loaded.tables["shipments"].foreign_keys[0]
        assert ship_fk.is_composite is True
        assert ship_fk.columns == ["order_id", "product_id"]
        assert ship_fk.constraint_name == "fk_shipments_order_items"

        # Single-column FK back-compat accessor.
        order_fk = loaded.tables["orders"].foreign_keys[0]
        assert order_fk.is_composite is False
        assert order_fk.column == "user_id"
        assert order_fk.foreign_column == "id"

        # Partition metadata.
        assert loaded.tables["events"].is_partitioned is True
        assert loaded.tables["events_2026"].partition_of == "events"


class TestViewStatusSerialization:
    @pytest.mark.parametrize("status", [True, False], ids=["view", "table"])
    def test_known_status_is_written_last_and_reloads(self, status):
        table = Table(name="t", columns=[], is_view=status)
        dumped = table.model_dump()
        assert list(dumped)[-1] == "is_view"
        assert Table.model_validate(dumped).is_view is status

    def test_unknown_status_is_not_written(self):
        table = Table(name="t", columns=[])
        assert table.is_view is None
        assert "is_view" not in table.model_dump()


class TestMappingConfigByteStability:
    def test_current_code_reproduces_golden(self):
        text = _dump(build_reference_mapping())
        _maybe_regen(MAPPING_GOLDEN, text)
        assert text == MAPPING_GOLDEN.read_text(encoding="utf-8").rstrip("\n"), (
            "MappingConfig serialization drifted from the frozen corpus. If "
            "intentional, regenerate with R2G_REGEN_COMPAT=1."
        )

    def test_golden_load_roundtrips_byte_identically(self):
        golden = MAPPING_GOLDEN.read_text(encoding="utf-8").rstrip("\n")
        loaded = MappingConfig.model_validate_json(golden)
        assert _dump(loaded) == golden

    def test_edge_and_expression_accessors_survive(self):
        loaded = MappingConfig.model_validate_json(
            MAPPING_GOLDEN.read_text(encoding="utf-8")
        )
        assert loaded.edges[0].is_composite is False
        assert loaded.edges[0].from_field == "id"
        assert loaded.edges[1].is_composite is True
        assert loaded.edges[1].from_fields == ["order_id", "product_id"]
        fe = loaded.collections["users"].field_expressions[0]
        assert fe.sources == ["first_name", "last_name"]
        assert fe.is_identity is False
        assert loaded.naming_convention is not None
        assert loaded.naming_convention.collections == "pascal"


class TestCatalogEmbedsReferenceSchemaShape:
    """The persisted catalog must carry the identical column shape (classification
    included). We compare only the embedded schema portion, since the catalog
    wrapper also holds volatile/encrypted fields (timestamps, secrets)."""

    def test_snapshot_schema_matches_golden_after_reload(self, tmp_path):
        golden = SCHEMA_GOLDEN.read_text(encoding="utf-8").rstrip("\n")
        mgr = CatalogManager(catalog_dir=tmp_path)
        mgr.add_source("pg1", "postgresql", "postgresql://u:p@host/db")
        mgr.create_snapshot("pg1", build_reference_schema(), pg_schema="public")

        # Reload from disk with a fresh manager to force a full JSON round trip.
        reloaded = CatalogManager(catalog_dir=tmp_path)
        snap = reloaded.get_latest_snapshot("pg1")
        assert snap is not None
        assert _dump(snap.schema_data) == golden
