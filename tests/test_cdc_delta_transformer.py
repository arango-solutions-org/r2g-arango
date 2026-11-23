"""Tests for the CDC DeltaTransformer."""

from __future__ import annotations

import pytest

from r2g.cdc.delta_transformer import DeltaTransformer
from r2g.cdc.models import (
    ArangoOperation,
    ChangeEvent,
    ChangeOperation,
)
from r2g.types import (
    CollectionMapping,
    Column,
    EdgeDefinition,
    ForeignKey,
    MappingConfig,
    Schema,
    Table,
)


@pytest.fixture
def schema() -> Schema:
    return Schema(tables={
        "users": Table(
            name="users",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="name", data_type="text"),
                Column(name="email", data_type="text"),
            ],
            primary_key=["id"],
        ),
        "orders": Table(
            name="orders",
            columns=[
                Column(name="id", data_type="integer", is_primary_key=True),
                Column(name="user_id", data_type="integer"),
                Column(name="total", data_type="numeric"),
            ],
            primary_key=["id"],
            foreign_keys=[
                ForeignKey(column="user_id", foreign_table="users", foreign_column="id"),
            ],
        ),
    })


@pytest.fixture
def config() -> MappingConfig:
    return MappingConfig(
        collections={
            "users": CollectionMapping(source_table="users", target_collection="users"),
            "orders": CollectionMapping(source_table="orders", target_collection="orders"),
        },
        edges=[
            EdgeDefinition(
                edge_collection="orders_to_users",
                from_collection="orders",
                to_collection="users",
                from_field="user_id",
                to_field="id",
            ),
        ],
    )


class TestInsertTransform:
    def test_insert_produces_document_delta(self, schema, config):
        xform = DeltaTransformer(schema, config)
        evt = ChangeEvent(
            operation=ChangeOperation.INSERT,
            table_name="users",
            new_row={"id": 1, "name": "Alice", "email": "alice@example.com"},
        )
        deltas = xform.transform(evt)
        assert len(deltas) == 1
        assert deltas[0].operation == ArangoOperation.INSERT
        assert deltas[0].collection == "users"
        assert deltas[0].document["_key"] == "1"
        assert deltas[0].document["name"] == "Alice"

    def test_insert_with_edges(self, schema, config):
        xform = DeltaTransformer(schema, config)
        evt = ChangeEvent(
            operation=ChangeOperation.INSERT,
            table_name="orders",
            new_row={"id": 5, "user_id": 1, "total": 99.99},
        )
        deltas = xform.transform(evt)
        assert len(deltas) == 2
        doc_delta = deltas[0]
        edge_delta = deltas[1]
        assert doc_delta.collection == "orders"
        assert doc_delta.document["_key"] == "5"
        assert edge_delta.collection == "orders_to_users"
        assert edge_delta.is_edge
        assert edge_delta.document["_from"] == "orders/5"
        assert edge_delta.document["_to"] == "users/1"


class TestUpdateTransform:
    def test_update_produces_replace_delta(self, schema, config):
        xform = DeltaTransformer(schema, config)
        evt = ChangeEvent(
            operation=ChangeOperation.UPDATE,
            table_name="users",
            old_row={"id": 1, "name": "Alice", "email": "old@example.com"},
            new_row={"id": 1, "name": "Alice", "email": "new@example.com"},
        )
        deltas = xform.transform(evt)
        assert len(deltas) == 1
        assert deltas[0].operation == ArangoOperation.REPLACE
        assert deltas[0].document["email"] == "new@example.com"

    def test_update_with_fk_change(self, schema, config):
        xform = DeltaTransformer(schema, config)
        evt = ChangeEvent(
            operation=ChangeOperation.UPDATE,
            table_name="orders",
            old_row={"id": 5, "user_id": 1, "total": 99.99},
            new_row={"id": 5, "user_id": 2, "total": 99.99},
        )
        deltas = xform.transform(evt)
        assert len(deltas) == 2
        edge_delta = deltas[1]
        assert edge_delta.operation == ArangoOperation.REPLACE
        assert edge_delta.document["_to"] == "users/2"


class TestDeleteTransform:
    def test_delete_produces_delete_delta(self, schema, config):
        xform = DeltaTransformer(schema, config)
        evt = ChangeEvent(
            operation=ChangeOperation.DELETE,
            table_name="users",
            old_row={"id": 1, "name": "Alice", "email": "alice@example.com"},
        )
        deltas = xform.transform(evt)
        assert len(deltas) == 1
        assert deltas[0].operation == ArangoOperation.DELETE
        assert deltas[0].key == "1"

    def test_delete_with_edge_cleanup(self, schema, config):
        xform = DeltaTransformer(schema, config)
        evt = ChangeEvent(
            operation=ChangeOperation.DELETE,
            table_name="orders",
            old_row={"id": 5, "user_id": 1, "total": 99.99},
        )
        deltas = xform.transform(evt)
        assert len(deltas) == 2
        assert deltas[0].operation == ArangoOperation.DELETE
        assert deltas[0].collection == "orders"
        assert deltas[0].key == "5"
        assert deltas[1].operation == ArangoOperation.DELETE
        assert deltas[1].collection == "orders_to_users"
        assert deltas[1].is_edge


class TestKeylessTableDelete:
    """A keyless table is keyed by every column, on delete as on insert."""

    @staticmethod
    def _xform() -> DeltaTransformer:
        schema = Schema(tables={"hero_power": Table(
            name="hero_power",
            columns=[Column(name="hero_id", data_type="integer"), Column(name="power_id", data_type="integer")],
            primary_key=[],
        )})
        config = MappingConfig(collections={
            "hero_power": CollectionMapping(source_table="hero_power", target_collection="hero_power"),
        })
        return DeltaTransformer(schema, config)

    def test_delete_with_full_old_row_deletes_the_inserted_document(self):
        # Regression: the insert was keyed hero_power/1_7, the delete produced nothing.
        xform = self._xform()
        row = {"hero_id": 1, "power_id": 7}
        [ins] = xform.transform(ChangeEvent(operation=ChangeOperation.INSERT, table_name="hero_power", new_row=row))
        [dele] = xform.transform(ChangeEvent(operation=ChangeOperation.DELETE, table_name="hero_power", old_row=row))
        assert (ins.collection, ins.key) == ("hero_power", "1_7")
        assert (dele.operation, dele.collection, dele.key) == (ArangoOperation.DELETE, "hero_power", "1_7")

    def test_delete_without_full_old_row_deletes_nothing(self):
        # Without REPLICA IDENTITY FULL the old row is partial; a guessed key could hit another row.
        xform = self._xform()
        for old in (None, {"hero_id": 1}):
            evt = ChangeEvent(operation=ChangeOperation.DELETE, table_name="hero_power", old_row=old)
            assert xform.transform(evt) == []


class TestUnmappedTable:
    def test_unmapped_table_produces_no_deltas(self, schema, config):
        xform = DeltaTransformer(schema, config)
        evt = ChangeEvent(
            operation=ChangeOperation.INSERT,
            table_name="audit_log",
            new_row={"id": 1, "msg": "test"},
        )
        deltas = xform.transform(evt)
        assert len(deltas) == 0


class TestNullForeignKey:
    def test_null_fk_skips_edge(self, schema, config):
        xform = DeltaTransformer(schema, config)
        evt = ChangeEvent(
            operation=ChangeOperation.INSERT,
            table_name="orders",
            new_row={"id": 5, "user_id": None, "total": 50.0},
        )
        deltas = xform.transform(evt)
        assert len(deltas) == 1
        assert deltas[0].collection == "orders"


class TestCompositeKey:
    def test_composite_pk_key_generation(self):
        schema = Schema(tables={
            "enrollments": Table(
                name="enrollments",
                columns=[
                    Column(name="student_id", data_type="integer", is_primary_key=True),
                    Column(name="course_id", data_type="integer", is_primary_key=True),
                    Column(name="grade", data_type="text"),
                ],
                primary_key=["student_id", "course_id"],
            ),
        })
        config = MappingConfig(
            collections={
                "enrollments": CollectionMapping(
                    source_table="enrollments",
                    target_collection="enrollments",
                ),
            },
        )
        xform = DeltaTransformer(schema, config)
        evt = ChangeEvent(
            operation=ChangeOperation.INSERT,
            table_name="enrollments",
            new_row={"student_id": 10, "course_id": 20, "grade": "A"},
        )
        deltas = xform.transform(evt)
        assert deltas[0].document["_key"] == "10_20"
