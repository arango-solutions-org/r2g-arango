"""Unit tests for the schema_diff module."""
from __future__ import annotations

from r2g.schema_diff import diff_schemas
from r2g.types import Column, ForeignKey, Schema, Table


def _make_schema(**tables_spec) -> Schema:
    tables = {}
    for name, spec in tables_spec.items():
        cols = [Column(name=c[0], data_type=c[1], is_nullable=c[2], is_primary_key=c[3])
                for c in spec.get("columns", [])]
        fks = [ForeignKey(column=f[0], foreign_table=f[1], foreign_column=f[2])
               for f in spec.get("fks", [])]
        tables[name] = Table(
            name=name,
            columns=cols,
            primary_key=spec.get("pk", []),
            foreign_keys=fks,
        )
    return Schema(tables=tables)


class TestDiffSchemas:
    def test_identical_schemas(self):
        s = _make_schema(users={"columns": [("id", "integer", False, True)], "pk": ["id"]})
        result = diff_schemas(s, s)
        assert result["added_tables"] == []
        assert result["removed_tables"] == []
        assert result["modified_tables"] == {}

    def test_added_table(self):
        old = _make_schema(users={"columns": [("id", "integer", False, True)], "pk": ["id"]})
        new = _make_schema(
            users={"columns": [("id", "integer", False, True)], "pk": ["id"]},
            orders={"columns": [("id", "integer", False, True)], "pk": ["id"]},
        )
        result = diff_schemas(old, new)
        assert "orders" in result["added_tables"]
        assert result["removed_tables"] == []

    def test_removed_table(self):
        old = _make_schema(
            users={"columns": [("id", "integer", False, True)], "pk": ["id"]},
            orders={"columns": [("id", "integer", False, True)], "pk": ["id"]},
        )
        new = _make_schema(users={"columns": [("id", "integer", False, True)], "pk": ["id"]})
        result = diff_schemas(old, new)
        assert "orders" in result["removed_tables"]

    def test_added_column(self):
        old = _make_schema(users={"columns": [("id", "integer", False, True)], "pk": ["id"]})
        new = _make_schema(users={
            "columns": [("id", "integer", False, True), ("name", "text", False, False)],
            "pk": ["id"],
        })
        result = diff_schemas(old, new)
        mods = result["modified_tables"]["users"]
        assert len(mods["added_columns"]) == 1
        assert mods["added_columns"][0]["name"] == "name"

    def test_removed_column(self):
        old = _make_schema(users={
            "columns": [("id", "integer", False, True), ("name", "text", False, False)],
            "pk": ["id"],
        })
        new = _make_schema(users={"columns": [("id", "integer", False, True)], "pk": ["id"]})
        result = diff_schemas(old, new)
        mods = result["modified_tables"]["users"]
        assert "name" in mods["removed_columns"]

    def test_type_change(self):
        old = _make_schema(users={
            "columns": [("id", "integer", False, True), ("name", "text", False, False)],
            "pk": ["id"],
        })
        new = _make_schema(users={
            "columns": [("id", "integer", False, True), ("name", "varchar(255)", False, False)],
            "pk": ["id"],
        })
        result = diff_schemas(old, new)
        changes = result["modified_tables"]["users"]["type_changes"]
        assert len(changes) == 1
        assert changes[0]["column"] == "name"
        assert changes[0]["old_type"] == "text"
        assert changes[0]["new_type"] == "varchar(255)"

    def test_scale_recorded_where_an_older_snapshot_had_none_is_not_a_change(self):
        """r2g-arango#24: the Snowflake connector now records ``number(38,0)``
        where a snapshot written before then has ``number``. That is the same
        column, so it must not report every number column as changed."""
        old = _make_schema(t={
            "columns": [("ID", "number", False, True), ("AMOUNT", "number", True, False)],
            "pk": ["ID"],
        })
        new = _make_schema(t={
            "columns": [("ID", "number(38,0)", False, True), ("AMOUNT", "number(10,2)", True, False)],
            "pk": ["ID"],
        })
        assert diff_schemas(old, new)["modified_tables"] == {}
        assert diff_schemas(new, old)["modified_tables"] == {}

    def test_a_changed_scale_is_a_change(self):
        old = _make_schema(t={"columns": [("AMOUNT", "number(38,0)", True, False)]})
        new = _make_schema(t={"columns": [("AMOUNT", "number(10,2)", True, False)]})
        changes = diff_schemas(old, new)["modified_tables"]["t"]["type_changes"]
        assert changes == [
            {"column": "AMOUNT", "old_type": "number(38,0)", "new_type": "number(10,2)"}
        ]

    def test_bare_and_parameterised_text_types_still_differ(self):
        old = _make_schema(t={"columns": [("NAME", "varchar", True, False)]})
        new = _make_schema(t={"columns": [("NAME", "varchar(40)", True, False)]})
        assert diff_schemas(old, new)["modified_tables"]["t"]["type_changes"]

    def test_nullable_change(self):
        old = _make_schema(users={
            "columns": [("id", "integer", False, True), ("email", "text", False, False)],
            "pk": ["id"],
        })
        new = _make_schema(users={
            "columns": [("id", "integer", False, True), ("email", "text", True, False)],
            "pk": ["id"],
        })
        result = diff_schemas(old, new)
        changes = result["modified_tables"]["users"]["nullable_changes"]
        assert changes[0]["column"] == "email"
        assert changes[0]["new_nullable"] is True

    def test_pk_change(self):
        old = _make_schema(users={"columns": [("id", "integer", False, True)], "pk": ["id"]})
        new = _make_schema(users={"columns": [("id", "integer", False, True)], "pk": ["id", "name"]})
        result = diff_schemas(old, new)
        mods = result["modified_tables"]["users"]
        assert mods["pk_changed"] is True
        assert mods["old_pk"] == ["id"]
        assert mods["new_pk"] == ["id", "name"]

    def test_added_fk(self):
        old = _make_schema(orders={
            "columns": [("id", "integer", False, True), ("user_id", "integer", False, False)],
            "pk": ["id"],
        })
        new = _make_schema(orders={
            "columns": [("id", "integer", False, True), ("user_id", "integer", False, False)],
            "pk": ["id"],
            "fks": [("user_id", "users", "id")],
        })
        result = diff_schemas(old, new)
        mods = result["modified_tables"]["orders"]
        assert len(mods["added_fks"]) == 1
        assert mods["added_fks"][0]["foreign_table"] == "users"

    def test_removed_fk(self):
        old = _make_schema(orders={
            "columns": [("id", "integer", False, True), ("user_id", "integer", False, False)],
            "pk": ["id"],
            "fks": [("user_id", "users", "id")],
        })
        new = _make_schema(orders={
            "columns": [("id", "integer", False, True), ("user_id", "integer", False, False)],
            "pk": ["id"],
        })
        result = diff_schemas(old, new)
        mods = result["modified_tables"]["orders"]
        assert len(mods["removed_fks"]) == 1

    def test_multiple_changes(self):
        old = _make_schema(
            users={"columns": [("id", "integer", False, True), ("name", "text", False, False)], "pk": ["id"]},
            orders={"columns": [("id", "integer", False, True)], "pk": ["id"]},
        )
        new = _make_schema(
            users={
                "columns": [
                    ("id", "integer", False, True),
                    ("name", "varchar", False, False),
                    ("email", "text", True, False),
                ],
                "pk": ["id"],
            },
            products={"columns": [("id", "integer", False, True)], "pk": ["id"]},
        )
        result = diff_schemas(old, new)
        assert "products" in result["added_tables"]
        assert "orders" in result["removed_tables"]
        mods = result["modified_tables"]["users"]
        assert len(mods["added_columns"]) == 1
        assert len(mods["type_changes"]) == 1
