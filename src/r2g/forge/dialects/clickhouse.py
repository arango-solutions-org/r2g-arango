"""``clickhouse`` — ClickHouse SQL, ``MergeTree`` tables ordered by the spine.

ClickHouse has neither enforced nor declared foreign keys, so the FK *intent*
is recorded where a reader can still find it: as a ``COMMENT`` on the FK
column and as header comments in the loader. This is the constraint-stripped
shape ADR-0006 D-3 names — the roundtrip's contract is two-branch: the
analyzers either recover the keys by inference or report them absent; a wrong
key or silence is the failure.

``ORDER BY (id)`` makes the surrogate key the table's sorting/primary key,
which ``system.columns.is_in_primary_key`` surfaces and r2g's
``ClickHouseConnector`` reads back as the PK.
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

from ..core import SURROGATE_KEY, Rows, SchemaPlan, TablePlan
from .base import SqlDialect

CLICKHOUSE_TYPE_FOR_JSON_TYPE: Dict[str, str] = {
    "integer": "Int64",
    "float": "Float64",
    "boolean": "Bool",
    "string": "String",
}


def fk_intent_comment(references: str, key: str) -> str:
    """The machine-readable FK intent stored as a column comment."""
    return f"forge:foreign-key -> {references}({key})"


class ClickHouseDialect(SqlDialect):
    name: ClassVar[str] = "clickhouse"
    type_for_json: ClassVar[Dict[str, str]] = CLICKHOUSE_TYPE_FOR_JSON_TYPE
    true_literal: ClassVar[str] = "true"
    false_literal: ClassVar[str] = "false"

    def ddl_header(self, plan: SchemaPlan) -> List[str]:
        lines = [
            "-- ClickHouse declares no foreign keys; the FK intent below is recorded as",
            "-- column comments (constraint-stripped shape, ADR-0006 D-3 two-branch contract).",
        ]
        lines.extend(
            f"-- FOREIGN KEY intent: {self.physical_table(e.from_table)}."
            f"{self.physical_column(e.fk_column)} -> "
            f"{self.physical_table(e.to_table)}({self.physical_column(e.to_key)})"
            f"  [{e.relationship}]"
            for e in plan.edges
        )
        return lines

    def column_ddl(self, table: TablePlan, column_index: int) -> str:
        col = table.columns[column_index]
        physical_type = self.type_for_json[col.json_type]
        if col.nullable:
            physical_type = f"Nullable({physical_type})"
        # Through the seam, not raw: render_loader already projects every name,
        # so emitting the canonical spelling here would make a folding dialect
        # (Snowflake uppercases both) produce DDL its own loader cannot target.
        line = f"{self.physical_column(col.name)} {physical_type}"
        if col.references is not None:
            # The parent's key, carried on the plan — not this table's.
            # Through the seam. This comment is documented as machine-readable
            # FK intent for a future comment-aware inference, so under a folding
            # dialect it must name the tables that physically exist — the header
            # lines already project, and a comment saying `accounts(id)` beside
            # `CREATE TABLE ACCOUNTS` resolves against nothing.
            comment = fk_intent_comment(
                self.physical_table(col.references),
                self.physical_column(col.references_column or SURROGATE_KEY),
            )
            line += f" COMMENT '{comment}'"
        return line

    def table_constraints(self, table: TablePlan) -> List[str]:
        return []  # no constraint syntax in ClickHouse; see table_suffix

    def table_suffix(self, table: TablePlan) -> str:
        return f" ENGINE = MergeTree ORDER BY ({self.physical_column(table.primary_key.name)})"

    def render_loader(self, plan: SchemaPlan, rows: Rows, seed: int) -> str:
        header = [
            f"-- FOREIGN KEY intent (not enforceable in ClickHouse): "
            f"{self.physical_table(e.from_table)}.{self.physical_column(e.fk_column)}"
            f" -> {self.physical_table(e.to_table)}({self.physical_column(e.to_key)})"
            for e in plan.edges
        ]
        body = super().render_loader(plan, rows, seed)
        if not header:
            return body
        lines = body.split("\n")
        # After the two standard header lines ("-- Federation Forge…", "-- dialect…").
        return "\n".join(lines[:2] + header + lines[2:])

    def bulk_header(self) -> List[str]:
        return [
            "-- FROM INFILE is read by clickhouse-client, not the server. Run from this",
            "-- directory, after the DDL:",
            "--   clickhouse-client --multiquery --queries-file forge.sql --queries-file forge.bulk.sql",
            "-- \\N reads as NULL (format_csv_null_representation's default).",
        ]

    def bulk_load_statement(self, physical: str, columns: str, path: str) -> str:
        return f"INSERT INTO {physical} ({columns}) FROM INFILE '{path}' FORMAT CSV;"


__all__ = ["CLICKHOUSE_TYPE_FOR_JSON_TYPE", "ClickHouseDialect", "fk_intent_comment"]
