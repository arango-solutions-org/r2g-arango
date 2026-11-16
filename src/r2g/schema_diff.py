"""Compare two Schema objects and produce a structured diff.

Used by ``r2g diff-schema`` to detect added/removed tables, column
changes, primary key changes, and foreign key changes between two
schema snapshots.
"""
from __future__ import annotations

from typing import Any

from r2g.types import Schema


def _fk_signature(fk: Any) -> tuple[tuple[str, ...], str, tuple[str, ...]]:
    return (tuple(fk.columns), fk.foreign_table, tuple(fk.foreign_columns))


#: Fixed-point type names that a snapshot may record with or without
#: ``(precision,scale)``. r2g's Snowflake connector added the parameters in
#: r2g-arango#24, so a snapshot written before then has the bare name.
_FIXED_POINT_TYPES = frozenset({"number", "numeric", "decimal"})


def _same_type(old: str, new: str) -> bool:
    """Whether two recorded column types describe the same column type.

    A bare fixed-point name and the same name with parameters
    (``number`` / ``number(38,0)``) are the same type: the bare one comes from a
    snapshot that did not record the parameters, not from a changed column. Two
    different parameter lists are a change.
    """
    if old == new:
        return True
    old_base, _, old_params = old.lower().partition("(")
    new_base, _, new_params = new.lower().partition("(")
    old_base, new_base = old_base.strip(), new_base.strip()
    return (
        old_base == new_base
        and old_base in _FIXED_POINT_TYPES
        and (not old_params or not new_params)
    )


def diff_schemas(old: Schema, new: Schema) -> dict[str, Any]:
    """Return a structured diff between *old* and *new* schemas.

    Result keys:
      - ``added_tables``: list of table names present in *new* but not *old*
      - ``removed_tables``: list of table names present in *old* but not *new*
      - ``modified_tables``: dict mapping table name → per-table change details
    """
    old_names = set(old.tables)
    new_names = set(new.tables)

    added = sorted(new_names - old_names)
    removed = sorted(old_names - new_names)
    common = sorted(old_names & new_names)

    modified: dict[str, dict[str, Any]] = {}

    for name in common:
        ot = old.tables[name]
        nt = new.tables[name]
        changes: dict[str, Any] = {}

        old_cols = {c.name: c for c in ot.columns}
        new_cols = {c.name: c for c in nt.columns}

        added_cols = [
            {"name": c.name, "type": c.data_type, "nullable": c.is_nullable}
            for c in nt.columns
            if c.name not in old_cols
        ]
        removed_cols = sorted(set(old_cols) - set(new_cols))

        type_changes = []
        nullable_changes = []
        for col_name in sorted(set(old_cols) & set(new_cols)):
            oc = old_cols[col_name]
            nc = new_cols[col_name]
            if not _same_type(oc.data_type, nc.data_type):
                type_changes.append({
                    "column": col_name,
                    "old_type": oc.data_type,
                    "new_type": nc.data_type,
                })
            if oc.is_nullable != nc.is_nullable:
                nullable_changes.append({
                    "column": col_name,
                    "old_nullable": oc.is_nullable,
                    "new_nullable": nc.is_nullable,
                })

        if added_cols:
            changes["added_columns"] = added_cols
        if removed_cols:
            changes["removed_columns"] = removed_cols
        if type_changes:
            changes["type_changes"] = type_changes
        if nullable_changes:
            changes["nullable_changes"] = nullable_changes

        if ot.primary_key != nt.primary_key:
            changes["pk_changed"] = True
            changes["old_pk"] = ot.primary_key
            changes["new_pk"] = nt.primary_key

        old_fks = {_fk_signature(fk) for fk in ot.foreign_keys}
        new_fks = {_fk_signature(fk) for fk in nt.foreign_keys}

        added_fk_sigs = new_fks - old_fks
        removed_fk_sigs = old_fks - new_fks

        if added_fk_sigs:
            changes["added_fks"] = [
                {"columns": list(s[0]), "foreign_table": s[1], "foreign_columns": list(s[2])}
                for s in sorted(added_fk_sigs)
            ]
        if removed_fk_sigs:
            changes["removed_fks"] = [
                {"columns": list(s[0]), "foreign_table": s[1], "foreign_columns": list(s[2])}
                for s in sorted(removed_fk_sigs)
            ]

        if changes:
            modified[name] = changes

    return {
        "added_tables": added,
        "removed_tables": removed,
        "modified_tables": modified,
    }
