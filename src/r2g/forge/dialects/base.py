"""The dialect seam: how a :class:`~r2g.forge.core.SchemaPlan` and its rows are
projected onto one target system (ADR-0006 D-2/D-4).

To add a dialect: subclass :class:`ForgeDialect` (or :class:`SqlDialect` for
anything that speaks ``CREATE TABLE`` / ``INSERT``) in a sibling module,
declare ``name`` plus one physical type per :data:`~r2g.forge.core.JSON_TYPES`
entry, override only the hooks the target needs, and register the instance in
:data:`r2g.forge.dialects.DIALECTS`. The unit tests in ``tests/test_forge.py``
parametrize over the registry, so a new dialect is covered by the shared
contract tests (type table completeness, byte-identical rows) for free.
"""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, ClassVar, Dict, Iterable, List, Mapping, Tuple

from ..core import JSON_TYPES, SURROGATE_KEY, ForgeError, Rows, SchemaPlan, TablePlan


@dataclass(frozen=True)
class BulkTarget:
    """One thing a bulk load fills: a table, or a document or edge collection.

    ``name`` is the canonical (snake_case) name the data files are named after;
    ``physical`` is the dialect's spelling the loader addresses; ``columns``
    are the physical columns a CSV record carries, in order (empty for JSON
    lines, whose records name their own fields)."""

    name: str
    physical: str
    kind: str
    columns: Tuple[str, ...] = ()


#: Turns one canonical row of a table into ``(target name, record line)``
#: pairs — one for a SQL table, a document plus one per edge for ArangoDB.
BulkProjector = Callable[[Dict[str, Any]], Iterable[Tuple[str, str]]]


class ForgeDialect(ABC):
    """One target system's projection of the canonical plan and rows."""

    #: Registry key and the ``--dialect`` CLI value.
    name: ClassVar[str]
    #: File names :meth:`~r2g.forge.core.ForgeArtifacts.write_to` uses for
    #: ``ddl`` and ``load_sql``. SQL dialects keep the skeleton's names.
    ddl_filename: ClassVar[str] = "forge.sql"
    loader_filename: ClassVar[str] = "forge.load.sql"
    #: One physical type per conceptual JSON type. Must cover every
    #: :data:`~r2g.forge.core.JSON_TYPES` entry (asserted at import by
    #: :func:`check_type_table`).
    type_for_json: ClassVar[Mapping[str, str]]

    def physical_table(self, table: str) -> str:
        """Project a canonical snake_case table name onto the target's spelling.
        Default: unchanged."""
        return table

    def physical_column(self, column: str) -> str:
        """Project a canonical snake_case column name onto the target's spelling.
        Default: unchanged."""
        return column

    @abstractmethod
    def render_ddl(self, plan: SchemaPlan) -> str:
        """The schema definition (SQL DDL, or a manifest for non-SQL targets)."""

    @abstractmethod
    def render_loader(self, plan: SchemaPlan, rows: Rows, seed: int) -> str:
        """The loader that puts ``rows`` into the schema (INSERTs, or a script)."""

    # -- bulk (PLAN F-11): chunked data files and a loader that reads them --

    #: The bulk loader's file name and the data files' extension.
    bulk_loader_filename: ClassVar[str] = "forge.bulk.sql"
    bulk_extension: ClassVar[str] = "csv"

    @abstractmethod
    def bulk_targets(self, plan: SchemaPlan) -> List[BulkTarget]:
        """Every target a bulk load of ``plan`` fills, in load order."""

    @abstractmethod
    def bulk_projector(self, plan: SchemaPlan, table: TablePlan) -> BulkProjector:
        """How one row of ``table`` becomes record lines (built once per table)."""

    @abstractmethod
    def render_bulk_loader(self, plan: SchemaPlan, manifest: Dict[str, Any]) -> str:
        """The loader that reads the chunk files ``manifest`` lists, in order."""


def check_type_table(dialect: ForgeDialect) -> None:
    """Refuse a dialect whose type table does not cover every JSON type.

    ``name`` and ``type_for_json`` are annotation-only ClassVars on the ABC, so
    a dialect that simply omits one used to fail with ``AttributeError`` — and
    because the registry runs this at import time, that surfaced as
    ``import r2g.forge`` blowing up rather than a ForgeError naming the dialect.
    """
    name = getattr(dialect, "name", None) or type(dialect).__name__
    table = getattr(dialect, "type_for_json", None)
    if table is None:
        raise ForgeError(
            f"dialect {name!r} declares no `type_for_json` table; a dialect must "
            f"map every JSON type in {sorted(JSON_TYPES)}"
        )
    missing = [t for t in JSON_TYPES if t not in table]
    if missing:
        raise ForgeError(f"dialect {name!r} declares no physical type for {missing}")


# ── Shared SQL machinery ────────────────────────────────────────────────


def sql_literal(value: Any, *, true: str = "TRUE", false: str = "FALSE") -> str:
    """Render one synthesized value as a SQL literal. Strings are
    single-quoted with ``''`` escaping, which every target here accepts."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return true if value else false
    if isinstance(value, (int, float)):
        # repr() happily renders inf/nan as `inf`/`nan` and an IntEnum as
        # `<E.A: 1>` — none of which is a SQL literal, and all of which would
        # land in a loader that only fails when the engine parses it. Rows are
        # caller-supplied (re-read from forge.rows.json, where json.load turns
        # `Infinity` back into a float), so this is reachable without the
        # synthesizer ever producing one.
        if isinstance(value, float) and not math.isfinite(value):
            raise ForgeError(f"{value!r} has no SQL literal form")
        return repr(int(value) if isinstance(value, int) else value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


#: How a NULL is written in a bulk CSV: unquoted ``\N``, which Postgres
#: (``NULL '\N'``), ClickHouse and Snowflake (their default ``NULL_IF``) all
#: read as NULL. Strings are always quoted, so a string ``\N`` stays a string.
CSV_NULL = "\\N"


def csv_value(value: Any) -> str:
    """One synthesized value as a bulk-CSV field (no header, comma-separated).

    Strings are always double-quoted with ``""`` doubling, so neither a comma,
    a newline nor a literal ``\\N`` can be misread. Non-finite floats are
    refused, as :func:`sql_literal` refuses them: no engine reads ``inf`` back
    as the value it was."""
    if value is None:
        return CSV_NULL
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(int(value))
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ForgeError(f"{value!r} has no CSV form a database reads back")
        return repr(value)
    return '"' + str(value).replace('"', '""') + '"'


def split_sql_statements(sql: str, *, backslash_escapes: bool = False) -> List[str]:
    """Split a loader/DDL script into individual statements.

    Drops comments and splits on ``;`` outside quoted text, so drivers that
    execute one statement per call (ClickHouse HTTP, Snowflake
    ``cursor.execute``) can replay a script verbatim.

    Public, and pointed at hand-written scripts as well as the forge's own
    output, so it understands:

    * ``--`` line comments anywhere on a line, and ``/* block */`` comments;
    * ``'literals'`` and ``"quoted identifiers"``, with ``''`` / ``""`` doubling;
    * ``$$ dollar quoted $$`` bodies (Postgres function definitions);
    * ```backtick identifiers``` (ClickHouse, MySQL).

    ``backslash_escapes`` is **off** by default, which is standard SQL: with
    ``standard_conforming_strings`` on, Postgres reads ``'C:\\'`` as a complete
    literal ending in a backslash. ClickHouse and MySQL instead treat ``\\`` as
    an escape, so a caller replaying a script for those engines should pass
    ``True``. Guessing per-engine here would silently corrupt the other.

    Unterminated quoting or an unclosed ``/*`` raises :class:`ForgeError` rather
    than returning a truncated list: an earlier version ran to end-of-input and
    dropped every remaining statement, so a stray ``/*`` meant the tail of a
    script was never executed and nothing said so.
    """
    statements: List[str] = []
    current: List[str] = []
    i, n = 0, len(sql)
    state = None          # None | "'" | '"' | "`" | "$$"

    def flush() -> None:
        statement = "".join(current).strip()
        if statement:
            statements.append(statement)
        current.clear()

    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if state is None:
            if ch == "-" and nxt == "-":
                end = sql.find("\n", i)
                i = n if end == -1 else end       # newline survives as whitespace
                continue
            if ch == "/" and nxt == "*":
                end = sql.find("*/", i + 2)
                if end == -1:
                    raise ForgeError(
                        "unterminated /* block comment; refusing to drop the "
                        "rest of the script silently"
                    )
                i = end + 2
                continue
            if ch == "$" and nxt == "$":
                state = "$$"
                current.append(ch)
                current.append(nxt)
                i += 2
                continue
            if ch == ";":
                flush()
                i += 1
                continue
            if ch in "'\"`":
                state = ch
        elif state == "$$":
            if ch == "$" and nxt == "$":
                state = None
                current.append(ch)
                current.append(nxt)
                i += 2
                continue
        elif ch == "\\" and backslash_escapes and state in ("'", '"'):
            current.append(ch)
            if nxt:
                current.append(nxt)
                i += 2
                continue
        elif ch == state:
            if nxt == state:                      # '' / "" / `` doubling
                current.append(ch)
                current.append(nxt)
                i += 2
                continue
            state = None
        current.append(ch)
        i += 1

    if state is not None:
        raise ForgeError(
            f"unterminated {'dollar quoting' if state == '$$' else repr(state) + ' quoting'} "
            f"at end of script; refusing to return a truncated statement list"
        )
    flush()
    return statements


class SqlDialect(ForgeDialect):
    """``CREATE TABLE`` + ``INSERT`` targets. Subclasses tune the hooks; the
    rendering skeleton is shared so every SQL dialect reads the same way."""

    #: Boolean literal spellings.
    true_literal: ClassVar[str] = "TRUE"
    false_literal: ClassVar[str] = "FALSE"
    #: One multi-row INSERT per table (warehouse-friendly) or one INSERT per
    #: row (the Postgres skeleton's shape).
    multi_row_insert: ClassVar[bool] = True

    # -- hooks ----------------------------------------------------------

    def ddl_header(self, plan: SchemaPlan) -> List[str]:
        """Extra comment lines after the standard DDL header."""
        return []

    def column_ddl(self, table: TablePlan, column_index: int) -> str:
        """One column definition line (without leading indent or comma)."""
        col = table.columns[column_index]
        physical_type = self.type_for_json[col.json_type]
        null = "" if col.nullable else " NOT NULL"
        return f"{self.physical_column(col.name)} {physical_type}{null}"

    def table_constraints(self, table: TablePlan) -> List[str]:
        """Constraint lines appended inside the ``CREATE TABLE`` body.
        Default: declared PK + FKs (F-4)."""
        lines = [f"PRIMARY KEY ({self.physical_column(table.primary_key.name)})"]
        for fk in table.foreign_keys:
            assert fk.references is not None
            # The referenced column belongs to the PARENT. Using this table's
            # primary key was correct only by coincidence of a uniform surrogate key.
            lines.append(
                f"FOREIGN KEY ({self.physical_column(fk.name)}) REFERENCES "
                f"{self.physical_table(fk.references)} "
                f"({self.physical_column(fk.references_column or SURROGATE_KEY)})"
            )
        return lines

    def table_suffix(self, table: TablePlan) -> str:
        """Text between the closing ``)`` and the ``;`` (e.g. an ENGINE clause)."""
        return ""

    def literal(self, value: Any) -> str:
        return sql_literal(value, true=self.true_literal, false=self.false_literal)

    # -- rendering ------------------------------------------------------

    def render_ddl(self, plan: SchemaPlan) -> str:
        parts: List[str] = [
            "-- Federation Forge — generated schema",
            f"-- dialect: {self.name}",
            "-- Regenerate with: r2g forge generate (see PLAN-federation-forge.md)",
            *self.ddl_header(plan),
            "",
        ]
        for table in plan.tables:
            lines = [self.column_ddl(table, i) for i in range(len(table.columns))]
            lines.extend(self.table_constraints(table))
            body = ",\n".join(f"    {line}" for line in lines)
            parts.append(
                f"CREATE TABLE {self.physical_table(table.table)} (\n{body}\n){self.table_suffix(table)};\n"
            )
        return "\n".join(parts)

    def render_loader(self, plan: SchemaPlan, rows: Rows, seed: int) -> str:
        parts: List[str] = [
            "-- Federation Forge — generated data",
            f"-- dialect: {self.name}; seed: {seed}",
            "",
        ]
        for table in plan.tables:
            physical = self.physical_table(table.table)
            columns = ", ".join(self.physical_column(c.name) for c in table.columns)
            tuples = [
                "(" + ", ".join(self.literal(row[c.name]) for c in table.columns) + ")"
                for row in rows[table.table]
            ]
            if self.multi_row_insert:
                values = ",\n".join(f"    {t}" for t in tuples)
                parts.append(f"INSERT INTO {physical} ({columns}) VALUES\n{values};")
            else:
                parts.extend(f"INSERT INTO {physical} ({columns}) VALUES {t};" for t in tuples)
            parts.append("")
        return "\n".join(parts)

    # -- bulk -----------------------------------------------------------

    def bulk_targets(self, plan: SchemaPlan) -> List[BulkTarget]:
        return [
            BulkTarget(
                name=t.table,
                physical=self.physical_table(t.table),
                kind="table",
                columns=tuple(self.physical_column(c.name) for c in t.columns),
            )
            for t in plan.tables
        ]

    def bulk_projector(self, plan: SchemaPlan, table: TablePlan) -> BulkProjector:
        name = table.table
        columns = [c.name for c in table.columns]

        def project(row: Dict[str, Any]) -> Iterable[Tuple[str, str]]:
            return ((name, ",".join(csv_value(row[c]) for c in columns)),)

        return project

    def render_bulk_loader(self, plan: SchemaPlan, manifest: Dict[str, Any]) -> str:
        parts = [
            "-- Federation Forge — generated bulk loader",
            f"-- dialect: {self.name}; seed: {manifest.get('seed')}",
            *self.bulk_header(),
            "",
        ]
        for target in manifest["targets"]:
            columns = ", ".join(target["columns"])
            parts.extend(
                self.bulk_load_statement(target["physical"], columns, f["path"])
                for f in target["files"]
            )
        parts.append("")
        return "\n".join(parts)

    def bulk_header(self) -> List[str]:
        """Comment lines saying how to run the bulk loader."""
        return []

    def bulk_load_statement(self, physical: str, columns: str, path: str) -> str:
        """One statement loading one chunk file into ``physical``."""
        raise ForgeError(f"dialect {self.name!r} renders no bulk loader")


__all__ = [
    "CSV_NULL",
    "BulkProjector",
    "BulkTarget",
    "ForgeDialect",
    "SqlDialect",
    "check_type_table",
    "csv_value",
    "split_sql_statements",
    "sql_literal",
]
