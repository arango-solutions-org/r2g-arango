"""``postgres`` — the S1 walking-skeleton dialect (PLAN F-3/F-4).

Declared PK/FK constraints, one canonical roundtrip-stable type per JSON type,
one ``INSERT`` per row.

**Not** byte-compatible with the skeleton, despite what this docstring used to
claim: moving into the dialect seam dropped the ``(walking skeleton)`` suffix
from both header lines, so ``forge.sql`` and ``forge.load.sql`` differ from
pre-package output on line 1. Rows are unchanged. Any checksum or ``diff``
against skeleton-era artifacts needs regenerating.
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

from .base import CSV_NULL, SqlDialect

#: The one canonical Postgres spelling per JSON type (PLAN F-3): the generated
#: DDL type must introspect back through ``config.pg_type_to_json_type`` to the
#: same JSON type.
PG_TYPE_FOR_JSON_TYPE: Dict[str, str] = {
    "integer": "bigint",
    "float": "double precision",
    "boolean": "boolean",
    "string": "text",
}


class PostgresDialect(SqlDialect):
    name: ClassVar[str] = "postgres"
    type_for_json: ClassVar[Dict[str, str]] = PG_TYPE_FOR_JSON_TYPE
    multi_row_insert: ClassVar[bool] = False

    def bulk_header(self) -> List[str]:
        return [
            "-- psql meta-commands (\\copy reads each file client-side). Run from this",
            "-- directory, after the DDL:",
            "--   psql -v ON_ERROR_STOP=1 -f forge.sql -f forge.bulk.sql",
            "-- Files load parents before children, so declared foreign keys hold throughout.",
        ]

    def bulk_load_statement(self, physical: str, columns: str, path: str) -> str:
        return f"\\copy {physical} ({columns}) FROM '{path}' WITH (FORMAT csv, NULL '{CSV_NULL}')"


__all__ = ["PG_TYPE_FOR_JSON_TYPE", "PostgresDialect"]
