"""CSV directory source connector.

A CSV source is a directory of delimited files: each file is one table,
the filename stem is the table name, and the header row supplies the
column names. Column types are inferred from every row of the file
using Polars' CSV type inference, which is also how a load types the
values it reads, then mapped onto the same source-agnostic
type strings (``integer`` / ``double precision`` / ``boolean`` /
``timestamp`` / ``text``) that the transformer's type map already
understands.

CSV has no notion of primary or foreign keys. We apply a small,
documented heuristic so auto-mapping produces a usable ``_key``: the
first of ``id`` / ``{table}_id`` / ``{singular(table)}_id`` /
``{table}id`` / ``{singular(table)}id`` (case-insensitive) that exists
and is **unique and non-null across the whole file** is treated as the
primary key (e.g. ``customers.csv`` keyed on ``customer_id``). When the
file has no data rows we fall back to a name-only match so introspection
still proposes a key. Relationships can be recovered afterwards with FK
inference (``r2g source infer-fks``), whose value-overlap sampling and
name heuristics both rely on these detected keys.

Both schema introspection (:meth:`CsvConnector.get_schema`) and bulk
reads for loads (:class:`CsvSession`) are supported, so a CSV source
can be streamed into ArangoDB exactly like a relational source.
"""

from __future__ import annotations

import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional, Sequence

import polars as pl

from r2g.input.dump_reader import DumpReader
from r2g.log import get_logger
from r2g.naming import singularize
from r2g.types import Column, Schema, Table

logger = get_logger(__name__)

# Operators running the Studio UI / MCP server on a shared host can set this
# to confine CSV sources to a trusted directory tree. When unset (the default,
# matching the frictionless local-CLI posture) any path is accepted.
CSV_BASE_DIR_ENV = "R2G_CSV_BASE_DIR"


class CsvSourceError(RuntimeError):
    """Raised when a CSV source directory is invalid or outside the jail."""


def resolve_source_directory(connection_string: str) -> Path:
    """Resolve a CSV source path, enforcing ``R2G_CSV_BASE_DIR`` when set.

    The directory is ``expanduser``-ed for the returned value, but the jail
    check resolves symlinks and ``..`` on both the target and the base dir
    first so the confinement cannot be escaped. With no base dir configured,
    the path is returned unchanged.
    """
    directory = Path(connection_string).expanduser()
    base = os.environ.get(CSV_BASE_DIR_ENV)
    if not base:
        return directory
    base_resolved = Path(base).expanduser().resolve()
    target_resolved = directory.resolve()
    if target_resolved != base_resolved and base_resolved not in target_resolved.parents:
        raise CsvSourceError(
            f"CSV source directory '{target_resolved}' is outside the allowed "
            f"base directory '{base_resolved}' (set by {CSV_BASE_DIR_ENV})."
        )
    return directory

# Polars dtype "base name" -> source-agnostic type string understood by
# r2g.config.DEFAULT_TYPE_MAP / pg_type_to_json_type.
_POLARS_TYPE_MAP: dict[str, str] = {
    "Int8": "integer",
    "Int16": "integer",
    "Int32": "integer",
    "Int64": "integer",
    "UInt8": "integer",
    "UInt16": "integer",
    "UInt32": "integer",
    "UInt64": "integer",
    "Float32": "double precision",
    "Float64": "double precision",
    "Boolean": "boolean",
    "Date": "date",
    "Datetime": "timestamp",
    "Time": "text",
    "Utf8": "text",
    "String": "text",
}

CSV_EXTENSIONS = (".csv", ".tsv", ".txt")

#: Text a CSV source reads as NULL, besides an unquoted empty field (which Polars
#: always reads as null). ``\N`` is how PostgreSQL's text-format ``COPY`` and
#: Snowflake's default ``NULL_IF`` write NULL. It applies quoted or not, so a real
#: value ``\N`` reads as null too; a source that holds one sets ``null_values`` to
#: ``[]`` in its source params. ``""`` is deliberately not a default: it would also
#: null a quoted ``""``, which PostgreSQL's CSV format writes for an empty string.
DEFAULT_NULL_VALUES: tuple[str, ...] = ("\\N",)


def null_values_param(params: dict[str, Any] | None) -> tuple[str, ...]:
    """A CSV source's ``null_values`` param as a tuple of markers, or the default.

    One string is one marker. Anything else that is not a list of strings is
    refused, rather than guessed at: a wrong marker silently changes data.
    """
    value = (params or {}).get("null_values", DEFAULT_NULL_VALUES)
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple)) and all(isinstance(v, str) for v in value):
        return tuple(value)
    raise ValueError(
        "CSV source param 'null_values' must be a string or a list of strings, "
        f"not {value!r}"
    )


def resolve_csv_table_path(directory: Path, table: str) -> Path | None:
    """Return ``<directory>/<table><ext>`` for the first matching CSV extension,
    or ``None`` if no such file exists. Shared by the connector and the FK
    value-overlap sampler so the table→file convention stays in one place."""
    for ext in CSV_EXTENSIONS:
        candidate = directory / f"{table}{ext}"
        if candidate.is_file():
            return candidate
    return None


def _polars_dtype_to_type(dtype: Any) -> str:
    """Map a Polars dtype to a source-agnostic type string."""
    base = str(dtype).split("(")[0].strip()
    return _POLARS_TYPE_MAP.get(base, "text")


class CsvConnector:
    """Introspect and read a directory of CSV files as a schema.

    ``connection_string`` is the directory path. ``schema_name`` is
    carried for protocol parity but is otherwise unused (a CSV directory
    has no schema namespace).
    """

    def __init__(
        self,
        connection_string: str,
        *,
        schema_name: str = "public",
        delimiter: str = ",",
        has_header: bool = True,
        null_values: Sequence[str] = DEFAULT_NULL_VALUES,
    ) -> None:
        self.connection_string = connection_string
        self.schema_name = schema_name
        self.delimiter = delimiter
        self.has_header = has_header
        self.null_values = tuple(null_values)
        self.directory = resolve_source_directory(connection_string)

    def _table_files(self) -> dict[str, Path]:
        if not self.directory.is_dir():
            raise RuntimeError(f"CSV source directory not found: {self.directory}")
        files: dict[str, Path] = {}
        for path in sorted(self.directory.iterdir()):
            if path.is_file() and path.suffix.lower() in CSV_EXTENSIONS:
                files[path.stem] = path
        if not files:
            raise RuntimeError(
                f"No CSV/TSV files found in {self.directory} "
                f"(looked for {', '.join(CSV_EXTENSIONS)})"
            )
        return files

    def get_schema(self) -> Schema:
        schema = Schema()
        for table_name, path in self._table_files().items():
            schema.tables[table_name] = self._process_file(table_name, path)
        return schema

    def open_session(self) -> "CsvSession":
        return CsvSession(
            self.connection_string,
            schema_name=self.schema_name,
            delimiter=self.delimiter,
            has_header=self.has_header,
            null_values=self.null_values,
        )

    def _process_file(self, table_name: str, path: Path) -> Table:
        try:
            read_options: dict[str, Any] = {
                "separator": self.delimiter,
                "has_header": self.has_header,
                # The same markers the session reads as null, so a column's
                # inferred type matches the values it streams.
                "null_values": list(self.null_values) or None,
            }
            # Type each column from every row. A sample of the first rows
            # misses a rare value almost every time (one text value among
            # 60,000 rows is outside a 1,000-row sample 98% of the time), and
            # the load types from the whole file, so the snapshot must too or
            # the two disagree.
            lazy = pl.scan_csv(str(path), infer_schema_length=None, **read_options)
            file_schema = lazy.collect_schema()
            if self.has_header:
                # Read with the types just inferred rather than infer them a
                # second time, which more than doubles the snapshot. Without
                # a header the column count itself is inferred from every
                # row, so that file keeps the scan the load makes.
                lazy = pl.scan_csv(str(path), schema=file_schema, **read_options)
            pk = self._detect_primary_key(table_name, lazy, file_schema)
        except Exception as e:  # noqa: BLE001
            logger.error("csv_introspect_failed", path=str(path), error=str(e))
            raise RuntimeError(f"Failed to read CSV file for '{table_name}': {e}")

        pk_set = set(pk)

        columns: list[Column] = []
        for col_name, dtype in zip(file_schema.names(), file_schema.dtypes()):
            is_pk = col_name in pk_set
            columns.append(
                Column(
                    name=col_name,
                    data_type=_polars_dtype_to_type(dtype),
                    is_nullable=not is_pk,
                    is_primary_key=is_pk,
                )
            )

        return Table(name=table_name, columns=columns, primary_key=pk, foreign_keys=[])

    @staticmethod
    def _key_candidates(table_name: str, columns: Sequence[str]) -> list[str]:
        """Columns whose name marks them as the table's key, in priority order."""
        by_lower: dict[str, str] = {}
        for c in columns:
            by_lower.setdefault(c.lower(), c)

        t = table_name.lower()
        singular = singularize(t)
        candidates: list[str] = []
        for cand in ("id", f"{t}_id", f"{singular}_id", f"{t}id", f"{singular}id"):
            real = by_lower.get(cand)
            if real and real not in candidates:
                candidates.append(real)
        return candidates

    def _detect_primary_key(
        self,
        table_name: str,
        lazy: "pl.LazyFrame",
        file_schema: "pl.Schema",
    ) -> list[str]:
        """Pick a single-column primary key by name + uniqueness in the file.

        Candidate names are tried in priority order; the first that maps
        to a real column wins. With data rows present the winner must be
        unique and non-null across every row of the file, so we never
        invent a key from a non-unique column. The first rows alone cannot
        show that: a value repeated once is seen only when both copies
        fall inside the sample. With no data rows we accept the first
        name match (legacy behaviour) so a fresh / empty export still
        gets a proposed key.

        The pass reads every column, with or without a candidate, so a
        file the load cannot read (invalid UTF-8, an unclosed quote, a row
        with more fields than the header) raises here too.
        """
        candidates = self._key_candidates(table_name, file_schema.names())

        # One pass over the file. Counting each column's nulls makes the
        # reader parse every field; a pass over the candidates alone skips
        # the other columns and misses a row the load would reject. The
        # streaming engine keeps the file out of memory.
        measures = [pl.len().alias("rows"), pl.all().null_count().name.prefix("nulls:")]
        for i, col in enumerate(candidates):
            measures.append(pl.col(col).n_unique().alias(f"distinct:{i}"))
        stats = lazy.select(measures).collect(engine="streaming").row(0, named=True)

        if not candidates:
            return []
        rows = stats["rows"]
        if rows == 0:
            return [candidates[0]]

        for i, col in enumerate(candidates):
            if stats[f"nulls:{col}"] == 0 and stats[f"distinct:{i}"] == rows:
                return [col]
        return []


def rows_since(frame: pl.DataFrame, column: str, value: str) -> pl.DataFrame:
    """Keep the rows of ``frame`` whose ``column`` is at or after ``value``.

    This is the CSV side of ``stream --since``: what the relational sessions
    express as ``WHERE "<column>" >= %s``. The comparison follows the column's
    read type -- a timestamp column against the ISO 8601 value, a numeric
    column against the number, and any other column as text, where ISO 8601
    strings in one format order correctly. A null never qualifies, as in SQL.
    A column the file lacks is an error rather than a full reload: a
    misspelled ``--since-column`` must not silently rewrite every row.
    """
    if column not in frame.columns:
        raise RuntimeError(
            f"--since-column '{column}' is not a column of this CSV table "
            f"(columns: {', '.join(frame.columns)})"
        )
    dtype = frame.schema[column]
    col = pl.col(column)
    if dtype.is_temporal():
        bound = _parse_since_timestamp(value)
        tz = getattr(dtype, "time_zone", None)
        if tz and bound.tzinfo is None:
            bound = bound.replace(tzinfo=timezone.utc)
        elif not tz and bound.tzinfo is not None:
            bound = bound.astimezone(timezone.utc).replace(tzinfo=None)
        target = pl.Datetime("us", tz) if tz else pl.Datetime("us")
        return frame.filter(col.cast(target) >= pl.lit(bound).cast(target))
    if dtype.is_numeric():
        try:
            number = float(value)
        except ValueError as err:
            raise RuntimeError(
                f"--since value '{value}' is not a number, but column '{column}' is numeric"
            ) from err
        return frame.filter(col >= number)
    return frame.filter(col.cast(pl.String) >= value)


def _parse_since_timestamp(value: str) -> datetime:
    text = value.strip()
    if text.endswith(("Z", "z")):  # Python 3.10's fromisoformat has no "Z"
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError as err:
        raise RuntimeError(
            f"--since value '{value}' is not an ISO 8601 timestamp"
        ) from err


class CsvSession:
    """Bulk-read session over a CSV directory.

    There is no live connection to manage; each table read opens its
    file lazily via :class:`DumpReader`. The session object exists to
    satisfy the :class:`~r2g.connectors.session.SourceSession` protocol
    so the streaming pipeline can treat CSV like any other source.
    """

    def __init__(
        self,
        connection_string: str,
        *,
        schema_name: str = "public",
        delimiter: str = ",",
        has_header: bool = True,
        null_values: Sequence[str] = DEFAULT_NULL_VALUES,
    ) -> None:
        self.connection_string = connection_string
        self.schema_name = schema_name
        self.delimiter = delimiter
        self.has_header = has_header
        self.null_values = tuple(null_values)
        self.directory = resolve_source_directory(connection_string)
        # The pipeline counts a table and then streams it. With a bound both
        # need the whole file in a frame, so the frame filtered for the count
        # is kept for the stream that follows: one read per table, not two.
        self._since_frame: tuple[tuple[str, str, str], pl.DataFrame] | None = None

    def _frame_since(self, table: str, column: str, value: str) -> pl.DataFrame:
        key = (table, column, value)
        if self._since_frame is not None and self._since_frame[0] == key:
            return self._since_frame[1]
        frame = rows_since(self._reader(table).read_dataframe(), column, value)
        self._since_frame = (key, frame)
        return frame

    def _resolve(self, table: str) -> Path:
        path = resolve_csv_table_path(self.directory, table)
        if path is None:
            raise RuntimeError(f"No CSV file for table '{table}' in {self.directory}")
        return path

    def _reader(self, table: str) -> DumpReader:
        return DumpReader(
            str(self._resolve(table)),
            delimiter=self.delimiter,
            has_header=self.has_header,
            null_values=self.null_values,
        )

    def count_rows(
        self,
        table: str,
        *,
        since_column: Optional[str] = None,
        since_value: Optional[str] = None,
    ) -> int:
        if since_column and since_value is not None:
            return int(self._frame_since(table, since_column, since_value).height)
        return self._reader(table).row_count()

    def stream_rows(
        self,
        table: str,
        *,
        batch_size: int = 10_000,
        since_column: Optional[str] = None,
        since_value: Optional[str] = None,
    ) -> Iterator[dict[str, Any]]:
        if since_column and since_value is not None:
            frame = self._frame_since(table, since_column, since_value)
            self._since_frame = None  # the stream is the last use of this frame
            for row in frame.iter_rows(named=True):
                yield dict(row)
            return
        yield from self._reader(table).read_rows()

    def dump_table_to_csv(
        self,
        table: str,
        out_path: Path,
        *,
        header: bool = True,
    ) -> int:
        src = self._resolve(table)
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.null_values:
            shutil.copyfile(src, out_path)
            return self._reader(table).row_count()
        # A dump is read back with no null markers, so write each one as an
        # unquoted empty field: the NULL every other source's dump writes, and
        # what Polars reads as null. Every column is read as text, so the other
        # values (leading zeros, trailing decimal zeros, quoting) are kept. The
        # file is scanned and sunk, never held whole, as the byte copy above
        # never was, so a file larger than memory can still be dumped.
        lazy = pl.scan_csv(
            str(src),
            separator=self.delimiter,
            has_header=self.has_header,
            infer_schema_length=0,
            null_values=list(self.null_values),
        )
        lazy.sink_csv(str(out_path), separator=self.delimiter, include_header=self.has_header)
        return int(lazy.select(pl.len()).collect().item())

    def close(self) -> None:
        return None

    def __enter__(self) -> "CsvSession":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


__all__ = ["DEFAULT_NULL_VALUES", "CsvConnector", "CsvSession", "null_values_param"]
