from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Generator, Iterator, Mapping, Optional, Sequence

import polars as pl

from r2g.log import get_logger

logger = get_logger(__name__)

# Integer types wider than 64 bits. Polars infers one for a column of whole
# numbers when a value does not fit a 64-bit integer. Absent from old Polars,
# which fails on such a value instead.
_WIDE_INTEGER_TYPES = tuple(
    t for t in (getattr(pl, "Int128", None), getattr(pl, "UInt128", None)) if t is not None
)


def wide_integer_columns(schema: Mapping[str, Any]) -> list[str]:
    """Columns of whole numbers too large for a 64-bit integer.

    r2g reads these as text. ArangoDB stores a number above the 64-bit range
    as a double, so its low digits are lost (``10000000000000000001`` comes
    back as ``1e+19``), and such a column usually holds identifiers. Read as
    a number, a value past 128 bits also fails to parse at all.
    """
    return [name for name, dtype in schema.items() if dtype in _WIDE_INTEGER_TYPES]


def text_overrides(columns: Sequence[str]) -> dict[str, Any]:
    """``schema_overrides`` that read ``columns`` as text, as written in the file."""
    return {name: pl.String for name in columns}


class DumpReader:
    def __init__(
        self,
        file_path: str,
        delimiter: str = ",",
        has_header: bool = True,
        null_values: Optional[Sequence[str]] = None,
    ):
        self.file_path = Path(file_path)
        self.delimiter = delimiter
        self.has_header = has_header
        # Text that reads as null besides an unquoted empty field, which Polars
        # already reads as null. None or empty adds nothing.
        self.null_values = tuple(null_values or ())

    def _csv_path(self) -> str:
        return str(self.file_path)

    def _polars_csv_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "separator": self.delimiter,
            "has_header": self.has_header,
            # Type each column from the whole file, as the CSV source's
            # snapshot does. Polars' default looks at the first 100 rows only
            # and then fails on a later value that does not fit (text in a
            # column that began with whole numbers).
            "infer_schema_length": None,
        }
        if self.null_values:
            kwargs["null_values"] = list(self.null_values)
        return kwargs

    def read_dataframe(self) -> pl.DataFrame:
        if not self.file_path.exists():
            raise FileNotFoundError(f"Dump file not found: {self.file_path}")
        path, kwargs = self._csv_path(), self._polars_csv_kwargs()
        try:
            try:
                frame = pl.read_csv(path, **kwargs)
                wide = wide_integer_columns(frame.schema)
            except pl.exceptions.ComputeError:
                # A whole number past 128 bits does not parse as the integer
                # Polars inferred for its column. Any other failure is real.
                wide = wide_integer_columns(pl.scan_csv(path, **kwargs).collect_schema())
                if not wide:
                    raise
            if wide:
                # Rare, so the second read is paid only by a file that needs it.
                frame = pl.read_csv(path, schema_overrides=text_overrides(wide), **kwargs)
            return frame
        except Exception as e:
            logger.error("error_reading_dump", path=str(self.file_path), error=str(e))
            raise

    def row_count(self) -> int:
        if not self.file_path.exists():
            raise FileNotFoundError(f"Dump file not found: {self.file_path}")
        try:
            lf = pl.scan_csv(self._csv_path(), **self._polars_csv_kwargs())
            return int(lf.select(pl.len()).collect().item())
        except Exception as e:
            logger.error("error_counting_rows", path=str(self.file_path), error=str(e))
            raise

    def read_chunks(self, chunk_size: int) -> Iterator[pl.DataFrame]:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        if not self.file_path.exists():
            raise FileNotFoundError(f"Dump file not found: {self.file_path}")
        try:
            path, kwargs = self._csv_path(), self._polars_csv_kwargs()
            lf = pl.scan_csv(path, **kwargs)
            wide = wide_integer_columns(lf.collect_schema())
            if wide:
                lf = pl.scan_csv(path, schema_overrides=text_overrides(wide), **kwargs)
            yield from lf.collect_batches(chunk_size=chunk_size)
        except Exception as e:
            logger.error("error_reading_chunks", path=str(self.file_path), error=str(e))
            raise

    def read_rows(self) -> Generator[Dict[str, Any], None, None]:
        df = self.read_dataframe()
        if self.has_header:
            for row in df.iter_rows(named=True):
                yield dict(row)
        else:
            for values in df.iter_rows(named=False):
                yield {str(i): val for i, val in enumerate(values)}
