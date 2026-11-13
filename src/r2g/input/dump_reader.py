from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Generator, Iterator, Optional, Sequence

import polars as pl

from r2g.log import get_logger

logger = get_logger(__name__)


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
        try:
            return pl.read_csv(self._csv_path(), **self._polars_csv_kwargs())
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
            lf = pl.scan_csv(self._csv_path(), **self._polars_csv_kwargs())
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
