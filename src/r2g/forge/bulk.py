"""Bulk output: a row stream written as chunked files plus a loader (PLAN F-11).

``generate`` renders the skeleton's artifacts as strings — ``INSERT``
statements and ``forge.rows.json`` — which is right for a dozen rows and
impossible for 10^8: the text alone would exceed memory. The bulk path writes
the same rows the same synthesis yields, but as they arrive:

- ``forge.sql`` / ``forge.collections.json`` — the dialect's schema, unchanged;
- ``data/<target>.<nnnnn>.<csv|jsonl>`` — at most ``chunk_rows`` records each;
  CSV (no header, ``\\N`` for NULL) for the SQL dialects, JSON lines of
  documents and edges for ``arango``;
- the dialect's bulk loader (``forge.bulk.sql`` or ``forge.bulk.py``), which
  reads those files in order with each engine's own bulk path (``\\copy``,
  ``FROM INFILE``, ``PUT``/``COPY INTO``, ``import_bulk`` in batches);
- ``forge.bulk.json`` — the manifest: every target, its columns, its row
  count, and each file with its row count and SHA-256, so two generations are
  compared by manifest rather than by re-reading gigabytes.

Memory is one open file and one buffered record per target; nothing written
is kept. Files are written in the order rows arrive, which for
:func:`~r2g.forge.synthesis.stream_rows` is plan order — parents before
children, and a self-link's rows after the rows they name — so a loader that
reads them in order never inserts a dangling key.
"""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, field
from typing import IO, Any, Dict, Iterable, List, Optional, Tuple

from .core import ForgeError, SchemaPlan
from .dialects import get_dialect
from .dialects.base import BulkProjector, BulkTarget, ForgeDialect

BULK_MANIFEST_FILE = "forge.bulk.json"
BULK_MANIFEST_VERSION = 1
DATA_DIR = "data"
#: Records per chunk file when the caller does not say: about 5–10 MB of CSV
#: for a narrow table, small enough for any loader, few enough files at 10^8.
DEFAULT_CHUNK_ROWS = 100_000


@dataclass
class _TargetState:
    target: BulkTarget
    rows: int = 0
    files: List[Dict[str, Any]] = field(default_factory=list)
    handle: Optional[IO[str]] = None
    in_chunk: int = 0
    digest: Any = None
    path: str = ""


class BulkWriter:
    """Write one plan's rows, as they arrive, into chunked files for ``dialect``.

    Use as a context manager, or call :meth:`close` (which writes the schema,
    the loader and the manifest, and returns the manifest). A writer that
    leaves its ``with`` block on an exception closes its files but writes no
    manifest, so a half-written directory never looks complete.
    """

    def __init__(
        self,
        plan: SchemaPlan,
        dialect: str,
        out_dir: str,
        *,
        seed: int,
        chunk_rows: int = DEFAULT_CHUNK_ROWS,
        provenance: Optional[Dict[str, Any]] = None,
    ) -> None:
        if isinstance(chunk_rows, bool) or not isinstance(chunk_rows, int) or chunk_rows < 1:
            raise ForgeError(f"chunk_rows must be an integer >= 1, got {chunk_rows!r}")
        self._dialect: ForgeDialect = get_dialect(dialect)
        self._plan = plan
        self._out_dir = out_dir
        self._seed = seed
        self._chunk_rows = chunk_rows
        self._provenance = dict(provenance or {})
        self._targets: Dict[str, _TargetState] = {
            t.name: _TargetState(t) for t in self._dialect.bulk_targets(plan)
        }
        self._projectors: Dict[str, BulkProjector] = {
            t.table: self._dialect.bulk_projector(plan, t) for t in plan.tables
        }
        self._closed = False
        os.makedirs(os.path.join(out_dir, DATA_DIR), exist_ok=True)

    def __enter__(self) -> "BulkWriter":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is None:
            self.close()
        else:
            self._close_files()
            self._closed = True

    def write(self, table: str, row: Dict[str, Any]) -> None:
        """Project one canonical row of ``table`` and append its records."""
        if self._closed:
            raise ForgeError("this bulk writer is closed")
        projector = self._projectors.get(table)
        if projector is None:
            raise ForgeError(
                f"row for table {table!r}, which this plan does not hold "
                f"(tables: {sorted(self._projectors)})"
            )
        for name, line in projector(row):
            self._append(self._targets[name], line)

    def write_all(self, rows: Iterable[Tuple[str, Dict[str, Any]]]) -> None:
        for table, row in rows:
            self.write(table, row)

    def _append(self, state: _TargetState, line: str) -> None:
        if state.handle is None or state.in_chunk >= self._chunk_rows:
            self._rotate(state)
        assert state.handle is not None
        data = line + "\n"
        state.handle.write(data)
        state.digest.update(data.encode("utf-8"))
        state.in_chunk += 1
        state.rows += 1

    def _rotate(self, state: _TargetState) -> None:
        self._finish_chunk(state)
        index = len(state.files)
        state.path = f"{DATA_DIR}/{state.target.name}.{index:05d}.{self._dialect.bulk_extension}"
        # newline="" so a string holding "\r\n" is written exactly as it is.
        state.handle = open(
            os.path.join(self._out_dir, state.path), "w", encoding="utf-8", newline=""
        )
        state.digest = hashlib.sha256()
        state.in_chunk = 0

    def _finish_chunk(self, state: _TargetState) -> None:
        if state.handle is None:
            return
        state.handle.close()
        state.files.append(
            {"path": state.path, "rows": state.in_chunk, "sha256": state.digest.hexdigest()}
        )
        state.handle = None

    def _close_files(self) -> None:
        for state in self._targets.values():
            if state.handle is not None:
                state.handle.close()
                state.handle = None

    def manifest(self) -> Dict[str, Any]:
        return {
            "forgeBulkVersion": BULK_MANIFEST_VERSION,
            "dialect": self._dialect.name,
            "seed": self._seed,
            "chunkRows": self._chunk_rows,
            "format": self._dialect.bulk_extension,
            "nullToken": "\\N" if self._dialect.bulk_extension == "csv" else None,
            **({"provenance": self._provenance} if self._provenance else {}),
            "targets": [
                {
                    "name": s.target.name,
                    "physical": s.target.physical,
                    "kind": s.target.kind,
                    "columns": list(s.target.columns),
                    "rows": s.rows,
                    "files": list(s.files),
                }
                for s in self._targets.values()
            ],
        }

    def close(self) -> Dict[str, Any]:
        """Finish every chunk and write the schema, loader and manifest."""
        if self._closed:
            raise ForgeError("this bulk writer is already closed")
        for state in self._targets.values():
            self._finish_chunk(state)
        self._closed = True
        manifest = self.manifest()
        for filename, content in (
            (self._dialect.ddl_filename, self._dialect.render_ddl(self._plan)),
            (
                self._dialect.bulk_loader_filename,
                self._dialect.render_bulk_loader(self._plan, manifest),
            ),
            (BULK_MANIFEST_FILE, json.dumps(manifest, indent=2, sort_keys=True) + "\n"),
        ):
            with open(os.path.join(self._out_dir, filename), "w", encoding="utf-8") as fh:
                fh.write(content)
        return manifest


def write_bulk(
    plan: SchemaPlan,
    dialect: str,
    rows: Iterable[Tuple[str, Dict[str, Any]]],
    out_dir: str,
    *,
    seed: int,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    provenance: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Write ``rows`` (any ``(table, row)`` stream over ``plan``) in bulk form
    and return the manifest."""
    with BulkWriter(
        plan, dialect, out_dir, seed=seed, chunk_rows=chunk_rows, provenance=provenance
    ) as writer:
        writer.write_all(rows)
    return writer.manifest()


__all__ = [
    "BULK_MANIFEST_FILE",
    "BULK_MANIFEST_VERSION",
    "DATA_DIR",
    "DEFAULT_CHUNK_ROWS",
    "BulkWriter",
    "write_bulk",
]
