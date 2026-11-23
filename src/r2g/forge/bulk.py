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

A run replaces an earlier one in ``out_dir``: its files are written to
``out_dir/.forge-staging/`` and moved in only once all of them are written. A
run that fails before then removes its staging directory and leaves
``out_dir`` as it was; so does a run that finds a directory where it writes a
file, a ``data`` that is not a directory, or an ``out_dir`` or ``data`` it
cannot write to. When the run moves in, Forge removes what it wrote before:
each chunk the earlier ``forge.bulk.json`` lists, if it still holds the SHA-256
recorded there, and every file at one of Forge's fixed names (``forge.sql``,
``forge.rows.json``, …), whoever wrote it. It keeps every other file, except
one at a path the new run writes, which is overwritten as before, and anything
at ``out_dir/.forge-staging``, which Forge uses as its staging directory and
removes. The earlier manifest is removed first and the new one is written
last, so a run stopped while it moves in leaves no manifest rather than one
that does not match the files.

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
import re
import shutil
from dataclasses import dataclass, field
from typing import IO, Any, Dict, Iterable, List, Optional, Set, Tuple

from .core import ForgeError, SchemaPlan
from .dialects import DIALECTS, get_dialect
from .dialects.base import BulkProjector, BulkTarget, ForgeDialect

BULK_MANIFEST_FILE = "forge.bulk.json"
BULK_MANIFEST_VERSION = 1
DATA_DIR = "data"
#: Records per chunk file when the caller does not say: about 5–10 MB of CSV
#: for a narrow table, small enough for any loader, few enough files at 10^8.
DEFAULT_CHUNK_ROWS = 100_000
_STAGING_DIR = ".forge-staging"
# A chunk's manifest path: one file directly under data/. data/ may be a symlink, so a
# listed chunk is removed only if it also holds the content the manifest records.
_CHUNK_PATH = re.compile(re.escape(DATA_DIR) + r"/[^/\\]+\.\d{5}\.[A-Za-z0-9]+\Z")


def _forge_files(out_dir: str) -> Tuple[Set[str], Dict[str, str]]:
    """What Forge wrote in ``out_dir``: its fixed file names, and each chunk the
    earlier run's manifest lists, with the SHA-256 recorded for it."""
    owned = {BULK_MANIFEST_FILE, "forge.rows.json"}  # forge.rows.json: GeneratedArtifacts.write_to
    for d in DIALECTS.values():
        owned |= {d.ddl_filename, d.loader_filename, d.bulk_loader_filename}
    try:
        with open(os.path.join(out_dir, BULK_MANIFEST_FILE), encoding="utf-8") as fh:
            listed = [(f["path"], f["sha256"]) for t in json.load(fh)["targets"] for f in t["files"]]
    except FileNotFoundError:
        return owned, {}
    except (OSError, ValueError, KeyError, TypeError):
        # Unreadable: its chunks cannot be told from a user's files, so none is removed.
        return owned, {}
    return owned, {
        p: h for p, h in listed if isinstance(p, str) and isinstance(h, str) and _CHUNK_PATH.match(p)
    }


def _sha256(path: str) -> Optional[str]:
    """The SHA-256 of the regular file at ``path``, or ``None`` if there is none to read."""
    if not os.path.isfile(path):
        return None
    digest = hashlib.sha256()
    try:
        with open(path, "rb") as fh:
            for block in iter(lambda: fh.read(1 << 20), b""):
                digest.update(block)
    except OSError:
        return None  # unreadable: not provably the earlier run's, so it is kept
    return digest.hexdigest()


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
    the loader and the manifest, moves the run into ``out_dir`` and returns the
    manifest). A writer that leaves its ``with`` block on an exception closes
    its files, removes its staging directory and leaves ``out_dir`` as it was.
    A writer abandoned without either leaves ``out_dir/.forge-staging/``, which
    the next writer on that ``out_dir`` removes.
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
        if os.path.lexists(out_dir) and not os.path.isdir(out_dir):
            raise ForgeError(f"bulk output directory {out_dir} exists and is not a directory")
        self._created = not os.path.lexists(out_dir)
        os.makedirs(out_dir, exist_ok=True)
        self._stage = os.path.join(out_dir, _STAGING_DIR)
        if os.path.lexists(self._stage):
            shutil.rmtree(self._stage)  # left by a writer that was killed or never closed
        os.makedirs(os.path.join(self._stage, DATA_DIR))

    def __enter__(self) -> "BulkWriter":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if exc_type is None:
            self.close()
        elif not self._closed:
            self._abandon()

    def _abandon(self) -> None:
        self._close_files()
        self._closed = True
        shutil.rmtree(self._stage, ignore_errors=True)
        if self._created:
            try:
                os.rmdir(self._out_dir)  # only if empty: a failed first run leaves nothing
            except OSError:
                pass

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
            os.path.join(self._stage, state.path), "w", encoding="utf-8", newline=""
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
        """Finish every chunk, write the schema, loader and manifest, and move
        the run into ``out_dir`` in place of what Forge wrote there before."""
        if self._closed:
            raise ForgeError("this bulk writer is already closed")
        try:
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
                with open(os.path.join(self._stage, filename), "w", encoding="utf-8") as fh:
                    fh.write(content)
            self._move_in(manifest)
        except BaseException:
            self._abandon()
            raise
        return manifest

    def _move_in(self, manifest: Dict[str, Any]) -> None:
        fixed, listed = _forge_files(self._out_dir)  # read before the earlier manifest goes
        written = [f["path"] for t in manifest["targets"] for f in t["files"]]
        written += [self._dialect.ddl_filename, self._dialect.bulk_loader_filename]
        written_set = set(written)
        # A listed chunk is the earlier run's only while it holds what that run wrote: a
        # repointed data/ symlink or a user's overwrite makes it someone else's file.
        earlier = fixed | {
            rel for rel, sha in listed.items()
            if rel not in written_set and _sha256(os.path.join(self._out_dir, rel)) == sha
        }
        # A file cannot replace a directory, so refuse one now, while out_dir is as it was.
        in_the_way = sorted(
            rel for rel in written_set | {BULK_MANIFEST_FILE}
            if os.path.isdir(os.path.join(self._out_dir, rel))
            and not os.path.islink(os.path.join(self._out_dir, rel))
        )
        if in_the_way:
            raise ForgeError(
                f"bulk output directory {self._out_dir} has a directory where this run "
                f"writes a file: {', '.join(in_the_way)}"
            )
        # Moving a file in or out of a directory needs write and search on it: refuse now.
        not_writable = [
            d for d in (self._out_dir, os.path.join(self._out_dir, DATA_DIR))
            if os.path.isdir(d) and not os.access(d, os.W_OK | os.X_OK)
        ]
        if not_writable:
            raise ForgeError(
                f"bulk output directory {self._out_dir} needs writable directories, and these "
                f"are not: {', '.join(not_writable)}"
            )
        os.makedirs(os.path.join(self._out_dir, DATA_DIR), exist_ok=True)  # data/ a file: fails here
        # From here until the last line out_dir has no manifest, never one that lies.
        if os.path.lexists(os.path.join(self._out_dir, BULK_MANIFEST_FILE)):
            os.remove(os.path.join(self._out_dir, BULK_MANIFEST_FILE))
        for rel in written:
            os.replace(os.path.join(self._stage, rel), os.path.join(self._out_dir, rel))
        for rel in sorted(earlier - written_set - {BULK_MANIFEST_FILE}):
            if os.path.isfile(os.path.join(self._out_dir, rel)):
                os.remove(os.path.join(self._out_dir, rel))
        os.replace(
            os.path.join(self._stage, BULK_MANIFEST_FILE),
            os.path.join(self._out_dir, BULK_MANIFEST_FILE),
        )
        # The run is published: a clean-up failure must not report it as failed, and the
        # next writer clears a stale staging directory anyway.
        shutil.rmtree(self._stage, ignore_errors=True)


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
