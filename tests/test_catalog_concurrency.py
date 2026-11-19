"""The catalog under concurrent use.

The Studio runs each load on a background thread that records its result in
``catalog.json`` while request threads read and write the same file. Two things
went wrong there (seen in CI as ``Invalid JSON: EOF while parsing`` from
``test_governed_changes.py::test_only_declared_actions_are_asked``):

* a save emptied the file before writing it, so a reader in between got nothing;
* two saves each read the whole catalog, changed their part and wrote it all
  back, so the later one silently dropped the earlier one's change.
"""

from __future__ import annotations

import os
import stat
import threading

import pytest

import r2g.catalog as catalog_module
from r2g.catalog import CatalogManager

WRITERS = 8
WRITES_EACH = 15


def _run(workers: list[threading.Thread]) -> None:
    for t in workers:
        t.start()
    for t in workers:
        t.join(timeout=60)
    assert not any(t.is_alive() for t in workers), "a worker did not finish"


def test_a_reader_never_sees_a_half_written_catalog(tmp_path):
    mgr = CatalogManager(catalog_dir=tmp_path)
    mgr.add_source("seed", "postgresql", "postgresql://u:p@host/db")
    done = threading.Event()
    failures: list[BaseException] = []

    def write() -> None:
        try:
            for i in range(WRITERS * WRITES_EACH):
                mgr.update_source("seed", description=f"pass {i}")
        finally:
            done.set()

    def read() -> None:
        # A separate manager, as a second request or the MCP server would have.
        reader = CatalogManager(catalog_dir=tmp_path)
        while not done.is_set():
            try:
                assert reader.get_source("seed") is not None
            except BaseException as exc:  # noqa: BLE001 - the failure is the finding
                failures.append(exc)
                return

    _run([threading.Thread(target=write)] + [threading.Thread(target=read) for _ in range(3)])
    assert not failures, f"a reader saw a partial catalog: {failures[0]!r}"


def test_concurrent_changes_are_all_kept(tmp_path):
    managers = [CatalogManager(catalog_dir=tmp_path) for _ in range(WRITERS)]
    start = threading.Barrier(WRITERS)

    def add(n: int) -> None:
        start.wait()
        for i in range(WRITES_EACH):
            managers[n].add_source(f"s{n}_{i}", "postgresql", f"postgresql://u:p@host/db{n}")

    _run([threading.Thread(target=add, args=(n,)) for n in range(WRITERS)])
    names = {s.name for s in CatalogManager(catalog_dir=tmp_path).list_sources()}
    missing = {f"s{n}_{i}" for n in range(WRITERS) for i in range(WRITES_EACH)} - names
    assert not missing, f"{len(missing)} of {WRITERS * WRITES_EACH} sources were lost"


def test_a_failed_save_leaves_the_previous_catalog_and_no_temporary_file(tmp_path, monkeypatch):
    mgr = CatalogManager(catalog_dir=tmp_path)
    mgr.add_source("kept", "postgresql", "postgresql://u:p@host/db")
    before = (tmp_path / "catalog.json").read_bytes()

    def disk_full(fd: int) -> None:
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(catalog_module.os, "fsync", disk_full)
    with pytest.raises(OSError):
        mgr.add_source("lost", "postgresql", "postgresql://u:p@host/db2")

    assert (tmp_path / "catalog.json").read_bytes() == before
    assert sorted(p.name for p in tmp_path.iterdir() if p.name.startswith(".catalog")) == []
    assert [s.name for s in mgr.list_sources()] == ["kept"]


@pytest.mark.skipif(os.name != "posix", reason="POSIX permission bits")
def test_the_catalog_stays_owner_only_after_an_atomic_save(tmp_path):
    mgr = CatalogManager(catalog_dir=tmp_path)
    mgr.add_source("pg", "postgresql", "postgresql://u:p@host/db")
    mgr.update_source("pg", description="again")
    assert stat.S_IMODE((tmp_path / "catalog.json").stat().st_mode) == 0o600
