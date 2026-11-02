"""The Studio's changes under a control plane: asked before, recorded after (P14.9-P14.12).

1. **Every change route is classified** (P14.12). Five are governed, fourteen store nothing
   and are served as reads, eleven are refused saying why, and any other is refused by
   default, so a route added later is closed until someone governs it (:func:`classify`).
2. **A governed change is asked before it happens.** Each part it touches is checked on the
   class that governs it (:mod:`r2g.mapping_changes`), every question is asked before
   anything is written, and if any is refused the change is refused whole, saying which
   part and why (:func:`govern`). Each refusal is recorded too.
3. **It is recorded after it happens**, attributed by the user's context (P14.11). Entries go
   to a queue in the tenant's catalog folder before they are sent, are sent after the
   response, and are resent under the same id by a background worker until delivered, or
   until the context that attributes them expires (:class:`Outbox`). The control plane
   records an id at most once.
4. **Every saved version of a mapping is kept**, named by its content hash, so an entry
   points at the versions before and after (:func:`keep_version`).

Outside a control plane none of this runs, and the Studio is unchanged.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
import uuid
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from r2g import control_plane
from r2g.log import get_logger
from r2g.mapping_changes import Item

logger = get_logger(__name__)

#: How often the worker resends what is still queued (seconds).
DELIVERY_INTERVAL_SECONDS = 15.0

_PROJECT = r"/api/projects/[^/]+"

#: Change routes checked against the policy, as (method, path).
GOVERNED: tuple[tuple[str, str], ...] = (
    ("PUT", f"{_PROJECT}/mapping"),
    ("POST", f"{_PROJECT}/load"),
    ("POST", f"{_PROJECT}/migrate"),
    ("PATCH", _PROJECT),
    ("POST", r"/api/projects"),  # only the one the control-plane project names
)

#: POST routes that store nothing: compiling, previewing, inferring, analysing,
#: validating, comparing, and drafts the page loads without saving. ``suggest-ontology``
#: sends the source's schema to an LLM provider and costs money; it is a read by decision,
#: as the ontology tool's suggestions are.
READS: tuple[tuple[str, str], ...] = tuple(("POST", p) for p in (
    r"/api/expressions/compile",
    r"/api/expressions/preview",
    r"/api/sources/[^/]+/infer-fks",
    r"/api/shared-keys/infer",
    r"/api/sources/[^/]+/analyze-denorm",
    f"{_PROJECT}/arangoimport-preview",
    f"{_PROJECT}/validate",
    f"{_PROJECT}/validate-draft",
    f"{_PROJECT}/diff",
    f"{_PROJECT}/suggest-ontology",
    f"{_PROJECT}/apply-ontology",
    f"{_PROJECT}/auto-map",
    f"{_PROJECT}/apply-naming",
    r"/api/targets/[^/]+/introspect",
))

_SHARED = ("{} are shared by every project in the tenant, so a change here would reach the "
           "others; an administrator changes them with r2g's command line")

#: Change routes refused under a control plane, with why.
REFUSED: tuple[tuple[str, str, str], ...] = (
    ("POST", r"/api/sources", _SHARED.format("Sources")),
    ("DELETE", r"/api/sources/[^/]+", _SHARED.format("Sources")),
    ("POST", r"/api/sources/[^/]+/snapshot", _SHARED.format("Sources and their snapshots")),
    ("POST", r"/api/targets", _SHARED.format("Targets")),
    ("DELETE", r"/api/targets/[^/]+", _SHARED.format("Targets")),
    ("POST", r"/api/catalogs", _SHARED.format("Catalogs")),
    ("DELETE", r"/api/catalogs/[^/]+", _SHARED.format("Catalogs")),
    ("POST", r"/api/catalogs/[^/]+/import-source", _SHARED.format("Sources")),
    ("DELETE", _PROJECT,
     "the control-plane project names this transformation project, so it is not deleted "
     "from here"),
    ("POST", r"/api/demo-presets/[^/]+/install",
     "a demo preset adds a source, and " + _SHARED.format("sources")),
    ("POST", f"{_PROJECT}/governance/emit",
     "governance files are written to the server's disk outside the policy"),
    # A read, but of the source's rows, of any source in the tenant: the policy decides on
    # mapping items, not on reading data, so nothing would check who sees them.
    ("GET", r"/api/sources/[^/]+/preview/[^/]+",
     "a table preview returns the source's rows, which the policy does not govern, and "
     + _SHARED.format("sources")),
)

_READ_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


def _match(pattern: str, path: str) -> bool:
    return re.fullmatch(pattern, path) is not None


def classify(method: str, path: str) -> tuple[str, str | None]:
    """How a request is treated under a control plane: ``read``, ``governed`` (the route
    asks the policy itself), or ``refused`` with the reason. A read is served unless it is
    refused by name."""
    if method in _READ_METHODS:
        for m, p, why in REFUSED:
            if m == method and _match(p, path):
                return "refused", f"Not available under the control plane: {why}"
        return "read", None
    if any(m == method and _match(p, path) for m, p in GOVERNED):
        return "governed", None
    if any(m == method and _match(p, path) for m, p in READS):
        return "read", None
    for m, p, why in REFUSED:
        if m == method and _match(p, path):
            return "refused", f"Not available under the control plane: {why}"
    return "refused", ("Not available under the control plane: this change is not governed "
                       "by its policy")


# --- asking ---------------------------------------------------------------------------------


class Refused(Exception):  # noqa: N818 - a refusal, not an error
    """A change the policy refuses in whole: each part refused, with why."""

    def __init__(self, action: str, refused: list[tuple[Item, str | None, str]]) -> None:
        self.action = action
        self.refused = refused
        item, _, reason = refused[0]
        more = f" (and {len(refused) - 1} more)" if len(refused) > 1 else ""
        super().__init__(f"Not allowed: {action} on {item.where}: {reason}{more}")

    def detail(self) -> dict[str, Any]:
        return {"message": str(self),
                "refused": [{"action": self.action, "where": item.where, "term": term,
                             "reason": reason}
                            for item, term, reason in self.refused]}


@dataclass
class Governed:
    """A permitted change in progress: :meth:`record` each part once it has changed."""

    outbox: Outbox
    identity: control_plane.Identity
    project: str
    action: str
    decisions: dict[str | None, control_plane.Decision] = field(default_factory=dict)

    def record(self, item: Item, *, before: str | None = None, after: str | None = None,
               background: Any = None) -> None:
        """One entry per part, on the class that governs it now (the last checked)."""
        term = item.terms[-1]
        self.outbox.send(self.identity.token, _entry(
            self.action, f"{self.project}/{item.where}", term, self.decisions[term],
            before=before, after=after, what=item.what), background=background)


def _entry(action: str, entity: str, term: str | None, decision: control_plane.Decision, *,
           before: str | None = None, after: str | None = None,
           what: Iterable[str] = ()) -> control_plane.Entry:
    diff = ", ".join(what)
    return control_plane.Entry(
        event_id=uuid.uuid4().hex, action=action, entity=entity[:512], term=term,
        decision=decision, occurred_at=int(time.time() * 1000), before=before, after=after,
        diff=f"changed: {diff}"[:512] if diff else None)


def govern(outbox: Outbox, identity: control_plane.Identity, project: str, action: str,
           items: Iterable[Item]) -> Governed:
    """Ask about every part ``items`` touch, on every class that governs it, before anything
    changes. Records each refusal, then raises :class:`Refused` if there is any."""
    gateway = control_plane.get()
    items = list(items)
    decisions: dict[str | None, control_plane.Decision] = {}
    refused: list[tuple[Item, str | None, str]] = []
    for item in items:
        for term in item.terms:
            if term not in decisions:
                decisions[term] = gateway.decide(identity, action, term)
            if not decisions[term].permitted:
                refused.append((item, term, decisions[term].reason))
    for item, term, _ in refused:  # denied attempts are part of the history too
        outbox.send(identity.token, _entry(action, f"{project}/{item.where}", term,
                                           decisions[term], what=item.what))
    if refused:
        raise Refused(action, refused)
    return Governed(outbox, identity, project, action, decisions)


# --- keeping versions -----------------------------------------------------------------------


def keep_version(project_dir: Path, text: str) -> str:
    """Keep this version of a mapping (its file's text) beside the mapping, named by its
    content hash, and return the reference an entry points at."""
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    path = project_dir / "versions" / f"{digest}.yaml"
    if not path.exists():
        _write_atomically(path, text, mode=0o644)
    return f"{project_dir.name}/mapping@sha256:{digest}"


def _write_atomically(path: Path, text: str, *, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


# --- recording ------------------------------------------------------------------------------


class Outbox:
    """Entries for the project's history, queued in the tenant's catalog folder.

    ``pending/<id>.json`` holds an entry and the context token that attributes it, readable
    by the server's user only. Once delivered, or once the token can no longer attribute it,
    the entry moves to ``done/`` without the token.
    """

    _workers: dict[Path, threading.Event] = {}
    _workers_lock = threading.Lock()

    def __init__(self, root: Path) -> None:
        self.root = root
        self.pending = root / "pending"
        self.done = root / "done"
        self._lock = threading.Lock()

    def send(self, token: str, entry: control_plane.Entry, *, background: Any = None) -> None:
        """Queue ``entry`` first, so it is not lost, then deliver it: after the response
        when ``background`` (the request's background tasks) is given, at once otherwise."""
        path = self.pending / f"{entry.event_id}.json"
        _write_atomically(path, json.dumps({"token": token, "entry": asdict(entry),
                                            "attempts": 0, "queued_at": time.time()}),
                          mode=0o600)
        if background is None:
            self.deliver(path)
        else:
            background.add_task(self.deliver, path)

    def deliver(self, path: Path) -> str:
        """Try once. Returns the entry's status: pending, delivered or undeliverable."""
        with self._lock:  # the worker and a request's background task may meet here
            try:
                queued = json.loads(path.read_text(encoding="utf-8"))
            except FileNotFoundError:
                return "delivered"  # the other one got there first
            data = queued["entry"]
            entry = control_plane.Entry(**{**data, "decision": control_plane.Decision(
                **data["decision"])})
            try:
                control_plane.get().record(queued["token"], entry)
            except control_plane.ContextUnavailable as exc:
                queued["attempts"] += 1
                queued["last_error"] = str(exc)[:500]
                _write_atomically(path, json.dumps(queued), mode=0o600)
                logger.warning("history_entry_not_delivered_yet", event_id=entry.event_id,
                               error=str(exc))
                return "pending"
            except control_plane.ContextRejected as exc:
                self._finish(path, queued, "undeliverable", error=str(exc))
                logger.error("history_entry_undeliverable", event_id=entry.event_id,
                             error=str(exc))
                return "undeliverable"
            self._finish(path, queued, "delivered")
            return "delivered"

    def _finish(self, path: Path, queued: dict[str, Any], status: str, *,
                error: str | None = None) -> None:
        done = {"entry": queued["entry"], "status": status, "attempts": queued["attempts"] + 1,
                "finished_at": time.time(), **({"last_error": error[:500]} if error else {})}
        _write_atomically(self.done / path.name, json.dumps(done), mode=0o600)
        path.unlink(missing_ok=True)

    def deliver_pending(self, limit: int = 100) -> dict[str, int]:
        """Resend the oldest queued entries once each. Returns counts by new status."""
        if not self.pending.is_dir():
            return {}
        paths = sorted(self.pending.glob("*.json"), key=lambda p: p.stat().st_mtime)[:limit]
        counts: dict[str, int] = {}
        for path in paths:
            status = self.deliver(path)
            counts[status] = counts.get(status, 0) + 1
        return counts

    def start_worker(self, interval: float = DELIVERY_INTERVAL_SECONDS) -> threading.Event:
        """Resend queued entries in the background, one worker per queue folder."""
        with Outbox._workers_lock:
            running = Outbox._workers.get(self.root)
            if running is not None and not running.is_set():
                return running
            stop = Outbox._workers[self.root] = threading.Event()

        def loop() -> None:
            while not stop.wait(interval):
                try:
                    self.deliver_pending()
                except Exception:  # the loop must survive a bad round
                    logger.exception("history_delivery_round_failed")

        threading.Thread(target=loop, name="r2g-history-outbox", daemon=True).start()
        return stop
