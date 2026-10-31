"""r2g as a tool of a context-and-governance control plane.

When ``CONTROL_PLANE_URL`` is set, the control plane decides who a request is for: it
signs a short-lived context token for one user in one project, and the browser sends it
as ``Authorization: Bearer`` (or as ``?token=`` on the load-progress stream, which an
``EventSource`` cannot give a header). In this mode:

* the context token is the only credential: r2g's own ``R2G_API_TOKEN`` is not used;
* r2g opens on the transformation project the control-plane project names for it, and
  shows no other;
* the project's ontology is read from AOE, with the same token, so the user sees the
  classes the project works on;
* storage is per tenant (``R2G_CATALOG_DIR``), and a context for another tenant is
  refused;
* every change is checked against the control plane's policy before it happens, and
  recorded in the project's history after (P14.9-P14.13, :mod:`r2g.governed`).

The control plane's SDK (``control_plane_sdk``) does the verification, offline, against
the control plane's published keys. It is imported only in this mode, so r2g installs and
runs without it otherwise. Everything else talks to the small :class:`ControlPlane`
interface below, which keeps the server testable without the SDK.
"""

from __future__ import annotations

import importlib
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

TOOL_ID = "r2g"

#: The actions r2g declares to the control plane (P14.13), as a policy's ``actions`` names
#: them. An item that names no class asks without a term, which the control plane's SDK
#: decides by the rules with no target (its FR-5.8; older SDKs reject such a policy).
ACTIONS: dict[str, dict[str, str | None]] = {
    "mapping.update": {"tool": TOOL_ID, "version": "1", "targets": "optional-ontology-term"},
    "data.load": {"tool": TOOL_ID, "version": "1", "targets": "optional-ontology-term"},
    "data.migrate": {"tool": TOOL_ID, "version": "1", "targets": "optional-ontology-term"},
    "transformation.update": {"tool": TOOL_ID, "version": "1", "targets": None},
}


def sdk_problem() -> str | None:
    """Why the installed control-plane SDK cannot decide r2g's :data:`ACTIONS`, or ``None``.

    They need its optional terms (FR-5.8, SDK 0.1.0). An older SDK rejects the policy that
    declares them and then denies every action, so r2g refuses to start on one (P14.13).
    """
    need = "the control plane's SDK 0.1.0 or later (arango-argos, sdk/python)"
    try:
        policy = importlib.import_module("control_plane_sdk.policy")
    except ImportError:
        return f"{need} is not installed"
    # TARGET_KINDS came with optional terms; an older SDK has neither.
    if "optional-ontology-term" not in getattr(policy, "TARGET_KINDS", ()):
        return f"the installed control-plane SDK has no optional terms: install {need}"
    return None


class ContextRejected(Exception):  # noqa: N818 - mirrors the SDK's TokenRejected
    """The token is not a valid context: refuse the request (401)."""


class ContextUnavailable(Exception):  # noqa: N818
    """The token could not be checked (e.g. the control plane's keys could not be
    fetched): a temporary failure (503), never a reason to let the request in."""


@dataclass(frozen=True)
class Identity:
    """What r2g needs from a verified context."""

    principal: str
    roles: tuple[str, ...]
    tenant: str
    project: str
    #: ``(ontology_id, version)`` of the project ontology the context names, if any.
    ontology: tuple[str, str] | None
    token: str = field(repr=False, default="")
    verified: Any = field(repr=False, default=None)


@dataclass(frozen=True)
class Project:
    """The control-plane project, as a tool may see it (its FR-2.5 and FR-2.6)."""

    id: str
    name: str
    #: The transformation project the control-plane project names for r2g, if any.
    opens: str | None


@dataclass(frozen=True)
class Decision:
    """The policy's answer to one question, as the history records it."""

    outcome: str  # "permitted" or "denied"
    reason: str
    policy_version: int

    @property
    def permitted(self) -> bool:
        return self.outcome == "permitted"


@dataclass(frozen=True)
class Entry:
    """One entry for the project's history: what changed, or what was refused."""

    event_id: str
    action: str
    entity: str
    term: str | None
    decision: Decision
    occurred_at: int
    before: str | None = None
    after: str | None = None
    diff: str | None = None


class ControlPlane(Protocol):
    def verify(self, token: str) -> Identity: ...

    def project(self, identity: Identity) -> Project: ...

    def decide(self, identity: Identity, action: str, term: str | None) -> Decision:
        """Never raises: anything that prevents a decision is a denial saying why."""
        ...

    def record(self, token: str, entry: Entry) -> None:
        """Send ``entry``, attributed by ``token``. Raises :class:`ContextRejected` when
        the token can no longer attribute it, :class:`ContextUnavailable` to retry later."""
        ...


def mode() -> bool:
    """Whether r2g runs under a control plane."""
    return bool(os.environ.get("CONTROL_PLANE_URL", "").strip())


#: Why the MCP server does not run under a control plane: it checks no context.
MCP_REFUSED = (
    "CONTROL_PLANE_URL is set: the MCP server checks no context token, so it would "
    "reach this deployment's catalog around the control plane. It does not run here."
)


class _Sdk:
    """The real implementation: the control plane's SDK, created once per process."""

    def __init__(self, url: str) -> None:
        from control_plane_sdk import ControlPlane as Client  # only in this mode

        self.client = Client(url, tool=TOOL_ID)
        self._projects: dict[str, tuple[float, Project]] = {}
        self._lock = threading.Lock()

    def verify(self, token: str) -> Identity:
        from control_plane_sdk import ContextError, TokenRejected, Unavailable

        try:
            verified = self.client.verify(token)
        except (TokenRejected, ContextError) as exc:
            raise ContextRejected(str(exc)) from exc
        except Unavailable as exc:
            raise ContextUnavailable(str(exc)) from exc
        ctx = verified.context
        return Identity(
            principal=ctx.principal.id,
            roles=tuple(ctx.principal.roles),
            tenant=ctx.tenant,
            project=ctx.project,
            ontology=None if ctx.ontology is None
            else (ctx.ontology.ontology_id, ctx.ontology.version),
            token=token,
            verified=verified,
        )

    def project(self, identity: Identity) -> Project:
        from control_plane_sdk import ControlPlaneError, ProjectNotFound, TokenRejected

        key = identity.verified.token_id
        now = time.monotonic()
        with self._lock:
            hit = self._projects.get(key)
            if hit is not None and now - hit[0] < 60:
                return hit[1]
        try:
            details = self.client.project(identity.verified)
        except (TokenRejected, ProjectNotFound) as exc:
            # The control plane refused this context, or its project is gone: a refusal
            # (401), not a temporary failure the page should wait out.
            raise ContextRejected(str(exc)) from exc
        except ControlPlaneError as exc:
            raise ContextUnavailable(f"cannot read the project: {exc}") from exc
        project = Project(details.id, details.name, details.opened_by(TOOL_ID))
        with self._lock:
            self._projects[key] = (now, project)
        return project

    def decide(self, identity: Identity, action: str, term: str | None) -> Decision:
        d = self.client.decide(identity.verified, action, term)
        return Decision(d.outcome, d.reason, d.policy_version)

    def record(self, token: str, entry: Entry) -> None:
        from control_plane_sdk import (
            Action,
            Change,
            ContextError,
            EventConflict,
            InvalidEvent,
            NotPermitted,
            ProjectNotFound,
            Target,
            TokenRejected,
            Unavailable,
        )
        from control_plane_sdk import Decision as SdkDecision

        try:  # a queued entry is resent later, so its token is checked again here
            verified = self.client.verify(token)
            self.client.record(
                verified,
                action=Action(entry.action),
                target=Target(entry.entity, type=entry.term),
                change=Change(before=entry.before, after=entry.after, diff=entry.diff),
                # Version 0: no policy could be read. The history caps a reason at 500.
                decision=SdkDecision("permitted" if entry.decision.permitted else "denied",
                                     str(entry.decision.policy_version or "") or None,
                                     entry.decision.reason[:500]),
                occurred_at=entry.occurred_at,
                event_id=entry.event_id,
            )
        except (TokenRejected, ContextError, NotPermitted, ProjectNotFound, EventConflict,
                InvalidEvent) as exc:
            raise ContextRejected(str(exc)) from exc  # resending would not help
        except Unavailable as exc:
            raise ContextUnavailable(str(exc)) from exc


_instance: ControlPlane | None = None
_instance_lock = threading.Lock()


def get() -> ControlPlane:
    global _instance
    with _instance_lock:
        if _instance is None:
            _instance = _Sdk(os.environ["CONTROL_PLANE_URL"])
        return _instance


def set_instance(instance: ControlPlane | None) -> None:
    """Replace the control plane (tests), or reset it to be created on next use."""
    global _instance
    with _instance_lock:
        _instance = instance


#: AOE pages its class list; a bound on how many pages r2g follows.
_MAX_PAGES = 100


def ontology_terms(identity: Identity, *, http: Any = None) -> dict[str, Any]:
    """The project ontology's classes and properties, read from AOE (``AOE_URL``) with the
    user's own context, so AOE applies the same project scope it applies to the user. Every
    page of classes is read: a term missing from a partial list would be refused when a
    mapping names it (P14.8). Never raises: a failure is reported with what went wrong."""
    if identity.ontology is None:
        return {"ontology_id": None, "classes": [], "properties": [],
                "problem": "the project names no ontology"}
    ontology_id, version = identity.ontology
    empty = {"ontology_id": ontology_id, "version": version, "classes": [], "properties": []}
    base = os.environ.get("AOE_URL", "").rstrip("/")
    if not base:
        return {**empty, "problem": "AOE_URL is not set, so the ontology cannot be read"}
    import httpx

    client = http or httpx.Client(timeout=10.0)
    headers = {"Authorization": f"Bearer {identity.token}"}
    url = f"{base}/api/v1/ontology/{ontology_id}"
    try:
        class_rows: list[dict[str, Any]] = []
        cursor: str | None = None
        for _ in range(_MAX_PAGES):
            params: dict[str, Any] = {"limit": 500}
            if cursor:
                params["cursor"] = cursor
            r = client.get(f"{url}/classes", params=params, headers=headers)
            if r.status_code != 200:
                return {**empty, "problem": f"AOE answered {r.status_code}"}
            body = r.json()
            class_rows.extend(body.get("data", []))
            cursor = body.get("next_cursor")
            if not cursor:
                break
        else:
            return {**empty, "problem": f"the ontology has more than {_MAX_PAGES} pages of classes"}
        r = client.get(f"{url}/properties", headers=headers)
        if r.status_code != 200:
            return {**empty, "problem": f"AOE answered {r.status_code} for the properties"}
        property_rows = r.json().get("data", [])
    except Exception as exc:  # the panel is informative; r2g works without it
        return {**empty, "problem": f"cannot reach AOE: {exc}"}
    finally:
        if http is None:
            client.close()
    return {**empty, "classes": _terms(class_rows), "properties": _terms(property_rows),
            "problem": None}


def _terms(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(({"uri": row.get("uri"), "label": row.get("label") or row.get("uri")}
                   for row in rows if row.get("uri")), key=lambda t: str(t["label"]).lower())


def named_terms(mapping: Any) -> set[tuple[str, str, str]]:
    """Every ontology term a mapping names (P14.8), as ``(where, kind, term)``: a table's
    class at ``collections/<key>``, a column's property at ``collections/<key>/<column>``,
    and a relationship's property at ``edges/<edge>(<from>.<fields> -> <to>.<fields>)``. A
    set, so two elements that share a name each keep their own term."""
    out: set[tuple[str, str, str]] = set()
    if mapping is None:
        return out
    for key, coll in mapping.collections.items():
        if coll.term:
            out.add((f"collections/{key}", "class", coll.term))
        for column, term in coll.field_terms.items():
            out.add((f"collections/{key}/{column}", "property", term))
    for edge in mapping.edges:
        if edge.term:
            where = (f"edges/{edge.edge_collection}({edge.from_collection}."
                     f"{','.join(edge.from_fields)} -> {edge.to_collection}."
                     f"{','.join(edge.to_fields)})")
            out.add((where, "property", edge.term))
    return out


def new_terms(saved: Any, new: Any) -> list[tuple[str, str, str]]:
    """The terms ``new`` names that ``saved`` did not name at the same place: only these
    are checked, so a class later removed from the ontology does not block unrelated
    edits."""
    return sorted(named_terms(new) - named_terms(saved))


def unknown_terms(saved: Any, new: Any, ontology: dict[str, Any]) -> list[str]:
    """The new terms (see :func:`new_terms`) that are not in the project ontology: a table
    needs a class, a column or relationship a property."""
    known = {"class": {t["uri"] for t in ontology.get("classes", [])},
             "property": {t["uri"] for t in ontology.get("properties", [])}}
    return [f"{where}: {term} is not a {kind} of {ontology.get('ontology_id')}"
            for where, kind, term in new_terms(saved, new) if term not in known[kind]]
