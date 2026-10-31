from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import re
import secrets
import threading
from pathlib import Path
from typing import Any, List, Literal, Optional, cast
from urllib.parse import unquote_plus

import yaml
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field, field_validator
from starlette._utils import get_route_path  # what the router matches on (Starlette 0.35+)
from starlette.concurrency import run_in_threadpool

from r2g import control_plane
from r2g import governed as governing
from r2g.catalog import CatalogManager, validate_project_name
from r2g.config import ConfigManager, validate_config
from r2g.connectors.arango_writer import ArangoWriter
from r2g.generators.arangoimport import (
    ArangoImportGenerator,
    CsvImportGenerator,
    ImportCommandSpec,
)
from r2g.log import get_logger
from r2g.security import redact_source_dump as _redact_source
from r2g.security import redact_target_dump as _redact_target
from r2g.streaming.pipeline import StreamingPipeline
from r2g.types import MappingConfig, NameCase


def _safe_detail(exc: Exception) -> str:
    """Stringify an exception for a client ``detail`` with DSN credentials scrubbed.

    Connection/driver errors frequently embed the full connection string
    (``scheme://user:pass@host``); strip the credentials before returning.
    """
    from r2g.security import scrub_dsn_credentials

    return scrub_dsn_credentials(str(exc))


def _resolve_env_ref(value: str) -> str:
    """Resolve a ``$ENV_VAR`` reference to its environment value (else unchanged).

    Same convention as source connection strings, so an API key can be passed as
    ``$OPENAI_API_KEY`` and stay out of request logs / persisted state.
    """
    if value and value.startswith("$"):
        return os.environ.get(value[1:], value)
    return value


# The environment variables a suggestion request may name as its API key. Any other
# name could hand one of the server's secrets to the provider (and back in its error).
_LLM_KEY_VARS = frozenset({"OPENAI_API_KEY", "ANTHROPIC_API_KEY"})


def _graph_state(endpoint: str, database: str, username: str, password: str,
                 graph_name: str) -> Any:
    """What a load's target holds now (:class:`r2g.mapping_changes.GraphState`): the named
    graph's declaration and the collections. A database that does not exist yet holds
    nothing. None when the target cannot be read, which the check treats as a change."""
    from arango import ArangoClient
    from arango.exceptions import ArangoServerError

    from r2g.mapping_changes import GraphState

    client = ArangoClient(hosts=endpoint)
    try:
        db = client.db(database, username=username, password=password)
        try:
            names = frozenset(c["name"] for c in cast(list[dict[str, Any]], db.collections()))
        except ArangoServerError as exc:
            if exc.error_code == 1228:  # database not found: the load creates it
                return GraphState(None, frozenset())
            raise
        declared = (cast(list[dict[str, Any]], db.graph(graph_name).edge_definitions())
                    if db.has_graph(graph_name) else None)
        return GraphState(declared, names)
    except Exception as exc:
        logger.warning("load_target_unreadable", error_type=type(exc).__name__)
        return None
    finally:
        client.close()


def _redact_dlq_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Mask source-row VALUES in a DLQ entry returned to API clients.

    Failed rows can contain PII; expose only which fields were present (and the
    error/collection metadata needed to debug), not their values.
    """
    out = dict(entry)
    row = out.get("row")
    if isinstance(row, dict):
        out["row_fields"] = sorted(row.keys())
        out["row"] = {k: ("<null>" if v is None else "<redacted>") for k, v in row.items()}
    return out

logger = get_logger(__name__)

_STATIC_DIR = Path(__file__).parent / "static"

_running_loads: dict[str, dict[str, Any]] = {}  # load_id -> {"thread", "queue", "pipeline"}


_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1", ""}


#: A route about one transformation project: ``/api/projects/<name>[/...]``.
_PROJECT_ROUTE = re.compile(r"^/api/projects/([^/]+)")
#: The load-progress stream, the one route that takes its context as ``?token=``.
_STREAM_ROUTE = re.compile(r"^/api/projects/[^/]+/load/[^/]+/stream$")
#: Under a control plane these need no context: the page and its assets, and how to
#: start. Everything else needs one, so a route added later is closed until opened here.
_OPEN_PATHS = frozenset({"", "/", "/api/health", "/api/mode"})
_OPEN_PREFIXES = ("/static/",)


def _redact_token_in_query(target: str) -> str:
    """``target`` with the value of every query parameter whose key reads ``token`` once
    decoded (``token``, ``%74oken``, after ``&`` or ``;``) replaced by ``<redacted>``."""
    head, sep, query = target.partition("?")
    if not sep:
        return target
    parts = re.split(r"([&;])", query)
    for i in range(0, len(parts), 2):
        key, eq, _ = parts[i].partition("=")
        if eq and unquote_plus(key).strip().lower() == "token":
            parts[i] = f"{key}=<redacted>"
    return head + sep + "".join(parts)


class _RedactTokenInQuery(logging.Filter):
    """Keep the stream's context token out of the access log: it is a credential."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.args, tuple):
            record.args = tuple(_redact_token_in_query(a) if isinstance(a, str) else a
                                for a in record.args)
        return True


_REDACT_TOKEN_IN_QUERY = _RedactTokenInQuery()


def _add_control_plane_auth(app: FastAPI, *, cors: bool) -> None:
    """Under a control plane: a context token on every ``/api`` call (``?token=`` on the
    load-progress stream, which cannot carry a header), from this deployment's tenant,
    and only the transformation project the control-plane project opens here."""
    tenant = os.environ.get("R2G_TENANT", "").strip()
    if not tenant:
        # Without it every tenant's context would get in, sharing one catalog.
        raise RuntimeError(
            "CONTROL_PLANE_URL is set but R2G_TENANT is not: name the tenant this r2g serves"
        )

    # uvicorn's access logger, once per process however many apps are created.
    logging.getLogger("uvicorn.access").addFilter(_REDACT_TOKEN_IN_QUERY)

    @app.middleware("http")
    async def _require_context(request: Request, call_next):
        # The path the router will match, not request.url.path: mounted with a root_path,
        # uvicorn puts the prefix in front of the path, and the router strips it again.
        path = get_route_path(request.scope)
        # A CORS preflight carries no credentials by design; with CORS configured, the CORS
        # middleware answers it without reaching a route. Any other OPTIONS is checked.
        preflight = (cors and request.method == "OPTIONS" and "origin" in request.headers
                     and "access-control-request-method" in request.headers)
        if path in _OPEN_PATHS or path.startswith(_OPEN_PREFIXES) or preflight:
            return await call_next(request)
        auth = request.headers.get("authorization", "")
        token = auth[len("Bearer "):].strip() if auth.startswith("Bearer ") else ""
        if not token and _STREAM_ROUTE.match(path):
            token = request.query_params.get("token", "").strip()
        if not token:
            return JSONResponse({"detail": "A context token is required"}, status_code=401)
        gateway = control_plane.get()
        try:
            identity = await run_in_threadpool(gateway.verify, token)
            if identity.tenant != tenant:
                return JSONResponse({"detail": f"This r2g serves tenant {tenant!r}"},
                                    status_code=403)
            project = await run_in_threadpool(gateway.project, identity)
        except control_plane.ContextRejected as exc:
            return JSONResponse({"detail": f"Context refused: {exc}"}, status_code=401)
        except control_plane.ContextUnavailable as exc:
            return JSONResponse({"detail": f"Cannot check the context: {exc}"}, status_code=503)
        m = _PROJECT_ROUTE.match(path)
        if m and m.group(1) != project.opens:  # scope['path'] is already decoded
            # Another project is not visible here, the way a foreign one is not found.
            return JSONResponse({"detail": "Project not found"}, status_code=404)
        kind, why = governing.classify(request.method, path)
        if kind == "refused":  # P14.12: not governed, so not available here
            return JSONResponse({"detail": why}, status_code=403)
        request.state.cp_identity = identity
        request.state.cp_project = project
        return await call_next(request)


def create_app(
    catalog_dir: str | None = None,
    *,
    host: str = "127.0.0.1",
    api_token: str | None = None,
) -> FastAPI:
    """Factory function for the FastAPI application.

    Local-first auth model (P5g/security): when bound to a loopback host with no
    token configured, the API is open (frictionless local use). When bound to a
    non-loopback host, or when ``R2G_API_TOKEN`` / ``api_token`` is set, a Bearer
    token is required on all ``/api`` routes (except ``/api/health`` and the SSE
    ``/stream`` endpoints, which are gated by their unguessable load id).
    """
    app = FastAPI(title="Relational-to-Graph Studio", version="0.1.0")

    # ── CORS: same-origin only by default (the bundled UI is same-origin). ──
    cors_origins = [o.strip() for o in os.environ.get("R2G_CORS_ORIGINS", "").split(",") if o.strip()]
    if cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    # ── Under a control plane, its context token is the only credential. ──
    governed = control_plane.mode()
    app.state.control_plane = governed
    if governed:
        _add_control_plane_auth(app, cors=bool(cors_origins))

    # ── Otherwise, optional Bearer-token auth. ──
    token = (api_token or os.environ.get("R2G_API_TOKEN") or "").strip()
    loopback = host in _LOOPBACK_HOSTS
    require_auth = not governed and (bool(token) or not loopback)
    if require_auth and not token:
        token = secrets.token_urlsafe(32)
    app.state.api_token = token
    app.state.api_auth_required = require_auth

    if require_auth:
        @app.middleware("http")
        async def _require_token(request: Request, call_next):
            path = request.url.path
            protected = (
                path.startswith("/api/")
                and path != "/api/health"
                and not path.endswith("/stream")
            )
            if protected and request.method != "OPTIONS":
                if request.headers.get("authorization", "") != f"Bearer {token}":
                    return JSONResponse({"detail": "Unauthorized"}, status_code=401)
            return await call_next(request)

    catalog = CatalogManager(catalog_dir)
    # Mapping files for API-created projects are confined here, never to a
    # client-supplied path (prevents path traversal / arbitrary file writes).
    _projects_root = catalog.dir.expanduser() / "projects"
    # A load's rejected rows, kept with the catalog (so per tenant under a control plane)
    # and per project, so one project's errors are never read through another's route.
    _dlq_root = catalog.dir.expanduser() / "dlq"
    # Under a control plane, the project's history is sent through a queue kept with the
    # catalog, so per tenant (P14.11), and resent by a worker until delivered.
    outbox = governing.Outbox(catalog.dir.expanduser() / "control_plane_outbox")
    if governed:
        outbox.start_worker()
    _save_locks: dict[str, threading.Lock] = {}

    async def _govern(request: Request, name: str, action: str,
                      items: list[Any]) -> governing.Governed:
        """Ask the policy about every part a change touches; 403, saying which and why, if
        any is refused (P14.9, P14.10)."""
        try:
            return await run_in_threadpool(governing.govern, outbox,
                                           request.state.cp_identity, name, action, items)
        except governing.Refused as exc:
            raise HTTPException(status_code=403, detail=exc.detail())

    def _dead_letters(name: str, load_id: str) -> Any:
        """The rejected rows of project ``name``'s load ``load_id``: the one place both the
        load (writing) and the errors route (reading) find them."""
        from r2g.dlq import DeadLetterQueue

        return DeadLetterQueue(load_id, dlq_dir=_dlq_root / name)

    def _running_load(name: str, load_id: str) -> dict[str, Any] | None:
        """The running load ``load_id`` if it belongs to project ``name``."""
        entry = _running_loads.get(load_id)
        return entry if entry is not None and entry.get("project") == name else None

    def _resolve_target(project: Any) -> tuple[str, str, str, str]:
        """Resolve (endpoint, database, username, password) for a project's target.

        Prefers an explicitly linked target, then a target matching the project's
        endpoint+database, falling back to the project fields + env credentials.
        """
        target = None
        target_name = getattr(project, "target_name", None)
        if target_name:
            target = catalog.get_target(target_name)
        if target is None:
            for _tgt in catalog.list_targets():
                if _tgt.endpoint == project.arango_endpoint and _tgt.database == project.arango_database:
                    target = _tgt
                    break
        if target is not None:
            return target.endpoint, target.database, target.username, target.password
        return (
            project.arango_endpoint,
            project.arango_database,
            os.environ.get("ARANGO_USER", "root"),
            os.environ.get("ARANGO_PASSWORD", ""),
        )

    if _STATIC_DIR.is_dir():
        app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    @app.get("/")
    async def index():
        index_path = _STATIC_DIR / "index.html"
        if index_path.exists():
            return FileResponse(str(index_path), media_type="text/html")
        return HTMLResponse("<h1>Relational-to-Graph Studio</h1><p>Frontend not built yet.</p>")

    @app.get("/api/health")
    async def health():
        return {"status": "ok"}

    def _only_the_one_it_opens(request: Request, name: str) -> None:
        """Under a control plane, a project other than the one the control-plane project
        opens here is not found, whether read or created (routes with no name segment)."""
        if governed and name != request.state.cp_project.opens:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")

    @app.get("/api/mode")
    async def run_mode():
        """Whether r2g runs under a control plane, and where its home is: the page needs
        to know before it has a context, to say where to open r2g from."""
        return {"control_plane": governed,
                "home": os.environ.get("CONTROL_PLANE_HOME", "/") if governed else None}

    @app.get("/api/control-plane/context")
    async def control_plane_context(request: Request):
        """The context this tab is in: who, the control-plane project, the transformation
        project it opens here, and the project ontology's classes and properties (read
        from AOE), which mapping elements may name (P14.8)."""
        if not governed:
            raise HTTPException(status_code=404, detail="Not running under a control plane")
        identity: control_plane.Identity = request.state.cp_identity
        project: control_plane.Project = request.state.cp_project
        ontology = await run_in_threadpool(control_plane.ontology_terms, identity)
        return {
            "principal": identity.principal,
            "roles": list(identity.roles),
            "project": {"id": project.id, "name": project.name},
            "opens": project.opens,
            "ontology": ontology,
        }

    @app.get("/api/expressions/functions")
    async def list_expression_functions():
        """Advertise the AQL subset the in-UI expression editor can use."""
        from r2g.expressions import SUPPORTED_FUNCTIONS

        return {
            "engine": "aql",
            "functions": list(SUPPORTED_FUNCTIONS),
            "operators": [
                "+", "-", "*", "/", "%",
                "==", "!=", "<", "<=", ">", ">=",
                "&&", "||", "NOT",
                "??", "? :",
            ],
            "bind_syntax": "@column_name",
        }

    @app.post("/api/demo-presets/snowflake-customer-360/install")
    async def install_snowflake_customer_360(
        body: DemoPresetInstallRequest,
        request: Request,
    ):
        """Idempotently install the bundled Customer 360 source and project.

        Snowflake schema creation remains an explicit operator bootstrap. This
        endpoint receives only the read-only runtime role and a fixed bundled
        overlay; it cannot select a local overlay path or setup role.
        """
        from r2g.demo.snowflake_customer_360 import (
            OVERLAY_SOURCE,
            PROJECT_NAME,
            SOURCE_NAME,
            configured_location,
            connection_string,
            load_overlay,
        )
        from r2g.snapshot import capture_source_snapshot, overlay_fingerprint

        _only_the_one_it_opens(request, PROJECT_NAME)
        required = (
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PRIVATE_KEY_FILE",
            "SNOWFLAKE_WAREHOUSE",
            "SNOWFLAKE_ROLE",
        )
        missing = [name for name in required if not os.environ.get(name)]
        if missing:
            raise HTTPException(
                status_code=400,
                detail={
                    "code": "snowflake_environment_incomplete",
                    "missing": missing,
                    "message": "Configure the listed Snowflake environment variables and restart Studio.",
                },
            )

        project = catalog.get_project(PROJECT_NAME)
        target = None
        if body.target_name:
            target = catalog.get_target(body.target_name)
            if target is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Target '{body.target_name}' not found",
                )
        elif project is not None and project.target_name:
            target = catalog.get_target(project.target_name)
        if target is None:
            targets = catalog.list_targets()
            if len(targets) == 1:
                target = targets[0]
        if target is None:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "arango_target_required",
                    "message": "Choose or create a non-system ArangoDB target first.",
                    "targets": [t.name for t in catalog.list_targets()],
                },
            )
        if target.database.strip().lower() == "_system":
            raise HTTPException(
                status_code=400,
                detail="The Customer 360 demo requires a dedicated ArangoDB database, not _system.",
            )

        database, schema_name = configured_location()
        overlay = load_overlay()
        source = catalog.get_source(SOURCE_NAME)
        if source is None:
            source = catalog.add_source(
                SOURCE_NAME,
                "snowflake",
                connection_string(),
                description=(
                    "Synthetic Customer 360 demo "
                    f"({database}.{schema_name}; read-only runtime role)"
                ),
                key_overlay=overlay,
                key_overlay_source=OVERLAY_SOURCE,
            )
            source_status = "created"
        else:
            if source.source_type != "snowflake":
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Source '{SOURCE_NAME}' already exists with type "
                        f"'{source.source_type}', expected 'snowflake'."
                    ),
                )
            source = catalog.update_source(
                SOURCE_NAME,
                connection_string=connection_string(),
                key_overlay=overlay,
                key_overlay_source=OVERLAY_SOURCE,
            )
            source_status = "reused"

        latest = catalog.get_latest_snapshot(SOURCE_NAME)
        overlay_tables = overlay.get("tables", {})
        expected_tables = set(overlay_tables)
        expected_overlay_summary = {
            "tables": len(overlay_tables),
            "primaryKeys": sum(
                bool(spec.get("primaryKey"))
                for spec in overlay_tables.values()
            ),
            "foreignKeys": sum(
                len(spec.get("foreignKeys", []))
                for spec in overlay_tables.values()
            ),
            "uniqueConstraints": sum(
                len(spec.get("uniqueConstraints", []))
                for spec in overlay_tables.values()
            ),
        }
        expected_overlay_fingerprint = overlay_fingerprint(overlay)
        snapshot_reusable = (
            latest is not None
            and latest.pg_schema.upper() == schema_name
            and set(latest.schema_data.tables) == expected_tables
            and latest.key_overlay_source == OVERLAY_SOURCE
            and latest.key_overlay_summary == expected_overlay_summary
            and latest.key_overlay_fingerprint
            == expected_overlay_fingerprint
            # An older snapshot that dropped overlay UNIQUE keys is not reusable
            # even if everything else matches: its natural-key FKs are gone.
            and not latest.lost_overlay_unique_keys
        )
        try:
            if snapshot_reusable:
                snap = latest
                assert snap is not None
                overlay_summary = snap.key_overlay_summary
                snapshot_status = "reused"
            else:
                snap, result = capture_source_snapshot(
                    catalog,
                    SOURCE_NAME,
                    schema_name=schema_name,
                )
                overlay_summary = result.key_overlay_summary
                snapshot_status = "created"
        except ImportError as exc:
            raise HTTPException(status_code=501, detail=_safe_detail(exc))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=_safe_detail(exc))
        except Exception as exc:
            raise HTTPException(status_code=502, detail=_safe_detail(exc))

        mapping_path = _projects_root / PROJECT_NAME / "mapping.yaml"
        project = catalog.get_project(PROJECT_NAME)
        if project is None:
            project = catalog.create_project(
                name=PROJECT_NAME,
                source_name=SOURCE_NAME,
                mapping_config_path=str(mapping_path),
                arango_endpoint=target.endpoint,
                arango_database=target.database,
                mapping_name="Snowflake Customer 360",
                mapping_description=(
                    "Synthetic account health graph with reviewed, non-enforced Snowflake keys."
                ),
            )
            project_status = "created"
        elif project.source_name != SOURCE_NAME:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Project '{PROJECT_NAME}' already uses source "
                    f"'{project.source_name}', expected '{SOURCE_NAME}'."
                ),
            )
        else:
            project_status = "reused"

        project = catalog.update_project(
            PROJECT_NAME,
            schema_snapshot_id=snap.id,
            target_name=target.name,
            arango_endpoint=target.endpoint,
            arango_database=target.database,
        )
        try:
            existing = (
                ConfigManager.load_config(project.mapping_config_path)
                if Path(project.mapping_config_path).is_file()
                else MappingConfig()
            )
            if not existing.collections:
                existing = ConfigManager.generate_default_config(snap.schema_data)
                ConfigManager.save_config(existing, project.mapping_config_path)
                mapping_status = "created"
            else:
                mapping_status = "reused"
        except Exception as exc:
            raise HTTPException(status_code=500, detail=_safe_detail(exc))

        return {
            "preset": "snowflake-customer-360",
            "source": {"name": source.name, "status": source_status},
            "snapshot": {
                "id": snap.id,
                "status": snapshot_status,
                "tables": len(snap.schema_data.tables),
            },
            "overlay": {
                "source": OVERLAY_SOURCE,
                "summary": overlay_summary,
                "note": "Curated key overlay; Snowflake does not enforce these relationships.",
            },
            "mapping": {
                "status": mapping_status,
                "collections": len(existing.collections),
                "edges": len(existing.edges),
            },
            "project": {
                "name": project.name,
                "status": project_status,
                "target": target.name,
            },
        }

    @app.post("/api/expressions/compile")
    async def compile_expression_endpoint(body: dict[str, Any]):
        """Parse-check an expression without evaluating it.

        Body: ``{"expression": "...", "engine": "aql"}``. Returns the list of
        referenced bind parameters when the expression is syntactically valid
        (so the UI can suggest adding them as sources).
        """
        from r2g.expressions import ExpressionError, compile_expression

        engine = (body.get("engine") or "aql").lower()
        expr = body.get("expression") or ""
        if engine != "aql":
            return {"valid": False, "error": f"engine '{engine}' is not supported"}
        try:
            compiled = compile_expression(expr)
        except ExpressionError as err:
            return {"valid": False, "error": str(err)}
        return {"valid": True, "references": list(compiled.references)}

    @app.post("/api/expressions/preview")
    async def preview_expression_endpoint(body: dict[str, Any]):
        """Compile and evaluate an expression against a sample row.

        Body: ``{"expression": "...", "engine": "aql", "row": {...}}``.
        Returns ``{"valid": true, "result": <value>, "references": [...]}``
        on success, or ``{"valid": false, "error": "..."}`` when the
        expression cannot be compiled. Evaluation errors (e.g. a referenced
        column missing from the sample row) are reported via ``runtime_error``
        rather than failing the request, so the editor can show a live result.
        """
        from r2g.expressions import ExpressionError, compile_expression

        engine = (body.get("engine") or "aql").lower()
        expr = body.get("expression") or ""
        row = body.get("row")
        if not isinstance(row, dict):
            row = {}
        if engine != "aql":
            return {"valid": False, "error": f"engine '{engine}' is not supported"}
        try:
            compiled = compile_expression(expr)
        except ExpressionError as err:
            return {"valid": False, "error": str(err)}
        try:
            result = compiled.evaluate(row)
        except ExpressionError as err:
            return {
                "valid": True,
                "references": list(compiled.references),
                "runtime_error": str(err),
            }
        return {
            "valid": True,
            "references": list(compiled.references),
            "result": result,
        }

    # ── Source endpoints ──────────────────────────────────────────────

    @app.get("/api/sources")
    async def list_sources():
        sources = catalog.list_sources()
        return [_redact_source(s.model_dump()) for s in sources]

    @app.post("/api/sources", status_code=201)
    async def add_source(body: SourceCreateRequest):
        try:
            source = catalog.add_source(
                name=body.name,
                source_type=body.source_type,
                connection_string=body.connection_string,
                description=body.description,
                owner=body.owner,
                source_params=body.source_params,
            )
            return _redact_source(source.model_dump())
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))

    @app.delete("/api/sources/{name}")
    async def remove_source(name: str, cascade: bool = False):
        try:
            result = catalog.remove_source(name, cascade=cascade)
            if not result:
                raise HTTPException(status_code=404, detail=f"Source '{name}' not found")
        except HTTPException:
            raise
        except Exception as e:
            if "DependencyError" in type(e).__name__ or "Cannot remove" in str(e):
                raise HTTPException(status_code=409, detail=str(e))
            raise
        return {"removed": name}

    @app.post("/api/sources/{name}/snapshot")
    async def create_snapshot(name: str, pg_schema: str = "public"):
        source = catalog.get_source(name)
        if source is None:
            raise HTTPException(status_code=404, detail=f"Source '{name}' not found")
        try:
            from r2g.snapshot import capture_source_snapshot

            snap, result = capture_source_snapshot(
                catalog,
                name,
                schema_name=pg_schema,
            )
            return {
                "id": snap.id,
                "tables": len(result.schema.tables),
                "captured_at": snap.captured_at.isoformat(),
                "key_overlay_summary": result.key_overlay_summary,
                "key_overlay_source": result.key_overlay_source,
                "classifications_applied": result.classifications_applied,
            }
        except ImportError as e:
            raise HTTPException(status_code=501, detail=_safe_detail(e))
        except ValueError as e:
            raise HTTPException(status_code=400, detail=_safe_detail(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=_safe_detail(e))

    @app.post("/api/sources/{name}/infer-fks")
    async def infer_fks(name: str, body: InferFksRequest | None = None):
        """Return ranked FK candidates for the source's latest snapshot.

        When ``sample: true`` is passed in the body, the engine scores
        value-overlap via :class:`PostgresValueSampler` (bounded
        ``LEFT JOIN`` queries) for PostgreSQL sources or
        :class:`CsvValueSampler` (Polars file reads) for CSV sources;
        Snowflake sampling is not yet supported and falls back to the
        name heuristic. Without sampling, the suggestions are
        name-heuristic only and the endpoint is schema-metadata-only
        (safe / free).
        """
        from r2g.connectors.base import normalize_source_type
        from r2g.fk_inference import (
            InferenceOptions,
            create_value_sampler,
            infer_foreign_keys,
        )

        source = catalog.get_source(name)
        if source is None:
            raise HTTPException(status_code=404, detail=f"Source '{name}' not found")
        snap = catalog.get_latest_snapshot(name)
        if snap is None:
            raise HTTPException(
                status_code=400,
                detail="No schema snapshot for this source — take one first.",
            )

        req = body or InferFksRequest()
        opts = InferenceOptions(
            min_confidence=req.min_confidence,
            sample_overlap=req.sample,
            overlap_veto_on_zero=req.veto_on_zero_overlap,
        )

        sampler = None
        sampler_used = False
        if req.sample:
            try:
                sampler = create_value_sampler(
                    source.source_type,
                    source.connection_string,
                    pg_schema=snap.pg_schema,
                    source_params=source.source_params,
                    limit=req.sample_limit,
                )
                sampler_used = sampler is not None
                if sampler is None:
                    logger.info(
                        "fk_infer_sampler_unsupported",
                        source_type=normalize_source_type(source.source_type),
                        note="value-overlap sampling supports PostgreSQL and CSV; name heuristic still runs",
                    )
            except Exception as err:  # noqa: BLE001
                logger.warning("fk_infer_sampler_init_failed", error=str(err))

        try:
            candidates = infer_foreign_keys(
                snap.schema_data,
                options=opts,
                sampler=sampler,
            )
        finally:
            if sampler is not None:
                sampler.close()

        return {
            "source": name,
            "snapshot_id": snap.id,
            "sample_used": sampler_used,
            "candidates": [c.model_dump() for c in candidates],
        }

    @app.post("/api/shared-keys/infer")
    async def infer_shared_keys_endpoint(body: InferSharedKeysRequest | None = None):
        """Propose cross-source shared-key relationships (PRD P6.7).

        Unlike ``/api/sources/{name}/infer-fks`` this is *not* source-scoped:
        the whole point is to see keys that co-occur across two or more
        sources, which single-schema inference structurally cannot find. Reads
        the latest snapshot of each requested source (all snapshotted sources
        by default); schema-metadata-only and free unless ``sample`` is set.

        Candidates are proposals — nothing is written to any mapping here.
        """
        from r2g.fk_inference import create_value_sampler
        from r2g.xsk import SharedKeyOptions, infer_shared_keys

        req = body or InferSharedKeysRequest()
        wanted = req.sources if req.sources else [s.name for s in catalog.list_sources()]

        schemas: dict[str, Any] = {}
        skipped: list[dict[str, str]] = []
        for source_name in wanted:
            source = catalog.get_source(source_name)
            if source is None:
                skipped.append({"source": source_name, "reason": "not found in catalog"})
                continue
            snap = catalog.get_latest_snapshot(source_name)
            if snap is None:
                skipped.append({"source": source_name, "reason": "no schema snapshot"})
                continue
            schemas[source_name] = snap.schema_data

        if len(schemas) < 2:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Cross-source inference needs at least two snapshotted sources; "
                    f"got {len(schemas)}. Take a snapshot of the other source first."
                ),
            )

        samplers: dict[str, Any] = {}
        if req.sample:
            for source_name in schemas:
                source = catalog.get_source(source_name)
                snap = catalog.get_latest_snapshot(source_name)
                if source is None or snap is None:
                    # Unreachable in practice — `schemas` only holds sources that
                    # passed the None-guards above — but keep the types honest.
                    skipped.append(
                        {"source": source_name, "reason": "source or snapshot missing"}
                    )
                    continue
                try:
                    sampler = create_value_sampler(
                        source.source_type,
                        source.connection_string,
                        pg_schema=snap.pg_schema,
                        source_params=source.source_params,
                        limit=req.sample_limit,
                    )
                except Exception as err:  # noqa: BLE001
                    logger.warning(
                        "xsk_sampler_init_failed", source=source_name, error=str(err)
                    )
                    sampler = None
                if sampler is None:
                    skipped.append(
                        {
                            "source": source_name,
                            "reason": "value sampling unsupported for this source type",
                        }
                    )
                samplers[source_name] = sampler

        opts = SharedKeyOptions(
            min_confidence=req.min_confidence,
            sample_overlap=req.sample,
            sample_limit=req.sample_limit,
        )
        try:
            result = infer_shared_keys(schemas, options=opts, samplers=samplers)
        finally:
            for sampler in samplers.values():
                if sampler is not None:
                    try:
                        sampler.close()
                    except Exception:  # noqa: BLE001
                        pass

        payload = result.model_dump()
        payload["skipped"] = skipped
        return payload

    @app.post("/api/sources/{name}/analyze-denorm")
    async def analyze_denorm(name: str, body: AnalyzeDenormRequest | None = None):
        """Return ranked denormalization findings for the source's latest snapshot.

        Structural detectors (repeating groups) always run from schema metadata
        alone (safe / free). When ``sample: true`` is passed, bounded value
        probes additionally detect embedded lookups (functional dependencies) on
        PostgreSQL / MySQL / SQL Server / CSV sources; other types fall back to
        the structural signals. Read-only — it never changes the schema or data.
        """
        from r2g.connectors.base import normalize_source_type
        from r2g.denorm import AnalyzeOptions, analyze_denormalization, with_hints
        from r2g.fk_inference import create_value_sampler

        source = catalog.get_source(name)
        if source is None:
            raise HTTPException(status_code=404, detail=f"Source '{name}' not found")
        snap = catalog.get_latest_snapshot(name)
        if snap is None:
            raise HTTPException(
                status_code=400,
                detail="No schema snapshot for this source — take one first.",
            )

        req = body or AnalyzeDenormRequest()

        sampler = None
        sampler_used = False
        if req.sample:
            try:
                sampler = create_value_sampler(
                    source.source_type,
                    source.connection_string,
                    pg_schema=snap.pg_schema,
                    source_params=source.source_params,
                    limit=req.sample_limit,
                )
                sampler_used = sampler is not None
                if sampler is None:
                    logger.info(
                        "denorm_sampler_unsupported",
                        source_type=normalize_source_type(source.source_type),
                        note="value sampling supports PG/MySQL/SQL Server/CSV; structural detectors still run",
                    )
            except Exception as err:  # noqa: BLE001
                logger.warning("denorm_sampler_init_failed", error=str(err))

        opts = AnalyzeOptions(
            sample=sampler_used,
            sample_limit=req.sample_limit,
            min_confidence=req.min_confidence,
            no_sample_columns=frozenset(req.no_sample_columns),
        )
        try:
            findings = analyze_denormalization(snap.schema_data, options=opts, sampler=sampler)
        finally:
            if sampler is not None and hasattr(sampler, "close"):
                sampler.close()

        return {
            "source": name,
            "snapshot_id": snap.id,
            "sample_used": sampler_used,
            "findings": with_hints(findings),
        }

    # ── Schema endpoints ──────────────────────────────────────────────

    @app.get("/api/sources/{name}/schema")
    async def get_schema(name: str):
        snap = catalog.get_latest_snapshot(name)
        if snap is None:
            raise HTTPException(status_code=404, detail=f"No snapshots for source '{name}'")
        return snap.schema_data.model_dump()

    @app.get("/api/sources/{name}/preview/{table}")
    async def preview_table(name: str, table: str, limit: int = 20):
        source = catalog.get_source(name)
        if source is None:
            raise HTTPException(status_code=404, detail=f"Source '{name}' not found")
        stype = (source.source_type or "postgresql").lower()
        if stype not in ("postgresql", "postgres", "pg"):
            raise HTTPException(
                status_code=501,
                detail=f"Data preview is only supported for PostgreSQL sources (got '{stype}')",
            )
        # Validate the table name against the latest snapshot so we never
        # interpolate an unvetted identifier into SQL.
        snap = catalog.get_latest_snapshot(name)
        if snap is None or table not in snap.schema_data.tables:
            raise HTTPException(
                status_code=404,
                detail=f"Table '{table}' is not in the latest snapshot of source '{name}'",
            )
        limit = max(1, min(int(limit), 1000))
        try:
            from r2g.connectors.postgres import preview_table_rows

            schema_name = snap.pg_schema or "public"
            rows = preview_table_rows(source.connection_string, schema_name, table, limit)
            return {"table": table, "rows": rows, "count": len(rows)}
        except HTTPException:
            raise
        except Exception as e:
            logger.warning("preview_table_failed", source=name, table=table, error=str(e))
            raise HTTPException(status_code=500, detail="Failed to preview table data")

    # ── Project endpoints ─────────────────────────────────────────────

    @app.get("/api/projects")
    async def list_projects(request: Request):
        projects = catalog.list_projects()
        if governed:  # only the one the control-plane project opens here
            opens = request.state.cp_project.opens
            projects = [p for p in projects if p.name == opens]
        return [p.model_dump() for p in projects]

    @app.get("/api/projects/{name}")
    async def get_project(name: str):
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        return project.model_dump()

    @app.post("/api/projects", status_code=201)
    async def create_project(body: ProjectCreateRequest, request: Request,
                             background: BackgroundTasks):
        _only_the_one_it_opens(request, body.name)
        try:
            validate_project_name(body.name)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        # The mapping file is always confined to <catalog>/projects/<name>/.
        # If the client passed a path to an existing, parseable mapping, import
        # its contents into the safe location (read-only); otherwise start empty.
        safe_path = _projects_root / body.name / "mapping.yaml"
        seed = MappingConfig()
        if body.mapping_config_path:
            try:
                p = Path(body.mapping_config_path).expanduser()
                # Under a control plane only the tenant's own files are read: the server
                # holds every tenant's catalog.
                # Nor another transformation project's files, which P14.3 does not show.
                r = p.resolve()
                inside = (r.is_relative_to(catalog.dir.expanduser().resolve())
                          and (not r.is_relative_to(_projects_root.resolve())
                               or r.is_relative_to((_projects_root / body.name).resolve())))
                if p.is_file() and (inside or not governed):
                    seed = ConfigManager.load_config(p)
            except Exception:
                seed = MappingConfig()
        if governed and safe_path.is_file():
            # A folder left by a project deleted outside the Studio: its mapping, not the
            # seed, is what the project starts with, so that is what is checked.
            try:
                seed = ConfigManager.load_config(safe_path)
            except Exception:
                seed = MappingConfig()
        details = governing.Item("project", (None,), ("added",))
        g = seeded = None
        seed_items: list[Any] = []
        if governed:  # the project, and the mapping it starts with as if saved (P14.9)
            from r2g.mapping_changes import save_changes

            g = await _govern(request, body.name, "transformation.update", [details])
            seed_items = save_changes(None, seed)
            if seed_items:
                seeded = await _govern(request, body.name, "mapping.update", seed_items)
        try:
            project = catalog.create_project(
                name=body.name,
                source_name=body.source_name,
                mapping_config_path=str(safe_path),
                arango_endpoint=body.arango_endpoint,
                arango_database=body.arango_database,
                mapping_name=body.mapping_name,
                mapping_description=body.mapping_description,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        if not safe_path.exists():
            try:
                ConfigManager.save_config(seed, str(safe_path))
            except Exception as e:
                raise HTTPException(status_code=500, detail=_safe_detail(e))
        if g is not None:
            g.record(details, background=background)
        if seeded is not None:
            after = governing.keep_version(_projects_root / body.name,
                                           safe_path.read_text(encoding="utf-8"))
            for item in seed_items:
                seeded.record(item, after=after, background=background)
        return project.model_dump()

    @app.patch("/api/projects/{name}")
    async def update_project_metadata(name: str, body: ProjectUpdateRequest, request: Request,
                                      background: BackgroundTasks):
        """Update editable project metadata (mapping name / description)."""
        if catalog.get_project(name) is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        fields = {k: v for k, v in body.model_dump(exclude_none=True).items()}
        if not fields:
            raise HTTPException(status_code=400, detail="No updatable fields provided")
        details = governing.Item("project", (None,), tuple(sorted(fields)))
        g = await _govern(request, name, "transformation.update", [details]) \
            if governed else None
        try:
            project = catalog.update_project(name, **fields)
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e))
        if g is not None:
            g.record(details, background=background)
        return project.model_dump()

    @app.delete("/api/projects/{name}")
    async def delete_project(name: str):
        """Delete a project (and its load history). Mapping file is left on disk."""
        if not catalog.delete_project(name):
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        return {"deleted": name}

    @app.get("/api/projects/{name}/mapping")
    async def get_mapping(name: str):
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        try:
            config = ConfigManager.load_config(project.mapping_config_path)
            return config.model_dump()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    async def _check_ontology_terms(request: Request, project: Any, config: MappingConfig) -> None:
        """P14.8: every ontology term the new mapping names, where the saved mapping did not
        name the same one, must be in the project ontology. When the ontology cannot be read,
        a save that names a new term is refused rather than saved unchecked."""
        saved_path = Path(project.mapping_config_path)
        try:
            saved = ConfigManager.load_config(saved_path) if saved_path.is_file() else None
        except Exception:
            saved = None  # unreadable: every term in the new mapping is checked
        if not control_plane.new_terms(saved, config):
            return
        identity: control_plane.Identity = request.state.cp_identity
        ontology = await run_in_threadpool(control_plane.ontology_terms, identity)
        if ontology.get("problem"):
            raise HTTPException(status_code=503,
                                detail=f"cannot check the ontology terms: {ontology['problem']}")
        unknown = control_plane.unknown_terms(saved, config, ontology)
        if unknown:
            raise HTTPException(status_code=400, detail="; ".join(unknown))

    def _governed_save(identity: control_plane.Identity, background: BackgroundTasks,
                       name: str, project: Any, config: MappingConfig) -> dict[str, Any]:
        """P14.9 and P14.11: compare the new mapping with the saved one in full, ask about
        every difference before writing, then keep both versions and record each part
        changed. One save per project at a time, so none overwrites changes it did not
        check."""
        from r2g.mapping_changes import save_changes

        path = Path(project.mapping_config_path)
        text = ConfigManager.dump_config(config)
        written = MappingConfig.model_validate(yaml.safe_load(text) or {})  # as it will read
        with _save_locks.setdefault(name, threading.Lock()):
            saved_text = path.read_text(encoding="utf-8") if path.is_file() else None
            try:
                saved = None if saved_text is None else ConfigManager.load_config(path)
            except Exception:
                saved = None  # unreadable: every part of the new mapping is checked
            items = save_changes(saved, written)
            g = governing.govern(outbox, identity, name, "mapping.update", items) if items \
                else None
            versions = _projects_root / name
            before = governing.keep_version(versions, saved_text) if saved_text else None
            ConfigManager.save_config(config, path)
            catalog.touch_project(name)
            after = governing.keep_version(versions, text)
            for item in items:
                assert g is not None
                g.record(item, before=before, after=after, background=background)
        return {"saved": True, "collections": len(config.collections),
                "edges": len(config.edges), "changes": [i.where for i in items]}

    @app.put("/api/projects/{name}/mapping")
    async def save_mapping(name: str, body: dict[str, Any], request: Request,
                           background: BackgroundTasks):
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        try:
            config = MappingConfig.model_validate(body)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
        if governed:
            await _check_ontology_terms(request, project, config)
            try:
                return await run_in_threadpool(_governed_save, request.state.cp_identity,
                                               background, name, project, config)
            except governing.Refused as exc:
                raise HTTPException(status_code=403, detail=exc.detail())
        try:
            ConfigManager.save_config(config, project.mapping_config_path)
            catalog.touch_project(name)
            return {"saved": True, "collections": len(config.collections), "edges": len(config.edges)}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/projects/{name}/arangoimport-preview")
    async def preview_arangoimport(
        name: str,
        body: ArangoImportPreviewRequest,
    ):
        """Render a secret-safe batch artifact without execution or file I/O."""
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(
                status_code=404, detail=f"Project '{name}' not found"
            )
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(
                status_code=400, detail="No schema snapshot available"
            )
        try:
            if body.mapping is None:
                config = ConfigManager.load_config(
                    project.mapping_config_path
                )
                mapping_source = "saved"
            else:
                config = body.mapping
                mapping_source = "draft"

            issues = validate_config(snap.schema_data, config)
            if issues:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "message": "Mapping is not valid for this snapshot",
                        "issues": issues,
                    },
                )

            endpoint, database, username, _password = _resolve_target(project)
            generator: ArangoImportGenerator | CsvImportGenerator
            if body.mode == "jsonl":
                generator = ArangoImportGenerator(
                    config,
                    endpoint=endpoint,
                    database=database,
                    username=username,
                    password="",
                    data_dir=body.artifact_dir or "./output",
                )
            else:
                generator = CsvImportGenerator(
                    config,
                    snap.schema_data,
                    endpoint=endpoint,
                    database=database,
                    username=username,
                    password="",
                    data_dir=body.artifact_dir or "./dumps",
                )
            plan = generator.build_plan(
                graph_name=name,
                overwrite_on_initial=body.overwrite_on_initial,
                secret_safe=True,
            )

            focused: ImportCommandSpec | None = None
            if body.scope != "all":
                if not body.mapping_id:
                    raise HTTPException(
                        status_code=422,
                        detail="mapping_id is required for focused previews",
                    )
                candidates = (
                    plan.documents
                    if body.scope == "collection"
                    else plan.edges
                )
                focused = next(
                    (
                        item
                        for item in candidates
                        if item.mapping_id == body.mapping_id
                    ),
                    None,
                )
                if focused is None:
                    raise HTTPException(
                        status_code=404,
                        detail=(
                            f"Unknown {body.scope} mapping identity "
                            f"'{body.mapping_id}'"
                        ),
                    )

            response = plan.model_dump()
            response.update(
                {
                    "scope": body.scope,
                    "focused_command": (
                        focused.model_dump() if focused is not None else None
                    ),
                    "mapping_source": mapping_source,
                    "load_note": (
                        "Studio Load streams data directly over HTTP; this "
                        "generated batch script is an alternative artifact."
                    ),
                }
            )
            return response
        except HTTPException:
            raise
        except ValueError as e:
            raise HTTPException(status_code=422, detail=_safe_detail(e))
        except Exception as e:
            diagnostic = _safe_detail(e)
            logger.warning(
                "arangoimport_preview_failed",
                project=name,
                error=diagnostic,
            )
            raise HTTPException(
                status_code=500,
                detail={
                    "message": "Could not render import preview",
                    "diagnostic": diagnostic,
                },
            )

    @app.post("/api/projects/{name}/validate")
    async def validate_mapping(name: str):
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot available")
        try:
            config = ConfigManager.load_config(project.mapping_config_path)
            issues = validate_config(snap.schema_data, config)
            return {"valid": len(issues) == 0, "issues": issues}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/api/projects/{name}/validate-draft")
    async def validate_mapping_draft(name: str, body: dict[str, Any]):
        """Validate an in-memory (draft) mapping without persisting it.

        Used by the UI for inline / debounced validation while editing.
        """
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot available")
        try:
            draft_config = MappingConfig.model_validate(body)
            issues = validate_config(snap.schema_data, draft_config)
            return {"valid": len(issues) == 0, "issues": issues}
        except Exception as e:
            # Surface the exception as a validation issue rather than a 500
            return {"valid": False, "issues": [f"Draft parse error: {e}"]}

    @app.post("/api/projects/{name}/diff")
    async def diff_mapping(name: str, body: dict[str, Any]):
        """Diff the current mapping against a proposed mapping."""
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot available")
        try:
            from r2g.mapping_diff import diff_mappings

            old_config = ConfigManager.load_config(project.mapping_config_path)
            new_config = MappingConfig.model_validate(body)
            plan = diff_mappings(old_config, new_config, snap.schema_data)
            return plan.model_dump()
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/api/projects/{name}/migration-plan")
    async def migration_plan(name: str):
        """Diff the *live* database state (last loaded mapping) against the
        current saved mapping, returning the change-management plan.

        ``loaded`` is False when the project has never been loaded; in that case
        there is nothing in the database to migrate and the caller can just load.
        """
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot available")

        loaded = getattr(project, "loaded_mapping", None)
        if not loaded:
            return {"loaded": False, "plan": {"changes": [], "actions": []}}

        from r2g.mapping_diff import diff_mappings

        try:
            old_config = MappingConfig.model_validate(loaded)
            new_config = ConfigManager.load_config(project.mapping_config_path)
            plan = diff_mappings(old_config, new_config, snap.schema_data)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return {"loaded": True, "loaded_at": str(getattr(project, "loaded_at", "")), "plan": plan.model_dump()}

    @app.post("/api/projects/{name}/migrate")
    async def migrate(name: str, body: MigrateRequest, request: Request,
                      background: BackgroundTasks):
        """Apply mapping changes to an already-loaded database in place.

        Renames document/edge collections, reloads edges whose endpoints moved,
        renames properties via AQL, and rebuilds the named graph. Runs
        synchronously and returns an execution report.
        """
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot available")
        loaded = getattr(project, "loaded_mapping", None)
        if not loaded:
            raise HTTPException(
                status_code=400,
                detail="Project has not been loaded yet; run a load instead of a migration.",
            )

        from r2g.mapping_diff import diff_mappings
        from r2g.selective_reload import SelectiveReloader

        old_config = MappingConfig.model_validate(loaded)
        new_config = ConfigManager.load_config(project.mapping_config_path)
        plan = diff_mappings(old_config, new_config, snap.schema_data)
        if not plan.actions:
            return {"migrated": True, "report": {"actions_executed": [], "actions_skipped": [], "errors": []}}
        g, written = None, []
        if governed:  # P14.10: every collection it writes, on what that collection holds
            from r2g.mapping_changes import migration_items

            written = migration_items(plan.actions, old_config, new_config)
            g = await _govern(request, name, "data.migrate", written)

        arango_endpoint, arango_database, arango_username, arango_password = _resolve_target(project)
        if (arango_database or "").strip().lower() == "_system":
            raise HTTPException(status_code=400, detail="Refusing to migrate the '_system' database.")

        writer = ArangoWriter(
            endpoint=arango_endpoint,
            database=arango_database,
            username=arango_username,
            password=arango_password,
        )

        # A live source connector is only needed for edge reloads; build it
        # best-effort so property-only / pure-rename plans still work offline.
        source_connector = None
        source = catalog.get_source(project.source_name)
        if source is not None:
            try:
                from r2g.connectors.base import create_source_connector

                source_connector = create_source_connector(
                    source.source_type or "postgresql",
                    source.connection_string,
                    schema_name=snap.pg_schema,
                    source_params=source.source_params,
                )
            except Exception as e:
                logger.warning("migrate_source_unavailable", error=str(e))

        reloader = SelectiveReloader(
            writer=writer,
            plan=plan,
            schema=snap.schema_data,
            config=new_config,
            source_connector=source_connector,
            graph_name=name,
            on_duplicate="replace",
            pg_schema=snap.pg_schema,
        )
        report = reloader.execute(dry_run=body.dry_run)
        if not body.dry_run and not report.errors:
            catalog.set_loaded_mapping(name, new_config.model_dump())
        if g is not None and not body.dry_run:  # a dry run changes nothing to record
            for item in written:
                g.record(item, background=background)
        return {
            "migrated": not body.dry_run and not report.errors,
            "report": {
                "actions_executed": report.actions_executed,
                "actions_skipped": report.actions_skipped,
                "errors": report.errors,
                "rows_reloaded": report.rows_reloaded,
            },
        }

    @app.get("/api/projects/{name}/graph-data")
    async def get_graph_data(name: str):
        """Return D3-compatible graph data for the project mapping."""
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot")
        try:
            from r2g.generators.visualizer import MappingVisualizer

            config = ConfigManager.load_config(project.mapping_config_path)
            viz = MappingVisualizer(snap.schema_data, config)
            return {
                "graph": viz._build_graph_data(),
                "tables": viz._build_tables_data(),
                "edges": viz._build_edges_data(),
                "config": viz._build_config_data(),
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/api/projects/{name}/history")
    async def get_project_history(name: str, limit: int = 20):
        records = catalog.get_history(project_name=name, limit=limit)
        return [r.model_dump() for r in records]

    # ── Load endpoints ─────────────────────────────────────────────────

    @app.get("/api/projects/{name}/entitlements")
    async def get_entitlements(name: str, threshold: str = "confidential"):
        """Pre-load entitlement report: classified fields + mosaic levels (Phase 9b)."""
        from r2g.classification import SENSITIVITY_ORDER
        from r2g.governance import build_entitlement_report

        threshold = (threshold or "confidential").strip().lower()
        if threshold not in SENSITIVITY_ORDER:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid threshold; expected one of: {', '.join(SENSITIVITY_ORDER)}",
            )
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot available")
        try:
            config = ConfigManager.load_config(project.mapping_config_path)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to load mapping config: {e}")

        report = build_entitlement_report(
            config, snap.schema_data, threshold=threshold, project=name
        )
        return {
            "project": name,
            "threshold": threshold,
            "summary": report.summary(),
            "collection_levels": report.collection_levels,
            "edge_levels": report.edge_levels,
            "fields": [f.model_dump() for f in report.fields],
            "above_threshold": [f.model_dump() for f in report.above_threshold],
        }

    @app.post("/api/projects/{name}/governance/emit")
    async def emit_governance(
        name: str,
        threshold: str = "confidential",
        tier_layout: bool = False,
    ):
        """Emit the Phase 9c governance artifacts for a project (advise, not enforce)."""
        from r2g.classification import SENSITIVITY_ORDER
        from r2g.governance import build_entitlement_report, write_governance_artifacts

        threshold = (threshold or "confidential").strip().lower()
        if threshold not in SENSITIVITY_ORDER:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid threshold; expected one of: {', '.join(SENSITIVITY_ORDER)}",
            )
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot available")
        try:
            config = ConfigManager.load_config(project.mapping_config_path)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to load mapping config: {e}")

        source = catalog.get_source(project.source_name)
        owners = source.data_owners if source else []
        synced = (
            source.classifications_synced_at.isoformat()
            if source and source.classifications_synced_at
            else None
        )
        report = build_entitlement_report(
            config, snap.schema_data, threshold=threshold, project=name
        )
        out_dir = Path(project.mapping_config_path).parent
        try:
            written = write_governance_artifacts(
                report,
                out_dir,
                owners=owners,
                database=project.arango_database,
                tier_layout=tier_layout,
                synced_at=synced,
            )
        except OSError as e:
            raise HTTPException(status_code=500, detail=f"Failed to write artifacts: {e}")
        return {
            "project": name,
            "threshold": threshold,
            "artifacts": {k: str(v) for k, v in written.items()},
            "summary": report.summary(),
        }

    def _suggestion_key(body: Any) -> str | None:
        """The API key a suggestion is made with. A ``$VAR`` key names one of the server's
        LLM keys (``_LLM_KEY_VARS``), and goes only to the provider's own address: with a
        ``base_url`` of the request's choosing, only the request's own key travels, and the
        provider does not fall back to the server's (``key_from_env``). Under a control
        plane, where the caller is any user with a context, the server's environment is
        not read on a request's behalf at all: no ``$VAR``, and no ``base_url`` without the
        request's own key (P14.12)."""
        if not governed:
            from r2g.llm import normalize_llm_type

            key: str | None = body.api_key or None
            named = bool(key and key.startswith("$"))
            if key and named and key[1:] not in _LLM_KEY_VARS:
                raise HTTPException(status_code=400, detail=(
                    "An API key may name only "
                    + " or ".join(f"${v}" for v in sorted(_LLM_KEY_VARS))))
            # A keyless call to a local OpenAI-compatible server stays allowed.
            keyless = not key and normalize_llm_type(body.provider) == "openai-compatible"
            if body.base_url and (named or not (key or keyless)):
                raise HTTPException(status_code=400, detail=(
                    "The server's API key is sent only to the provider's own address "
                    "(or the server's $OPENAI_BASE_URL); with base_url, pass the key "
                    "itself"))
            return _resolve_env_ref(key) if key and named else key
        if body.api_key and body.api_key.startswith("$"):
            raise HTTPException(status_code=403, detail=(
                "Not available under the control plane: an API key may not name the "
                "server's environment; pass the key itself"))
        if body.base_url and not body.api_key:
            raise HTTPException(status_code=403, detail=(
                "Not available under the control plane: an address of the request's "
                "choosing needs the request's own API key"))
        return body.api_key or None

    @app.post("/api/projects/{name}/suggest-ontology")
    async def suggest_ontology(name: str, body: SuggestOntologyRequest):
        """Ask an LLM to propose a richer ontology (Phase 10b). Read-only.

        Returns the structured proposal, the resulting *validated* candidate
        mapping, a ``diff_mappings`` diff against the current mapping, the
        validation/provenance notes, and provenance. Nothing is written — the
        client reviews the diff and applies an accepted subset via
        ``POST /apply-ontology`` (which reuses the same save path as the mapper).
        """
        import datetime as _dt

        from r2g.llm import proposal_to_mapping
        from r2g.mapping_diff import diff_mappings

        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot available")
        try:
            current = ConfigManager.load_config(project.mapping_config_path)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to load mapping config: {e}")

        engine_norm = (body.engine or "llm").strip().lower()
        if engine_norm not in ("llm", "rsa"):
            raise HTTPException(status_code=400, detail=f"Unknown engine '{body.engine}'")

        if engine_norm == "rsa":
            from r2g.rsa_ontology import propose_ontology_from_schema

            resolved_key = _suggestion_key(body)
            rsa_provider = body.provider if body.refine else None
            try:
                proposal, rsa_meta = propose_ontology_from_schema(
                    snap.schema_data,
                    provider=rsa_provider,
                    model=body.model,
                    api_key=resolved_key,
                )
            except ImportError as e:
                raise HTTPException(status_code=400, detail=str(e))
            except Exception as e:
                raise HTTPException(
                    status_code=502, detail=f"Ontology analysis failed: {_safe_detail(e)}"
                )
            new_config, notes = proposal_to_mapping(
                proposal, snap.schema_data, source_schema=current.source_schema
            )
            plan = diff_mappings(current, new_config, snap.schema_data)
            provenance = {
                "engine": "relational-schema-analyzer",
                "refined": bool(body.refine),
                "provider": body.provider if body.refine else None,
                "model": (body.model or "(provider default)") if body.refine else None,
                "domain_hint": body.domain,
                "table_count": len(snap.schema_data.tables),
                "analyzer_confidence": rsa_meta.get("confidence"),
                "detected_patterns": rsa_meta.get("detectedPatterns"),
                "review_required": rsa_meta.get("reviewRequired"),
                "physical_schema_fingerprint": rsa_meta.get("physicalSchemaFingerprint"),
                "llm_refinement": rsa_meta.get("llm"),
                "proposed_collections": len(proposal.collections),
                "proposed_edges": len(proposal.edges),
                "proposed_renames": len(proposal.renames),
                "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            }
            return {
                "project": name,
                "proposal": proposal.model_dump(),
                "mapping": new_config.model_dump(),
                "diff": plan.model_dump(),
                "notes": notes,
                "provenance": provenance,
            }

        from r2g.llm import create_llm_provider
        from r2g.llm.base import OntologyRequest
        from r2g.llm.prompt import build_schema_digest

        samples: dict = {}
        grounding = ""
        if body.sample or body.ground:
            from r2g.llm.grounding import build_grounding
            from r2g.llm.sampling import build_sampler_for_source, collect_samples

            source = catalog.get_source(project.source_name)
            sampler = (
                build_sampler_for_source(source, pg_schema=snap.pg_schema)
                if source is not None
                else None
            )
            try:
                if body.sample and sampler is not None:
                    samples = collect_samples(
                        sampler,
                        snap.schema_data,
                        per_column=max(1, body.samples_per_column),
                    )
                if body.ground:
                    grounding = build_grounding(snap.schema_data, sampler=sampler)
            finally:
                if sampler is not None:
                    close = getattr(sampler, "close", None)
                    if callable(close):
                        close()

        try:
            digest = build_schema_digest(
                snap.schema_data,
                domain_hint=body.domain,
                include_samples=bool(samples),
                samples=samples,
                samples_per_column=max(1, body.samples_per_column),
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

        resolved_key = _suggestion_key(body)
        params = {"base_url": body.base_url} if body.base_url else None
        try:
            llm = create_llm_provider(
                body.provider, model=body.model, api_key=resolved_key, params=params,
                key_from_env=not body.base_url,
            )
        except (ValueError, ImportError) as e:
            raise HTTPException(status_code=400, detail=str(e))

        request = OntologyRequest(
            schema_digest=digest,
            domain_hint=body.domain,
            grounding=grounding,
            table_count=len(snap.schema_data.tables),
        )
        try:
            proposal = llm.propose_ontology(request)
        except ValueError as e:
            # Missing key / malformed model output → actionable 400.
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            # Upstream/network failure.
            raise HTTPException(status_code=502, detail=f"LLM proposal failed: {_safe_detail(e)}")

        new_config, notes = proposal_to_mapping(
            proposal, snap.schema_data, source_schema=current.source_schema
        )
        plan = diff_mappings(current, new_config, snap.schema_data)
        provenance = {
            "engine": "llm",
            "provider": body.provider,
            "model": body.model or "(provider default)",
            "domain_hint": body.domain,
            "table_count": len(snap.schema_data.tables),
            "sampled": bool(samples),
            "sampled_columns": sum(len(cols) for cols in samples.values()),
            "grounded": bool(grounding),
            "proposed_collections": len(proposal.collections),
            "proposed_edges": len(proposal.edges),
            "proposed_renames": len(proposal.renames),
            "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        }
        return {
            "project": name,
            "proposal": proposal.model_dump(),
            "mapping": new_config.model_dump(),
            "diff": plan.model_dump(),
            "notes": notes,
            "provenance": provenance,
        }

    @app.post("/api/projects/{name}/apply-ontology")
    async def apply_ontology(name: str, body: ApplyOntologyRequest):
        """Build a mapping from an accepted (possibly subset) proposal (Phase 10b).

        The client sends back the accepted subset of the proposal returned by
        ``/suggest-ontology``; the server rebuilds it through the same
        ``proposal_to_mapping`` hallucination gate (so the result is always valid)
        and returns it as an **editable draft** — like ``apply-naming``, nothing is
        persisted here. The Studio loads the draft, marks the project dirty, and
        the user Saves through the normal path (which offers migration if the
        target is already loaded).
        """
        from r2g.llm import proposal_to_mapping
        from r2g.llm.base import OntologyProposal
        from r2g.mapping_diff import diff_mappings

        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot available")
        try:
            current = ConfigManager.load_config(project.mapping_config_path)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to load mapping config: {e}")

        try:
            proposal = OntologyProposal.model_validate(body.proposal)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid proposal payload: {e}")

        new_config, notes = proposal_to_mapping(
            proposal, snap.schema_data, source_schema=current.source_schema
        )
        plan = diff_mappings(current, new_config, snap.schema_data)
        return {
            "mapping": new_config.model_dump(),
            "collections": len(new_config.collections),
            "edges": len(new_config.edges),
            "changes": len(plan.changes),
            "notes": notes,
        }

    @app.post("/api/projects/{name}/load", status_code=202)
    async def start_load(name: str, body: LoadRequest, request: Request,
                         background: BackgroundTasks):
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")

        source = catalog.get_source(project.source_name)
        if source is None:
            raise HTTPException(status_code=400, detail=f"Source '{project.source_name}' not found")

        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot available")

        try:
            config = ConfigManager.load_config(project.mapping_config_path)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to load mapping config: {e}")

        issues = validate_config(snap.schema_data, config)
        if issues:
            raise HTTPException(status_code=400, detail={"validation_errors": issues})

        g, written = None, []
        if governed:  # P14.10: every table and relationship it writes, on its class
            from r2g.mapping_changes import graph_items, load_collections, load_items

            if body.emit_governance:
                raise HTTPException(status_code=403, detail=governing.classify(
                    "POST", f"/api/projects/{name}/governance/emit")[1])
            written = load_items(config, body.include_tables, body.exclude_tables,
                                 drop=body.drop_collections)
            if not body.dry_run:  # it ends by declaring the named graph: checked if it changes
                state = await run_in_threadpool(_graph_state, *_resolve_target(project),
                                                body.graph_name or name)
                written += graph_items(config, state, load_collections(
                    config, body.include_tables, body.exclude_tables))
            g = await _govern(request, name, "data.load", written)

        # Phase 9b governance gate: exclude above-threshold, unmasked fields by
        # default (unless the caller opts in with allow_sensitive). Advisory — the
        # excluded set + lineage are reported, never silently dropped.
        from r2g.governance import (
            apply_sensitivity_gate,
            build_entitlement_report,
            write_lineage_manifest,
        )

        report = build_entitlement_report(
            config,
            snap.schema_data,
            threshold=body.sensitivity_threshold,
            project=name,
        )
        config, gate_excluded = apply_sensitivity_gate(
            config, report, allow_sensitive=body.allow_sensitive
        )
        # Reflect the gate's decisions onto the report so the lineage manifest
        # records each field's true handling (excluded vs loaded).
        excluded_keys = {(f.target_collection, f.target_property) for f in gate_excluded}
        for f in report.fields:
            if (f.target_collection, f.target_property) in excluded_keys:
                f.excluded = True
        out_dir = Path(project.mapping_config_path).parent
        try:
            if body.emit_governance:
                from r2g.governance import write_governance_artifacts

                synced = (
                    source.classifications_synced_at.isoformat()
                    if source.classifications_synced_at
                    else None
                )
                write_governance_artifacts(
                    report,
                    out_dir,
                    owners=source.data_owners,
                    database=project.arango_database,
                    tier_layout=body.tier_layout,
                    synced_at=synced,
                )
            else:
                write_lineage_manifest(report, out_dir)
        except OSError as e:
            logger.warning("lineage_manifest_write_failed", project=name, error=str(e))

        load_record = catalog.start_load(project_name=name, load_type="streaming")
        load_id = load_record.id

        arango_endpoint, arango_database, arango_username, arango_password = _resolve_target(project)

        # Never load into the ArangoDB system database; it is not a data graph.
        if (arango_database or "").strip().lower() == "_system":
            catalog.complete_load(load_id, 0, 0, [], "failed")
            raise HTTPException(
                status_code=400,
                detail=(
                    "Refusing to load into the '_system' database. Configure a "
                    "dedicated graph database for this project or target."
                ),
            )

        writer = ArangoWriter(
            endpoint=arango_endpoint,
            database=arango_database,
            username=arango_username,
            password=arango_password,
        )

        try:
            from r2g.connectors.base import create_source_connector

            source_connector = create_source_connector(
                source.source_type or "postgresql",
                source.connection_string,
                schema_name=snap.pg_schema,
                source_params=source.source_params,
            )
        except ImportError as e:
            raise HTTPException(status_code=501, detail=_safe_detail(e))
        except ValueError as e:
            raise HTTPException(status_code=400, detail=_safe_detail(e))

        pipeline = StreamingPipeline(
            source_connector=source_connector,
            arango_writer=writer,
            schema=snap.schema_data,
            config=config,
            batch_size=body.batch_size,
            on_duplicate=body.on_duplicate,
            pg_schema=snap.pg_schema,
            dry_run=body.dry_run,
            drop_collections=body.drop_collections,
            workers=body.workers,
            include_tables=set(body.include_tables) if body.include_tables else None,
            exclude_tables=set(body.exclude_tables) if body.exclude_tables else None,
            dlq=_dead_letters(name, load_id),
        )

        progress_queue: queue.Queue[dict[str, Any]] = queue.Queue()

        def progress_callback(event_data: dict[str, Any]) -> None:
            progress_queue.put(event_data)

        # Default to a named graph named after the project so the graph is
        # auto-created in ArangoDB when the caller doesn't specify one.
        graph_name = body.graph_name or name

        def run_pipeline() -> None:
            try:
                result = pipeline.run(graph_name=graph_name, on_event=progress_callback)
                total_rows = sum(r[1] for r in result.get("documents", []))
                total_rows += sum(r[1] for r in result.get("edges", []))
                total_errors = sum(len(e) for e in pipeline.errors.values())
                collections = [r[0] for r in result.get("documents", [])] + [
                    r[0] for r in result.get("edges", [])
                ]
                catalog.complete_load(load_id, total_rows, total_errors, collections, "completed")
                if not body.dry_run:
                    catalog.set_loaded_mapping(name, config.model_dump())
            except Exception as exc:
                import traceback as _tb

                err_type = type(exc).__name__
                err_msg = str(exc) or err_type
                # Keep the full traceback server-side only; never stream it to
                # clients (it leaks paths/internals). Clients get type + message.
                logger.error(
                    "load_pipeline_failed",
                    load_id=load_id,
                    error_type=err_type,
                    error=err_msg,
                    traceback=_tb.format_exc(),
                )
                progress_queue.put(
                    {
                        "event": "error",
                        "error": err_msg,
                        "error_type": err_type,
                    }
                )
                catalog.complete_load(
                    load_id,
                    0,
                    0,
                    [],
                    "failed",
                    error_message=err_msg,
                    error_type=err_type,
                )

        thread = threading.Thread(target=run_pipeline, daemon=True)
        _running_loads[load_id] = {"thread": thread, "queue": progress_queue, "pipeline": pipeline,
                                   "project": name}
        thread.start()
        if g is not None and not body.dry_run:
            # Recorded when it starts (P14.10): a load may outlast the user's context, so
            # its outcome stays in r2g's load history.
            for item in written:
                g.record(item, after=f"{name}/loads/{load_id}", background=background)

        return {
            "load_id": load_id,
            "status": "started",
            "excluded_sensitive_fields": [
                {
                    "collection": f.target_collection,
                    "property": f.target_property,
                    "level": f.level,
                    "source_columns": f.source_columns,
                }
                for f in gate_excluded
            ],
        }

    @app.get("/api/projects/{name}/load/{load_id}/status")
    async def get_load_status(name: str, load_id: str):
        events: list[dict[str, Any]] = []
        load = _running_load(name, load_id)
        if load is not None:
            q = load["queue"]
            while not q.empty():
                try:
                    events.append(q.get_nowait())
                except queue.Empty:
                    break
            thread_alive = load["thread"].is_alive()
            status = "running" if thread_alive else "completed"
            if events and events[-1].get("event") == "error":
                status = "failed"
        else:
            status = "unknown"

        return {"load_id": load_id, "status": status, "events": events}

    @app.get("/api/projects/{name}/load/{load_id}/stream")
    async def stream_load_progress(name: str, load_id: str):
        load = _running_load(name, load_id)
        if load is None:
            raise HTTPException(status_code=404, detail="Load not found")

        async def event_generator():
            q = load["queue"]
            while True:
                try:
                    event = q.get_nowait()
                    yield f"data: {json.dumps(event)}\n\n"
                    if event.get("event") in ("complete", "error"):
                        break
                except queue.Empty:
                    if not load["thread"].is_alive():
                        yield f"data: {json.dumps({'event': 'complete'})}\n\n"
                        break
                    await asyncio.sleep(0.5)
                    yield ": keepalive\n\n"

        return StreamingResponse(event_generator(), media_type="text/event-stream")

    @app.get("/api/projects/{name}/load/{load_id}/errors")
    async def get_load_errors(name: str, load_id: str, limit: int = 50, offset: int = 0):
        from r2g.dlq import DeadLetterQueue

        if catalog.get_project(name) is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        dlq = _dead_letters(name, load_id)
        if not dlq.path.exists() and not governed:
            # A load from before rejected rows were kept per project wrote them to
            # ~/.r2g/dlq/<id>.jsonl, whatever the catalog directory. Not under a control
            # plane, where every project's rows must stay its own.
            legacy = Path.home() / ".r2g" / "dlq" / f"{load_id}.jsonl"
            if legacy.exists():
                dlq = DeadLetterQueue(load_id, dlq_dir=legacy.parent)
        errors = [_redact_dlq_entry(e) for e in dlq.read_errors(limit=limit, offset=offset)]
        return {"load_id": load_id, "errors": errors, "count": len(errors)}

    # ── Auto-map endpoint ──────────────────────────────────────────────

    @app.post("/api/projects/{name}/auto-map")
    async def auto_map(name: str):
        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        snap = catalog.get_latest_snapshot(project.source_name)
        if snap is None:
            raise HTTPException(status_code=400, detail="No schema snapshot")
        config = ConfigManager.generate_default_config(snap.schema_data, source_schema=snap.pg_schema)
        return config.model_dump()

    @app.post("/api/projects/{name}/apply-naming")
    async def apply_naming(name: str, body: NamingConventionRequest):
        """Re-case collection / property / edge names per a chosen convention.

        Returns the transformed mapping (NOT persisted) so the UI can load it as
        an editable draft for review before saving.
        """
        from r2g.naming import apply_naming_convention
        from r2g.types import NamingConvention

        project = catalog.get_project(name)
        if project is None:
            raise HTTPException(status_code=404, detail=f"Project '{name}' not found")
        try:
            config = ConfigManager.load_config(project.mapping_config_path)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        snap = catalog.get_latest_snapshot(project.source_name)
        convention = NamingConvention(
            collections=body.collections,
            properties=body.properties,
            edges=body.edges,
        )
        new_config = apply_naming_convention(
            config, convention, snap.schema_data if snap else None
        )
        return new_config.model_dump()

    # ── Target endpoints ───────────────────────────────────────────────

    @app.get("/api/targets")
    async def list_targets():
        return [_redact_target(t.model_dump()) for t in catalog.list_targets()]

    @app.post("/api/targets", status_code=201)
    async def add_target(body: TargetCreateRequest):
        try:
            target = catalog.add_target(
                name=body.name,
                endpoint=body.endpoint,
                database=body.database,
                username=body.username,
                password=body.password,
                description=body.description,
            )
            return _redact_target(target.model_dump())
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))

    @app.delete("/api/targets/{name}")
    async def remove_target(name: str):
        if not catalog.remove_target(name):
            raise HTTPException(status_code=404, detail=f"Target '{name}' not found")
        return {"removed": name}

    @app.post("/api/targets/{name}/introspect")
    async def introspect_target(name: str):
        target = catalog.get_target(name)
        if not target:
            raise HTTPException(status_code=404, detail=f"Target '{name}' not found")
        try:
            from r2g.connectors.arango_reader import ArangoIntrospector

            intro = ArangoIntrospector(
                endpoint=target.endpoint,
                database=target.database,
                username=target.username,
                password=target.password,
            )
            return intro.introspect()
        except Exception as e:
            raise HTTPException(status_code=500, detail=_safe_detail(e))

    # ── External data catalogs (Phase 8b) ──────────────────────────────

    def _redact_catalog(dump: dict) -> dict:
        from r2g.security import redact_for_display

        out = dict(dump)
        out["token"] = redact_for_display(out.get("token") or "")
        return out

    def _build_catalog_provider(name: str):
        from r2g.catalogs.base import create_catalog_provider

        cfg = catalog.get_catalog(name)
        if cfg is None:
            raise HTTPException(status_code=404, detail=f"Catalog '{name}' not found")
        token = cfg.token
        if token and token.startswith("$"):
            token = os.environ.get(token[1:], token)
        return create_catalog_provider(
            cfg.provider_type, cfg.endpoint, name=cfg.name, token=token or None, params=cfg.params
        )

    @app.get("/api/catalogs")
    async def list_catalogs():
        return [_redact_catalog(c.model_dump()) for c in catalog.list_catalogs()]

    @app.post("/api/catalogs", status_code=201)
    async def add_catalog(body: CatalogCreateRequest):
        try:
            cfg = catalog.add_catalog(
                name=body.name,
                provider_type=body.provider_type,
                endpoint=body.endpoint,
                token=body.token,
                description=body.description,
            )
            return _redact_catalog(cfg.model_dump())
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))

    @app.delete("/api/catalogs/{name}")
    async def remove_catalog(name: str):
        if not catalog.remove_catalog(name):
            raise HTTPException(status_code=404, detail=f"Catalog '{name}' not found")
        return {"removed": name}

    @app.get("/api/catalogs/{name}/browse")
    async def browse_catalog(name: str, path: str | None = None, search: str | None = None):
        provider = _build_catalog_provider(name)
        try:
            if search:
                assets = provider.search(search)
            elif path:
                asset = provider.get_asset(path)
                if asset is None:
                    raise HTTPException(status_code=404, detail=f"Asset '{path}' not found")
                assets = provider.list_children(asset)
            else:
                assets = provider.list_data_sources()
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=502, detail=_safe_detail(e))
        return [a.model_dump() for a in assets]

    @app.post("/api/catalogs/{name}/import-source", status_code=201)
    async def import_catalog_source(name: str, body: CatalogImportRequest):
        provider = _build_catalog_provider(name)
        try:
            asset = provider.get_asset(body.asset_fqn)
            if asset is None:
                raise HTTPException(status_code=404, detail=f"Asset '{body.asset_fqn}' not found")
            resolved = provider.resolve_source(asset)
            source = catalog.add_source(
                name=body.source_name,
                source_type=resolved.source_type,
                connection_string=resolved.connection_string,
                description=body.description or f"Imported from catalog '{name}' ({body.asset_fqn})",
                source_params=resolved.source_params,
            )
        except HTTPException:
            raise
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=502, detail=_safe_detail(e))
        return {
            "source": _redact_source(source.model_dump()),
            "schema_name": resolved.schema_name,
            "notes": resolved.notes,
        }

    return app


# ── Request models ────────────────────────────────────────────────────


class SourceCreateRequest(BaseModel):
    name: str
    source_type: str = "postgresql"
    connection_string: str
    description: str = ""
    owner: str = ""
    source_params: dict[str, Any] = {}


class DemoPresetInstallRequest(BaseModel):
    target_name: str | None = None


class ProjectCreateRequest(BaseModel):
    name: str
    source_name: str
    # Optional: if it points to an existing mapping file, its contents are
    # imported. The persisted location is always derived server-side.
    mapping_config_path: str = ""
    arango_endpoint: str = "http://localhost:8529"
    arango_database: str = "_system"
    mapping_name: str = ""
    mapping_description: str = ""


class ProjectUpdateRequest(BaseModel):
    """Editable project metadata. Only provided fields are updated."""

    mapping_name: str | None = None
    mapping_description: str | None = None
    target_name: str | None = None


class ArangoImportPreviewRequest(BaseModel):
    """Validated request for an in-memory import artifact."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["jsonl", "csv"] = "jsonl"
    scope: Literal["all", "collection", "edge"] = "all"
    mapping_id: str | None = None
    mapping: MappingConfig | None = None
    overwrite_on_initial: bool = True
    artifact_dir: str | None = Field(default=None, max_length=4096)

    @field_validator("artifact_dir")
    @classmethod
    def _validate_artifact_dir(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.strip():
            raise ValueError("artifact_dir must not be empty")
        if any(ord(character) < 32 for character in value):
            raise ValueError("artifact_dir must not contain control characters")
        return value


class LoadRequest(BaseModel):
    workers: int = 1
    batch_size: int = 10000
    on_duplicate: str = "replace"
    drop_collections: bool = False
    include_tables: list[str] | None = None
    exclude_tables: list[str] | None = None
    dry_run: bool = False
    graph_name: str | None = None
    # Phase 9b governance gate. By default, fields at/above ``sensitivity_threshold``
    # (the mosaic-recomputed level) that are not masked are excluded from the load;
    # ``allow_sensitive`` is the explicit opt-out.
    allow_sensitive: bool = False
    sensitivity_threshold: str = "confidential"
    # Phase 9c: on a governed load, also emit the enforcement artifacts
    # (classification manifest, suggested-RBAC, policy.rego) next to lineage.json.
    emit_governance: bool = False
    tier_layout: bool = False


class SuggestOntologyRequest(BaseModel):
    """Parameters for POST /api/projects/{name}/suggest-ontology (Phase 10b)."""

    domain: str = ""
    # Derivation engine: "llm" (model-proposed) or "rsa" (deterministic conceptual
    # model via relational-schema-analyzer, offline unless `refine` is set).
    engine: str = "llm"
    provider: str = "openai"
    model: str | None = None
    # API key, or $OPENAI_API_KEY / $ANTHROPIC_API_KEY (to the provider's own address
    # only); when omitted, and with no base_url, the provider reads it from the
    # environment ($OPENAI_API_KEY). Never persisted.
    api_key: str | None = None
    # Endpoint base URL (required for openai-compatible / local providers).
    base_url: str | None = None
    # Opt-in bounded value sampling from non-sensitive columns to ground the model.
    sample: bool = False
    samples_per_column: int = 5
    # Opt-in deterministic denormalization findings (Phase 11) as advisory evidence.
    ground: bool = False
    # With engine="rsa": additively LLM-refine the deterministic model using `provider`.
    refine: bool = False


class ApplyOntologyRequest(BaseModel):
    """Parameters for POST /api/projects/{name}/apply-ontology (Phase 10b).

    ``proposal`` is the accepted (possibly subset) ontology proposal returned by
    ``/suggest-ontology`` — the client drops rejected items before sending.
    """

    proposal: dict[str, Any]


class TargetCreateRequest(BaseModel):
    name: str
    endpoint: str = "http://localhost:8529"
    database: str = "_system"
    username: str = "root"
    password: str = ""
    description: str = ""


class MigrateRequest(BaseModel):
    """Parameters for POST /api/projects/{name}/migrate (in-place migration)."""

    dry_run: bool = False


class NamingConventionRequest(BaseModel):
    """Case styles for POST /api/projects/{name}/apply-naming.

    Each value is one of ``preserve`` | ``snake`` | ``camel`` | ``pascal``.
    """

    collections: NameCase = "preserve"
    properties: NameCase = "preserve"
    edges: NameCase = "preserve"


class CatalogCreateRequest(BaseModel):
    """Parameters for POST /api/catalogs (register an external data catalog)."""

    name: str
    provider_type: str = "openmetadata"
    endpoint: str
    token: str = ""
    description: str = ""


class CatalogImportRequest(BaseModel):
    """Parameters for POST /api/catalogs/{name}/import-source."""

    asset_fqn: str
    source_name: str
    description: str = ""


class InferFksRequest(BaseModel):
    """Parameters for POST /api/sources/{name}/infer-fks.

    Defaults are "cheap" — name-heuristic only, no warehouse queries.
    Set ``sample=true`` to opt into value-overlap sampling (PostgreSQL
    and CSV today; Snowflake falls back to name-only).
    """

    sample: bool = False
    sample_limit: int = 10_000
    min_confidence: float = 0.4
    veto_on_zero_overlap: bool = True


class InferSharedKeysRequest(BaseModel):
    """Parameters for POST /api/shared-keys/infer (PRD P6.7).

    Defaults are "cheap" — every snapshotted source, name + JSON-type
    heuristics only, no queries against any source. Set ``sample=true`` to opt
    into bounded value probes (hub-column uniqueness and cross-source value
    overlap); sources whose type has no sampler are reported in ``skipped``
    and still participate on name/type evidence alone.
    """

    sources: Optional[List[str]] = None
    sample: bool = False
    sample_limit: int = 1_000
    min_confidence: float = 0.4


class AnalyzeDenormRequest(BaseModel):
    """Parameters for POST /api/sources/{name}/analyze-denorm.

    Defaults are "cheap" — structural detectors only (repeating groups), no
    warehouse queries. Set ``sample=true`` to opt into the bounded value probes
    that drive embedded-lookup / functional-dependency detection.
    ``no_sample_columns`` lists columns to keep out of the sampler (bare ``col``
    or ``table.col``), e.g. sensitive / PII fields.
    """

    sample: bool = False
    sample_limit: int = 10_000
    min_confidence: float = 0.4
    no_sample_columns: list[str] = Field(default_factory=list)

