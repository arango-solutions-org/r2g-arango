"""r2g as a tool of a context-and-governance control plane (``CONTROL_PLANE_URL``).

Under a control plane the context token is the only credential, r2g shows only the
transformation project the control-plane project opens in it, and a context from another
tenant is refused. Each refusal has a positive control. The control plane is a fake behind
``control_plane.set_instance``: its SDK is not installed here.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
import time
import types
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from r2g import control_plane
from r2g.catalog import CatalogManager
from r2g.ui.server import create_app

TOKEN = "context-for-equity"
OTHER_TENANT = "context-for-globex"
UNAVAILABLE = "context-while-down"
SERVICE = "control-plane-service-token"
SERVICE_UNAVAILABLE = "service-token-while-down"
STATIC = Path(__file__).resolve().parents[1] / "src" / "r2g" / "ui" / "static"


@pytest.fixture(autouse=True)
def _reset_structlog():
    """Restore structlog to real stderr after a CliRunner invocation.

    CliRunner redirects stdout/stderr; structlog caches the (now-closed) stream,
    which poisons logging in every later test. Mirrors the fixture in
    tests/test_cli.py; `r2g mcp` below runs through CliRunner.
    """
    yield
    import structlog

    structlog.reset_defaults()
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(0),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )


class FakeControlPlane:
    def verify(self, token: str) -> control_plane.Identity:
        if token == UNAVAILABLE:
            raise control_plane.ContextUnavailable("keys could not be fetched")
        if token not in (TOKEN, OTHER_TENANT):
            raise control_plane.ContextRejected("bad signature")
        return control_plane.Identity(
            principal="pj@example.com", roles=("analyst",),
            tenant="globex" if token == OTHER_TENANT else "local",
            project="equity", ontology=("fibo-equity", "working"), token=token)

    def verify_service(self, token: str) -> None:
        if token == SERVICE_UNAVAILABLE:
            raise control_plane.ContextUnavailable("keys could not be fetched")
        if token != SERVICE:
            raise control_plane.ContextRejected("not a service token for r2g")

    def project(self, identity: control_plane.Identity) -> control_plane.Project:
        return control_plane.Project("equity", "Equity research", "customer-360")

    def decide(self, identity, action, term):  # tests/test_governed_changes.py covers this
        return control_plane.Decision("permitted", "permitted by rule 'all'", 1)

    def record(self, token, entry):
        pass

    def project_ontology(self, identity: control_plane.Identity) -> dict | None:
        return {"ontology_id": "fibo-equity", "version": "working",
                "classes": [{"uri": "https://ex.org/Equity", "label": "Equity", "imported": False}],
                "properties": []}


@pytest.fixture
def governed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("CONTROL_PLANE_URL", "http://cp.test/api")
    monkeypatch.setenv("R2G_TENANT", "local")
    monkeypatch.setenv("R2G_API_TOKEN", "r2g-own-token")
    monkeypatch.delenv("AOE_URL", raising=False)
    control_plane.set_instance(FakeControlPlane())
    catalog = CatalogManager(tmp_path)
    catalog.add_source("src", "csv", str(tmp_path))
    for name in ("customer-360", "someone-elses"):
        (tmp_path / f"{name}.yaml").write_text("collections: {}\nedges: []\n")
        catalog.create_project(name, "src", str(tmp_path / f"{name}.yaml"))
    yield TestClient(create_app(catalog_dir=str(tmp_path), host="0.0.0.0"))
    control_plane.set_instance(None)


def bearer(token: str = TOKEN) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


class TestTheControlPlanesOwnReads:
    """P14.14: the control plane reads a project's mapping with its service token, to join
    the project ontology to where its data lives, and reads nothing else with it."""

    def test_its_service_token_reads_a_projects_mapping(self, governed):
        # Past the gate: the route answers (400, the fixture's source has no snapshot),
        # where a forged token is refused at the gate.
        for name in ("customer-360", "someone-elses"):
            path = f"/api/projects/{name}/graph-data"
            assert governed.get(path, headers=bearer(SERVICE)).status_code == 400
            assert governed.get(path, headers=bearer("forged")).status_code == 401

    def test_its_service_token_reads_nothing_else(self, governed):
        for path in ("/api/projects", "/api/sources", "/api/projects/customer-360",
                     "/api/projects/customer-360/graph-data/extra"):
            assert governed.get(path, headers=bearer(SERVICE)).status_code == 401, path
        r = governed.post("/api/projects/customer-360/graph-data", headers=bearer(SERVICE))
        assert r.status_code == 401

    def test_a_users_context_still_reads_only_the_project_it_opens(self, governed):
        assert governed.get("/api/projects/customer-360/graph-data",
                            headers=bearer()).status_code == 400
        assert governed.get("/api/projects/someone-elses/graph-data",
                            headers=bearer()).status_code == 404

    def test_an_unreachable_control_plane_is_503(self, governed):
        r = governed.get("/api/projects/customer-360/graph-data",
                         headers=bearer(SERVICE_UNAVAILABLE))
        assert r.status_code == 503


class TestMode:
    def test_the_page_learns_the_mode_before_it_has_a_context(self, governed):
        assert governed.get("/api/mode").json() == {"control_plane": True, "home": "/"}

    def test_without_a_control_plane_nothing_changes(self, tmp_path, monkeypatch):
        monkeypatch.delenv("CONTROL_PLANE_URL", raising=False)
        client = TestClient(create_app(catalog_dir=str(tmp_path)))
        assert client.get("/api/mode").json() == {"control_plane": False, "home": None}
        assert client.get("/api/projects").status_code == 200       # loopback stays open
        assert client.get("/api/control-plane/context").status_code == 404


class TestTheContextIsTheOnlyCredential:
    def test_a_context_opens_the_api(self, governed):
        assert governed.get("/api/projects", headers=bearer()).status_code == 200

    def test_no_token_or_a_bad_one_is_401(self, governed):
        assert governed.get("/api/projects").status_code == 401
        assert governed.get("/api/projects", headers=bearer("forged")).status_code == 401

    def test_r2gs_own_token_no_longer_works(self, governed):
        r = governed.get("/api/projects", headers=bearer("r2g-own-token"))
        assert r.status_code == 401

    def test_an_unreachable_control_plane_is_503_never_a_way_in(self, governed):
        assert governed.get("/api/projects", headers=bearer(UNAVAILABLE)).status_code == 503

    def test_another_tenants_context_is_refused(self, governed):
        r = governed.get("/api/projects", headers=bearer(OTHER_TENANT))
        assert r.status_code == 403 and "local" in r.text

    def test_the_progress_stream_takes_the_context_as_a_query_parameter(self, governed):
        # An EventSource cannot send a header. Positive control: with ?token= the request
        # reaches the route (404: no such load), without it the middleware refuses it.
        path = "/api/projects/customer-360/load/nope/stream"
        assert governed.get(path).status_code == 401
        assert governed.get(f"{path}?token={TOKEN}").status_code == 404

    def test_the_open_paths_need_nothing(self, governed):
        assert governed.get("/api/health").status_code == 200
        assert governed.get("/").status_code == 200


class TestTheCheckSeesTheRoutersPath:
    """The check decides on the path the router matches, so a prefix cannot step past it."""

    def test_a_root_path_does_not_open_the_api(self, governed):
        # Mounted with a root_path, uvicorn puts the prefix in front of the path and the
        # router strips it; the check must strip it the same way.
        mounted = TestClient(governed.app, root_path="/r2g")
        assert mounted.get("/r2g/api/projects/someone-elses").status_code == 401
        assert mounted.get("/r2g/api/projects/someone-elses", headers=bearer()).status_code == 404
        assert mounted.get("/r2g/api/projects/customer-360", headers=bearer()).status_code == 200
        assert mounted.get("/r2g/").status_code == 200
        assert mounted.get("/r2g/static/r2g-icon.png").status_code == 200

    def test_routes_outside_the_api_are_closed_too(self, governed):
        # Only the page, its assets and the two start-up calls are open; the rest needs a
        # context, so a route added later is closed until someone opens it on purpose.
        assert governed.get("/openapi.json").status_code == 401
        assert governed.get("/openapi.json", headers=bearer()).status_code == 200
        assert governed.get("/static/r2g-icon.png").status_code == 200


@pytest.mark.parametrize("query, kept", [
    (f"?token={TOKEN}&n=1", "token=<redacted>&n=1"),
    (f"?n=1&%74oken={TOKEN}", "%74oken=<redacted>"),     # the key, percent-encoded
    (f"?x=1;token={TOKEN}", "token=<redacted>"),          # after a semicolon
])
def test_the_access_log_does_not_keep_the_stream_token(governed, caplog, query, kept):
    # uvicorn logs the request line with its query; ?token= is a credential.
    with caplog.at_level(logging.INFO, logger="uvicorn.access"):
        logging.getLogger("uvicorn.access").info(
            '%s - "%s %s HTTP/%s" %d', "127.0.0.1:5000", "GET",
            f"/api/projects/customer-360/load/x/stream{query}", "1.1", 200)
    line = caplog.records[-1].getMessage()
    assert TOKEN not in line and kept in line


def test_only_the_progress_stream_takes_the_token_in_its_address(governed):
    # A route whose last segment merely reads "stream" is not the progress stream.
    assert governed.get(f"/api/sources/src/preview/stream?token={TOKEN}").status_code == 401
    assert governed.get(f"/api/targets/stream?token={TOKEN}").status_code == 401


def test_options_is_checked_unless_it_is_a_cors_preflight(governed, tmp_path, monkeypatch):
    preflight = {"Origin": "http://x.test", "Access-Control-Request-Method": "GET"}
    assert governed.options("/api/projects").status_code == 401
    # Without CORS configured there is no preflight to answer: checked like the rest.
    assert governed.options("/api/projects", headers=preflight).status_code == 401
    monkeypatch.setenv("R2G_CORS_ORIGINS", "http://x.test")
    with_cors = TestClient(create_app(catalog_dir=str(tmp_path), host="0.0.0.0"))
    assert with_cors.options("/api/projects", headers=preflight).status_code == 200
    no_origin = {"Access-Control-Request-Method": "GET"}
    assert with_cors.options("/api/projects", headers=no_origin).status_code == 401


class TestALoadBelongsToItsProject:
    """A load is reached only through the project that started it (P14.3), and its
    rejected rows are kept with the tenant's catalog (P14.6)."""

    def _load(self, project: str):
        q = MagicMock(empty=MagicMock(return_value=True))
        return {"thread": MagicMock(is_alive=MagicMock(return_value=True)), "queue": q,
                "pipeline": None, "project": project}

    def test_another_projects_load_is_not_found(self, governed, monkeypatch):
        from r2g.ui import server

        monkeypatch.setitem(server._running_loads, "theirs", self._load("someone-elses"))
        monkeypatch.setitem(server._running_loads, "ours", self._load("customer-360"))
        base = "/api/projects/customer-360/load"
        assert governed.get(f"{base}/theirs/status", headers=bearer()).json()["status"] == "unknown"
        assert governed.get(f"{base}/theirs/stream", headers=bearer()).status_code == 404
        assert governed.get(f"{base}/ours/status", headers=bearer()).json()["status"] == "running"

    def test_rejected_rows_are_kept_per_project_with_the_catalog(self, governed, tmp_path):
        from r2g.dlq import DeadLetterQueue

        DeadLetterQueue("theirs", dlq_dir=tmp_path / "dlq" / "someone-elses").record_failure(
            "c", {"email": "x@y"}, "bad")
        DeadLetterQueue("theirs", dlq_dir=tmp_path / "dlq").record_failure(  # the older flat path
            "c", {"email": "x@y"}, "bad")
        DeadLetterQueue("ours", dlq_dir=tmp_path / "dlq" / "customer-360").record_failure(
            "c", {"email": "x@y"}, "bad")
        base = "/api/projects/customer-360/load"
        assert governed.get(f"{base}/theirs/errors", headers=bearer()).json()["count"] == 0
        assert governed.get(f"{base}/ours/errors", headers=bearer()).json()["count"] == 1

    def test_without_a_control_plane_older_rejected_rows_still_show(self, tmp_path, monkeypatch):
        from r2g.dlq import DeadLetterQueue

        monkeypatch.delenv("CONTROL_PLANE_URL", raising=False)
        catalog = CatalogManager(tmp_path)
        catalog.add_source("src", "csv", str(tmp_path))
        (tmp_path / "p.yaml").write_text("collections: {}\nedges: []\n")
        catalog.create_project("p", "src", str(tmp_path / "p.yaml"))
        home = tmp_path / "home"
        monkeypatch.setenv("HOME", str(home))  # older loads wrote ~/.r2g/dlq, whatever the catalog
        DeadLetterQueue("old", dlq_dir=home / ".r2g" / "dlq").record_failure("c", {"a": 1}, "bad")
        client = TestClient(create_app(catalog_dir=str(tmp_path)))
        assert client.get("/api/projects/p/load/old/errors").json()["count"] == 1
        assert client.get("/api/projects/nope/load/old/errors").status_code == 404


class TestTheControlPlanesRefusalIsARefusal:
    """A context the control plane refuses when asked for its project is a 401, as a bad
    signature is, not a 503 the page would wait out."""

    @pytest.fixture
    def sdk(self, monkeypatch):
        mod = types.ModuleType("control_plane_sdk")

        class ControlPlaneError(Exception):
            pass

        for name in ("TokenRejected", "ProjectNotFound", "ContextError", "Unavailable"):
            setattr(mod, name, type(name, (ControlPlaneError,), {}))
        mod.ControlPlaneError = ControlPlaneError
        mod.ControlPlane = MagicMock()
        monkeypatch.setitem(sys.modules, "control_plane_sdk", mod)
        return mod

    def _identity(self):
        return control_plane.Identity("pj", (), "local", "equity", None,
                                      verified=MagicMock(token_id="jti-1"))

    @pytest.mark.parametrize("raised, expected", [
        ("TokenRejected", control_plane.ContextRejected),
        ("ProjectNotFound", control_plane.ContextRejected),
        ("ControlPlaneError", control_plane.ContextUnavailable),
    ])
    def test_each_failure_maps_to_its_answer(self, sdk, raised, expected):
        gateway = control_plane._Sdk("http://cp.test/api")
        gateway.client.project.side_effect = getattr(sdk, raised)("no")
        with pytest.raises(expected):
            gateway.project(self._identity())


def test_a_load_writes_its_rejected_rows_where_the_errors_route_reads_them():
    # Starting a real load needs a source, a snapshot and ArangoDB; instead, the load and
    # the errors route must both get their queue from the one helper that decides where.
    source = (STATIC.parent / "server.py").read_text()
    start = source[source.index("async def start_load"):source.index("async def get_load_status")]
    assert "dlq=_dead_letters(name, load_id)" in start
    assert "DeadLetterQueue(" not in start


class TestOnlyTheProjectsTransformationProject:
    def test_the_list_holds_only_the_one_it_opens(self, governed):
        names = [p["name"] for p in governed.get("/api/projects", headers=bearer()).json()]
        assert names == ["customer-360"]

    def test_another_one_does_not_exist(self, governed):
        assert governed.get("/api/projects/customer-360", headers=bearer()).status_code == 200
        r = governed.get("/api/projects/someone-elses", headers=bearer())
        assert r.status_code == 404
        r = governed.get("/api/projects/someone%2Delses/graph-data", headers=bearer())
        assert r.status_code == 404


class TestTheContextItself:
    def test_says_who_where_and_what_to_open(self, governed):
        ctx = governed.get("/api/control-plane/context", headers=bearer()).json()
        assert ctx["principal"] == "pj@example.com"
        assert ctx["project"] == {"id": "equity", "name": "Equity research"}
        assert ctx["opens"] == "customer-360"
        assert ctx["ontology"]["ontology_id"] == "fibo-equity"
        assert ctx["ontology"]["problem"] is None               # read from the control plane
        assert [c["label"] for c in ctx["ontology"]["classes"]] == ["Equity"]


class TestTheOntologyFromTheControlPlane:
    """The project ontology comes from the control plane, read with r2g's own context: AOE
    refuses a context issued for another tool, so r2g does not read AOE itself."""

    IDENTITY = control_plane.Identity("pj@example.com", ("analyst",), "local", "equity",
                                      ("fibo-equity", "working"), token=TOKEN)

    class Serving(FakeControlPlane):
        def __init__(self, answer: Any) -> None:
            self.answer, self.asked = answer, []

        def project_ontology(self, identity):
            self.asked.append(identity)
            if isinstance(self.answer, Exception):
                raise self.answer
            return self.answer

    def test_classes_and_properties_are_read_with_r2gs_own_context(self):
        cp = self.Serving({"classes": [{"uri": "https://ex.org/Income", "label": "Income"},
                                       {"uri": "https://ex.org/Equity", "label": "equity"}],
                           "properties": [{"uri": "https://ex.org/amount", "label": "amount"}]})
        control_plane.set_instance(cp)
        try:
            out = control_plane.ontology_terms(self.IDENTITY)
        finally:
            control_plane.set_instance(None)
        assert [c["label"] for c in out["classes"]] == ["equity", "Income"]
        assert [p["uri"] for p in out["properties"]] == ["https://ex.org/amount"]
        assert out["problem"] is None and cp.asked == [self.IDENTITY]

    @pytest.mark.parametrize("answer, says", [
        (control_plane.ContextUnavailable("control plane answered 503"), "503"),
        (control_plane.ContextRejected("refused"), "refused"),
        (None, "serves no ontology"),
    ])
    def test_a_failure_is_reported_not_raised_with_no_partial_list(self, answer, says):
        control_plane.set_instance(self.Serving(answer))
        try:
            out = control_plane.ontology_terms(self.IDENTITY)
        finally:
            control_plane.set_instance(None)
        assert out["classes"] == [] and out["properties"] == [] and says in out["problem"]

    def test_an_sdk_that_cannot_read_it_says_so(self, monkeypatch):
        adapter = control_plane._Sdk.__new__(control_plane._Sdk)
        adapter.client = object()  # an SDK from before the control plane served the ontology
        with pytest.raises(control_plane.ContextUnavailable, match="cannot read the project"):
            adapter.project_ontology(self.IDENTITY)


def test_the_catalog_directory_follows_r2g_catalog_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("R2G_CATALOG_DIR", str(tmp_path / "tenant-local"))
    assert CatalogManager().dir == tmp_path / "tenant-local"


def test_the_page_uses_no_root_absolute_addresses():
    """Behind a proxy that mounts r2g at /r2g/, a root-absolute /api or /static address
    misses the prefix. Every address must be relative, or built on API (the mount base).
    The one exception is the control plane's own /api/context, which lives at the root."""
    pattern = re.compile(r"""(?:fetch|EventSource)\(\s*['"`]/|(?:src|href)=["']/(?!/)""")
    offenders = []
    for path in sorted(STATIC.glob("*.htm*")) + sorted(STATIC.glob("*.js")):
        for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            # Exceptions, both the control plane's and at its root on purpose: renewing the
            # context, and the link home.
            if pattern.search(line) and "'/api/context'" not in line \
                    and 'id="cp-gate-home"' not in line:
                offenders.append(f"{path.name}:{n}: {line.strip()}")
    assert offenders == []


def test_the_page_renews_its_context_for_r2g():
    """A context is issued for one tool: the renewal asks for r2g's again, by its own id."""
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    renewal = html[html.index("fetch('/api/context'"):]
    renewal = renewal[: renewal.index("});")]
    assert f"tool: '{control_plane.TOOL_ID}'" in renewal and "project: claims.ctx.project" in renewal


def test_api_fetch_keeps_the_auth_header_when_a_caller_passes_headers():
    """Spreading opts after headers used to let a caller's headers drop the token."""
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    assert "{ ...opts, headers: _authHeaders(opts.headers) }" in html
    assert "{ headers: _authHeaders(opts.headers), ...opts }" not in html




def test_r2g_tenant_is_required_in_this_mode(tmp_path, monkeypatch):
    """Without it every tenant's context would get in, sharing one catalog (P14.6)."""
    monkeypatch.setenv("CONTROL_PLANE_URL", "http://cp.test/api")
    monkeypatch.delenv("R2G_TENANT", raising=False)
    with pytest.raises(RuntimeError, match="R2G_TENANT"):
        create_app(catalog_dir=str(tmp_path))
    monkeypatch.setenv("R2G_TENANT", "local")             # positive control
    create_app(catalog_dir=str(tmp_path))


@pytest.mark.parametrize("policy, refusal", [
    (None, "is not installed"),                                            # no SDK
    (types.ModuleType("control_plane_sdk.policy"), "no optional terms"),   # one before 0.1.0
])
def test_the_studio_does_not_start_on_an_sdk_without_optional_terms(
    tmp_path, monkeypatch, policy, refusal
):
    """An older SDK rejects the policy declaring r2g's actions and then denies every action,
    so the Studio refuses to start rather than refuse every change (P14.13)."""
    monkeypatch.setenv("CONTROL_PLANE_URL", "http://cp.test/api")
    monkeypatch.setenv("R2G_TENANT", "local")
    monkeypatch.setitem(sys.modules, "control_plane_sdk.policy", policy)
    with pytest.raises(RuntimeError, match=refusal):
        create_app(catalog_dir=str(tmp_path))
    # Outside a control plane the SDK is not needed.
    monkeypatch.delenv("CONTROL_PLANE_URL")
    create_app(catalog_dir=str(tmp_path))


def test_the_studio_starts_on_an_sdk_with_optional_terms(tmp_path, monkeypatch):
    monkeypatch.setenv("CONTROL_PLANE_URL", "http://cp.test/api")
    monkeypatch.setenv("R2G_TENANT", "local")
    policy = types.ModuleType("control_plane_sdk.policy")
    policy.TARGET_KINDS = frozenset({"ontology-term", "optional-ontology-term"})  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "control_plane_sdk.policy", policy)
    assert control_plane.sdk_problem() is None
    create_app(catalog_dir=str(tmp_path))


class TestCreatingIsScopedToo:
    """POST /api/projects and the demo preset have no name in the path, so the
    middleware's route check cannot see them (P14.3)."""

    def test_creating_another_project_is_not_found(self, governed, tmp_path):
        class OpensANewOne(FakeControlPlane):
            def project(self, identity):
                return control_plane.Project("equity", "Equity research", "not-created-yet")

        control_plane.set_instance(OpensANewOne())
        r = governed.post("/api/projects", headers=bearer(),
                          json={"name": "not-named-here", "source_name": "src"})
        assert r.status_code == 404
        assert CatalogManager(tmp_path).get_project("not-named-here") is None
        # Positive control: the one it opens can be created, on a registered target.
        CatalogManager(tmp_path).add_target("equity-db", database="equity")
        r = governed.post("/api/projects", headers=bearer(),
                          json={"name": "not-created-yet", "source_name": "src",
                                "target_name": "equity-db"})
        assert r.status_code == 201

    def test_a_demo_preset_is_refused_even_for_the_project_opened(self, governed):
        """It adds a source, which every project in the tenant shares (P14.12, which
        supersedes P14.3's preset case under a control plane)."""
        class OpensThePreset(FakeControlPlane):
            def project(self, identity):
                return control_plane.Project("equity", "Equity research",
                                             "snowflake_customer_360")

        control_plane.set_instance(OpensThePreset())
        r = governed.post("/api/demo-presets/snowflake-customer-360/install",
                          headers=bearer(), json={})
        assert r.status_code == 403 and "adds a source" in r.json()["detail"]


class TestTheMcpServerDoesNotRunHere:
    """It checks no context, so it would reach the catalog around the control plane (P14.7)."""

    def test_r2g_mcp_refuses(self, monkeypatch):
        from typer.testing import CliRunner

        from r2g.main import app

        monkeypatch.setenv("CONTROL_PLANE_URL", "http://cp.test/api")
        result = CliRunner().invoke(app, ["mcp"])
        assert result.exit_code == 1 and "CONTROL_PLANE_URL" in result.output

    @pytest.mark.parametrize("url, refused", [("http://cp.test/api", True), ("", False)])
    def test_importing_the_server_refuses_too(self, url, refused):
        """`mcp run` and the like import the module rather than calling `r2g mcp`."""
        pytest.importorskip("mcp")
        env = {**os.environ, "CONTROL_PLANE_URL": url}
        p = subprocess.run([sys.executable, "-c", "import r2g.mcp_server"],
                           env=env, capture_output=True, text=True)
        assert (p.returncode != 0) is refused
        assert ("CONTROL_PLANE_URL is set" in p.stderr) is refused


# --- P14.16: once its project exists, a project sees its own sources, targets, catalogs ---


class OpensOneNotYetCreated(FakeControlPlane):
    def project(self, identity: control_plane.Identity) -> control_plane.Project:
        return control_plane.Project("equity", "Equity research", "new-one")


def _tenant(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fake: FakeControlPlane) -> TestClient:
    """Two projects in one tenant, each with its own source (imported from its own catalog,
    with a snapshot) and its own target."""
    from r2g.types import Schema

    monkeypatch.setenv("CONTROL_PLANE_URL", "http://cp.test/api")
    monkeypatch.setenv("R2G_TENANT", "local")
    monkeypatch.delenv("AOE_URL", raising=False)
    control_plane.set_instance(fake)
    catalog = CatalogManager(tmp_path)
    for name, project in (("crm", "customer-360"), ("hr", "someone-elses")):
        catalog.add_catalog(f"{name}-catalog", "openmetadata", "http://om.test")
        catalog.add_source(name, "csv", str(tmp_path), catalog_name=f"{name}-catalog")
        catalog.create_snapshot(name, Schema())
        catalog.add_target(f"{name}-graph", database=name)
        (tmp_path / f"{project}.yaml").write_text("collections: {}\nedges: []\n")
        catalog.create_project(project, name, str(tmp_path / f"{project}.yaml"))
        catalog.update_project(project, target_name=f"{name}-graph")
    return TestClient(create_app(catalog_dir=str(tmp_path), host="0.0.0.0"))


@pytest.fixture
def tenant(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    yield _tenant(tmp_path, monkeypatch, FakeControlPlane())
    control_plane.set_instance(None)


@pytest.fixture
def before_its_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TestClient:
    yield _tenant(tmp_path, monkeypatch, OpensOneNotYetCreated())
    control_plane.set_instance(None)


def _names(client: TestClient, path: str) -> list[str]:
    r = client.get(path, headers=bearer())
    assert r.status_code == 200, r.text
    return sorted(item["name"] for item in r.json())


def test_the_lists_show_only_the_projects_own(tenant):
    assert _names(tenant, "/api/sources") == ["crm"]
    assert _names(tenant, "/api/targets") == ["crm-graph"]
    assert _names(tenant, "/api/catalogs") == ["crm-catalog"]


@pytest.mark.parametrize("method, path", [
    ("GET", "/api/sources/{source}/schema"),
    ("POST", "/api/sources/{source}/infer-fks"),
    ("POST", "/api/sources/{source}/analyze-denorm"),
    ("GET", "/api/sources/{source}/preview/t"),
    ("DELETE", "/api/sources/{source}"),
    ("POST", "/api/targets/{target}/introspect"),
    ("DELETE", "/api/targets/{target}"),
    ("GET", "/api/catalogs/{catalog}/browse"),
    ("DELETE", "/api/catalogs/{catalog}"),
])
def test_another_projects_is_not_found_like_one_the_tenant_lacks(tenant, method, path):
    def ask(name: str):
        url = path.format(source=name, target=name, catalog=name)
        return tenant.request(method, url, headers=bearer(),
                              json=None if method in ("GET", "DELETE") else {})

    theirs = ask({"source": "hr", "target": "hr-graph", "catalog": "hr-catalog"}[
        path.split("/")[2][:-1]])
    nobodys = ask("no-such-name")
    assert theirs.status_code == nobodys.status_code == 404
    assert theirs.json() == nobodys.json()


def test_its_own_are_served(tenant):
    assert tenant.get("/api/sources/crm/schema", headers=bearer()).status_code == 200
    r = tenant.post("/api/sources/crm/infer-fks", headers=bearer(), json={})
    assert r.status_code == 200, r.text


def test_shared_key_inference_reads_only_the_projects_sources(tenant):
    # The project sees one source, so there is nothing to infer across; the other
    # project's source is not counted, whether asked for by name or not.
    for sources in (None, ["crm", "hr"]):
        r = tenant.post("/api/shared-keys/infer", headers=bearer(),
                        json={} if sources is None else {"sources": sources})
        assert r.status_code == 400 and "got 1" in r.json()["detail"], r.text
    theirs = tenant.post("/api/shared-keys/infer", headers=bearer(),
                         json={"sources": ["crm", "hr"]})
    nobodys = tenant.post("/api/shared-keys/infer", headers=bearer(),
                          json={"sources": ["crm", "no-such-name"]})
    assert theirs.json() == nobodys.json()


def test_shared_key_inference_names_no_other_projects_source(tenant, tmp_path, monkeypatch):
    # A project that sees two sources (one r2g project has one; the control plane may
    # attach more): inferring across them by default neither reads nor names the others.
    from r2g.types import Schema
    from r2g.ui import server

    catalog = CatalogManager(tmp_path)
    catalog.add_source("crm2", "csv", str(tmp_path))
    catalog.create_snapshot("crm2", Schema())
    real = server._what_it_sees
    monkeypatch.setattr(server, "_what_it_sees", lambda c, opens: {
        **real(c, opens), "sources": {"crm", "crm2"}})
    r = tenant.post("/api/shared-keys/infer", headers=bearer(), json={})
    assert r.status_code == 200, r.text
    assert r.json()["skipped"] == []


def test_before_its_project_exists_the_tenants_sources_are_offered(before_its_project, tmp_path):
    client = before_its_project
    assert _names(client, "/api/sources") == ["crm", "hr"]
    assert client.get("/api/sources/hr/schema", headers=bearer()).status_code == 200
    # Creating it narrows what it sees to its own from then on. It is created on a target
    # an administrator registered and no other project holds (P14.18).
    CatalogManager(tmp_path).add_target("new-graph", database="new_one")
    r = client.post("/api/projects", headers=bearer(),
                    json={"name": "new-one", "source_name": "crm", "target_name": "new-graph"})
    assert r.status_code == 201, r.text
    assert _names(client, "/api/sources") == ["crm"]
    assert _names(client, "/api/targets") == ["new-graph"]
    assert client.get("/api/sources/hr/schema", headers=bearer()).status_code == 404


def test_before_its_project_exists_only_targets_it_could_be_bound_to_are_offered(
        before_its_project, tmp_path):
    """Every target here is another project's, bound or registered for it (P14.18), so
    none is listed, nor read with its stored credentials, before the project exists."""
    client = before_its_project
    CatalogManager(tmp_path).add_target("hr-only", database="hr2", project="someone-elses")
    CatalogManager(tmp_path).add_target("free-graph", database="free")
    assert _names(client, "/api/targets") == ["free-graph"]
    for target in ("hr-graph", "hr-only"):
        r = client.post(f"/api/targets/{target}/introspect", headers=bearer())
        assert r.status_code == 404, r.text


class OpensNone(FakeControlPlane):
    def project(self, identity: control_plane.Identity) -> control_plane.Project:
        return control_plane.Project("equity", "Equity research", None)


def test_a_context_that_opens_no_project_sees_none_of_the_tenants(tmp_path, monkeypatch):
    """A control-plane project that names no transformation project has none of its
    own, so it sees none of the tenant's: nothing listed, nothing read with another
    project's stored credentials."""
    client = _tenant(tmp_path, monkeypatch, OpensNone())
    try:
        for kind in ("sources", "targets", "catalogs"):
            assert _names(client, f"/api/{kind}") == []
        assert client.get("/api/sources/hr/schema", headers=bearer()).status_code == 404
        r = client.post("/api/targets/hr-graph/introspect", headers=bearer())
        assert r.status_code == 404, r.text
    finally:
        control_plane.set_instance(None)


def test_without_a_control_plane_every_project_sees_the_tenants(tmp_path, monkeypatch):
    client = _tenant(tmp_path, monkeypatch, FakeControlPlane())
    monkeypatch.delenv("CONTROL_PLANE_URL")
    control_plane.set_instance(None)
    client = TestClient(create_app(catalog_dir=str(tmp_path)))
    assert _names(client, "/api/sources") == ["crm", "hr"]
    assert _names(client, "/api/targets") == ["crm-graph", "hr-graph"]
    assert client.get("/api/sources/hr/schema").status_code == 200


class TestATargetIsBoundAtCreation:
    """Under a control plane a project's load target is set when the project is created,
    from the targets an administrator registered, and no request changes it. Before this,
    a user scoped to one project could point its loads at another project's target (with
    that target's credentials), or at a host of their choosing (with the server's own
    ``ARANGO_*`` credentials), and could turn the sensitive-field gate off."""

    @pytest.fixture
    def loads(self, tmp_path, monkeypatch):
        """What a load would write to, captured instead of connecting."""
        from r2g.types import Column, Schema, Table
        from r2g.ui import server

        monkeypatch.setenv("ARANGO_USER", "server-user")
        monkeypatch.setenv("ARANGO_PASSWORD", "server-env-secret")
        catalog = CatalogManager(tmp_path)
        (tmp_path / "t.csv").write_text("id,name\n1,a\n")
        catalog.create_snapshot("src", Schema(tables={"t": Table(name="t", columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="name", data_type="text", is_nullable=True)], primary_key=["id"])}))
        catalog.add_target("tgt-a", endpoint="http://arango-a:8529", database="graph_a",
                           username="a", password="pw-a")
        catalog.add_target("tgt-b", endpoint="http://arango-b:8529", database="graph_b",
                           username="b", password="pw-b")
        for name, tgt in (("customer-360", "tgt-a"), ("someone-elses", "tgt-b")):
            (tmp_path / f"{name}.yaml").write_text(
                "collections:\n  t:\n    source_table: t\n    target_collection: t\nedges: []\n")
            catalog.update_project(name, target_name=tgt)
        seen: dict[str, Any] = {}

        class Writer:
            def __init__(self, **kw):
                seen["writer"] = kw

        class Pipeline:
            def __init__(self, **kw):
                self.errors: dict = {}

            def run(self, **kw):
                return {"documents": [], "edges": []}

        monkeypatch.setattr(server, "ArangoWriter", Writer)
        monkeypatch.setattr(server, "StreamingPipeline", Pipeline)
        monkeypatch.setattr(server, "_graph_state", lambda *a: seen.setdefault("read", a) and None)
        return seen

    def test_a_request_cannot_relink_the_project_to_another_target(self, governed, loads,
                                                                     tmp_path):
        r = governed.patch("/api/projects/customer-360", headers=bearer(),
                           json={"target_name": "tgt-b"})
        assert r.status_code == 403, r.text
        assert "set when the project is created" in r.json()["detail"]
        assert CatalogManager(tmp_path).get_project("customer-360").target_name == "tgt-a"
        r = governed.post("/api/projects/customer-360/load", headers=bearer(), json={})
        assert r.status_code == 202, r.text
        assert loads["writer"]["endpoint"] == "http://arango-a:8529"
        assert loads["writer"]["password"] == "pw-a"
        # Positive control: other details still change.
        r = governed.patch("/api/projects/customer-360", headers=bearer(),
                           json={"mapping_name": "renamed"})
        assert r.status_code == 200, r.text

    def test_a_project_bound_to_no_registered_target_does_not_load(self, governed, loads,
                                                                   tmp_path):
        """No fallback to the server's credentials: they are never sent where a request
        pointed them."""
        CatalogManager(tmp_path).update_project(
            "customer-360", target_name="", arango_endpoint="http://attacker.example:8529",
            arango_database="loot")
        for dry_run in (True, False):
            r = governed.post("/api/projects/customer-360/load", headers=bearer(),
                              json={"dry_run": dry_run})
            assert r.status_code == 403, r.text
            assert "registered target" in r.json()["detail"]
        assert "writer" not in loads and "read" not in loads
        assert CatalogManager(tmp_path).get_history("customer-360") == []

    def test_creating_binds_only_a_registered_target_no_other_project_holds(
        self, governed, loads, tmp_path,
    ):
        catalog = CatalogManager(tmp_path)
        catalog.delete_project("customer-360")
        create = {"name": "customer-360", "source_name": "src"}
        refusals = [
            dict(create, arango_endpoint="http://attacker.example:8529", arango_database="loot"),
            dict(create, target_name="tgt-b"),  # someone-elses's, by name
            dict(create, arango_endpoint="http://arango-b:8529", arango_database="graph_b"),
        ]
        for body in refusals:
            r = governed.post("/api/projects", headers=bearer(), json=body)
            assert r.status_code == 403, (body, r.text)
            assert CatalogManager(tmp_path).get_project("customer-360") is None
        r = governed.post("/api/projects", headers=bearer(), json=dict(create, target_name="nope"))
        assert r.status_code == 400
        # Positive control: a registered target no other project holds, by its address.
        r = governed.post("/api/projects", headers=bearer(),
                          json=dict(create, arango_endpoint="http://arango-a:8529/",
                                    arango_database="graph_a"))
        assert r.status_code == 201, r.text
        assert r.json()["target_name"] == "tgt-a"

    @pytest.mark.parametrize("body", [{"allow_sensitive": True},
                                      {"sensitivity_threshold": "restricted"}])
    def test_a_load_may_not_loosen_the_sensitive_field_gate(self, governed, loads, body):
        r = governed.post("/api/projects/customer-360/load", headers=bearer(), json=body)
        assert r.status_code == 403, r.text
        assert "stricter but not looser" in r.json()["detail"]
        assert "writer" not in loads

    def test_a_load_may_make_the_gate_stricter(self, governed, loads):
        r = governed.post("/api/projects/customer-360/load", headers=bearer(),
                          json={"sensitivity_threshold": "internal"})
        assert r.status_code == 202, r.text

    def test_a_target_registered_for_another_project_is_neither_bound_nor_loaded(
        self, governed, loads, tmp_path,
    ):
        """``r2g target add --project``: the first project created does not get to take
        a target an administrator registered for another one."""
        catalog = CatalogManager(tmp_path)
        catalog.add_target("tgt-c", endpoint="http://arango-c:8529", database="graph_c",
                           username="c", password="pw-c", project="someone-elses")
        catalog.delete_project("customer-360")
        r = governed.post("/api/projects", headers=bearer(),
                          json={"name": "customer-360", "source_name": "src",
                                "target_name": "tgt-c"})
        assert r.status_code == 403, r.text
        assert CatalogManager(tmp_path).get_project("customer-360") is None
        # Nor does a project already bound to it, before it was reserved, load there.
        (tmp_path / "customer-360.yaml").write_text(
            "collections:\n  t:\n    source_table: t\n    target_collection: t\nedges: []\n")
        catalog.create_project("customer-360", "src", str(tmp_path / "customer-360.yaml"))
        catalog.update_project("customer-360", target_name="tgt-c")
        r = governed.post("/api/projects/customer-360/load", headers=bearer(), json={})
        assert r.status_code == 403, r.text
        assert "registered for another project" in r.json()["detail"]
        assert "writer" not in loads
        # Positive control: the project it is registered for is created on it.
        catalog.delete_project("customer-360")
        catalog.update_project("someone-elses", target_name="")
        catalog.add_target("tgt-d", endpoint="http://arango-d:8529", database="graph_d",
                           project="customer-360")
        r = governed.post("/api/projects", headers=bearer(),
                          json={"name": "customer-360", "source_name": "src",
                                "target_name": "tgt-d"})
        assert r.status_code == 201, r.text

    def test_a_target_bound_while_the_creation_waited_is_refused(self, governed, loads,
                                                                 tmp_path):
        """Two creations at once: the target is checked again after the control plane
        answers, so the second does not bind a target the first just took."""
        catalog = CatalogManager(tmp_path)
        catalog.delete_project("customer-360")
        catalog.update_project("someone-elses", target_name="")  # tgt-b is free

        class Meanwhile(FakeControlPlane):
            def decide(self, identity, action, term):
                CatalogManager(tmp_path).update_project("someone-elses", target_name="tgt-b")
                return super().decide(identity, action, term)

        control_plane.set_instance(Meanwhile())
        r = governed.post("/api/projects", headers=bearer(),
                          json={"name": "customer-360", "source_name": "src",
                                "target_name": "tgt-b"})
        assert r.status_code == 403, r.text
        assert CatalogManager(tmp_path).get_project("customer-360") is None


class TestTheGateHoldsUnderAControlPlane:
    """The sensitive-field gate is the control plane's: no mapping and no route gets a
    field it rates sensitive past it. A migration reloaded a gated load's collections
    without it; a mask marker in a field's description passed an unmasked field; and an
    expression loaded what it computed from a column the gate had excluded."""

    PLAIN = "collections:\n  t:\n    source_table: t\n    target_collection: t\nedges: []\n"
    ROW = {"id": 1, "name": "a", "ssn": "123-45-6789"}

    @pytest.fixture
    def setup(self, tmp_path, monkeypatch):
        import r2g.streaming.pipeline as sp
        from r2g.types import Classification, Column, Schema, Table
        from r2g.ui import server

        catalog = CatalogManager(tmp_path)
        catalog.create_snapshot("src", Schema(tables={"t": Table(name="t", columns=[
            Column(name="id", data_type="integer", is_nullable=False, is_primary_key=True),
            Column(name="name", data_type="text", is_nullable=True),
            Column(name="ssn", data_type="text", is_nullable=True,
                   classification=Classification(tags=["PII.Sensitive"])),
        ], primary_key=["id"])}))
        catalog.add_target("tgt-a", endpoint="http://arango-a:8529", database="graph_a",
                           username="a", password="pw-a")
        catalog.update_project("customer-360", target_name="tgt-a")
        configs: list[Any] = []

        class Writer:
            def __init__(self, **kw):
                pass

        class Pipeline:
            def __init__(self, **kw):
                configs.append(kw["config"])
                self.errors: dict = {}

            def run(self, **kw):
                return {"documents": [("t", 1)], "edges": []}

        monkeypatch.setattr(server, "ArangoWriter", Writer)
        monkeypatch.setattr(server, "StreamingPipeline", Pipeline)
        monkeypatch.setattr(sp, "StreamingPipeline", Pipeline)
        monkeypatch.setattr(server, "_graph_state", lambda *a: None)
        return tmp_path, configs

    def _document(self, tmp_path, config) -> dict:
        from r2g.transformers.node_transformer import NodeTransformer

        table = CatalogManager(tmp_path).get_latest_snapshot("src").schema_data.tables["t"]
        return NodeTransformer(table, config.collections["t"]).transform_row(self.ROW)

    @pytest.mark.parametrize("field", [
        "{target: ssn, sources: [ssn], expression: '\"***\"', description: 'r2g:mask:redact'}"
        "\n      - {target: ssn, sources: [ssn], expression: '@ssn'}",
        "{target: ssn, sources: [ssn], expression: '', description: 'r2g:mask:hash'}",
        "{target: contact, sources: [name], expression: '@ssn'}",
        "{target: contact, sources: [ssn], expression: '@ssn'}",
        "{target: contact, sources: [name, ssn], expression: 'CONCAT(@name, @ssn)'}",
    ])
    def test_no_field_expression_loads_a_sensitive_column(self, governed, setup, field):
        tmp_path, configs = setup
        (tmp_path / "customer-360.yaml").write_text(
            self.PLAIN.replace("edges", f"    field_expressions:\n      - {field}\nedges"))
        r = governed.post("/api/projects/customer-360/load", headers=bearer(), json={})
        assert r.status_code == 202, r.text
        assert "123-45-6789" not in str(self._document(tmp_path, configs[-1]))

    def test_a_second_collection_on_the_table_is_refused(self, governed, setup):
        """A load writes one collection per source table. A decoy listed first took the
        gate's exclusion, and a migration asked the policy about the decoy while it
        rewrote the other; such a mapping is refused before either."""
        tmp_path, configs = setup
        (tmp_path / "customer-360.yaml").write_text(self.PLAIN)
        self._loaded(governed, tmp_path)
        (tmp_path / "customer-360.yaml").write_text(
            "collections:\n  decoy:\n    source_table: t\n    target_collection: decoy\n"
            + self.PLAIN.removeprefix("collections:\n"))
        loads = len(configs)
        for route in ("load", "migrate"):
            r = governed.post(f"/api/projects/customer-360/{route}", headers=bearer(), json={})
            assert r.status_code == 400, (route, r.text)
            assert "more than one document collection" in str(r.json()["detail"])
        assert len(configs) == loads

    def test_a_sensitive_key_is_refused(self, governed, setup):
        """A key is built from the row's values, so a sensitive primary key reaches the
        graph whatever the gate excludes; such a load or migration is refused."""
        tmp_path, configs = setup
        (tmp_path / "customer-360.yaml").write_text(self.PLAIN)
        self._loaded(governed, tmp_path)  # keyed by id: loads
        catalog = CatalogManager(tmp_path)
        snap = catalog.get_latest_snapshot("src")
        table = snap.schema_data.tables["t"]
        table.primary_key = ["ssn"]
        catalog.create_snapshot("src", snap.schema_data)
        loads = len(configs)
        for route in ("load", "migrate"):
            r = governed.post(f"/api/projects/customer-360/{route}", headers=bearer(), json={})
            assert r.status_code == 403, (route, r.text)
            assert "t.ssn" in r.json()["detail"] and "key overlay" in r.json()["detail"]
        assert len(configs) == loads
        # An empty include list is every table to the pipeline, so it is to the check.
        r = governed.post("/api/projects/customer-360/load", headers=bearer(),
                          json={"include_tables": []})
        assert r.status_code == 403, r.text
        assert len(configs) == loads
        # A load that leaves the table out writes no such key.
        r = governed.post("/api/projects/customer-360/load", headers=bearer(),
                          json={"exclude_tables": ["t"]})
        assert r.status_code == 202, r.text

    def test_a_mask_still_loads(self, governed, setup):
        tmp_path, configs = setup
        (tmp_path / "customer-360.yaml").write_text(self.PLAIN.replace(
            "edges", "    field_expressions:\n      - {target: ssn, sources: [ssn], "
            "expression: '\"***\"', description: 'r2g:mask:redact'}\nedges"))
        r = governed.post("/api/projects/customer-360/load", headers=bearer(), json={})
        assert r.status_code == 202, r.text
        assert self._document(tmp_path, configs[-1])["ssn"] == "***"

    def test_a_migration_reloads_nothing_the_gate_excluded(self, governed, setup):
        tmp_path, configs = setup
        (tmp_path / "customer-360.yaml").write_text(self.PLAIN)
        self._loaded(governed, tmp_path)
        # No edit since the load: nothing to migrate.
        r = governed.post("/api/projects/customer-360/migrate", headers=bearer(), json={})
        assert r.status_code == 200, r.text
        assert r.json()["report"]["actions_executed"] == []
        # An edit: what is reloaded is gated.
        (tmp_path / "customer-360.yaml").write_text(
            self.PLAIN.replace("edges", "    field_mappings: {name: full_name}\nedges"))
        r = governed.post("/api/projects/customer-360/migrate", headers=bearer(), json={})
        assert r.status_code == 200, r.text
        assert all("123-45-6789" not in str(self._document(tmp_path, c)) for c in configs)

    def _loaded(self, governed, tmp_path):
        r = governed.post("/api/projects/customer-360/load", headers=bearer(), json={})
        assert r.status_code == 202, r.text
        for _ in range(50):
            if CatalogManager(tmp_path).get_project("customer-360").loaded_mapping:
                return
            time.sleep(0.05)
        raise AssertionError("the load never recorded its mapping")

    def test_a_migration_checks_the_mapping_as_a_load_does(self, governed, setup):
        """An expression that cannot run here would go to ArangoDB as AQL; ``row.ssn``
        reads the column with no ``@ssn`` for the gate to see."""
        tmp_path, configs = setup
        (tmp_path / "customer-360.yaml").write_text(self.PLAIN)
        self._loaded(governed, tmp_path)
        (tmp_path / "customer-360.yaml").write_text(self.PLAIN.replace(
            "edges", "    field_expressions:\n      - {target: leak, expression: 'row.ssn'}\nedges"))
        loads = len(configs)
        r = governed.post("/api/projects/customer-360/migrate", headers=bearer(), json={})
        assert r.status_code == 400, r.text
        assert "validation_errors" in r.json()["detail"]
        assert len(configs) == loads

    def test_the_migration_plan_plans_what_the_migration_does(self, governed, setup):
        tmp_path, _ = setup
        (tmp_path / "customer-360.yaml").write_text(self.PLAIN)
        self._loaded(governed, tmp_path)
        r = governed.get("/api/projects/customer-360/migration-plan", headers=bearer())
        assert r.status_code == 200, r.text
        assert r.json()["plan"]["actions"] == []

    @pytest.mark.parametrize("body", [{"allow_sensitive": True},
                                      {"sensitivity_threshold": "restricted"}])
    def test_a_migration_may_not_loosen_the_gate(self, governed, setup, body):
        tmp_path, configs = setup
        (tmp_path / "customer-360.yaml").write_text(self.PLAIN)
        self._loaded(governed, tmp_path)
        r = governed.post("/api/projects/customer-360/migrate", headers=bearer(), json=body)
        assert r.status_code == 403, r.text
        assert "stricter but not looser" in r.json()["detail"]


def test_outside_a_control_plane_targets_are_unchanged(tmp_path, monkeypatch):
    """Locally the request is the user's own: relinking works, and a project with no
    registered target still loads with the user's ARANGO_* credentials."""
    monkeypatch.delenv("CONTROL_PLANE_URL", raising=False)
    monkeypatch.delenv("R2G_API_TOKEN", raising=False)
    monkeypatch.setenv("ARANGO_USER", "me")
    monkeypatch.setenv("ARANGO_PASSWORD", "mine")
    catalog = CatalogManager(tmp_path)
    catalog.add_source("src", "csv", str(tmp_path))
    catalog.add_target("tgt-b", endpoint="http://arango-b:8529", database="graph_b")
    (tmp_path / "p.yaml").write_text("collections: {}\nedges: []\n")
    catalog.create_project("p", "src", str(tmp_path / "p.yaml"),
                           arango_endpoint="http://anywhere:8529", arango_database="g")
    client = TestClient(create_app(catalog_dir=str(tmp_path)))
    r = client.patch("/api/projects/p", json={"target_name": "tgt-b"})
    assert r.status_code == 200 and r.json()["target_name"] == "tgt-b"
    r = client.post("/api/projects", json={"name": "q", "source_name": "src",
                                           "arango_endpoint": "http://anywhere:8529",
                                           "arango_database": "g"})
    assert r.status_code == 201 and r.json()["target_name"] in (None, "")
