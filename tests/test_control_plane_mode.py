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
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from r2g import control_plane
from r2g.catalog import CatalogManager
from r2g.ui.server import create_app

TOKEN = "context-for-equity"
OTHER_TENANT = "context-for-globex"
UNAVAILABLE = "context-while-down"
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

    def project(self, identity: control_plane.Identity) -> control_plane.Project:
        return control_plane.Project("equity", "Equity research", "customer-360")

    def decide(self, identity, action, term):  # tests/test_governed_changes.py covers this
        return control_plane.Decision("permitted", "permitted by rule 'all'", 1)

    def record(self, token, entry):
        pass


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
        assert "AOE_URL" in ctx["ontology"]["problem"]          # says why there is no list


class TestTheOntologyFromAoe:
    IDENTITY = control_plane.Identity("pj@example.com", ("analyst",), "local", "equity",
                                      ("fibo-equity", "working"), token=TOKEN)

    def test_classes_are_read_with_the_users_own_context(self, monkeypatch):
        monkeypatch.setenv("AOE_URL", "http://aoe:8000/aoe/")
        http = MagicMock()
        http.get.return_value.status_code = 200
        http.get.return_value.json.return_value = {"data": [
            {"uri": "https://ex.org/Income", "label": "Income"},
            {"uri": "https://ex.org/Equity", "label": "equity"},
            {"label": "no uri, skipped"}]}
        out = control_plane.ontology_terms(self.IDENTITY, http=http)
        assert [c["label"] for c in out["classes"]] == ["equity", "Income"]
        assert out["problem"] is None
        url = http.get.call_args_list[0].args[0]
        assert url == "http://aoe:8000/aoe/api/v1/ontology/fibo-equity/classes"
        assert http.get.call_args.kwargs["headers"] == {"Authorization": f"Bearer {TOKEN}"}

    def test_a_refusal_is_reported_not_raised(self, monkeypatch):
        monkeypatch.setenv("AOE_URL", "http://aoe:8000/aoe")
        http = MagicMock()
        http.get.return_value.status_code = 404
        out = control_plane.ontology_terms(self.IDENTITY, http=http)
        assert out["classes"] == [] and "404" in out["problem"]


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
        # Positive control: the one it opens can be created.
        r = governed.post("/api/projects", headers=bearer(),
                          json={"name": "not-created-yet", "source_name": "src"})
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
