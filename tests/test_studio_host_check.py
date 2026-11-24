"""An open Studio (loopback bind, no token) answers only requests addressed to a local name.

It trusts that only this machine can reach it. A web page can rebind its own host name to
127.0.0.1 and then call the API from its own origin; the one thing it cannot change is
the ``Host`` its requests carry, so that is what the Studio checks.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from r2g.ui.server import _host_name, create_app


@pytest.fixture
def open_studio(tmp_path, monkeypatch):
    monkeypatch.delenv("R2G_ALLOWED_HOSTS", raising=False)
    monkeypatch.delenv("R2G_API_TOKEN", raising=False)
    monkeypatch.delenv("CONTROL_PLANE_URL", raising=False)
    return TestClient(create_app(catalog_dir=str(tmp_path), host="127.0.0.1"))


@pytest.mark.parametrize("host", ["rebind.attacker.example", "rebind.attacker.example:8501",
                                  "127.0.0.1.attacker.example", "localhost.attacker.example",
                                  "testserver", ""])
def test_a_request_addressed_to_another_name_is_refused(open_studio, host):
    r = open_studio.get("/api/targets", headers={"Host": host})
    assert r.status_code == 400, r.text
    assert "R2G_ALLOWED_HOSTS" in r.json()["detail"]
    r = open_studio.post("/api/sources", headers={"Host": host},
                         json={"name": "x", "source_type": "csv", "connection_string": "/tmp"})
    assert r.status_code == 400


@pytest.mark.parametrize("host", ["localhost", "localhost:8501", "LocalHost:8501",
                                  "127.0.0.1", "127.0.0.1:8501", "[::1]:8501", "[::1]"])
def test_a_request_addressed_to_a_local_name_is_answered(open_studio, host):
    assert open_studio.get("/api/targets", headers={"Host": host}).status_code == 200


def test_more_names_can_be_allowed(tmp_path, monkeypatch):
    monkeypatch.setenv("R2G_ALLOWED_HOSTS", " studio.lan , other.lan")
    monkeypatch.delenv("R2G_API_TOKEN", raising=False)
    client = TestClient(create_app(catalog_dir=str(tmp_path), host="127.0.0.1"))
    assert client.get("/api/targets", headers={"Host": "studio.lan:8501"}).status_code == 200
    assert client.get("/api/targets", headers={"Host": "evil.lan"}).status_code == 400


def test_a_studio_with_a_token_is_not_limited_by_name(tmp_path, monkeypatch):
    """A rebound page has no token, so the token already stops it; a reverse proxy that
    forwards its own Host to a token-protected Studio keeps working."""
    monkeypatch.delenv("R2G_ALLOWED_HOSTS", raising=False)
    client = TestClient(create_app(catalog_dir=str(tmp_path), host="127.0.0.1",
                                   api_token="t0ken"))
    h = {"Host": "studio.example.com"}
    assert client.get("/api/targets", headers=h).status_code == 401
    assert client.get("/api/targets",
                      headers=dict(h, Authorization="Bearer t0ken")).status_code == 200


@pytest.mark.parametrize("header, name", [("[::1]:8501", "::1"), ("LOCALHOST", "localhost"),
                                          ("a.b:1", "a.b"), ("[::1", "[::1"),
                                          ("[::1]evil.example", "[::1]evil.example")])
def test_host_name_parsing(header, name):
    assert _host_name(header) == name


def test_a_name_after_a_bracketed_address_is_not_that_address(open_studio):
    r = open_studio.get("/api/targets", headers={"Host": "[::1]evil.example"})
    assert r.status_code == 400


@pytest.mark.parametrize("origin", ["http://evil.example", "https://evil.example:8501",
                                    "null", "http://localhost.evil.example:8501"])
def test_a_page_on_another_site_changes_nothing(open_studio, origin):
    """A form or script on another site can send a request to localhost, Host and all;
    it cannot read the answer, but without this the change would still be made. A
    snapshot takes no body, so content-type checks do not stop it."""
    h = {"Host": "localhost:8501", "Origin": origin}
    r = open_studio.post("/api/sources/s/snapshot", headers=h)
    assert r.status_code == 403, r.text
    # Reads are left to the browser's same-origin rule, which keeps the answer from it.
    assert open_studio.get("/api/targets", headers=h).status_code == 200


@pytest.mark.parametrize("host, origin", [
    ("localhost:8501", None), ("localhost:8501", "http://localhost:8501"),
    ("127.0.0.1:8501", "http://127.0.0.1:8501"), ("[::1]:8501", "http://[::1]:8501"),
    ("LocalHost:8501", "http://localhost:8501"),
])
def test_the_studios_own_pages_and_tools_without_an_origin_still_change_things(
        open_studio, host, origin):
    h = {"Host": host} | ({"Origin": origin} if origin else {})
    r = open_studio.post("/api/sources/s/snapshot", headers=h)
    assert r.status_code != 403, r.text  # reaches the route: no such source


@pytest.mark.parametrize("origin", ["http://localhost:3000", "http://127.0.0.1:8501",
                                    "https://localhost:8501"])
def test_another_local_app_changes_nothing(open_studio, origin):
    """A dev server or a notebook on this machine is another site: the Origin must name
    the very host and port the Studio was reached at."""
    r = open_studio.post("/api/sources/s/snapshot",
                         headers={"Host": "localhost:8501", "Origin": origin})
    assert r.status_code == 403, r.text


def test_an_origin_allowed_for_cors_may_change_things(tmp_path, monkeypatch):
    monkeypatch.setenv("R2G_CORS_ORIGINS", "http://localhost:3000")
    monkeypatch.delenv("R2G_ALLOWED_HOSTS", raising=False)
    monkeypatch.delenv("R2G_API_TOKEN", raising=False)
    monkeypatch.delenv("CONTROL_PLANE_URL", raising=False)
    client = TestClient(create_app(catalog_dir=str(tmp_path), host="127.0.0.1"))
    h = {"Host": "localhost:8501", "Origin": "http://localhost:3000"}
    assert client.post("/api/sources/s/snapshot", headers=h).status_code != 403


def test_an_empty_bind_address_is_not_loopback(tmp_path, monkeypatch):
    """``""`` binds every interface, so a Studio bound there needs a token like any
    other that others can reach."""
    monkeypatch.delenv("R2G_API_TOKEN", raising=False)
    monkeypatch.delenv("CONTROL_PLANE_URL", raising=False)
    app = create_app(catalog_dir=str(tmp_path), host="")
    assert app.state.api_auth_required
