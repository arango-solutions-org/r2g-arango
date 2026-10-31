"""The Studio's changes under a control plane: asked before, recorded after (P14.9-P14.13).

The Library demo's rules: an analyst may change, load and migrate what is under *Agent*
(authors and members) and nothing else; an owner may do everything. The control plane is a
fake that decides by those rules and keeps what it is sent, so every refusal is paired with
the same change permitted, and every record can be read back.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from r2g import control_plane, governed
from r2g.catalog import CatalogManager
from r2g.config import ConfigManager
from r2g.mapping_changes import (
    PROJECT_WIDE,
    GraphState,
    Item,
    graph_items,
    load_collections,
    load_items,
    migration_items,
    save_changes,
    written_by,
)
from r2g.mapping_diff import ReloadAction
from r2g.types import Column, MappingConfig, Schema, Table
from r2g.ui.server import create_app

LIB = "https://example.org/library#"
AGENT, AUTHOR, MEMBER, BOOK, LOAN = (LIB + c for c in ("Agent", "Author", "Member", "Book", "Loan"))
UNDER_AGENT = {AGENT, AUTHOR, MEMBER}
ANALYST, OWNER = "analyst-token", "owner-token"


def library() -> dict[str, Any]:
    """The demo mapping, each table naming its class except loans."""
    return {
        "collections": {
            name: {"source_table": name, "target_collection": name,
                   **({"term": term} if term else {})}
            for name, term in (("authors", AUTHOR), ("books", BOOK), ("members", MEMBER),
                               ("loans", None))},
        "edges": [
            {"edge_collection": "wrote", "from_collection": "books", "to_collection": "authors",
             "from_field": "author_id", "to_field": "id"},
            {"edge_collection": "borrowed_by", "from_collection": "loans",
             "to_collection": "members", "from_field": "member_id", "to_field": "id"},
        ],
    }


def mapping(doc: dict[str, Any] | None = None) -> MappingConfig:
    return MappingConfig.model_validate(doc if doc is not None else library())


class Library:
    """Analysts may act under Agent; owners on anything; nobody else on anything."""

    def __init__(self) -> None:
        self.asked: list[tuple[str, str, str | None]] = []
        self.recorded: list[tuple[str, control_plane.Entry]] = []
        self.unavailable = False
        self.expired: set[str] = set()

    def verify(self, token: str) -> control_plane.Identity:
        if token not in (ANALYST, OWNER):
            raise control_plane.ContextRejected("bad signature")
        return control_plane.Identity(
            principal=f"{token.split('-')[0]}@example.com",
            roles=(token.split("-")[0],), tenant="local", project="library",
            ontology=("library", "working"), token=token)

    def project(self, identity: control_plane.Identity) -> control_plane.Project:
        return control_plane.Project("library", "Library", "library-catalog")

    def decide(self, identity: control_plane.Identity, action: str,
               term: str | None) -> control_plane.Decision:
        role = identity.roles[0]
        self.asked.append((role, action, term))
        if role == "owner" or (term in UNDER_AGENT and action != "transformation.update"):
            return control_plane.Decision("permitted", f"permitted by rule '{role}s'", 7)
        on = f" on {term}" if term else ""
        return control_plane.Decision(
            "denied", f"no rule lets '{identity.principal}' (roles ['{role}']) perform "
                      f"'{action}'{on}", 7)

    def record(self, token: str, entry: control_plane.Entry) -> None:
        if token in self.expired:
            raise control_plane.ContextRejected("token expired")
        if self.unavailable:
            raise control_plane.ContextUnavailable("control plane is down")
        self.recorded.append((token, entry))


@pytest.fixture
def cp(monkeypatch: pytest.MonkeyPatch) -> Library:
    monkeypatch.setenv("CONTROL_PLANE_URL", "http://cp.test/api")
    monkeypatch.setenv("R2G_TENANT", "local")
    monkeypatch.delenv("AOE_URL", raising=False)
    fake = Library()
    control_plane.set_instance(fake)
    # The project ontology, as AOE would list it (the P14.8 check on new terms).
    monkeypatch.setattr(control_plane, "ontology_terms", lambda identity, http=None: {
        "ontology_id": "library", "problem": None,
        "classes": [{"uri": c} for c in (AGENT, AUTHOR, MEMBER, BOOK, LOAN, LIB + "Volume")],
        "properties": [{"uri": LIB + p} for p in ("wrote", "title")]})
    yield fake
    control_plane.set_instance(None)


SCHEMA = Schema(tables={
    name: Table(name=name, columns=[Column(name=c, data_type="integer") for c in cols],
                primary_key=["id"])
    for name, cols in (("authors", ("id",)), ("books", ("id", "author_id")),
                       ("members", ("id",)), ("loans", ("id", "member_id", "book_id")))})


@pytest.fixture
def studio(cp: Library, tmp_path: Path) -> TestClient:
    catalog = CatalogManager(tmp_path)
    catalog.add_source("library-csv", "csv", str(tmp_path))
    catalog.create_snapshot("library-csv", SCHEMA)
    path = tmp_path / "projects" / "library-catalog" / "mapping.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(yaml.safe_dump(library()))
    catalog.create_project("library-catalog", "library-csv", str(path),
                           arango_endpoint="http://db.test:8529", arango_database="library")
    return TestClient(create_app(catalog_dir=str(tmp_path), host="0.0.0.0"))


def as_(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def saved(tmp_path: Path) -> MappingConfig:
    return MappingConfig.model_validate(yaml.safe_load(
        (tmp_path / "projects" / "library-catalog" / "mapping.yaml").read_text()))


def edited(change) -> dict[str, Any]:
    doc = library()
    change(doc)
    return doc


# --- P14.12: every change route classified -----------------------------------------------


def test_every_change_route_is_governed_read_or_refused_as_the_prd_lists(cp, tmp_path):
    app = create_app(catalog_dir=str(tmp_path), host="0.0.0.0")
    kinds: dict[str, list[str]] = {"governed": [], "read": [], "refused": []}
    for route in app.routes:
        for method in sorted(getattr(route, "methods", set()) - {"GET", "HEAD", "OPTIONS"}):
            path = route.path.replace("{name}", "x")
            kind, why = governed.classify(method, path)
            kinds[kind].append(f"{method} {route.path}")
            assert (why is not None) == (kind == "refused")
    assert [len(kinds[k]) for k in ("governed", "read", "refused")] == [5, 14, 11], kinds
    assert sorted(kinds["governed"]) == [
        "PATCH /api/projects/{name}", "POST /api/projects", "POST /api/projects/{name}/load",
        "POST /api/projects/{name}/migrate", "PUT /api/projects/{name}/mapping"]


def test_a_change_route_nobody_classified_is_refused_by_default():
    kind, why = governed.classify("POST", "/api/projects/x/brand-new-thing")
    assert kind == "refused" and "not governed" in why
    assert governed.classify("GET", "/api/projects/x/brand-new-thing")[0] == "read"


@pytest.mark.parametrize("method, path, says", [
    ("POST", "/api/sources", "Sources are shared"),
    ("DELETE", "/api/sources/library-csv", "Sources are shared"),
    ("POST", "/api/sources/library-csv/snapshot", "snapshots are shared"),
    ("POST", "/api/targets", "Targets are shared"),
    ("DELETE", "/api/targets/t", "Targets are shared"),
    ("POST", "/api/catalogs", "Catalogs are shared"),
    ("DELETE", "/api/catalogs/c", "Catalogs are shared"),
    ("POST", "/api/catalogs/c/import-source", "Sources are shared"),
    ("DELETE", "/api/projects/library-catalog", "names this transformation project"),
    ("POST", "/api/demo-presets/snowflake-customer-360/install", "adds a source"),
    ("POST", "/api/projects/library-catalog/governance/emit", "governance files"),
])
def test_a_refused_route_says_why_and_changes_nothing(studio, tmp_path, method, path, says):
    before = (tmp_path / "catalog.json").read_bytes() if (tmp_path / "catalog.json").exists() \
        else None
    r = studio.request(method, path, headers=as_(OWNER), json={})
    assert r.status_code == 403 and says in r.json()["detail"]
    after = (tmp_path / "catalog.json").read_bytes() if (tmp_path / "catalog.json").exists() \
        else None
    assert after == before


def test_a_read_route_is_served(studio):
    r = studio.post("/api/projects/library-catalog/validate", headers=as_(ANALYST))
    assert r.status_code == 200


def test_without_a_control_plane_the_same_routes_are_not_refused(tmp_path, monkeypatch):
    monkeypatch.delenv("CONTROL_PLANE_URL", raising=False)
    client = TestClient(create_app(catalog_dir=str(tmp_path)))
    r = client.post("/api/sources", json={"name": "s", "source_type": "csv",
                                          "connection_string": str(tmp_path)})
    assert r.status_code == 201


# --- P14.9: what a save is checked on ---------------------------------------------------


def test_a_column_is_checked_on_its_tables_class():
    new = edited(lambda d: d["collections"]["books"].update(field_mappings={"title": "name"}))
    assert save_changes(mapping(), mapping(new)) == [
        Item("collections/books", (BOOK,), ("field_mappings",))]


def test_a_relationship_is_checked_on_the_class_of_the_table_it_starts_from():
    def change(d):
        d["edges"][0]["term"] = LIB + "wrote"
    (item,) = save_changes(mapping(), mapping(edited(change)))
    assert item.where == "edges/wrote(books.author_id -> authors.id)"
    assert item.terms == (BOOK,) and item.what == ("term",)


def test_changing_a_class_is_checked_on_the_old_class_and_the_new():
    new = edited(lambda d: d["collections"]["authors"].update(term=BOOK))
    assert save_changes(mapping(), mapping(new))[0].terms == (AUTHOR, BOOK)


def test_a_table_without_a_class_and_its_relationships_are_project_wide():
    def change(d):
        d["collections"]["loans"]["extra_labels"] = ["Event"]
        d["edges"][1]["to_fields"] = ["member_no"]
    items = save_changes(mapping(), mapping(edited(change)))
    assert {i.where: i.terms for i in items} == {
        "collections/loans": (PROJECT_WIDE,),
        "edges/borrowed_by(loans.member_id -> members.id)": (PROJECT_WIDE,),
        "edges/borrowed_by(loans.member_id -> members.member_no)": (PROJECT_WIDE,)}


def test_adding_and_removing_a_table_are_checked_on_its_class():
    def change(d):
        del d["collections"]["members"]
        d["collections"]["staff"] = {"source_table": "staff", "target_collection": "staff",
                                     "term": AGENT}
    items = save_changes(mapping(), mapping(edited(change)))
    assert [(i.where, i.terms, i.what) for i in items] == [
        ("collections/members", (MEMBER,), ("removed",)),
        ("collections/staff", (AGENT,), ("added",))]


def test_nothing_changed_is_nothing_to_check():
    assert save_changes(mapping(), mapping()) == []


#: A changed value for every field of the mapping model. A field the model gains fails
#: the test below until it is listed here, and so until a save is shown to check it.
CHANGED: dict[str, Any] = {
    "MappingConfig.source_schema": "sales",
    "MappingConfig.type_overrides": {"books.id": "string"},
    "MappingConfig.key_separator": "-",
    "MappingConfig.naming_convention": {"collections": "snake", "properties": "camel",
                                        "edges": "snake"},
    "MappingConfig.shared_keys": [{"key": "isbn", "concept": "Book"}],
    "MappingConfig.graph_layout": "lpg",
    "MappingConfig.lpg": {"node_collection": "things"},
    "CollectionMapping.source_table": "books_v2",
    "CollectionMapping.target_collection": "volumes",
    "CollectionMapping.collection_type": "edge",
    "CollectionMapping.is_join_table": True,
    "CollectionMapping.field_mappings": {"title": "name"},
    "CollectionMapping.exclude_fields": ["isbn"],
    "CollectionMapping.include_fields": ["id"],
    "CollectionMapping.field_expressions": [{"target": "slug", "expression": "LOWER(@title)"}],
    "CollectionMapping.extra_labels": ["Item"],
    "CollectionMapping.term": LIB + "Volume",
    "CollectionMapping.field_terms": {"title": LIB + "title"},
    "EdgeDefinition.edge_collection": "authored",
    "EdgeDefinition.from_collection": "loans",
    "EdgeDefinition.to_collection": "members",
    "EdgeDefinition.from_fields": ["writer_id"],
    "EdgeDefinition.to_fields": ["author_no"],
    "EdgeDefinition.term": LIB + "wrote",
}


def test_every_field_of_the_mapping_model_is_compared():
    """The reload diff misses field expressions, extra labels, layout and more; the full
    comparison must not, including for fields added to the model later (P14.9)."""
    from r2g.types import CollectionMapping, EdgeDefinition

    listed = {f"{m.__name__}.{f}" for m in (MappingConfig, CollectionMapping, EdgeDefinition)
              for f in m.model_fields} - {"MappingConfig.collections", "MappingConfig.edges"}
    assert listed == set(CHANGED), "list a changed value for each new field"
    unchecked = []
    for where, value in CHANGED.items():
        model, name = where.split(".")
        doc = library()
        if where == "MappingConfig.lpg":  # saved only in that layout, so only there a change
            doc["graph_layout"] = "lpg"
        target = {"MappingConfig": doc, "CollectionMapping": doc["collections"]["books"],
                  "EdgeDefinition": doc["edges"][0]}[model]
        if model == "EdgeDefinition" and name in ("from_fields", "to_fields"):
            target.pop(name[:-1].removesuffix("s"), None)
            target.pop(name.replace("fields", "field"), None)
        target[name] = value
        layout = "lpg" if where == "MappingConfig.lpg" else "pg"
        base = mapping({**library(), "graph_layout": layout})
        if not save_changes(base, mapping(doc)):
            unchecked.append(where)
    assert unchecked == []


# --- P14.9 / P14.11: saving through the Studio ------------------------------------------


def test_an_analyst_changes_a_mapping_under_agent_and_it_is_recorded(studio, cp, tmp_path):
    new = edited(lambda d: d["collections"]["authors"].update(extra_labels=["Person"]))
    r = studio.put("/api/projects/library-catalog/mapping", headers=as_(ANALYST), json=new)
    assert r.status_code == 200 and r.json()["changes"] == ["collections/authors"]
    assert saved(tmp_path).collections["authors"].extra_labels == ["Person"]
    (token, entry), = cp.recorded
    assert token == ANALYST and entry.action == "mapping.update"
    assert entry.entity == "library-catalog/collections/authors" and entry.term == AUTHOR
    assert entry.decision.permitted and entry.diff == "changed: extra_labels"
    # Both versions are kept, named by their content, and the entry points at them.
    versions = tmp_path / "projects" / "library-catalog" / "versions"
    for ref in (entry.before, entry.after):
        digest = ref.rsplit(":", 1)[1]
        assert (versions / f"{digest}.yaml").is_file()
    assert entry.before != entry.after
    after = (versions / f"{entry.after.rsplit(':', 1)[1]}.yaml").read_text()
    assert after == (tmp_path / "projects" / "library-catalog" / "mapping.yaml").read_text()


def test_a_refused_change_refuses_the_whole_save_and_is_recorded(studio, cp, tmp_path):
    before = (tmp_path / "projects" / "library-catalog" / "mapping.yaml").read_bytes()

    def change(d):
        d["collections"]["authors"]["extra_labels"] = ["Person"]   # permitted
        d["collections"]["books"]["extra_labels"] = ["Item"]       # refused
    r = studio.put("/api/projects/library-catalog/mapping", headers=as_(ANALYST),
                   json=edited(change))
    assert r.status_code == 403
    detail = r.json()["detail"]
    assert "collections/books" in detail["message"] and "mapping.update" in detail["message"]
    assert [x["where"] for x in detail["refused"]] == ["collections/books"]
    assert (tmp_path / "projects" / "library-catalog" / "mapping.yaml").read_bytes() == before
    (_, entry), = cp.recorded
    assert entry.decision.outcome == "denied" and entry.term == BOOK
    assert entry.entity == "library-catalog/collections/books" and entry.after is None
    # Positive control: an owner's identical save goes through.
    r = studio.put("/api/projects/library-catalog/mapping", headers=as_(OWNER),
                   json=edited(change))
    assert r.status_code == 200


def test_a_mapping_wide_setting_is_decided_project_wide(studio, cp):
    r = studio.put("/api/projects/library-catalog/mapping", headers=as_(ANALYST),
                   json=edited(lambda d: d.update(key_separator="-")))
    assert r.status_code == 403
    assert ("analyst", "mapping.update", None) in cp.asked
    assert studio.put("/api/projects/library-catalog/mapping", headers=as_(OWNER),
                      json=edited(lambda d: d.update(key_separator="-"))).status_code == 200


def test_moving_a_table_out_of_agent_needs_both_classes(studio, cp):
    r = studio.put("/api/projects/library-catalog/mapping", headers=as_(ANALYST),
                   json=edited(lambda d: d["collections"]["authors"].update(term=BOOK)))
    assert r.status_code == 403 and r.json()["detail"]["refused"][0]["term"] == BOOK
    # Positive control: within Agent it is permitted, and recorded on the class it has now.
    r = studio.put("/api/projects/library-catalog/mapping", headers=as_(ANALYST),
                   json=edited(lambda d: d["collections"]["authors"].update(term=AGENT)))
    assert r.status_code == 200
    (_, entry), = [x for x in cp.recorded if x[1].decision.permitted]
    assert entry.term == AGENT and entry.diff == "changed: term"


def test_every_question_is_asked_before_anything_is_written(studio, cp, tmp_path,
                                                            monkeypatch):
    from r2g.config import ConfigManager

    writes: list[Any] = []
    real = ConfigManager.save_config
    monkeypatch.setattr(ConfigManager, "save_config",
                        staticmethod(lambda c, p: (writes.append(len(cp.asked)), real(c, p))))

    def change(d):
        d["collections"]["authors"]["extra_labels"] = ["Person"]
        d["collections"]["members"]["extra_labels"] = ["Person"]
    assert studio.put("/api/projects/library-catalog/mapping", headers=as_(ANALYST),
                      json=edited(change)).status_code == 200
    assert writes == [2]  # both asked, then one write


def test_a_save_that_changes_nothing_asks_and_records_nothing(studio, cp):
    r = studio.put("/api/projects/library-catalog/mapping", headers=as_(ANALYST), json=library())
    assert r.status_code == 200 and r.json()["changes"] == []
    assert cp.asked == [] and cp.recorded == []


def test_an_unreadable_saved_mapping_means_every_part_is_checked(studio, cp, tmp_path):
    (tmp_path / "projects" / "library-catalog" / "mapping.yaml").write_text("{ not yaml")
    r = studio.put("/api/projects/library-catalog/mapping", headers=as_(ANALYST), json=library())
    assert r.status_code == 403
    assert {x["where"] for x in r.json()["detail"]["refused"]} >= {"collections/books"}


# --- P14.10: loads and migrations ------------------------------------------------------


class FakePipeline:
    def __init__(self, **kwargs: Any) -> None:
        self.errors: dict[str, list[str]] = {}

    def run(self, graph_name: str, on_event: Any) -> dict[str, Any]:
        return {"documents": [], "edges": []}


def laid_out(config: MappingConfig) -> GraphState:
    """A target already holding the graph ``config`` declares, and every collection."""
    defs = ConfigManager.graph_edge_definitions(config)
    names = {c.target_collection for c in config.collections.values()}
    names |= {n for d in defs for n in (d["edge_collection"], *d["from_vertex_collections"],
                                        *d["to_vertex_collections"])}
    return GraphState(defs, frozenset(names))


@pytest.fixture
def no_database(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[GraphState | None]:
    """No real target. It holds the saved mapping's graph and collections, unless a test
    puts another state first in the returned list."""
    from r2g.ui import server

    states: list[GraphState | None] = []

    def state(*args: Any) -> GraphState | None:
        if states:
            return states[0]
        return laid_out(ConfigManager.load_config(
            tmp_path / "projects" / "library-catalog" / "mapping.yaml"))

    monkeypatch.setattr(server, "ArangoWriter", lambda **kw: object())
    monkeypatch.setattr(server, "StreamingPipeline", FakePipeline)
    monkeypatch.setattr(server, "_graph_state", state)
    return states


def test_what_a_load_writes():
    assert [(i.where, i.terms) for i in load_items(mapping(), include=["authors", "books"])] == [
        ("collections/authors", (AUTHOR,)), ("collections/books", (BOOK,)),
        ("edges/wrote(books.author_id -> authors.id)", (BOOK,))]
    assert len(load_items(mapping(), exclude=["loans"])) == 4


def test_a_load_is_refused_whole_if_one_collection_is(studio, cp, no_database, tmp_path):
    r = studio.post("/api/projects/library-catalog/load", headers=as_(ANALYST),
                    json={"include_tables": ["authors", "books"]})
    assert r.status_code == 403
    assert [x["where"] for x in r.json()["detail"]["refused"]] == [
        "collections/books", "edges/wrote(books.author_id -> authors.id)"]
    assert CatalogManager(tmp_path).get_history(project_name="library-catalog") == []
    assert {e.decision.outcome for _, e in cp.recorded} == {"denied"}


def test_a_permitted_load_is_recorded_when_it_starts(studio, cp, no_database):
    r = studio.post("/api/projects/library-catalog/load", headers=as_(ANALYST),
                    json={"include_tables": ["authors", "members"]})
    assert r.status_code == 202
    load_id = r.json()["load_id"]
    assert [(e.entity, e.term, e.after) for _, e in cp.recorded] == [
        ("library-catalog/collections/authors", AUTHOR, f"library-catalog/loads/{load_id}"),
        ("library-catalog/collections/members", MEMBER, f"library-catalog/loads/{load_id}")]
    assert {e.action for _, e in cp.recorded} == {"data.load"}


def test_a_dry_run_load_is_checked_but_changes_nothing_to_record(studio, cp, no_database):
    r = studio.post("/api/projects/library-catalog/load", headers=as_(ANALYST),
                    json={"include_tables": ["authors"], "dry_run": True})
    assert r.status_code == 202 and cp.asked and cp.recorded == []


def test_a_load_may_not_write_governance_files(studio, no_database):
    r = studio.post("/api/projects/library-catalog/load", headers=as_(OWNER),
                    json={"emit_governance": True})
    assert r.status_code == 403 and "governance files" in r.json()["detail"]


def test_a_migration_is_checked_on_what_each_collection_holds():
    old = mapping()
    new = mapping(edited(lambda d: d["collections"]["books"].update(target_collection="volumes")))
    actions = [ReloadAction(action_type="rename_collection", collection="books", reason="r"),
               ReloadAction(action_type="reload_edge", collection="wrote", reason="r"),
               ReloadAction(action_type="rebuild_graph", collection="", reason="r")]
    assert [(i.where, i.terms, i.what) for i in migration_items(actions, old, new)] == [
        ("graph", (PROJECT_WIDE,), ("rebuild_graph",)),
        ("collections/books", (BOOK,), ("rename_collection",)),
        ("collections/wrote", (BOOK,), ("reload_edge",))]


def test_in_the_lpg_layout_the_shared_collections_hold_every_class():
    lpg = mapping(edited(lambda d: d.update(graph_layout="lpg")))
    assert set(written_by("nodes", lpg)) == {AUTHOR, BOOK, MEMBER, None}


def test_migrating_through_the_studio_is_refused_whole(studio, cp, tmp_path):
    catalog = CatalogManager(tmp_path)
    catalog.set_loaded_mapping("library-catalog", mapping().model_dump())
    path = tmp_path / "projects" / "library-catalog" / "mapping.yaml"
    path.write_text(yaml.safe_dump(edited(
        lambda d: d["collections"]["books"].update(target_collection="volumes"))))
    r = studio.post("/api/projects/library-catalog/migrate", headers=as_(ANALYST),
                    json={"dry_run": True})
    assert r.status_code == 403
    assert "collections/books" in [x["where"] for x in r.json()["detail"]["refused"]]
    assert ("analyst", "data.migrate", BOOK) in cp.asked


# --- transformation.update ---------------------------------------------------------------


def test_the_projects_details_need_transformation_update(studio, cp):
    body = {"mapping_description": "Loans and who wrote what"}
    assert studio.patch("/api/projects/library-catalog", headers=as_(ANALYST),
                        json=body).status_code == 403
    r = studio.patch("/api/projects/library-catalog", headers=as_(OWNER), json=body)
    assert r.status_code == 200
    (_, entry), = [x for x in cp.recorded if x[1].decision.permitted]
    assert entry.action == "transformation.update" and entry.term is None
    assert entry.entity == "library-catalog/project"


def test_creating_reads_no_mapping_file_outside_the_tenants_folder(cp, tmp_path):
    other = tmp_path / "other-tenant.yaml"
    other.write_text(yaml.safe_dump(library()))
    catalog_dir = tmp_path / "local"
    CatalogManager(catalog_dir).add_source("library-csv", "csv", str(tmp_path))
    client = TestClient(create_app(catalog_dir=str(catalog_dir), host="0.0.0.0"))
    body = {"name": "library-catalog", "source_name": "library-csv",
            "mapping_config_path": str(other), "arango_database": "library"}
    assert client.post("/api/projects", headers=as_(OWNER), json=body).status_code == 201
    created = catalog_dir / "projects" / "library-catalog" / "mapping.yaml"
    assert MappingConfig.model_validate(yaml.safe_load(created.read_text()) or {}).collections == {}
    # Positive control: a mapping inside the tenant's own folder is read.
    inside = catalog_dir / "seed.yaml"
    inside.write_text(yaml.safe_dump(library()))
    created.unlink()
    CatalogManager(catalog_dir).delete_project("library-catalog")
    body["mapping_config_path"] = str(inside)
    assert client.post("/api/projects", headers=as_(OWNER), json=body).status_code == 201
    assert "books" in yaml.safe_load(created.read_text())["collections"]


def test_the_mapping_a_project_starts_with_is_checked_as_a_save(cp, tmp_path):
    catalog_dir = tmp_path / "local"
    CatalogManager(catalog_dir).add_source("library-csv", "csv", str(tmp_path))
    (catalog_dir / "seed.yaml").write_text(yaml.safe_dump(library()))
    client = TestClient(create_app(catalog_dir=str(catalog_dir), host="0.0.0.0"))
    body = {"name": "library-catalog", "source_name": "library-csv", "arango_database": "library",
            "mapping_config_path": str(catalog_dir / "seed.yaml")}

    class Creates(Library):  # analysts may create the project, not map books
        def decide(self, identity, action, term):
            if action == "transformation.update":
                return control_plane.Decision("permitted", "ok", 7)
            return super().decide(identity, action, term)

    control_plane.set_instance(fake := Creates())
    r = client.post("/api/projects", headers=as_(ANALYST), json=body)
    assert r.status_code == 403 and "collections/books" in r.json()["detail"]["message"]
    assert CatalogManager(catalog_dir).get_project("library-catalog") is None
    assert ("analyst", "mapping.update", BOOK) in fake.asked
    # Positive control: an owner creates it, and each part of the mapping is recorded.
    r = client.post("/api/projects", headers=as_(OWNER), json=body)
    assert r.status_code == 201
    mapped = {e.entity for _, e in fake.recorded
              if e.action == "mapping.update" and e.decision.permitted}
    assert "library-catalog/collections/books" in mapped


# --- P14.11: the queue ---------------------------------------------------------------------


def entry(event_id: str = "e1") -> control_plane.Entry:
    return control_plane.Entry(event_id=event_id, action="mapping.update", entity="p/x",
                               term=None, decision=control_plane.Decision("permitted", "ok", 7),
                               occurred_at=1)


def test_an_entry_is_queued_before_it_is_sent_and_resent_until_delivered(cp, tmp_path):
    outbox = governed.Outbox(tmp_path / "outbox")
    cp.unavailable = True
    outbox.send(OWNER, entry())
    pending = tmp_path / "outbox" / "pending" / "e1.json"
    assert pending.is_file() and json.loads(pending.read_text())["attempts"] == 1
    assert oct(os.stat(pending).st_mode & 0o777) == "0o600"  # it holds a credential
    cp.unavailable = False
    assert outbox.deliver_pending() == {"delivered": 1}
    assert [e.event_id for _, e in cp.recorded] == ["e1"]   # the same id, so recorded once
    done = json.loads((tmp_path / "outbox" / "done" / "e1.json").read_text())
    assert done["status"] == "delivered" and "token" not in done and not pending.exists()


def test_an_entry_whose_context_expired_is_kept_without_its_token(cp, tmp_path):
    outbox = governed.Outbox(tmp_path / "outbox")
    cp.expired.add(OWNER)
    outbox.send(OWNER, entry())
    done = json.loads((tmp_path / "outbox" / "done" / "e1.json").read_text())
    assert done["status"] == "undeliverable" and "token" not in done
    assert outbox.deliver_pending() == {}


def test_a_request_delivers_after_its_response(cp, tmp_path):
    class Later:
        tasks: list[Any] = []

        def add_task(self, fn, *args):
            self.tasks.append((fn, args))

    later = Later()
    governed.Outbox(tmp_path / "outbox").send(OWNER, entry(), background=later)
    queued = tmp_path / "outbox" / "pending" / "e1.json"
    assert cp.recorded == [] and queued.is_file()
    assert oct(os.stat(queued).st_mode & 0o777) == "0o600"  # it holds a credential
    fn, args = later.tasks[0]
    assert fn(*args) == "delivered" and len(cp.recorded) == 1


# --- P14.13 ------------------------------------------------------------------------------


def test_the_actions_r2g_declares():
    assert control_plane.ACTIONS == {
        "mapping.update": {"tool": "r2g", "version": "1", "targets": "optional-ontology-term"},
        "data.load": {"tool": "r2g", "version": "1", "targets": "optional-ontology-term"},
        "data.migrate": {"tool": "r2g", "version": "1", "targets": "optional-ontology-term"},
        "transformation.update": {"tool": "r2g", "version": "1", "targets": None},
    }


def test_only_declared_actions_are_asked(studio, cp, no_database):
    studio.put("/api/projects/library-catalog/mapping", headers=as_(OWNER),
               json=edited(lambda d: d.update(key_separator="-")))
    studio.post("/api/projects/library-catalog/load", headers=as_(OWNER), json={})
    studio.patch("/api/projects/library-catalog", headers=as_(OWNER),
                 json={"mapping_description": "x"})
    assert {a for _, a, _ in cp.asked} <= set(control_plane.ACTIONS)
    assert {a for _, a, _ in cp.asked} == {"mapping.update", "data.load",
                                           "transformation.update"}


# --- found in review: what a load drops, seeds from another project, the server's keys ------


def test_a_load_that_drops_its_collections_is_checked_on_everything_they_hold():
    lpg = mapping({**library(), "graph_layout": "lpg"})
    dropped = [i for i in load_items(lpg, include=["authors"], drop=True) if "drop" in i.what]
    assert {i.where for i in dropped} == {f"collections/{lpg.lpg.node_collection}",
                                          f"collections/{lpg.lpg.edge_collection}"}
    assert BOOK in dropped[0].terms + dropped[1].terms
    # Positive control: in its own collection a table's drop is checked on its class only.
    own = [i for i in load_items(mapping(), include=["authors"], drop=True) if "drop" in i.what]
    assert [(i.where, i.terms) for i in own] == [("collections/authors", (AUTHOR,))]
    assert not [i for i in load_items(lpg, include=["authors"]) if "drop" in i.what]


def test_an_analyst_may_not_drop_what_other_classes_wrote(studio, cp, no_database, tmp_path):
    path = tmp_path / "projects" / "library-catalog" / "mapping.yaml"
    path.write_text(yaml.safe_dump({**library(), "graph_layout": "lpg"}))
    body = {"include_tables": ["authors"], "drop_collections": True}
    r = studio.post("/api/projects/library-catalog/load", headers=as_(ANALYST), json=body)
    assert r.status_code == 403, r.text
    assert BOOK in {x["term"] for x in r.json()["detail"]["refused"]}
    # Positive control: the same load that drops nothing is permitted.
    r = studio.post("/api/projects/library-catalog/load", headers=as_(ANALYST),
                    json={"include_tables": ["authors"]})
    assert r.status_code == 202, r.text


def test_creating_reads_no_mapping_from_another_projects_folder(cp, tmp_path):
    catalog_dir = tmp_path / "local"
    catalog = CatalogManager(catalog_dir)
    catalog.add_source("library-csv", "csv", str(tmp_path))
    other = catalog_dir / "projects" / "payroll" / "mapping.yaml"
    other.parent.mkdir(parents=True)
    other.write_text(yaml.safe_dump(library()))
    catalog.create_project("payroll", "library-csv", str(other))
    client = TestClient(create_app(catalog_dir=str(catalog_dir), host="0.0.0.0"))
    body = {"name": "library-catalog", "source_name": "library-csv",
            "mapping_config_path": str(other), "arango_database": "library"}
    assert client.post("/api/projects", headers=as_(OWNER), json=body).status_code == 201
    r = client.get("/api/projects/library-catalog/mapping", headers=as_(OWNER))
    assert r.json()["collections"] == {}


@pytest.mark.parametrize("extra", [
    {"api_key": "$R2G_REVIEW_SECRET", "base_url": "http://collector.test/v1"},
    {"api_key": "$R2G_REVIEW_SECRET"},
    {"base_url": "http://collector.test/v1"},  # the server's own key would go there
])
def test_a_suggestion_does_not_read_the_servers_environment(studio, monkeypatch, extra):
    import r2g.llm

    monkeypatch.setenv("R2G_REVIEW_SECRET", "s3cret")
    monkeypatch.setenv("OPENAI_API_KEY", "server-key")
    sent: list[Any] = []

    def provider(kind: str, **kw: Any) -> Any:
        sent.append(kw)
        raise ValueError("stop here")

    monkeypatch.setattr(r2g.llm, "create_llm_provider", provider)
    r = studio.post("/api/projects/library-catalog/suggest-ontology", headers=as_(ANALYST),
                    json={"provider": "openai", **extra})
    assert r.status_code == 403 and sent == [], r.text
    # Positive control: the request's own key, to the address it names, is passed on.
    r = studio.post("/api/projects/library-catalog/suggest-ontology", headers=as_(ANALYST),
                    json={"provider": "openai", "api_key": "own-key",
                          "base_url": "http://collector.test/v1"})
    assert r.status_code == 400 and sent[-1]["api_key"] == "own-key"


def test_a_project_starting_from_a_left_over_mapping_is_checked_on_it(cp, tmp_path):
    catalog_dir = tmp_path / "local"
    CatalogManager(catalog_dir).add_source("library-csv", "csv", str(tmp_path))
    left = catalog_dir / "projects" / "library-catalog" / "mapping.yaml"
    left.parent.mkdir(parents=True)
    left.write_text(yaml.safe_dump(library()))  # a project deleted outside the Studio
    client = TestClient(create_app(catalog_dir=str(catalog_dir), host="0.0.0.0"))
    body = {"name": "library-catalog", "source_name": "library-csv",
            "arango_database": "library"}
    assert client.post("/api/projects", headers=as_(OWNER), json=body).status_code == 201
    assert "library-catalog/collections/books" in {
        e.entity for _, e in cp.recorded if e.action == "mapping.update"}


# --- a load declares the named graph: checked when that changes something ------------------


def test_declaring_the_graph_is_checked_only_when_it_changes_something():
    config = mapping()
    writes = load_collections(config, include=["authors", "members"])
    assert writes == {"authors", "members"}
    assert graph_items(config, laid_out(config), writes) == []      # nothing new: no check
    [new] = graph_items(config, GraphState(None, frozenset()), writes)
    assert (new.where, new.terms) == ("graph", (PROJECT_WIDE,))
    assert new.what == ("graph: new", "creates, empty: books, borrowed_by, loans, wrote")
    # A collection the graph names and the database lacks is created, empty, unchecked
    # otherwise; one the load itself writes is checked on its class already.
    missing = laid_out(config)
    missing = GraphState(missing.declared, missing.collections - {"loans", "authors"})
    assert [i.what for i in graph_items(config, missing, writes)] == [("creates, empty: loans",)]
    other = GraphState(laid_out(config).declared[:1], laid_out(config).collections)
    assert [i.what for i in graph_items(config, other, writes)] == [
        ("graph: declared differently",)]
    assert [i.what for i in graph_items(config, None, writes)] == [
        ("graph: the target database could not be read",)]


def test_a_load_that_would_lay_out_the_graph_needs_the_project_wide_rules(
        studio, cp, no_database, tmp_path):
    no_database.append(GraphState(None, frozenset()))                # a new database
    body = {"include_tables": ["authors", "members"]}
    r = studio.post("/api/projects/library-catalog/load", headers=as_(ANALYST), json=body)
    assert r.status_code == 403, r.text
    assert [(x["where"], x["term"]) for x in r.json()["detail"]["refused"]] == [("graph", None)]
    assert CatalogManager(tmp_path).get_history(project_name="library-catalog") == []
    # An owner lays it out, and that is recorded with the load.
    r = studio.post("/api/projects/library-catalog/load", headers=as_(OWNER), json=body)
    assert r.status_code == 202, r.text
    graph = [e for _, e in cp.recorded if e.entity == "library-catalog/graph"]
    assert [(e.term, e.decision.outcome) for e in graph] == [(None, "denied"),
                                                             (None, "permitted")]
    # Once laid out, the analyst's same load is checked on its tables only.
    no_database.clear()
    r = studio.post("/api/projects/library-catalog/load", headers=as_(ANALYST), json=body)
    assert r.status_code == 202, r.text


def test_a_dry_run_load_does_not_read_the_target(studio, cp, no_database, monkeypatch):
    from r2g.ui import server

    monkeypatch.setattr(server, "_graph_state", lambda *a: pytest.fail("read the target"))
    r = studio.post("/api/projects/library-catalog/load", headers=as_(ANALYST),
                    json={"include_tables": ["authors"], "dry_run": True})
    assert r.status_code == 202
