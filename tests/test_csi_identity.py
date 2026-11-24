"""Publisher-scoped CSI mapping identities (src/r2g/csi_identities.py, r2g-arango#91).

The contract is arangodb-schema-analyzer's (issue #42). r2g restates it rather
than importing it, so the ASA-equivalence tests below run the analyzer's own
private helpers on the same documents as an oracle; they skip when the analyzer
is not installed, and the rest of the file does not need it.
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path

import jsonschema
import pytest

from r2g.csi import mapping_to_csi, validate_csi
from r2g.csi_identities import (
    MAPPING_ID_PATTERN,
    CsiIdentityError,
    carries_identity,
    identity_issues,
    mint_mapping_ids,
    normalize_publisher_uri,
    publish_identities,
    resolve_publisher_uri,
)
from r2g.types import (
    CollectionMapping,
    Column,
    EdgeDefinition,
    MappingConfig,
    Schema,
    Table,
)

BASE = "https://data.example.com/r2g/shop"
TS = "2026-01-01T00:00:00+00:00"
FIXTURES = Path(__file__).parent / "fixtures" / "csi_identity"


@pytest.fixture(autouse=True)
def _reset_structlog(monkeypatch):
    """Restore structlog after a CliRunner invocation (mirrors tests/test_csi.py).

    ``r2g.log`` caches each logger on first use, so ``r2g.main.log`` would stay
    bound to the stdout of the first CliRunner that logged — closed by the time
    a later failing export calls ``log.exception``. Each test gets a fresh one.
    """
    import structlog

    import r2g.main

    monkeypatch.setattr(r2g.main, "log", structlog.get_logger("test_csi_identity"))
    yield
    structlog.reset_defaults()
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(0),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )


def _config(collections=None, edges=None) -> MappingConfig:
    return MappingConfig(
        source_schema="shop",
        collections=collections
        if collections is not None
        else {
            "users": CollectionMapping(
                source_table="users", target_collection="User", field_mappings={"full_name": "name"}
            ),
            "orders": CollectionMapping(source_table="orders", target_collection="Order"),
            "tags": CollectionMapping(source_table="tags", target_collection="Tag"),
        },
        edges=edges
        if edges is not None
        else [
            EdgeDefinition(
                edge_collection="placed_by",
                from_collection="orders",
                to_collection="users",
                from_fields=["user_id"],
                to_fields=["id"],
            )
        ],
    )


def _schema() -> Schema:
    return Schema(
        tables={
            "users": Table(
                name="users",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="full_name", data_type="text"),
                    Column(name="e mail/addr", data_type="text"),
                ],
                primary_key=["id"],
            ),
            "orders": Table(
                name="orders",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="user_id", data_type="integer"),
                ],
                primary_key=["id"],
            ),
            # No columns: an entity with no properties.
            "tags": Table(name="tags", columns=[]),
        }
    )


def _export(**kwargs):
    kwargs.setdefault("generated_at", TS)
    kwargs.setdefault("producer_version", "test")
    return mapping_to_csi(kwargs.pop("config", None) or _config(), kwargs.pop("schema", _schema()), **kwargs)


def _with_reference(doc):
    """``doc`` with a lifted FOREIGN_KEY reference on its first relationship (ASA's shape)."""
    doc = copy.deepcopy(doc)
    doc["conceptualModel"]["relationships"][0]["reference"] = {
        "fromCollection": "Order",
        "fromFields": ["user_id"],
        "toCollection": "User",
        "toKeyFields": ["_key"],
        "origin": "declared",
    }
    return doc


# ── 1-3. Publisher URI ───────────────────────────────────────────────


@pytest.mark.parametrize(
    "uri",
    ["https://example.com", "http://example.com/a/b", "https://example.com:8443/x", "https://ex.com/é"],
)
def test_valid_publisher_uris_are_accepted(uri):
    assert normalize_publisher_uri(uri) == uri


@pytest.mark.parametrize(
    "uri",
    [
        "ftp://example.com/x",
        "urn:example:x",
        "example.com/x",
        "https://exa mple.com",
        "https://example.com/\tx",
        "",
        "https://",
        None,
        42,
    ],
)
def test_invalid_publisher_uris_are_refused(uri):
    with pytest.raises(CsiIdentityError, match="absolute http"):
        normalize_publisher_uri(uri)


@pytest.mark.parametrize("uri", ["https://example.com/x?v=1", "https://example.com/x#frag", "https://e.com?/"])
def test_query_and_fragment_are_refused(uri):
    with pytest.raises(CsiIdentityError, match="without query or fragment"):
        normalize_publisher_uri(uri)
    with pytest.raises(CsiIdentityError):
        _export(publisher_uri=uri)


def test_trailing_slashes_are_stripped():
    assert normalize_publisher_uri(BASE + "///") == BASE
    assert _export(publisher_uri=BASE + "/") == _export(publisher_uri=BASE)


def test_publisher_uri_matches_the_schema_mapping_id_pattern():
    from r2g.csi import csi_schema

    assert csi_schema()["$defs"]["MappingId"]["pattern"] == MAPPING_ID_PATTERN.pattern


# ── 4-5. Determinism ─────────────────────────────────────────────────


def test_repeated_exports_mint_identical_ids():
    first, second = _export(publisher_uri=BASE), _export(publisher_uri=BASE)
    assert json.dumps(first, indent=2) == json.dumps(second, indent=2)


def test_reordering_inputs_does_not_change_any_id():
    config = _config()
    reordered = _config(
        collections=dict(reversed(list(config.collections.items()))), edges=list(reversed(config.edges))
    )
    schema = _schema()
    schema_reordered = Schema(tables=dict(reversed(list(schema.tables.items()))))

    def ids(doc):
        out = {}
        for kind in ("entities", "relationships"):
            for name, spec in doc["arangoPhysicalMapping"][kind].items():
                out[(kind, name)] = (spec["id"], spec.get("propertyIds"))
        return out

    assert ids(_export(publisher_uri=BASE)) == ids(
        _export(config=reordered, schema=schema_reordered, publisher_uri=BASE)
    )


# ── 6-9, 12. Minting ─────────────────────────────────────────────────


def test_entity_relationship_and_property_id_paths():
    doc = _export(publisher_uri=BASE)
    user = doc["arangoPhysicalMapping"]["entities"]["User"]
    assert user["id"] == f"{BASE}/mapping/entity/User"
    assert user["propertyIds"] == {
        "id": f"{BASE}/mapping/entity/User/property/id",
        "name": f"{BASE}/mapping/entity/User/property/name",
        # quote(name, safe=""): a "/" or space in a name cannot forge a path segment.
        "eMailAddr": f"{BASE}/mapping/entity/User/property/eMailAddr",
    }
    rel = doc["arangoPhysicalMapping"]["relationships"]["placedBy"]
    assert rel["id"] == f"{BASE}/mapping/relationship/placedBy"
    assert doc["provenance"]["publisher"] == {"uri": BASE}


def test_names_are_percent_encoded_with_nothing_safe():
    doc = {
        "arangoPhysicalMapping": {
            "entities": {"A/B c": {"properties": {"x/y": {"field": "x/y"}}}},
            "relationships": {},
        }
    }
    minted = mint_mapping_ids(doc, BASE)["arangoPhysicalMapping"]["entities"]["A/B c"]
    assert minted["id"] == f"{BASE}/mapping/entity/A%2FB%20c"
    assert minted["propertyIds"]["x/y"] == f"{BASE}/mapping/entity/A%2FB%20c/property/x%2Fy"


def test_property_ids_cover_every_property_exactly():
    doc = _export(publisher_uri=BASE)
    for spec in doc["arangoPhysicalMapping"]["entities"].values():
        if spec["properties"]:
            assert set(spec["propertyIds"]) == set(spec["properties"])


def test_empty_properties_get_no_property_ids():
    tag = _export(publisher_uri=BASE)["arangoPhysicalMapping"]["entities"]["Tag"]
    assert tag["properties"] == {}
    assert "propertyIds" not in tag
    assert tag["id"] == f"{BASE}/mapping/entity/Tag"


def test_existing_ids_are_never_overwritten():
    doc = mint_mapping_ids(_with_reference(_export()), BASE)
    user = doc["arangoPhysicalMapping"]["entities"]["User"]
    user["id"] = f"{BASE}/mapping/entity/Customer"
    user["propertyIds"]["name"] = f"{BASE}/kept/name"
    doc["conceptualModel"]["relationships"][0]["reference"]["id"] = f"{BASE}/kept/ref"
    again = mint_mapping_ids(doc, BASE)
    assert again["arangoPhysicalMapping"]["entities"]["User"]["id"] == f"{BASE}/mapping/entity/Customer"
    assert again["arangoPhysicalMapping"]["entities"]["User"]["propertyIds"]["name"] == f"{BASE}/kept/name"
    assert again["conceptualModel"]["relationships"][0]["reference"]["id"] == f"{BASE}/kept/ref"


def test_minting_never_mutates_its_input():
    doc = _export()
    before = copy.deepcopy(doc)
    mint_mapping_ids(doc, BASE)
    publish_identities(doc, BASE)
    assert doc == before


def test_physical_mapping_is_a_deep_copy_alias():
    doc = _export(publisher_uri=BASE)
    assert doc["physicalMapping"] == doc["arangoPhysicalMapping"]
    assert doc["physicalMapping"] is not doc["arangoPhysicalMapping"]
    doc["physicalMapping"]["entities"]["User"]["id"] = "changed"
    assert doc["arangoPhysicalMapping"]["entities"]["User"]["id"] == f"{BASE}/mapping/entity/User"


def test_reference_ids_are_minted_when_a_reference_exists():
    doc = mint_mapping_ids(_with_reference(_export()), BASE)
    ref = doc["conceptualModel"]["relationships"][0]["reference"]
    assert ref["id"] == f"{BASE}/mapping/relationship/placedBy/reference"


def test_no_reference_means_no_reference_id():
    doc = _export(publisher_uri=BASE)
    assert all("reference" not in rel for rel in doc["conceptualModel"]["relationships"])


# ── 10-11. Integrity ─────────────────────────────────────────────────


def test_a_published_export_has_no_identity_issues():
    doc = _export(publisher_uri=BASE)
    assert identity_issues(doc) == []
    validate_csi(doc)


def test_duplicate_ids_are_reported():
    doc = _export(publisher_uri=BASE)
    doc["arangoPhysicalMapping"]["entities"]["Order"]["id"] = f"{BASE}/mapping/entity/User"
    doc["physicalMapping"] = copy.deepcopy(doc["arangoPhysicalMapping"])
    assert f"duplicate mapping id '{BASE}/mapping/entity/User'" in identity_issues(doc)


def test_foreign_publisher_ids_are_reported_and_never_re_minted():
    doc = _export(publisher_uri=BASE)
    doc["arangoPhysicalMapping"]["entities"]["User"]["id"] = "https://other.example/mapping/entity/User"
    doc["physicalMapping"] = copy.deepcopy(doc["arangoPhysicalMapping"])
    assert any("is not under publisher" in i for i in identity_issues(doc))
    # A sibling path that merely shares the prefix string is foreign too.
    doc["arangoPhysicalMapping"]["entities"]["User"]["id"] = BASE + "-evil/mapping/entity/User"
    doc["physicalMapping"] = copy.deepcopy(doc["arangoPhysicalMapping"])
    assert any("is not under publisher" in i for i in identity_issues(doc))
    with pytest.raises(CsiIdentityError, match="not under publisher"):
        publish_identities(doc, BASE)


def test_incomplete_and_stray_property_ids_are_reported():
    doc = _export(publisher_uri=BASE)
    ids = doc["arangoPhysicalMapping"]["entities"]["User"]["propertyIds"]
    del ids["name"]
    ids["ghost"] = f"{BASE}/mapping/entity/User/property/ghost"
    doc["physicalMapping"] = copy.deepcopy(doc["arangoPhysicalMapping"])
    issues = identity_issues(doc)
    assert "arangoPhysicalMapping.entities.User.propertyIds.name has no mapping id" in issues
    assert "arangoPhysicalMapping.entities.User.propertyIds.ghost names no property in properties" in issues
    # A stray key is never dropped silently when re-publishing.
    with pytest.raises(CsiIdentityError, match="ghost"):
        publish_identities(doc, BASE)


def test_missing_ids_are_reported_once_identity_is_claimed():
    doc = _export(publisher_uri=BASE)
    del doc["arangoPhysicalMapping"]["relationships"]["placedBy"]["id"]
    assert "arangoPhysicalMapping.relationships.placedBy has no mapping id" in identity_issues(doc)


def test_ids_without_a_publisher_are_reported():
    doc = _export(publisher_uri=BASE)
    del doc["provenance"]["publisher"]
    assert "mapping ids are present but provenance.publisher.uri is missing" in identity_issues(doc)


def test_inconsistent_alias_is_reported():
    doc = _export(publisher_uri=BASE)
    doc["physicalMapping"]["entities"]["User"]["collectionName"] = "Elsewhere"
    assert "physicalMapping differs from arangoPhysicalMapping" in identity_issues(doc)


def test_conflicting_publisher_metadata_is_refused():
    doc = _export(publisher_uri=BASE)
    with pytest.raises(CsiIdentityError, match="differs from provenance.publisher.uri"):
        publish_identities(doc, "https://other.example/r2g")
    # The same publisher, spelled with a trailing slash, is not a conflict.
    assert publish_identities(doc, BASE + "/") == doc
    with pytest.raises(CsiIdentityError, match="provenance.publisher.uri must be"):
        resolve_publisher_uri(BASE, existing="https://x.example/?q")


def test_validate_csi_appends_identity_issues():
    doc = _export(publisher_uri=BASE)
    doc["arangoPhysicalMapping"]["entities"]["Order"]["id"] = f"{BASE}/mapping/entity/User"
    doc["physicalMapping"] = copy.deepcopy(doc["arangoPhysicalMapping"])
    jsonschema.validate(instance=doc, schema=__import__("r2g.csi").csi.csi_schema())  # schema alone accepts it
    with pytest.raises(jsonschema.ValidationError, match="duplicate mapping id"):
        validate_csi(doc)


def test_carries_identity():
    assert not carries_identity(_export())
    assert carries_identity(_export(publisher_uri=BASE))
    doc = _export()
    doc["physicalMapping"] = doc["arangoPhysicalMapping"]
    assert carries_identity(doc)
    assert carries_identity(mint_mapping_ids(_with_reference(_export()), BASE))


# ── 15-16. Collapsed mappings ────────────────────────────────────────


def test_two_tables_naming_one_entity_are_refused_before_minting():
    config = _config(
        collections={
            "users": CollectionMapping(source_table="users", target_collection="users"),
            "user": CollectionMapping(source_table="user", target_collection="user"),
        },
        edges=[],
    )
    with pytest.raises(CsiIdentityError) as err:
        mapping_to_csi(config, publisher_uri=BASE)
    message = str(err.value)
    assert "entity 'User'" in message
    assert "table 'users' -> collection 'users'" in message
    assert "table 'user' -> collection 'user'" in message


def test_two_tables_sharing_one_collection_are_refused():
    config = _config(
        collections={
            "a": CollectionMapping(source_table="customers_eu", target_collection="customers"),
            "b": CollectionMapping(source_table="customers_us", target_collection="customers"),
        },
        edges=[],
    )
    with pytest.raises(CsiIdentityError, match="customers_eu.*customers_us"):
        mapping_to_csi(config, publisher_uri=BASE)


def test_a_curated_rename_that_resolves_a_collapse_lets_the_export_through():
    config = _config(
        collections={
            "users": CollectionMapping(source_table="users", target_collection="users"),
            "user": CollectionMapping(source_table="user", target_collection="user"),
        },
        edges=[],
    )
    # Both generate "User"; the plan renames that one name, so they still collapse.
    curated = {"curatedLabelsVersion": "1", "entities": [{"name": "User", "label": "Member"}]}
    with pytest.raises(CsiIdentityError, match="entity 'Member'"):
        mapping_to_csi(config, publisher_uri=BASE, curated_labels=curated)


def test_two_edges_naming_one_relationship_are_refused():
    edges = [
        EdgeDefinition(
            edge_collection="links", from_collection="orders", to_collection="users",
            from_fields=["user_id"], to_fields=["id"],
        ),
        EdgeDefinition(
            edge_collection="links", from_collection="users", to_collection="orders",
            from_fields=["last_order"], to_fields=["id"],
        ),
    ]
    with pytest.raises(CsiIdentityError) as err:
        _export(config=_config(edges=edges), publisher_uri=BASE)
    message = str(err.value)
    assert "relationship 'links'" in message
    assert "orders.user_id -> users.id" in message
    assert "users.last_order -> orders.id" in message


def test_collapsed_mappings_are_not_refused_without_a_publisher():
    config = _config(
        collections={
            "users": CollectionMapping(source_table="users", target_collection="users"),
            "user": CollectionMapping(source_table="user", target_collection="user"),
        },
        edges=[],
    )
    doc = mapping_to_csi(config)  # legacy behaviour kept, pending an owner decision
    assert list(doc["arangoPhysicalMapping"]["entities"]) == ["User"]


# ── 17. Legacy output ────────────────────────────────────────────────


def test_without_a_publisher_the_document_carries_no_identity():
    doc = _export()
    assert not carries_identity(doc)
    assert "physicalMapping" not in doc
    assert "publisher" not in doc["provenance"]


def test_without_a_publisher_the_export_matches_the_pre_identity_release():
    """Byte-for-byte against ``mapping_to_csi`` at 2e0decc (before identities), same inputs.

    The fixture was generated by running 2e0decc's ``src/`` on ``_config()``,
    ``_schema()`` and ``_export``'s defaults; regenerate it only for an
    intended change to legacy output.
    """
    expected = (FIXTURES / "legacy_no_publisher.json").read_text(encoding="utf-8")
    assert json.dumps(_export(), indent=2) + "\n" == expected


def test_publisher_export_differs_from_legacy_only_by_identity_fields():
    legacy, published = _export(), _export(publisher_uri=BASE)
    stripped = copy.deepcopy(published)
    del stripped["physicalMapping"]
    del stripped["provenance"]["publisher"]
    for kind in ("entities", "relationships"):
        for spec in stripped["arangoPhysicalMapping"][kind].values():
            spec.pop("id")
            spec.pop("propertyIds", None)
    assert json.dumps(stripped, indent=2) == json.dumps(legacy, indent=2)


# ── 14. CLI ──────────────────────────────────────────────────────────


def _write_cli_config(tmp_path: Path) -> Path:
    path = tmp_path / "mapping.yaml"
    path.write_text(
        "source_schema: shop\n"
        "collections:\n"
        "  users:\n    source_table: users\n    target_collection: User\n"
        "  orders:\n    source_table: orders\n    target_collection: Order\n"
        "edges:\n"
        "  - edge_collection: placed_by\n    from_collection: orders\n    to_collection: users\n"
        "    from_field: user_id\n    to_field: id\n",
        encoding="utf-8",
    )
    return path


def _run_cli(*args):
    from typer.testing import CliRunner

    from r2g.main import app

    return CliRunner().invoke(app, ["export-csi", *args])


def test_cli_publishes_ids(tmp_path):
    out = tmp_path / "out.csi.json"
    result = _run_cli("--config", str(_write_cli_config(tmp_path)), "-o", str(out), "--publisher-uri", BASE + "/")
    assert result.exit_code == 0, result.output
    doc = json.loads(out.read_text(encoding="utf-8"))
    validate_csi(doc)
    assert doc["provenance"]["publisher"] == {"uri": BASE}
    assert doc["arangoPhysicalMapping"]["entities"]["User"]["id"] == f"{BASE}/mapping/entity/User"


def test_cli_refuses_a_bad_publisher_uri_and_writes_nothing(tmp_path):
    out = tmp_path / "out.csi.json"
    result = _run_cli(
        "--config", str(_write_cli_config(tmp_path)), "-o", str(out), "--publisher-uri", "https://x.example/?q=1"
    )
    assert result.exit_code == 1
    assert "without query or fragment" in result.output
    assert not out.exists()


@pytest.mark.parametrize("publisher", [True, False])
def test_no_validate_cannot_bypass_identity_integrity(tmp_path, monkeypatch, publisher):
    """--no-validate skips JSON Schema only; broken ids are still refused."""
    import r2g.csi

    real = r2g.csi.mapping_to_csi

    def broken(*args, **kwargs):
        doc = real(*args, **{**kwargs, "publisher_uri": BASE})
        doc["arangoPhysicalMapping"]["entities"]["Order"]["id"] = f"{BASE}/mapping/entity/User"
        doc["physicalMapping"] = copy.deepcopy(doc["arangoPhysicalMapping"])
        return doc

    monkeypatch.setattr(r2g.csi, "mapping_to_csi", broken)
    out = tmp_path / "out.csi.json"
    args = ["--config", str(_write_cli_config(tmp_path)), "-o", str(out), "--no-validate"]
    if publisher:
        args += ["--publisher-uri", BASE]
    result = _run_cli(*args)
    assert result.exit_code == 1, result.output
    assert "duplicate mapping id" in result.output
    assert not out.exists()


def test_no_validate_still_writes_a_legacy_document(tmp_path):
    out = tmp_path / "out.csi.json"
    result = _run_cli("--config", str(_write_cli_config(tmp_path)), "-o", str(out), "--no-validate")
    assert result.exit_code == 0, result.output
    assert not carries_identity(json.loads(out.read_text(encoding="utf-8")))


# ── 18-19. ASA equivalence and conformance ───────────────────────────


def _asa():
    return pytest.importorskip("schema_analyzer.csi")


def _documents():
    """Exports and hand-made edge cases both implementations must agree on."""
    with_ref = _with_reference(_export())
    odd = {
        "arangoPhysicalMapping": {
            "entities": {
                "A/B": {"properties": {"x y": {"field": "x"}}},
                "Empty": {"properties": {}},
                "NoProps": {"collectionName": "n"},
                "NotADict": "x",
                "Kept": {"id": f"{BASE}/kept", "properties": {"p": {}}, "propertyIds": {"p": f"{BASE}/kp"}},
            },
            "relationships": {"r": {"edgeCollectionName": "r"}},
        },
        "conceptualModel": {"relationships": [{"type": "r", "reference": {}}, {"type": "s"}, "junk"]},
    }
    return {"export": _export(), "with_reference": with_ref, "odd": odd}


@pytest.mark.parametrize("name", ["export", "with_reference", "odd"])
def test_minting_matches_asa(name):
    asa = _asa()
    doc = _documents()[name]
    assert mint_mapping_ids(doc, BASE) == asa._mint_mapping_ids(doc, BASE)


def _identity_corpus():
    published = _export(publisher_uri=BASE)
    cases = {"legacy": _export(), "published": published, "not_a_dict": []}

    def mutate(label, fn):
        doc = copy.deepcopy(published)
        fn(doc)
        cases[label] = doc

    ents = lambda d: d["arangoPhysicalMapping"]["entities"]  # noqa: E731
    mutate("duplicate", lambda d: ents(d)["Order"].__setitem__("id", ents(d)["User"]["id"]))
    mutate("foreign", lambda d: ents(d)["User"].__setitem__("id", "https://o.example/x"))
    mutate("prefix_sibling", lambda d: ents(d)["User"].__setitem__("id", BASE + "x/y"))
    mutate("empty_id", lambda d: ents(d)["User"].__setitem__("id", ""))
    mutate("missing_id", lambda d: ents(d)["User"].pop("id"))
    mutate("missing_prop", lambda d: ents(d)["User"]["propertyIds"].pop("name"))
    mutate("stray_prop", lambda d: ents(d)["User"]["propertyIds"].__setitem__("ghost", BASE + "/g"))
    mutate("no_publisher", lambda d: d["provenance"].pop("publisher"))
    mutate("publisher_slash", lambda d: d["provenance"].__setitem__("publisher", {"uri": BASE + "/"}))
    mutate("alias_differs", lambda d: d["physicalMapping"]["entities"].pop("User"))
    mutate("non_str_id", lambda d: ents(d)["User"].__setitem__("id", 7))
    mutate("empty_property_ids", lambda d: ents(d)["Tag"].__setitem__("propertyIds", {}))
    with_ref = mint_mapping_ids(_with_reference(_export()), BASE)
    with_ref["provenance"]["publisher"] = {"uri": BASE}
    cases["reference_ok"] = with_ref
    no_ref_id = copy.deepcopy(with_ref)
    no_ref_id["conceptualModel"]["relationships"][0]["reference"].pop("id")
    cases["reference_missing_id"] = no_ref_id
    only_alias = _export()
    only_alias["physicalMapping"] = {"entities": {}, "relationships": {}}
    cases["only_alias_differs"] = only_alias
    return cases


@pytest.mark.parametrize("name", sorted(_identity_corpus()))
def test_identity_issues_match_asa(name):
    asa = _asa()
    doc = _identity_corpus()[name]
    assert identity_issues(doc) == asa._identity_issues(doc)


def test_identity_corpus_exercises_every_kind_of_issue():
    """Guard the equivalence corpus: it must hit every message shape, or agreement proves little."""
    issues = " | ".join(i for doc in _identity_corpus().values() for i in identity_issues(doc))
    for fragment in (
        "duplicate mapping id",
        "is not under publisher",
        "has no mapping id",
        "names no property",
        "provenance.publisher.uri is missing",
        "physicalMapping differs",
        ".reference has no mapping id",
    ):
        assert fragment in issues, fragment


def test_published_export_passes_asa_validate_csi():
    asa = _asa()
    doc = _export(publisher_uri=BASE)
    assert asa.validate_csi(doc, naming=False) == []


def test_asa_round_trip_keeps_every_id():
    """from_csi -> to_csi under the same publisher reproduces r2g's ids and refuses another."""
    asa = _asa()
    from schema_analyzer.errors import SchemaAnalyzerError

    doc = _export(publisher_uri=BASE)
    analysis = asa.from_csi(doc)
    assert analysis["metadata"]["publisherUri"] == BASE
    again = asa.to_csi(analysis, direction="forward", owl_naming=False)
    assert again["arangoPhysicalMapping"] == doc["arangoPhysicalMapping"]
    assert again["physicalMapping"] == doc["physicalMapping"]
    assert again["provenance"]["publisher"] == doc["provenance"]["publisher"]
    with pytest.raises(SchemaAnalyzerError, match="differs from metadata.publisherUri"):
        asa.to_csi(analysis, owl_naming=False, publisher_uri="https://other.example")


def test_asa_mints_the_same_ids_for_an_r2g_legacy_export():
    """ASA publishing r2g's legacy document agrees with r2g publishing it."""
    asa = _asa()
    legacy = _export()
    theirs = asa.to_csi(asa.from_csi(legacy), owl_naming=False, publisher_uri=BASE)
    ours = _export(publisher_uri=BASE)
    assert theirs["arangoPhysicalMapping"] == ours["arangoPhysicalMapping"]
    assert theirs["physicalMapping"] == ours["physicalMapping"]
