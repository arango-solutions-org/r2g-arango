"""Tests for the forward CSI v1 emitter (src/r2g/csi.py)."""

from __future__ import annotations

import sys

import pytest

from r2g.csi import (
    CSI_PROPERTY_EXTENSION_PREFIX,
    CSI_PROPERTY_MAPPING_KEYS,
    CSI_VERSION,
    CSI_VERSION_NESTED,
    csi_schema,
    csi_version_for,
    is_entity_collection,
    mapping_to_csi,
    validate_csi,
)
from r2g.types import (
    CollectionMapping,
    Column,
    EdgeDefinition,
    FieldExpression,
    ForeignKey,
    MappingConfig,
    Schema,
    SharedKey,
    SharedKeyBinding,
    Table,
)


@pytest.fixture(autouse=True)
def _reset_structlog():
    """Restore structlog to real stderr after a CliRunner invocation.

    CliRunner redirects stdout/stderr; structlog caches the (now-closed) stream,
    which poisons logging in every later test. Mirrors the fixture in
    tests/test_cli.py.
    """
    yield
    import structlog

    structlog.reset_defaults()
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(0),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )


def _sample_config() -> MappingConfig:
    """A two-table (users, orders) + one FK-edge mapping."""
    return MappingConfig(
        source_schema="shop",
        collections={
            "users": CollectionMapping(
                source_table="users",
                target_collection="User",
                field_mappings={"full_name": "name"},
            ),
            "orders": CollectionMapping(
                source_table="orders",
                target_collection="Order",
                field_expressions=[
                    FieldExpression(target="total_cents", sources=["total"], expression="total * 100"),
                ],
            ),
            # A join table -> becomes a relationship, not an entity.
            "user_orders": CollectionMapping(
                source_table="user_orders",
                target_collection="placed",
                collection_type="edge",
                is_join_table=True,
            ),
        },
        edges=[
            EdgeDefinition(
                edge_collection="placed_by",
                from_collection="orders",  # source-table name
                to_collection="users",  # source-table name
                from_fields=["user_id"],
                to_fields=["id"],
            ),
        ],
    )


def _sample_schema() -> Schema:
    return Schema(
        tables={
            "users": Table(
                name="users",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="full_name", data_type="text"),
                    Column(name="email", data_type="text"),
                ],
                primary_key=["id"],
            ),
            "orders": Table(
                name="orders",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="user_id", data_type="integer"),
                    Column(name="total", data_type="numeric"),
                ],
                primary_key=["id"],
            ),
        }
    )


def test_emits_valid_csi_without_schema():
    doc = mapping_to_csi(_sample_config(), source_type="postgresql")
    validate_csi(doc)  # raises if invalid
    assert doc["csiVersion"] == CSI_VERSION == "1"


def test_emits_valid_csi_with_schema():
    doc = mapping_to_csi(_sample_config(), _sample_schema(), source_type="postgresql")
    validate_csi(doc)


# ── CSI 1.1: version by rule, strict property-mapping keys ─────────────
#
# CSI 1.1 adds ``path`` (a value inside a nested object) and makes a property
# mapping strict (arango-sparql-py#72, r2g-arango#39). r2g writes only flat
# documents, so it must keep writing "1" and only the keys strict readers allow.


def test_csi_version_for_flat_mappings_is_1():
    entities = {"Account": {"properties": {"name": {"field": "name"}, "dotted": {"field": "a.b"}}}}
    assert csi_version_for(entities) == CSI_VERSION == "1"


def test_csi_version_for_a_path_is_1_1():
    entities = {
        "Account": {"properties": {"name": {"field": "name"}}},
        "Person": {"properties": {"city": {"path": ["address", "city"]}}},
    }
    assert csi_version_for(entities) == CSI_VERSION_NESTED == "1.1"


@pytest.mark.parametrize(
    "entities",
    [{}, {"Account": {}}, {"Account": {"properties": None}}, {"Account": {"properties": {"x": "junk"}}}],
    ids=["no-entities", "no-properties", "null-properties", "non-object-property"],
)
def test_csi_version_for_without_a_path_is_1(entities):
    assert csi_version_for(entities) == "1"


def _csi_from_every_emission_path():
    """One CSI document per way r2g fills a property mapping."""
    lpg = _sample_config()
    lpg.graph_layout = "lpg"
    curated = {
        "curatedLabelsVersion": "1",
        "entities": [{"name": "Contract", "label": "Agreement"}],
    }
    return {
        "pg-without-schema": mapping_to_csi(_sample_config()),
        "pg-with-schema": mapping_to_csi(_sample_config(), _sample_schema()),
        "lpg": mapping_to_csi(lpg, _sample_schema()),
        "collisions-qualified": mapping_to_csi(_colliding_config(), _colliding_schema()),
        "collisions-roles": mapping_to_csi(_colliding_config(), _colliding_schema(), label_policy="roles"),
        "curated-labels": mapping_to_csi(
            _colliding_config(), _colliding_schema(), label_policy="warn", curated_labels=curated
        ),
    }


_EMISSION_PATHS = [
    "pg-without-schema",
    "pg-with-schema",
    "lpg",
    "collisions-qualified",
    "collisions-roles",
    "curated-labels",
]


def test_emission_path_list_is_complete():
    assert list(_csi_from_every_emission_path()) == _EMISSION_PATHS


@pytest.mark.parametrize("path_name", _EMISSION_PATHS)
def test_emitted_property_mappings_hold_only_strict_keys(path_name):
    doc = _csi_from_every_emission_path()[path_name]
    entities = doc["arangoPhysicalMapping"]["entities"]
    assert entities, "fixture emitted no entities, so the check below proves nothing"
    seen = 0
    for entity, spec in entities.items():
        for prop, mapping in spec["properties"].items():
            seen += 1
            unknown = {
                key
                for key in mapping
                if key not in CSI_PROPERTY_MAPPING_KEYS and not key.startswith(CSI_PROPERTY_EXTENSION_PREFIX)
            }
            assert not unknown, f"{entity}.{prop} writes {sorted(unknown)}, which strict CSI readers refuse"
            # r2g never nests, so never ``path``, and a field is a real name.
            assert set(mapping) & {"field", "path"} == {"field"}, f"{entity}.{prop}: {mapping}"
            assert isinstance(mapping["field"], str) and mapping["field"], f"{entity}.{prop}: {mapping}"
    assert seen, "fixture emitted no property mappings, so the check above proves nothing"
    assert doc["csiVersion"] == "1"
    validate_csi(doc)


def test_entities_are_document_collections_only():
    doc = mapping_to_csi(_sample_config())
    names = {e["name"] for e in doc["conceptualModel"]["entities"]}
    # Join table 'placed' must NOT appear as an entity.
    assert names == {"User", "Order"}
    assert set(doc["arangoPhysicalMapping"]["entities"]) == {"User", "Order"}
    for phys in doc["arangoPhysicalMapping"]["entities"].values():
        assert phys["style"] == "COLLECTION"


# ── A join table Auto-Map flags but leaves a document collection ───────────
#
# generate-config flags a two-FK table ``is_join_table`` and keeps it a
# ``document`` collection; the loaders then write it as a vertex plus one edge
# per foreign key. The CSI used to drop that vertex and keep both of its edges,
# so each named a ``fromEntity`` the document never declared.


def _join_table_auto_map():
    from r2g.config import ConfigManager

    schema = Schema(
        tables={
            "posts": Table(
                name="posts",
                columns=[Column(name="id", data_type="integer", is_primary_key=True)],
                primary_key=["id"],
            ),
            "tags": Table(
                name="tags",
                columns=[Column(name="id", data_type="integer", is_primary_key=True)],
                primary_key=["id"],
            ),
            "post_tags": Table(
                name="post_tags",
                columns=[
                    Column(name="post_id", data_type="integer", is_primary_key=True),
                    Column(name="tag_id", data_type="integer", is_primary_key=True),
                    Column(name="created_at", data_type="timestamp"),
                ],
                primary_key=["post_id", "tag_id"],
                foreign_keys=[
                    ForeignKey(columns=["post_id"], foreign_table="posts", foreign_columns=["id"]),
                    ForeignKey(columns=["tag_id"], foreign_table="tags", foreign_columns=["id"]),
                ],
            ),
        }
    )
    config = ConfigManager.generate_default_config(schema)
    jt = config.collections["post_tags"]
    assert jt.is_join_table and jt.collection_type == "document"
    return config, schema


def test_join_table_left_a_document_collection_is_an_entity():
    doc = mapping_to_csi(*_join_table_auto_map())
    validate_csi(doc)
    names = {e["name"] for e in doc["conceptualModel"]["entities"]}
    assert names == {"Post", "Tag", "PostTag"}
    phys = doc["arangoPhysicalMapping"]["entities"]["PostTag"]
    # It describes the collection the loader writes, with its junction metadata.
    assert phys["collectionName"] == "post_tags"
    assert phys["properties"]["createdAt"] == {"field": "created_at"}


def test_every_relationship_endpoint_is_a_declared_entity():
    doc = mapping_to_csi(*_join_table_auto_map())
    names = {e["name"] for e in doc["conceptualModel"]["entities"]}
    rels = {(r["type"], r["fromEntity"], r["toEntity"]) for r in doc["conceptualModel"]["relationships"]}
    assert rels == {("postTagsToPosts", "PostTag", "Post"), ("postTagsToTags", "PostTag", "Tag")}
    assert all(f in names and t in names for _, f, t in rels)


def test_entity_collection_rule_follows_collection_type_not_the_join_flag():
    assert is_entity_collection(CollectionMapping(source_table="t", target_collection="t"))
    assert is_entity_collection(CollectionMapping(source_table="t", target_collection="t", is_join_table=True))
    assert not is_entity_collection(
        CollectionMapping(source_table="t", target_collection="t", collection_type="edge", is_join_table=True)
    )
    assert not is_entity_collection(CollectionMapping(source_table="t", target_collection="t", collection_type="edge"))


def test_join_table_left_a_document_collection_takes_a_curated_label():
    # The curated-label plan sees the same entities the CSI declares.
    config, schema = _join_table_auto_map()
    labels = {"curatedLabelsVersion": "1", "entities": [{"name": "PostTag", "label": "Tagging"}]}
    doc = mapping_to_csi(config, schema, label_policy="warn", curated_labels=labels)
    assert {e["name"] for e in doc["conceptualModel"]["entities"]} == {"Post", "Tag", "Tagging"}


def test_join_table_column_bound_to_a_shared_key_is_a_declared_join_key():
    # A declared join key keeps its generated name on a join table as on any entity.
    config, schema = _join_table_auto_map()
    config.shared_keys = [
        SharedKey(
            key="postId",
            concept="Post",
            bindings=[SharedKeyBinding(source="blog", table="post_tags", column="post_id")],
        )
    ]
    labels = {
        "curatedLabelsVersion": "1",
        "properties": [{"entity": "PostTag", "name": "postId", "label": "article id"}],
    }
    doc = mapping_to_csi(config, schema, label_policy="warn", curated_labels=labels)
    rec = next(r for r in doc["provenance"]["curatedLabels"] if r["name"] == "postId")
    assert rec["resolution"] == "refused"
    assert "join key" in rec["reason"]


def test_relationship_endpoints_resolve_to_target_collections():
    doc = mapping_to_csi(_sample_config())
    rels = doc["conceptualModel"]["relationships"]
    assert len(rels) == 1
    rel = rels[0]
    assert rel["type"] == "placedBy"  # CC-12 lowerCamel
    # from_collection='orders' -> 'Order', to_collection='users' -> 'User'.
    assert rel["fromEntity"] == "Order"
    assert rel["toEntity"] == "User"


def test_physical_relationships_omit_collection_name():
    doc = mapping_to_csi(_sample_config())
    phys = doc["arangoPhysicalMapping"]["relationships"]["placedBy"]
    assert phys["style"] == "DEDICATED_COLLECTION"
    assert phys["edgeCollectionName"] == "placed_by"
    # CSI schema forbids collectionName on relationships.
    assert "collectionName" not in phys


def test_properties_prefer_mapping_then_columns():
    doc = mapping_to_csi(_sample_config(), _sample_schema())
    entities = {e["name"]: e for e in doc["conceptualModel"]["entities"]}
    user_props = [p["name"] for p in entities["User"]["properties"]]
    # Renamed 'full_name' -> 'name' comes first; unmapped columns follow (as-is).
    assert user_props[0] == "name"
    assert "email" in user_props
    assert "id" in user_props
    # The renamed source column 'full_name' must not leak through as itself.
    assert "full_name" not in user_props


def test_properties_without_schema_use_explicit_mappings_only():
    doc = mapping_to_csi(_sample_config())
    entities = {e["name"]: e for e in doc["conceptualModel"]["entities"]}
    assert [p["name"] for p in entities["User"]["properties"]] == ["name"]
    assert [p["name"] for p in entities["Order"]["properties"]] == ["totalCents"]  # CC-12 lowerCamel


def test_provenance_shape():
    doc = mapping_to_csi(
        _sample_config(),
        source_type="mysql",
        source_ref="shopdb",
        producer_version="9.9.9",
        generated_at="2026-07-14T00:00:00+00:00",
        confidence=1.0,
    )
    prov = doc["provenance"]
    assert prov["producer"] == "r2g"
    assert prov["producerVersion"] == "9.9.9"
    assert prov["direction"] == "forward"
    assert prov["source"] == {"kind": "mysql", "ref": "shopdb", "fingerprint": None}
    assert prov["generatedAt"] == "2026-07-14T00:00:00+00:00"
    assert prov["confidence"] == 1.0


def test_provenance_bitemporal_passthrough():
    # Forward producer must pass the four bitemporal keys through so the downstream
    # temporal store (CDF) gets both clocks — converged with arango-schema-analyzer
    # §3.13.5 and the RSA twin. Absent by default (kept out of the pure path).
    plain = mapping_to_csi(_sample_config())["provenance"]
    assert "validTime" not in plain and "transactionTime" not in plain

    prov = mapping_to_csi(
        _sample_config(),
        generated_at="2026-07-14T00:00:00+00:00",
        transaction_time="2026-07-14T00:00:00+00:00",
        valid_time={"from": "2026-01-01T00:00:00+00:00"},
        valid_time_source="fingerprint-continuity",
        predecessor_fingerprint="sha256:prev",
    )["provenance"]
    assert prov["transactionTime"] == "2026-07-14T00:00:00+00:00"
    assert prov["validTime"] == {"from": "2026-01-01T00:00:00+00:00"}
    assert prov["validTimeSource"] == "fingerprint-continuity"
    assert prov["predecessorFingerprint"] == "sha256:prev"
    validate_csi(mapping_to_csi(_sample_config(), valid_time={"from": "2026-01-01T00:00:00+00:00"}))


def test_source_ref_defaults_to_source_schema():
    doc = mapping_to_csi(_sample_config())
    assert doc["provenance"]["source"]["ref"] == "shop"
    assert doc["provenance"]["source"]["kind"] == "relational"


def test_producer_version_defaults_to_installed():
    from r2g import __version__

    doc = mapping_to_csi(_sample_config())
    assert doc["provenance"]["producerVersion"] == __version__


def test_confidence_omitted_by_default():
    doc = mapping_to_csi(_sample_config())
    assert "confidence" not in doc["provenance"]


def test_emitter_is_deterministic():
    cfg = _sample_config()
    assert mapping_to_csi(cfg) == mapping_to_csi(cfg)


def test_csi_schema_loads():
    schema = csi_schema()
    assert schema["required"] == [
        "csiVersion",
        "conceptualModel",
        "arangoPhysicalMapping",
        "provenance",
    ]


def test_invalid_document_rejected():
    import jsonschema

    with pytest.raises(jsonschema.ValidationError):
        validate_csi({"csiVersion": "1"})  # missing required blocks


def test_export_csi_cli(tmp_path):
    import json

    from typer.testing import CliRunner

    from r2g.main import app

    config_path = tmp_path / "mapping.yaml"
    config_path.write_text(
        "source_schema: shop\n"
        "collections:\n"
        "  users:\n"
        "    source_table: users\n"
        "    target_collection: User\n"
        "    field_mappings:\n"
        "      full_name: name\n"
        "  orders:\n"
        "    source_table: orders\n"
        "    target_collection: Order\n"
        "edges:\n"
        "  - edge_collection: placed_by\n"
        "    from_collection: orders\n"
        "    to_collection: users\n"
        "    from_field: user_id\n"
        "    to_field: id\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.csi.json"
    result = CliRunner().invoke(
        app,
        ["export-csi", "--config", str(config_path), "--source-type", "postgresql", "-o", str(out)],
    )
    assert result.exit_code == 0, result.output
    doc = json.loads(out.read_text(encoding="utf-8"))
    validate_csi(doc)
    assert doc["provenance"]["source"]["kind"] == "postgresql"
    assert {e["name"] for e in doc["conceptualModel"]["entities"]} == {"User", "Order"}


def test_export_csi_cli_forwards_rsa_bitemporal_stamps(tmp_path):
    """A schema.json stamped by RSA >= 0.8.0 reaches CSI provenance unchanged."""
    import json

    from typer.testing import CliRunner

    from r2g.main import app

    (tmp_path / "mapping.yaml").write_text(
        "source_schema: shop\ncollections:\n  users:\n    source_table: users\n    target_collection: User\n",
        encoding="utf-8",
    )
    (tmp_path / "schema.json").write_text(
        json.dumps(
            {
                "tables": {"users": {"name": "users", "columns": [{"name": "id", "data_type": "integer"}]}},
                "transaction_time": "2026-09-14T00:00:00+00:00",
                "valid_from": "2026-06-01T00:00:00+00:00",
                "valid_time_source": "catalog",
                "predecessor_fingerprint": "sha256:prev",
            }
        ),
        encoding="utf-8",
    )
    out = tmp_path / "out.csi.json"
    result = CliRunner().invoke(
        app,
        [
            "export-csi",
            "--config",
            str(tmp_path / "mapping.yaml"),
            "--schema",
            str(tmp_path / "schema.json"),
            "--source-type",
            "snowflake",
            "-o",
            str(out),
        ],
    )
    assert result.exit_code == 0, result.output
    prov = json.loads(out.read_text(encoding="utf-8"))["provenance"]
    assert prov["transactionTime"] == "2026-09-14T00:00:00+00:00"
    assert prov["validTime"] == {"from": "2026-06-01T00:00:00+00:00"}
    assert prov["validTimeSource"] == "catalog"  # a catalog-dated schema must validate
    assert prov["predecessorFingerprint"] == "sha256:prev"


def test_export_csi_cli_without_stamps_emits_none(tmp_path):
    """No schema, or an unstamped one, adds no bitemporal keys (byte-stable output)."""
    import json

    from typer.testing import CliRunner

    from r2g.main import app

    (tmp_path / "mapping.yaml").write_text(
        "source_schema: shop\ncollections:\n  users:\n    source_table: users\n    target_collection: User\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.csi.json"
    result = CliRunner().invoke(
        app, ["export-csi", "--config", str(tmp_path / "mapping.yaml"), "-o", str(out)]
    )
    assert result.exit_code == 0, result.output
    prov = json.loads(out.read_text(encoding="utf-8"))["provenance"]
    assert not {"transactionTime", "validTime", "validTimeSource", "predecessorFingerprint"} & prov.keys()


@pytest.mark.parametrize("source", ["catalog", "event", "file", "fingerprint-continuity", "observed"])
def test_csi_schema_admits_every_rsa_valid_time_source(source):
    """RSA's tool contract emits five validTimeSource values; the CSI schema must admit all."""
    doc = mapping_to_csi(
        MappingConfig(
            source_schema="shop",
            collections={"users": CollectionMapping(source_table="users", target_collection="User")},
        ),
        None,
        source_type="postgresql",
        transaction_time="2026-09-14T00:00:00+00:00",
        valid_time={"from": "2026-06-01T00:00:00+00:00"},
        valid_time_source=source,
    )
    validate_csi(doc)


def test_vendored_csi_schema_matches_installed_analyzer():
    """The vendored CSI schema must not drift from arango-schema-analyzer's copy.

    Skipped when the analyzer is not installed. Expected to fail against analyzer
    releases before 0.13.1, which still carry the two-value validTimeSource enum.

    The version is read from distribution metadata, not ``schema_analyzer.__version__``
    — the package does not define that attribute, so the original ``getattr(..., "0")``
    fallback parsed ``(0,)``, compared it against ``(0, 13, 1)``, and xfailed on every
    analyzer ever released. The guard silently disabled itself: measured 2026-09-15 with
    analyzer 0.14.0 installed, this test reported ``xfailed`` and never compared the two
    schemas at all. A drift guard that cannot read a version must still run.
    """
    import json
    from importlib import resources
    from importlib.metadata import PackageNotFoundError
    from importlib.metadata import version as dist_version

    pytest.importorskip("schema_analyzer")

    analyzer_version: tuple[int, ...] = ()
    for dist in ("arangodb-schema-analyzer", "arango-schema-analyzer", "schema-analyzer"):
        try:
            raw = dist_version(dist)
        except PackageNotFoundError:
            continue
        analyzer_version = tuple(
            int(x) for x in raw.split(".")[:3] if x.isdigit()
        )
        break
    # An undeterminable version means compare anyway: skipping here is how the
    # guard went quiet for three analyzer releases.
    if analyzer_version and analyzer_version < (0, 13, 1):
        pytest.xfail("analyzer < 0.13.1 carries the two-value validTimeSource enum")
    theirs = json.loads(
        resources.files("schema_analyzer.csi.v1").joinpath("csi.schema.json").read_text(encoding="utf-8")
    )
    ours = json.loads(
        resources.files("r2g.schemas").joinpath("csi_v1.schema.json").read_text(encoding="utf-8")
    )
    theirs.pop("description", None)
    ours.pop("description", None)
    assert ours == theirs


# ── Attribute-label collisions ───────────────────────────────────────
#
# Two entities in ONE document emitting the same attribute label make that word
# ambiguous for any consumer deriving a flat vocabulary from it, and the
# distinction cannot be recovered downstream because it was discarded here.


def _colliding_config(**overrides) -> MappingConfig:
    """Contract + Opportunity, both carrying renewal_date / product_scope."""
    return MappingConfig(
        source_schema="crm",
        collections={
            "contracts": CollectionMapping(
                source_table="contracts", target_collection="Contract"
            ),
            "opportunities": CollectionMapping(
                source_table="opportunities", target_collection="Opportunity"
            ),
        },
        **overrides,
    )


def _colliding_schema() -> Schema:
    shared = ["renewal_date", "product_scope"]
    return Schema(
        tables={
            "contracts": Table(
                name="contracts",
                columns=[Column(name="id", data_type="integer", is_primary_key=True)]
                + [Column(name=c, data_type="text") for c in shared]
                + [Column(name="auto_renew", data_type="boolean")],
                primary_key=["id"],
            ),
            "opportunities": Table(
                name="opportunities",
                columns=[Column(name="id", data_type="integer", is_primary_key=True)]
                + [Column(name=c, data_type="text") for c in shared]
                + [Column(name="amount_usd", data_type="numeric")],
                primary_key=["id"],
            ),
        }
    )


def _labels_by_entity(doc):
    return {e["name"]: [p["name"] for p in e["properties"]] for e in doc["conceptualModel"]["entities"]}


def _all_labels(doc):
    out = []
    for e in doc["conceptualModel"]["entities"]:
        out += [p["name"] for p in e["properties"]]
    return out


class TestLabelCollisions:
    def test_default_policy_qualifies_every_occurrence(self):
        doc = mapping_to_csi(_colliding_config(), _colliding_schema())
        labels = _labels_by_entity(doc)
        assert "contractRenewalDate" in labels["Contract"]
        assert "opportunityRenewalDate" in labels["Opportunity"]
        # Neither entity keeps the bare label: leaving one behind would preserve
        # exactly the false confidence this fixes.
        assert "renewalDate" not in _all_labels(doc)

    def test_no_duplicate_labels_remain(self):
        doc = mapping_to_csi(_colliding_config(), _colliding_schema())
        labels = _all_labels(doc)
        dupes = {n for n in labels if labels.count(n) > 1}
        # Nothing survives here: 'id' qualifies cleanly to contractId /
        # opportunityId because neither name is otherwise taken. (When one IS
        # taken the group is refused instead — see the cascade test.)
        assert dupes == set()
        assert {"contractId", "opportunityId"} <= set(labels)

    def test_physical_mapping_still_resolves_to_the_source_column(self):
        """The rename is conceptual only — the stored field must not move."""
        doc = mapping_to_csi(_colliding_config(), _colliding_schema())
        phys = doc["arangoPhysicalMapping"]["entities"]
        assert phys["Contract"]["properties"]["contractRenewalDate"]["field"] == "renewal_date"
        assert phys["Opportunity"]["properties"]["opportunityRenewalDate"]["field"] == "renewal_date"
        assert phys["Contract"]["collectionName"] == "Contract"

    def test_every_conceptual_property_has_a_physical_field(self):
        doc = mapping_to_csi(_colliding_config(), _colliding_schema())
        phys = doc["arangoPhysicalMapping"]["entities"]
        for entity in doc["conceptualModel"]["entities"]:
            mapped = phys[entity["name"]]["properties"]
            for prop in entity["properties"]:
                assert prop["name"] in mapped, (entity["name"], prop["name"])

    def test_collisions_are_recorded_in_provenance(self):
        doc = mapping_to_csi(_colliding_config(), _colliding_schema())
        recs = {r["label"]: r for r in doc["provenance"]["labelCollisions"]}
        assert recs["renewalDate"]["resolution"] == "qualified"
        assert recs["renewalDate"]["entities"] == ["Contract", "Opportunity"]
        assert recs["renewalDate"]["renamedTo"] == {
            "Contract": "contractRenewalDate",
            "Opportunity": "opportunityRenewalDate",
        }

    def test_warn_policy_records_without_renaming(self):
        doc = mapping_to_csi(_colliding_config(), _colliding_schema(), label_policy="warn")
        assert "renewalDate" in _labels_by_entity(doc)["Contract"]
        assert "renewalDate" in _labels_by_entity(doc)["Opportunity"]
        recs = {r["label"]: r for r in doc["provenance"]["labelCollisions"]}
        assert recs["renewalDate"]["resolution"] == "reported"
        assert "renamedTo" not in recs["renewalDate"]

    def test_off_policy_skips_the_check_entirely(self):
        doc = mapping_to_csi(_colliding_config(), _colliding_schema(), label_policy="off")
        assert "renewalDate" in _labels_by_entity(doc)["Contract"]
        assert "labelCollisions" not in doc["provenance"]

    def test_qualification_that_would_create_a_new_collision_is_refused(self):
        """``Account.id`` must not become ``accountId`` when that is taken.

        Trading one ambiguity for another is worse than reporting it, because
        the new one looks deliberate.
        """
        config = MappingConfig(
            collections={
                "accounts": CollectionMapping(source_table="accounts", target_collection="Account"),
                "contacts": CollectionMapping(source_table="contacts", target_collection="Contact"),
            }
        )
        schema = Schema(
            tables={
                "accounts": Table(
                    name="accounts",
                    columns=[
                        Column(name="id", data_type="integer", is_primary_key=True),
                        # The business key already owns the qualified form.
                        Column(name="account_id", data_type="text"),
                    ],
                    primary_key=["id"],
                ),
                "contacts": Table(
                    name="contacts",
                    columns=[Column(name="id", data_type="integer", is_primary_key=True)],
                    primary_key=["id"],
                ),
            }
        )
        doc = mapping_to_csi(config, schema)
        rec = next(r for r in doc["provenance"]["labelCollisions"] if r["label"] == "id")
        assert rec["resolution"] == "unresolved"
        assert "accountId" in rec["reason"]
        # Left untouched rather than mangled.
        assert _labels_by_entity(doc)["Account"].count("id") == 1

    def test_collision_free_document_records_nothing(self):
        """Documents with no shared label keep their historical bytes."""
        config = MappingConfig(
            collections={
                "books": CollectionMapping(source_table="books", target_collection="Book"),
                "shelves": CollectionMapping(source_table="shelves", target_collection="Shelf"),
            }
        )
        schema = Schema(
            tables={
                "books": Table(
                    name="books", columns=[Column(name="isbn", data_type="text")], primary_key=[]
                ),
                "shelves": Table(
                    name="shelves", columns=[Column(name="aisle", data_type="text")], primary_key=[]
                ),
            }
        )
        doc = mapping_to_csi(config, schema)
        assert "labelCollisions" not in doc["provenance"]

    def test_output_is_independent_of_collection_insertion_order(self):
        forward = mapping_to_csi(_colliding_config(), _colliding_schema())
        reversed_config = MappingConfig(
            source_schema="crm",
            collections={
                "opportunities": CollectionMapping(
                    source_table="opportunities", target_collection="Opportunity"
                ),
                "contracts": CollectionMapping(
                    source_table="contracts", target_collection="Contract"
                ),
            },
        )
        backward = mapping_to_csi(reversed_config, _colliding_schema())
        assert _labels_by_entity(forward) == _labels_by_entity(backward)
        assert forward["provenance"]["labelCollisions"] == backward["provenance"]["labelCollisions"]

    def test_document_still_validates_against_the_csi_schema(self):
        doc = mapping_to_csi(_colliding_config(), _colliding_schema(), source_type="postgresql")
        validate_csi(doc)

    def test_contested_qualified_name_is_resolved_deterministically(self):
        """Two collision groups can want the SAME qualified name.

        ``User.nameId`` and ``UserName.id`` both qualify to ``userNameId``.
        Which group wins must not depend on mapping insertion order, so groups
        are processed in sorted label order: ``id`` claims it, ``nameId`` is
        then refused rather than silently overwriting it.
        """
        tables = {
            "users": Table(
                name="users",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="name_id", data_type="integer"),
                ],
                primary_key=["id"],
            ),
            "user_names": Table(
                name="user_names",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="label", data_type="text"),
                ],
                primary_key=["id"],
            ),
            "audits": Table(
                name="audits",
                columns=[
                    Column(name="name_id", data_type="integer"),
                    Column(name="note", data_type="text"),
                ],
                primary_key=[],
            ),
        }
        names = {"users": "User", "user_names": "UserName", "audits": "Audit"}

        def build(order):
            return mapping_to_csi(
                MappingConfig(
                    collections={
                        t: CollectionMapping(source_table=t, target_collection=names[t])
                        for t in order
                    }
                ),
                Schema(tables=tables),
            )

        forward = build(["users", "user_names", "audits"])
        backward = build(["audits", "user_names", "users"])

        recs = {r["label"]: r for r in forward["provenance"]["labelCollisions"]}
        assert recs["id"]["resolution"] == "qualified"
        assert recs["id"]["renamedTo"]["UserName"] == "userNameId"
        assert recs["nameId"]["resolution"] == "unresolved"
        assert "userNameId" in recs["nameId"]["reason"]

        # The contested name is claimed by exactly one entity, either way round.
        assert _labels_by_entity(forward) == _labels_by_entity(backward)
        assert _all_labels(forward).count("userNameId") == 1


class TestRolesPolicy:
    """`--label-policy roles`: classify a collision, then fit the remedy to it.

    A collision on a plain column (`renewalDate`) and one on a foreign key
    (`accountId`) are not the same problem, and qualifying both alike produces
    `accountAccountId` — a name that reads as a business attribute when the
    thing it describes is a join.
    """

    def _crm(self):
        """Account owns account_id; three others reference it. All share `id`."""
        config = MappingConfig(
            collections={
                "accounts": CollectionMapping(source_table="accounts", target_collection="Account"),
                "contacts": CollectionMapping(source_table="contacts", target_collection="Contact"),
                "contracts": CollectionMapping(source_table="contracts", target_collection="Contract"),
            },
            edges=[
                EdgeDefinition(
                    edge_collection="contacts_of_account",
                    from_collection="contacts", to_collection="accounts",
                    from_fields=["account_id"], to_fields=["account_id"],
                ),
                EdgeDefinition(
                    edge_collection="contracts_of_account",
                    from_collection="contracts", to_collection="accounts",
                    from_fields=["account_id"], to_fields=["account_id"],
                ),
            ],
        )
        schema = Schema(
            tables={
                "accounts": Table(
                    name="accounts",
                    columns=[
                        Column(name="id", data_type="integer", is_primary_key=True),
                        Column(name="account_id", data_type="text"),
                        Column(name="account_name", data_type="text"),
                    ],
                    primary_key=["id"],
                ),
                "contacts": Table(
                    name="contacts",
                    columns=[
                        Column(name="id", data_type="integer", is_primary_key=True),
                        Column(name="account_id", data_type="text"),
                        # A single-owner business key that blocks `id` from
                        # qualifying to `contactId` — exactly the situation in
                        # the real CRM catalog.
                        Column(name="contact_id", data_type="text"),
                        Column(name="renewal_date", data_type="date"),
                    ],
                    primary_key=["id"],
                ),
                "contracts": Table(
                    name="contracts",
                    columns=[
                        Column(name="id", data_type="integer", is_primary_key=True),
                        Column(name="account_id", data_type="text"),
                        Column(name="renewal_date", data_type="date"),
                    ],
                    primary_key=["id"],
                ),
            }
        )
        return config, schema

    def test_foreign_key_owner_keeps_the_bare_label(self):
        doc = mapping_to_csi(*self._crm(), label_policy="roles")
        labels = _labels_by_entity(doc)
        assert "accountId" in labels["Account"]
        assert "contactAccountId" in labels["Contact"]
        assert "contractAccountId" in labels["Contract"]
        # The stutter this policy exists to prevent.
        assert "accountAccountId" not in _all_labels(doc)

    def test_primary_key_becomes_identity_not_an_attribute(self):
        doc = mapping_to_csi(*self._crm(), label_policy="roles")
        assert "id" not in _all_labels(doc)
        rec = next(r for r in doc["provenance"]["labelCollisions"] if r["label"] == "id")
        assert rec["resolution"] == "identity"
        assert rec["kind"] == "structural"
        # Dropped from the physical mapping too, so the two stay in step.
        assert "id" not in doc["arangoPhysicalMapping"]["entities"]["Account"]["properties"]

    def test_semantic_collision_still_qualifies_every_occurrence(self):
        doc = mapping_to_csi(*self._crm(), label_policy="roles")
        labels = _labels_by_entity(doc)
        assert "contactRenewalDate" in labels["Contact"]
        assert "contractRenewalDate" in labels["Contract"]
        assert "renewalDate" not in _all_labels(doc)
        rec = next(r for r in doc["provenance"]["labelCollisions"] if r["label"] == "renewalDate")
        assert rec["kind"] == "semantic"

    def test_roles_leaves_no_duplicate_labels_where_qualify_does(self):
        config, schema = self._crm()
        q = _all_labels(mapping_to_csi(config, schema, label_policy="qualify"))
        r = _all_labels(mapping_to_csi(config, schema, label_policy="roles"))
        assert {n for n in q if q.count(n) > 1} == {"id"}  # qualify cannot fix it
        assert {n for n in r if r.count(n) > 1} == set()

    def test_owner_is_recorded(self):
        doc = mapping_to_csi(*self._crm(), label_policy="roles")
        rec = next(r for r in doc["provenance"]["labelCollisions"] if r["label"] == "accountId")
        assert rec["owner"] == "Account"
        assert rec["kind"] == "structural"
        assert "Account" not in rec["renamedTo"]

    def test_role_is_read_through_field_mappings(self):
        """A renamed key column must still classify as a key.

        The physical `field` is the *stored* attribute, which diverges from the
        source column whenever field_mappings renames one (pagila stores
        `actorId` for `actor_id`). Classifying off `field` would silently mark
        every renamed key as a plain attribute.
        """
        config = MappingConfig(
            collections={
                "actors": CollectionMapping(
                    source_table="actors", target_collection="Actor",
                    field_mappings={"actor_id": "actorId"},
                ),
                "film_actors": CollectionMapping(
                    source_table="film_actors", target_collection="FilmActor",
                    field_mappings={"actor_id": "actorId"},
                ),
            }
        )
        schema = Schema(
            tables={
                "actors": Table(
                    name="actors",
                    columns=[Column(name="actor_id", data_type="integer", is_primary_key=True)],
                    primary_key=["actor_id"],
                ),
                "film_actors": Table(
                    name="film_actors",
                    columns=[
                        Column(name="actor_id", data_type="integer", is_primary_key=True),
                        Column(name="film_id", data_type="integer", is_primary_key=True),
                    ],
                    primary_key=["actor_id", "film_id"],
                ),
            }
        )
        doc = mapping_to_csi(config, schema, label_policy="roles")
        rec = next(r for r in doc["provenance"]["labelCollisions"] if r["label"] == "actorId")
        assert rec["kind"] == "structural", "renamed key column misclassified as semantic"

    def test_seam_word_is_not_repeated(self):
        """`FilmCategory` + `categoryId` must not become `filmCategoryCategoryId`."""
        config = MappingConfig(
            collections={
                "categories": CollectionMapping(
                    source_table="categories", target_collection="Category"
                ),
                "film_categories": CollectionMapping(
                    source_table="film_categories", target_collection="FilmCategory"
                ),
            }
        )
        schema = Schema(
            tables={
                "categories": Table(
                    name="categories",
                    columns=[Column(name="category_id", data_type="integer", is_primary_key=True)],
                    primary_key=["category_id"],
                ),
                "film_categories": Table(
                    name="film_categories",
                    columns=[
                        Column(name="category_id", data_type="integer"),
                        Column(name="note", data_type="text"),
                    ],
                    primary_key=[],
                ),
            }
        )
        doc = mapping_to_csi(config, schema, label_policy="roles")
        labels = _all_labels(doc)
        assert "filmCategoryCategoryId" not in labels
        assert "filmCategoryId" in labels
        assert "categoryId" in _labels_by_entity(doc)["Category"]

    def test_physical_fields_are_never_invented_or_moved(self):
        config, schema = self._crm()
        base = mapping_to_csi(config, schema, label_policy="off")
        doc = mapping_to_csi(config, schema, label_policy="roles")
        for entity in doc["conceptualModel"]["entities"]:
            name = entity["name"]
            after = doc["arangoPhysicalMapping"]["entities"][name]["properties"]
            before = base["arangoPhysicalMapping"]["entities"][name]["properties"]
            assert {v["field"] for v in after.values()} <= {v["field"] for v in before.values()}
            # Conceptual and physical property sets stay in step.
            assert {p["name"] for p in entity["properties"]} == set(after)

    def test_document_still_validates(self):
        doc = mapping_to_csi(*self._crm(), source_type="postgresql", label_policy="roles")
        validate_csi(doc)

    def test_qualify_policy_is_unchanged_by_the_new_code(self):
        """The historical default must stay byte-identical and stutter-free-free."""
        doc = mapping_to_csi(*self._crm(), label_policy="qualify")
        assert "accountAccountId" in _all_labels(doc)  # blunt, as documented
        rec = next(r for r in doc["provenance"]["labelCollisions"] if r["label"] == "accountId")
        assert "kind" not in rec and "owner" not in rec

    def test_owner_found_by_primary_key_when_the_name_does_not_match(self):
        """The PK tier of owner selection, isolated.

        `ownerId` lives on a table called `people`, so the name-match tier finds
        nothing; only "who holds it as a primary key" identifies the owner. If
        that tier is skipped, Person gets qualified too and loses the bare label.
        """
        config = MappingConfig(
            collections={
                "people": CollectionMapping(source_table="people", target_collection="Person"),
                "assets": CollectionMapping(source_table="assets", target_collection="Asset"),
            },
            edges=[
                EdgeDefinition(
                    edge_collection="assets_of_person",
                    from_collection="assets", to_collection="people",
                    from_fields=["owner_id"], to_fields=["owner_id"],
                ),
            ],
        )
        schema = Schema(
            tables={
                "people": Table(
                    name="people",
                    columns=[
                        Column(name="owner_id", data_type="integer", is_primary_key=True),
                        Column(name="full_name", data_type="text"),
                    ],
                    primary_key=["owner_id"],
                ),
                "assets": Table(
                    name="assets",
                    columns=[
                        Column(name="owner_id", data_type="integer"),
                        Column(name="tag", data_type="text"),
                    ],
                    primary_key=[],
                ),
            }
        )
        doc = mapping_to_csi(config, schema, label_policy="roles")
        rec = next(r for r in doc["provenance"]["labelCollisions"] if r["label"] == "ownerId")
        assert rec["owner"] == "Person"
        assert "ownerId" in _labels_by_entity(doc)["Person"]
        assert "assetOwnerId" in _labels_by_entity(doc)["Asset"]


class TestJoinKeyExemption:
    """A declared P6.7 cross-source join key is the federation spine: its shared
    label must survive on every binding entity, so the collision resolver exempts
    it from renaming under *every* policy. Qualifying it would rename the very
    label ``conceptualModel.joinKeys`` references, yielding a CSI whose join key
    points at a property no entity still holds (the trap the CDF hit).
    """

    def _crm_with_join_key(self):
        """`accountId` is a DECLARED shared key across three entities that all
        carry `account_id` — so left un-exempted it collides and gets renamed."""
        config = MappingConfig(
            collections={
                "accounts": CollectionMapping(source_table="accounts", target_collection="Account"),
                "contacts": CollectionMapping(source_table="contacts", target_collection="Contact"),
                "contracts": CollectionMapping(source_table="contracts", target_collection="Contract"),
            },
            shared_keys=[
                SharedKey(
                    key="accountId",
                    concept="Account",
                    hub_kind="entity",
                    hub_source="crm",
                    hub_table="accounts",
                    hub_column="account_id",
                    bindings=[
                        SharedKeyBinding(source="crm", table="accounts", column="account_id"),
                        SharedKeyBinding(source="crm", table="contacts", column="account_id"),
                        SharedKeyBinding(source="crm", table="contracts", column="account_id"),
                    ],
                    confidence=1.0,
                    method="test",
                )
            ],
        )
        schema = Schema(
            tables={
                "accounts": Table(
                    name="accounts",
                    columns=[
                        Column(name="id", data_type="integer", is_primary_key=True),
                        Column(name="account_id", data_type="text"),
                        Column(name="account_name", data_type="text"),
                    ],
                    primary_key=["id"],
                ),
                "contacts": Table(
                    name="contacts",
                    columns=[
                        Column(name="id", data_type="integer", is_primary_key=True),
                        Column(name="account_id", data_type="text"),
                        Column(name="contact_id", data_type="text"),
                    ],
                    primary_key=["id"],
                ),
                "contracts": Table(
                    name="contracts",
                    columns=[
                        Column(name="id", data_type="integer", is_primary_key=True),
                        Column(name="account_id", data_type="text"),
                    ],
                    primary_key=["id"],
                ),
            }
        )
        return config, schema

    def test_join_key_stays_bare_under_default_qualify(self):
        # Default policy is "qualify" — which, left to itself, renames the spine
        # to accountAccountId / contactAccountId and desyncs it from joinKeys.
        doc = mapping_to_csi(*self._crm_with_join_key())
        labels = _labels_by_entity(doc)
        assert "accountId" in labels["Account"]
        assert "accountId" in labels["Contact"]
        assert "accountId" in labels["Contract"]
        assert "accountAccountId" not in _all_labels(doc)
        assert "contactAccountId" not in _all_labels(doc)

    def test_join_key_collision_is_recorded_as_join_key(self):
        doc = mapping_to_csi(*self._crm_with_join_key())
        rec = next(r for r in doc["provenance"]["labelCollisions"] if r["label"] == "accountId")
        assert rec["resolution"] == "join_key"

    def test_joinkeys_reference_a_label_the_entities_still_hold(self):
        # The consistency the exemption guarantees: every joinKeys entry names a
        # conceptual key that survives on each of its bound entities.
        doc = mapping_to_csi(*self._crm_with_join_key())
        join_keys = doc["conceptualModel"]["joinKeys"]
        assert any(k["key"] == "accountId" for k in join_keys)
        labels = _labels_by_entity(doc)
        for k in join_keys:
            for b in k["bindings"]:
                if "entity" in b:
                    assert "accountId" in labels[b["entity"]]

    def test_exemption_holds_under_roles_too(self):
        # Even the smarter roles policy (which would keep Account bare but rename
        # Contact -> contactAccountId) must not split a DECLARED join key.
        doc = mapping_to_csi(*self._crm_with_join_key(), label_policy="roles")
        labels = _labels_by_entity(doc)
        assert "accountId" in labels["Account"]
        assert "accountId" in labels["Contact"]
        assert "contactAccountId" not in _all_labels(doc)

    def test_without_the_declaration_qualify_is_unchanged(self):
        # Regression guard: drop the shared_keys declaration and the blunt default
        # is untouched — accountId still qualifies to accountAccountId.
        config, schema = self._crm_with_join_key()
        config.shared_keys = []
        doc = mapping_to_csi(config, schema)  # default qualify
        assert "accountAccountId" in _all_labels(doc)


_ABSENT = object()  # distinguishes "field omitted" from any value it could hold


# ── Declared uniqueness ──────────────────────────────────────────────
#
# `uniqueConstraints` states which property sets identify an entity uniquely, so
# a federated consumer can join and group on them without being told by hand.
#
# Nothing in the CSI schema guards it. An entity object is `additionalProperties:
# true`, so the field arrives unrecognised and passes however it is shaped — and
# the schema belongs to arango-schema-analyzer, which r2g only vendors (see
# test_vendored_csi_schema_matches_installed_analyzer), so r2g cannot add it
# there unilaterally. A malformed declaration is therefore not a document that
# fails; it is a document whose declared uniqueness is wrong, surfacing as a
# downstream join that groups incorrectly, arbitrarily far from the cause.


class TestJoinEntityYields:
    """A join table exported as an entity never renames a property of an entity it links.

    Its foreign-key columns are named after the keys they reference and its
    junction metadata often repeats a linked table's column, so its labels
    collide with the linked entities' labels. Those entities keep exactly the
    names they had before join tables were exported, and the join entity's
    colliding labels are qualified on it alone.
    """

    @staticmethod
    def _table(name, cols, pk, fks=()):
        return Table(
            name=name,
            columns=[Column(name=c, data_type=t, is_primary_key=c in pk) for c, t in cols],
            primary_key=list(pk),
            foreign_keys=[ForeignKey(columns=[c], foreign_table=ft, foreign_columns=[fc]) for c, ft, fc in fks],
        )

    def _enrollment(self, *, course_created_at=False, second_join_table=False, student_created_at=True):
        from r2g.config import ConfigManager

        course_cols = [("course_id", "integer"), ("title", "text")]
        if course_created_at:
            course_cols.append(("created_at", "timestamp"))
        student_cols = [("student_id", "integer"), ("full_name", "text")]
        if student_created_at:
            student_cols.append(("created_at", "timestamp"))
        tables = {
            "student": self._table("student", student_cols, ["student_id"]),
            "course": self._table("course", course_cols, ["course_id"]),
            "enrollment": self._table(
                "enrollment",
                [("student_id", "integer"), ("course_id", "integer"), ("created_at", "timestamp")],
                ["student_id", "course_id"],
                [("student_id", "student", "student_id"), ("course_id", "course", "course_id")],
            ),
        }
        if second_join_table:
            tables["tag"] = self._table("tag", [("tag_id", "integer"), ("name", "text")], ["tag_id"])
            tables["course_tag"] = self._table(
                "course_tag",
                [("course_id", "integer"), ("tag_id", "integer"), ("created_at", "timestamp")],
                ["course_id", "tag_id"],
                [("course_id", "course", "course_id"), ("tag_id", "tag", "tag_id")],
            )
        schema = Schema(tables=tables)
        config = ConfigManager.generate_default_config(schema)
        for name in ("enrollment", "course_tag"):
            if name in config.collections:
                assert config.collections[name].is_join_table
                assert config.collections[name].collection_type == "document"
        return config, schema

    @staticmethod
    def _props(doc):
        return {e["name"]: sorted(p["name"] for p in e["properties"]) for e in doc["conceptualModel"]["entities"]}

    @staticmethod
    def _join_record(doc, label):
        """The second pass's record for ``label`` (the first pass's records carry no such kind)."""
        return next(
            r
            for r in doc["provenance"]["labelCollisions"]
            if r["label"] == label and r.get("kind") == "join_entity"
        )

    @staticmethod
    def _without_join_tables(config):
        """The same mapping with every flagged join table left out, as main exported it."""
        trimmed = config.model_copy(deep=True)
        trimmed.collections = {k: cm for k, cm in config.collections.items() if not cm.is_join_table}
        return trimmed

    @pytest.mark.parametrize("policy", ["qualify", "roles", "warn"])
    @pytest.mark.parametrize(
        "shape",
        [{}, {"course_created_at": True}, {"course_created_at": True, "second_join_table": True}],
        ids=["linked-keys-and-metadata", "linked-entities-already-collide", "two-join-tables"],
    )
    def test_linked_entities_keep_the_names_they_had_without_the_join_table(self, policy, shape):
        config, schema = self._enrollment(**shape)
        with_join = self._props(mapping_to_csi(config, schema, label_policy=policy))
        without = self._props(mapping_to_csi(self._without_join_tables(config), schema, label_policy=policy))
        assert {name: with_join[name] for name in without} == without

    def test_qualify_renames_the_join_entity_only(self):
        config, schema = self._enrollment()
        doc = mapping_to_csi(config, schema, label_policy="qualify")
        validate_csi(doc)
        props = self._props(doc)
        assert props["Student"] == ["createdAt", "fullName", "studentId"]
        assert props["Course"] == ["courseId", "title"]
        assert props["Enrollment"] == ["enrollmentCourseId", "enrollmentCreatedAt", "enrollmentStudentId"]
        # The physical mapping follows the rename, so each name still resolves to its column.
        phys = doc["arangoPhysicalMapping"]["entities"]["Enrollment"]["properties"]
        assert phys["enrollmentStudentId"] == {"field": "student_id"}
        assert phys["enrollmentCreatedAt"] == {"field": "created_at"}
        rec = next(r for r in doc["provenance"]["labelCollisions"] if r["label"] == "studentId")
        assert rec == {
            "label": "studentId",
            "entities": ["Enrollment", "Student"],
            "kind": "join_entity",
            "resolution": "qualified",
            "renamedTo": {"Enrollment": "enrollmentStudentId"},
            "owner": "Student",
            "detail": rec["detail"],
        }

    def test_roles_collapses_the_seam_on_the_join_entity(self):
        config, schema = self._enrollment(second_join_table=True)
        props = self._props(mapping_to_csi(config, schema, label_policy="roles"))
        # ``courseTag`` + ``tagId`` collapses to ``courseTagId``, as ``roles`` does for
        # any referencing entity (``filmCategory`` + ``categoryId`` → ``filmCategoryId``).
        assert props["CourseTag"] == ["courseTagCourseId", "courseTagCreatedAt", "courseTagId"]
        assert props["Tag"] == ["name", "tagId"]

    def test_warn_records_and_renames_nothing(self):
        config, schema = self._enrollment()
        doc = mapping_to_csi(config, schema, label_policy="warn")
        assert self._props(doc)["Enrollment"] == ["courseId", "createdAt", "studentId"]
        recs = [r for r in doc["provenance"]["labelCollisions"] if r.get("kind") == "join_entity"]
        assert {r["label"] for r in recs} == {"courseId", "createdAt", "studentId"}
        assert {r["resolution"] for r in recs} == {"reported"}

    def test_off_skips_the_join_entity_too(self):
        config, schema = self._enrollment()
        doc = mapping_to_csi(config, schema, label_policy="off")
        assert self._props(doc)["Enrollment"] == ["courseId", "createdAt", "studentId"]
        assert "labelCollisions" not in doc["provenance"]

    def test_two_join_entities_sharing_a_label_both_yield(self):
        config, schema = self._enrollment(second_join_table=True)
        doc = mapping_to_csi(config, schema, label_policy="qualify")
        rec = self._join_record(doc, "courseId")
        assert rec["renamedTo"] == {"CourseTag": "courseTagCourseId", "Enrollment": "enrollmentCourseId"}
        assert rec["owner"] == "Course"
        assert self._props(doc)["Course"] == ["courseId", "title"]

    def test_two_join_entities_sharing_a_label_no_linked_entity_holds_are_both_qualified(self):
        # Only the two join tables carry ``created_at``: left bare, the label would
        # be shared by two entities, so both are qualified.
        config, schema = self._enrollment(second_join_table=True, student_created_at=False)
        doc = mapping_to_csi(config, schema, label_policy="qualify")
        rec = next(r for r in doc["provenance"]["labelCollisions"] if r["label"] == "createdAt")
        assert rec["entities"] == ["CourseTag", "Enrollment"]
        assert rec["renamedTo"] == {"CourseTag": "courseTagCreatedAt", "Enrollment": "enrollmentCreatedAt"}
        assert "owner" not in rec

    def test_a_label_several_linked_entities_keep_names_no_owner(self):
        # ``code`` on Alpha and Beta cannot be qualified (``alphaCode`` is taken), so
        # both keep it; the join entity's ``code`` yields, and no single owner is named.
        from r2g.config import ConfigManager

        schema = Schema(
            tables={
                "alpha": self._table(
                    "alpha", [("id", "integer"), ("code", "text"), ("alpha_code", "text")], ["id"]
                ),
                "beta": self._table("beta", [("id", "integer"), ("code", "text")], ["id"]),
                "alpha_beta": self._table(
                    "alpha_beta",
                    [("alpha_id", "integer"), ("beta_id", "integer"), ("code", "text")],
                    ["alpha_id", "beta_id"],
                    [("alpha_id", "alpha", "id"), ("beta_id", "beta", "id")],
                ),
            }
        )
        config = ConfigManager.generate_default_config(schema)
        config.collections["alpha_beta"].is_join_table = True
        config.collections["alpha_beta"].collection_type = "document"
        doc = mapping_to_csi(config, schema, label_policy="qualify")
        props = self._props(doc)
        assert "code" in props["Alpha"] and "code" in props["Beta"]
        rec = self._join_record(doc, "code")
        assert rec["entities"] == ["Alpha", "AlphaBeta", "Beta"]
        assert rec["renamedTo"] == {"AlphaBeta": "alphaBetaCode"}
        assert "owner" not in rec

    def test_a_collision_the_linked_entities_resolve_first_is_then_yielded_to(self):
        # Issue #88's schema: ``id`` on Author and Book is qualified to
        # ``authorId`` / ``bookId`` as before, and the join entity's own
        # ``authorId`` / ``bookId`` give way to those names.
        from r2g.config import ConfigManager

        schema = Schema(
            tables={
                "author": self._table("author", [("id", "integer"), ("name", "text")], ["id"]),
                "book": self._table("book", [("id", "integer"), ("title", "text")], ["id"]),
                "book_author": self._table(
                    "book_author",
                    [("book_id", "integer"), ("author_id", "integer")],
                    ["book_id", "author_id"],
                    [("book_id", "book", "id"), ("author_id", "author", "id")],
                ),
            }
        )
        config = ConfigManager.generate_default_config(schema)
        props = self._props(mapping_to_csi(config, schema, label_policy="qualify"))
        assert props["Author"] == ["authorId", "name"]
        assert props["Book"] == ["bookId", "title"]
        assert props["BookAuthor"] == ["bookAuthorAuthorId", "bookAuthorBookId"]

    def test_a_rename_that_would_collide_is_refused_on_the_join_entity(self):
        config, schema = self._enrollment()
        # The join table already has a column the rename would produce.
        schema.tables["enrollment"].columns.append(Column(name="enrollment_student_id", data_type="integer"))
        doc = mapping_to_csi(config, schema, label_policy="qualify")
        rec = self._join_record(doc, "studentId")
        assert rec["resolution"] == "unresolved"
        assert "enrollmentStudentId" in rec["reason"]
        props = self._props(doc)
        assert props["Student"] == ["createdAt", "fullName", "studentId"]
        assert "studentId" in props["Enrollment"]

    @staticmethod
    def _bind_join_table_created_at(config):
        config.shared_keys = [
            SharedKey(
                key="createdAt",
                concept="Enrollment",
                bindings=[SharedKeyBinding(source="school", table="enrollment", column="created_at")],
            )
        ]

    def test_a_join_key_bound_on_the_join_table_stays_bare_on_every_entity(self):
        # Student and the join entity share ``createdAt``; a shared key bound on the
        # join table's column keeps it bare on both, as a join key always is.
        config, schema = self._enrollment()
        self._bind_join_table_created_at(config)
        doc = mapping_to_csi(config, schema, label_policy="qualify")
        props = self._props(doc)
        assert props["Student"] == ["createdAt", "fullName", "studentId"]
        assert "createdAt" in props["Enrollment"]
        rec = self._join_record(doc, "createdAt")
        assert rec["resolution"] == "join_key"
        assert rec["entities"] == ["Enrollment", "Student"]

    def test_a_join_key_bound_only_on_the_join_table_does_not_exempt_the_linked_entities(self):
        # Student and Course collide on ``createdAt`` among themselves and are
        # qualified, exactly as when the join table was not exported: a binding on
        # the join table alone does not turn their collision into a join key.
        config, schema = self._enrollment(course_created_at=True)
        self._bind_join_table_created_at(config)
        doc = mapping_to_csi(config, schema, label_policy="qualify")
        props = self._props(doc)
        assert props["Student"] == ["fullName", "studentCreatedAt", "studentId"]
        assert props["Course"] == ["courseCreatedAt", "courseId", "title"]
        assert "createdAt" in props["Enrollment"]

    def test_a_curated_join_entity_name_is_the_one_that_yields(self):
        config, schema = self._enrollment()
        labels = {"curatedLabelsVersion": "1", "entities": [{"name": "Enrollment", "label": "Registration"}]}
        props = self._props(mapping_to_csi(config, schema, label_policy="qualify", curated_labels=labels))
        assert props["Student"] == ["createdAt", "fullName", "studentId"]
        assert props["Registration"] == [
            "registrationCourseId",
            "registrationCreatedAt",
            "registrationStudentId",
        ]

    @pytest.mark.parametrize("policy", ["qualify", "roles"])
    def test_a_join_key_two_join_entities_share_is_not_taken_by_an_earlier_qualification(self, policy):
        # ``enrollmentCourseId`` is a declared join key on two other join tables.
        # Enrollment's ``courseId`` sorts first and would qualify to that same label;
        # it is refused, so the join key stays on exactly the entities it binds.
        from r2g.config import ConfigManager

        keys = [("student_id", "student", "student_id"), ("enrollment_course_id", "course", "course_id")]
        schema = Schema(
            tables={
                "student": self._table("student", [("student_id", "integer")], ["student_id"]),
                "course": self._table("course", [("course_id", "integer")], ["course_id"]),
                "enrollment": self._table(
                    "enrollment",
                    [("student_id", "integer"), ("course_id", "integer")],
                    ["student_id", "course_id"],
                    [("student_id", "student", "student_id"), ("course_id", "course", "course_id")],
                ),
                **{
                    name: self._table(
                        name,
                        [("student_id", "integer"), ("enrollment_course_id", "integer")],
                        ["student_id", "enrollment_course_id"],
                        keys,
                    )
                    for name in ("waitlist", "audit_seat")
                },
            }
        )
        config = ConfigManager.generate_default_config(schema)
        for name in ("enrollment", "waitlist", "audit_seat"):
            assert config.collections[name].is_join_table
            assert config.collections[name].collection_type == "document"
        config.shared_keys = [
            SharedKey(
                key="enrollmentCourseId",
                concept="Course",
                bindings=[
                    SharedKeyBinding(source="school", table=t, column="enrollment_course_id")
                    for t in ("waitlist", "audit_seat")
                ],
            )
        ]
        doc = mapping_to_csi(config, schema, label_policy=policy)
        holders = sorted(name for name, labels in self._props(doc).items() if "enrollmentCourseId" in labels)
        assert holders == ["AuditSeat", "Waitlist"]
        assert self._join_record(doc, "enrollmentCourseId")["resolution"] == "join_key"
        rec = self._join_record(doc, "courseId")
        assert rec["resolution"] == "unresolved"
        assert "enrollmentCourseId" in rec["reason"]


class TestDeclaredUniqueness:
    """`validate_csi` rejects what the JSON Schema structurally cannot."""

    @staticmethod
    def _doc(unique_constraints, *, properties=("accountId", "region", "externalRef")):
        doc = mapping_to_csi(_sample_config(), source_type="postgresql")
        entity = doc["conceptualModel"]["entities"][0]
        entity["properties"] = [{"name": name} for name in properties]
        if unique_constraints is not _ABSENT:
            entity["uniqueConstraints"] = unique_constraints
        return doc, entity["name"]

    def test_a_well_formed_declaration_validates(self):
        doc, _ = self._doc([["accountId"], ["region", "externalRef"]])
        validate_csi(doc)

    def test_absent_and_empty_are_both_legal_and_mean_different_things(self):
        # Absent: nobody declared uniqueness. Empty: checked, and there is none.
        # Neither is an error, and collapsing them would destroy the distinction.
        absent, _ = self._doc(_ABSENT)
        validate_csi(absent)
        assert "uniqueConstraints" not in absent["conceptualModel"]["entities"][0]
        empty, _ = self._doc([])
        validate_csi(empty)

    def test_the_schema_alone_lets_every_one_of_these_through(self):
        # The guard rail is validate_csi, not the schema — if this ever fails,
        # the field became structural upstream and these checks should move there.
        import jsonschema

        for malformed in ("accountId", [["accountId"], "region"], [[]], {"a": 1}):
            doc, _ = self._doc(malformed)
            jsonschema.validate(instance=doc, schema=csi_schema())

    def test_a_flat_list_is_rejected_and_the_message_shows_the_fix(self):
        # The most likely mistake: a single-property key written without its
        # inner brackets. Being told the correct spelling is the whole value.
        doc, _ = self._doc(["accountId"])
        with pytest.raises(Exception) as caught:
            validate_csi(doc)
        assert '[["accountId"]]' in str(caught.value)

    def test_a_name_that_is_not_a_property_of_the_entity_is_rejected(self):
        doc, name = self._doc([["ACCT_ID"]])
        with pytest.raises(Exception) as caught:
            validate_csi(doc)
        message = str(caught.value)
        assert "ACCT_ID" in message and name in message
        assert "not source column names" in message

    def test_every_fault_is_reported_not_just_the_first(self):
        # A hand-written bundle is corrected in passes; one fault per run turns
        # that into one round trip per typo.
        doc, _ = self._doc([["nope"], ["region", "alsoNope"]])
        with pytest.raises(Exception) as caught:
            validate_csi(doc)
        message = str(caught.value)
        assert "nope" in message and "alsoNope" in message

    def test_an_empty_key_set_is_rejected(self):
        doc, _ = self._doc([[]])
        with pytest.raises(Exception, match="empty key set"):
            validate_csi(doc)

    def test_a_property_named_twice_in_one_key_set_is_rejected(self):
        doc, _ = self._doc([["region", "region"]])
        with pytest.raises(Exception, match="twice"):
            validate_csi(doc)

    def test_non_string_members_are_rejected(self):
        doc, _ = self._doc([["accountId", 7]])
        with pytest.raises(Exception, match="not a property name"):
            validate_csi(doc)

    def test_a_non_list_declaration_is_rejected(self):
        doc, _ = self._doc({"primary": ["accountId"]})
        with pytest.raises(Exception, match="must be a list of key sets"):
            validate_csi(doc)

    def test_an_entity_that_lists_no_properties_skips_the_reference_check(self):
        # Under-specified, not wrong: with nothing to resolve against, reporting
        # every name as unknown would bury real faults under noise.
        doc = mapping_to_csi(_sample_config(), source_type="postgresql")
        entity = doc["conceptualModel"]["entities"][0]
        entity.pop("properties", None)
        entity["uniqueConstraints"] = [["anythingAtAll"]]
        validate_csi(doc)

    def test_order_carries_no_meaning(self):
        for key_set in (["region", "externalRef"], ["externalRef", "region"]):
            doc, _ = self._doc([key_set])
            validate_csi(doc)


def test_export_csi_without_a_schema_says_its_names_can_differ(tmp_path):
    """Class names read the schema's foreign keys (P12.13); export-r2rml always has them."""
    from typer.testing import CliRunner

    from r2g.main import app

    config = tmp_path / "mapping.yaml"
    config.write_text(
        "source_schema: shop\ncollections:\n  goods:\n    source_table: goods\n    target_collection: goods\n",
        encoding="utf-8",
    )
    result = CliRunner().invoke(app, ["export-csi", "--config", str(config), "-o", str(tmp_path / "out.json")])
    assert result.exit_code == 0, result.output
    assert "No --schema" in " ".join(result.output.split())
