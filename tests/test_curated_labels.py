"""Curated labels applied at export (PRD P12.12, src/r2g/curated_labels.py).

The properties under test, in order of how much damage their failure does:

- CSI and R2RML exported from the same mapping and labels name every concept
  the same way (contextual-data-fabric refuses a catalog where they disagree);
- a rename never breaks a join key or reintroduces a label carried by two
  entities — it is refused instead, and the refusal is recorded;
- no curated decision is silently dropped: unmatched and refused entries are
  recorded, and the CLI will not write output with one missing unless told to;
- physical fields are never renamed.
"""

from __future__ import annotations

import json
import re
import sys

import pytest

from r2g.csi import mapping_to_csi, validate_csi
from r2g.curated_labels import (
    CuratedLabelsError,
    load_curated_labels,
    plan_curated_labels,
    validate_curated_labels,
)
from r2g.r2rml import mapping_to_r2rml
from r2g.types import (
    CollectionMapping,
    Column,
    EdgeDefinition,
    MappingConfig,
    Schema,
    SharedKey,
    SharedKeyBinding,
    Table,
)


@pytest.fixture(autouse=True)
def _reset_structlog():
    """Restore structlog after a CliRunner invocation (see tests/test_csi.py)."""
    yield
    import structlog

    structlog.reset_defaults()
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(0),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )


def _config(shared_keys=None) -> MappingConfig:
    return MappingConfig(
        source_schema="crm",
        collections={
            "accounts": CollectionMapping(source_table="accounts", target_collection="accounts"),
            "contracts": CollectionMapping(source_table="contracts", target_collection="contracts"),
        },
        edges=[
            EdgeDefinition(
                edge_collection="covers",
                from_collection="contracts",
                to_collection="accounts",
                from_fields=["account_id"],
                to_fields=["account_id"],
            )
        ],
        shared_keys=shared_keys or [],
    )


def _schema() -> Schema:
    return Schema(
        tables={
            "accounts": Table(
                name="accounts",
                columns=[
                    Column(name="account_id", data_type="integer", is_primary_key=True),
                    Column(name="cust_nm", data_type="text"),
                    Column(name="renewal_date", data_type="date"),
                ],
                primary_key=["account_id"],
            ),
            "contracts": Table(
                name="contracts",
                columns=[
                    Column(name="contract_id", data_type="integer", is_primary_key=True),
                    Column(name="account_id", data_type="integer"),
                    Column(name="renewal_date", data_type="date"),
                    Column(name="amt", data_type="numeric"),
                ],
                primary_key=["contract_id"],
            ),
        }
    )


def _labels(**sections) -> dict:
    return {"curatedLabelsVersion": "1", **sections}


def _entity(doc: dict, name: str) -> dict:
    return next(e for e in doc["conceptualModel"]["entities"] if e["name"] == name)


def _prop_names(doc: dict, entity: str) -> set:
    return {p["name"] for p in _entity(doc, entity)["properties"]}


def _record(doc: dict, name: str) -> dict:
    return next(r for r in doc["provenance"]["curatedLabels"] if r["name"] == name)


def _r2rml_predicates(ttl: str) -> set:
    return set(re.findall(r"rr:predicate <urn:arango-sparql:concept#(\w+)>", ttl))


def _r2rml_classes(ttl: str) -> set:
    return set(re.findall(r"rr:class <urn:arango-sparql:concept#(\w+)>", ttl))


# ── Applying ────────────────────────────────────────────────────────────────


def test_renames_entity_property_and_relationship_and_keeps_physical_fields():
    labels = _labels(
        entities=[{"name": "Account", "label": "Customers", "description": "A buying party"}],
        properties=[{"entity": "Account", "name": "custNm", "label": "Customer Name"}],
        relationships=[{"name": "covers", "label": "covers account"}],
    )
    doc = mapping_to_csi(_config(), _schema(), label_policy="warn", curated_labels=labels)
    validate_csi(doc)

    # The label is words; the name is the CC-12 identifier derived from it.
    customer = _entity(doc, "Customer")
    assert customer["prefLabel"] == "Customers"
    assert customer["description"] == "A buying party"
    assert customer["labels"][0] == "Customer"
    assert "customerName" in _prop_names(doc, "Customer")

    physical = doc["arangoPhysicalMapping"]["entities"]
    assert "Account" not in physical
    assert physical["Customer"]["collectionName"] == "accounts"
    assert physical["Customer"]["properties"]["customerName"] == {"field": "cust_nm"}

    rel = doc["conceptualModel"]["relationships"][0]
    assert rel == {
        "type": "coversAccount",
        "fromEntity": "Contract",
        "toEntity": "Customer",
        "prefLabel": "covers account",
        "altLabels": ["covers"],
    }
    assert "coversAccount" in doc["arangoPhysicalMapping"]["relationships"]


def test_generated_name_becomes_an_alt_label_after_any_synonyms():
    labels = _labels(
        properties=[
            {
                "entity": "Account",
                "name": "custNm",
                "label": "Customer Name",
                "altLabels": ["client name"],
            }
        ]
    )
    doc = mapping_to_csi(_config(), _schema(), label_policy="warn", curated_labels=labels)
    prop = next(p for p in _entity(doc, "Account")["properties"] if p["name"] == "customerName")
    # The old name still resolves a question that uses it.
    assert prop["altLabels"] == ["client name", "custNm"]
    assert prop["prefLabel"] == "Customer Name"


def test_label_that_derives_the_generated_name_is_recorded_unchanged():
    labels = _labels(properties=[{"entity": "Contract", "name": "amt", "label": "amt"}])
    doc = mapping_to_csi(_config(), _schema(), label_policy="warn", curated_labels=labels)
    assert _record(doc, "amt")["resolution"] == "unchanged"


def test_join_key_bindings_follow_a_renamed_entity():
    keys = [
        SharedKey(
            key="accountId",
            concept="Account",
            hub_kind="entity",
            hub_source="crm",
            hub_table="accounts",
            hub_column="account_id",
            bindings=[SharedKeyBinding(source="crm", table="accounts", column="account_id")],
        )
    ]
    labels = _labels(entities=[{"name": "Account", "label": "Customer"}])
    doc = mapping_to_csi(_config(keys), _schema(), label_policy="warn", curated_labels=labels)
    binding = doc["conceptualModel"]["joinKeys"][0]["bindings"][0]
    assert binding["entity"] == "Customer"
    assert doc["conceptualModel"]["relationships"][0]["toEntity"] == "Customer"


def test_curated_rename_resolves_a_label_collision():
    # renewalDate is on both entities. Curating one leaves no collision to report.
    before = mapping_to_csi(_config(), _schema(), label_policy="warn")
    assert any(c["label"] == "renewalDate" for c in before["provenance"]["labelCollisions"])

    labels = _labels(
        properties=[{"entity": "Contract", "name": "renewalDate", "label": "contract renewal date"}]
    )
    after = mapping_to_csi(_config(), _schema(), label_policy="warn", curated_labels=labels)
    collisions = after["provenance"].get("labelCollisions", [])
    assert not any(c["label"] == "renewalDate" for c in collisions)
    assert "contractRenewalDate" in _prop_names(after, "Contract")


# ── Refusing ────────────────────────────────────────────────────────────────


def test_refuses_a_name_another_entity_already_carries():
    labels = _labels(properties=[{"entity": "Contract", "name": "amt", "label": "cust nm"}])
    doc = mapping_to_csi(_config(), _schema(), label_policy="warn", curated_labels=labels)
    rec = _record(doc, "amt")
    assert rec["resolution"] == "refused"
    assert "Account.custNm" in rec["reason"]
    assert "amt" in _prop_names(doc, "Contract")


def test_refuses_a_name_a_relationship_carries():
    labels = _labels(properties=[{"entity": "Contract", "name": "amt", "label": "covers"}])
    plan = mapping_plan(labels)
    assert plan.records[0]["resolution"] == "refused"
    assert "relationship covers" in plan.records[0]["reason"]


def test_refuses_another_entitys_name():
    labels = _labels(entities=[{"name": "Account", "label": "Contract"}])
    doc = mapping_to_csi(_config(), _schema(), label_policy="warn", curated_labels=labels)
    assert _record(doc, "Account")["resolution"] == "refused"
    assert {e["name"] for e in doc["conceptualModel"]["entities"]} == {"Account", "Contract"}


def test_refuses_renaming_a_declared_join_key():
    keys = [
        SharedKey(
            key="accountId",
            concept="Account",
            hub_kind="entity",
            bindings=[SharedKeyBinding(source="crm", table="accounts", column="account_id")],
        )
    ]
    labels = _labels(properties=[{"entity": "Account", "name": "accountId", "label": "customer id"}])
    doc = mapping_to_csi(_config(keys), _schema(), label_policy="warn", curated_labels=labels)
    rec = _record(doc, "accountId")
    assert rec["resolution"] == "refused"
    assert "join key" in rec["reason"]
    assert "accountId" in _prop_names(doc, "Account")


def test_two_entries_for_one_concept_are_both_refused():
    labels = _labels(
        properties=[
            {"entity": "Contract", "name": "amt", "label": "amount"},
            {"entity": "Contract", "name": "amt", "label": "value"},
        ]
    )
    plan = mapping_plan(labels)
    assert [r["resolution"] for r in plan.records] == ["refused"]
    assert plan.properties == {}


def test_two_entries_taking_one_name_are_refused_not_ordered():
    # Neither may win by position in the file: both are refused.
    labels = _labels(
        properties=[
            {"entity": "Contract", "name": "amt", "label": "value"},
            {"entity": "Account", "name": "custNm", "label": "value"},
        ]
    )
    plan = mapping_plan(labels)
    assert {r["resolution"] for r in plan.records} == {"refused"}
    assert plan.properties == {}


def test_a_refusal_that_restores_a_name_refuses_what_wanted_it():
    # Contract.amt -> renewalDate is refused: two entities already carry it.
    # Account.custNm -> amt looked free while amt was leaving, so it is only
    # refused on the second pass, once the first refusal puts amt back.
    labels = _labels(
        properties=[
            {"entity": "Contract", "name": "amt", "label": "renewal date"},
            {"entity": "Account", "name": "custNm", "label": "amt"},
        ]
    )
    plan = mapping_plan(labels)
    by_name = {r["name"]: r for r in plan.records}
    assert by_name["amt"]["resolution"] == "refused"
    assert by_name["custNm"]["resolution"] == "refused"
    assert "Contract.amt" in by_name["custNm"]["reason"]
    assert plan.properties == {}


def test_a_swap_is_applied():
    # Each name ends up on exactly one entity, which is all the invariant asks.
    labels = _labels(
        properties=[
            {"entity": "Account", "name": "custNm", "label": "amt"},
            {"entity": "Contract", "name": "amt", "label": "cust nm"},
        ]
    )
    plan = mapping_plan(labels)
    assert {r["resolution"] for r in plan.records} == {"applied"}


def test_freed_name_can_be_taken():
    # Account.custNm leaves "custNm"; Contract.amt may then take it.
    labels = _labels(
        properties=[
            {"entity": "Account", "name": "custNm", "label": "customer name"},
            {"entity": "Contract", "name": "amt", "label": "cust nm"},
        ]
    )
    plan = mapping_plan(labels)
    assert {r["resolution"] for r in plan.records} == {"applied"}


def test_unmatched_entries_are_recorded_not_dropped():
    labels = _labels(
        entities=[{"name": "Invoice", "label": "Bill"}],
        properties=[
            {"entity": "Account", "name": "nope", "label": "x"},
            {"entity": "Invoice", "name": "total", "label": "y"},
        ],
        relationships=[{"name": "billedTo", "label": "z"}],
    )
    plan = mapping_plan(labels)
    assert len(plan.records) == 4
    assert {r["resolution"] for r in plan.records} == {"unmatched"}
    assert len(plan.problems) == 4


def test_accents_are_folded_into_the_identifier():
    # The converter keeps only ASCII; without folding, "Café" lost its "é".
    labels = _labels(
        entities=[{"name": "Account", "label": "Café owner"}],
        properties=[{"entity": "Contract", "name": "amt", "label": "Montant dû"}],
    )
    doc = mapping_to_csi(_config(), _schema(), label_policy="warn", curated_labels=labels)
    owner = _entity(doc, "CafeOwner")
    assert owner["prefLabel"] == "Café owner"
    assert "montantDu" in _prop_names(doc, "Contract")


@pytest.mark.parametrize(
    ("label", "reason"),
    [
        ("Straße", "no ASCII form (ß)"),
        ("名前", "no ASCII form (名前)"),
        ("!!!", "no letters or digits"),
        ("> <", "no letters or digits"),
    ],
)
def test_label_with_no_faithful_identifier_is_refused(label, reason):
    labels = _labels(properties=[{"entity": "Contract", "name": "amt", "label": label}])
    plan = mapping_plan(labels)
    assert plan.records[0]["resolution"] == "refused"
    assert reason in plan.records[0]["reason"]
    assert plan.properties == {}
    # Nothing raw reaches the R2RML either.
    ttl = mapping_to_r2rml(_config(), _schema(), curated_labels=labels)
    assert label not in ttl


def test_a_refused_label_still_counts_as_a_duplicate():
    labels = _labels(
        properties=[
            {"entity": "Contract", "name": "amt", "label": "!!!"},
            {"entity": "Contract", "name": "amt", "label": "amount"},
        ]
    )
    plan = mapping_plan(labels)
    assert plan.properties == {}
    assert plan.records[0]["resolution"] == "refused"


def test_unmatched_key_from_a_qualified_csi_says_so():
    # Under the default `qualify` policy Contract.renewalDate is exported as
    # contractRenewalDate, so an ontology imported from that CSI uses it as a key.
    labels = _labels(
        properties=[{"entity": "Contract", "name": "contractRenewalDate", "label": "x"}]
    )
    rec = mapping_plan(labels).records[0]
    assert rec["resolution"] == "unmatched"
    assert "Contract.renewalDate" in rec["reason"]
    assert "--label-policy warn" in rec["reason"]


# ── Both exporters agree ────────────────────────────────────────────────────


def test_r2rml_carries_the_same_curated_names_as_csi():
    labels = _labels(
        entities=[{"name": "Account", "label": "Customer"}],
        properties=[{"entity": "Account", "name": "custNm", "label": "Customer Name"}],
        relationships=[{"name": "covers", "label": "covers account"}],
    )
    doc = mapping_to_csi(_config(), _schema(), label_policy="warn", curated_labels=labels)
    ttl = mapping_to_r2rml(_config(), _schema(), curated_labels=labels)

    csi_classes = {e["name"] for e in doc["conceptualModel"]["entities"]}
    csi_predicates = {
        p["name"] for e in doc["conceptualModel"]["entities"] for p in e["properties"]
    } | {r["type"] for r in doc["conceptualModel"]["relationships"]}

    assert _r2rml_classes(ttl) == csi_classes
    assert _r2rml_predicates(ttl) == csi_predicates
    assert "<#Customer>" in ttl and "rr:parentTriplesMap <#Customer>" in ttl
    assert 'rr:column "cust_nm"' in ttl


# ── Absent and deterministic ────────────────────────────────────────────────


def test_without_curated_labels_output_is_unchanged():
    plain = mapping_to_csi(_config(), _schema())
    assert "curatedLabels" not in plain["provenance"]
    assert mapping_to_r2rml(_config(), _schema()) == mapping_to_r2rml(
        _config(), _schema(), curated_labels=None
    )


def test_entry_order_does_not_change_the_result():
    entries = [
        {"entity": "Account", "name": "custNm", "label": "customer name"},
        {"entity": "Contract", "name": "amt", "label": "cust nm"},
        {"entity": "Contract", "name": "renewalDate", "label": "value"},
    ]
    a = mapping_to_csi(_config(), _schema(), curated_labels=_labels(properties=entries))
    b = mapping_to_csi(_config(), _schema(), curated_labels=_labels(properties=entries[::-1]))
    assert a == b


def test_source_metadata_is_recorded():
    labels = _labels(producer="arango-ontoextract", ontology="crm")
    doc = mapping_to_csi(_config(), _schema(), curated_labels=labels)
    assert doc["provenance"]["curatedLabelsSource"] == {
        "producer": "arango-ontoextract",
        "ontology": "crm",
    }
    assert doc["provenance"]["curatedLabels"] == []


# ── The document ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("doc", "fragment"),
    [
        ([], "JSON object"),
        ({"curatedLabelsVersion": "2"}, "curatedLabelsVersion"),
        (_labels(entities=[{"name": "A"}]), "entities[0].label"),
        (_labels(properties=[{"name": "a", "label": "b"}]), "properties[0].entity"),
        (_labels(entities=[{"name": "A", "label": "B", "altLabels": "x"}]), "altLabels"),
        (_labels(entities=[{"name": "A", "label": "B", "colour": "red"}]), "unknown field"),
        (_labels(relationships={}), "relationships must be a list"),
    ],
)
def test_validation_names_each_problem(doc, fragment):
    problems = validate_curated_labels(doc)
    assert any(fragment in p for p in problems), problems


def test_load_refuses_a_malformed_file(tmp_path):
    path = tmp_path / "labels.json"
    path.write_text(json.dumps({"curatedLabelsVersion": "1", "entities": [{"name": "A"}]}))
    with pytest.raises(CuratedLabelsError, match="entities\\[0\\].label"):
        load_curated_labels(path)
    path.write_text("{not json")
    with pytest.raises(CuratedLabelsError, match="cannot read"):
        load_curated_labels(path)


# ── CLI ─────────────────────────────────────────────────────────────────────


def _write_inputs(tmp_path, labels: dict):
    config = tmp_path / "mapping.yaml"
    config.write_text(
        "source_schema: crm\n"
        "collections:\n"
        "  accounts:\n"
        "    source_table: accounts\n"
        "    target_collection: accounts\n"
        "  contracts:\n"
        "    source_table: contracts\n"
        "    target_collection: contracts\n",
        encoding="utf-8",
    )
    schema = tmp_path / "schema.json"
    schema.write_text(_schema().model_dump_json(), encoding="utf-8")
    labels_path = tmp_path / "labels.json"
    labels_path.write_text(json.dumps(labels), encoding="utf-8")
    return str(config), str(schema), str(labels_path)


@pytest.mark.parametrize("command", ["export-csi", "export-r2rml"])
def test_cli_applies_curated_labels(tmp_path, command):
    from typer.testing import CliRunner

    from r2g.main import app

    labels = _labels(properties=[{"entity": "Account", "name": "custNm", "label": "Customer Name"}])
    config, schema, labels_path = _write_inputs(tmp_path, labels)
    out = tmp_path / "out"
    result = CliRunner().invoke(
        app,
        [command, "-c", config, "-s", schema, "--curated-labels", labels_path, "-o", str(out)],
    )
    assert result.exit_code == 0, result.output
    assert "Applied 1 curated label" in result.output
    assert "customerName" in out.read_text(encoding="utf-8")


@pytest.mark.parametrize("command", ["export-csi", "export-r2rml"])
def test_cli_will_not_write_with_a_decision_missing(tmp_path, command):
    from typer.testing import CliRunner

    from r2g.main import app

    labels = _labels(properties=[{"entity": "Account", "name": "nope", "label": "x"}])
    config, schema, labels_path = _write_inputs(tmp_path, labels)
    out = tmp_path / "out"
    args = [command, "-c", config, "-s", schema, "--curated-labels", labels_path, "-o", str(out)]

    result = CliRunner().invoke(app, args)
    assert result.exit_code == 1, result.output
    assert "unmatched" in result.output
    assert not out.exists()

    result = CliRunner().invoke(app, [*args, "--allow-unapplied-labels"])
    assert result.exit_code == 0, result.output
    assert out.exists()


def mapping_plan(labels: dict):
    from r2g.csi import curated_plan_for_mapping

    return curated_plan_for_mapping(_config(), _schema(), labels)


def test_plan_is_pure_over_names():
    # The planner needs no mapping: just the generated names.
    plan = plan_curated_labels(
        _labels(properties=[{"entity": "A", "name": "x", "label": "why"}]),
        entities={"A": ["x"]},
        relationships=[],
    )
    assert plan.prop("A", "x") == "why"
    assert plan.prop("A", "untouched") == "untouched"


# ── A curator's explicit identifier (r2g-arango#34) ─────────────────────────


def _decision(doc: dict, kind: str, name: str) -> dict:
    return next(
        r for r in doc["provenance"]["curatedLabels"] if r["kind"] == kind and r["name"] == name
    )


def test_an_explicit_identifier_is_used_as_written():
    # "Sales" would singularize to Sale; the curator's identifier is the last word.
    doc = mapping_to_csi(
        _config(), _schema(), label_policy="warn",
        curated_labels=_labels(entities=[{"name": "Account", "label": "Sales", "identifier": "Sales"}]),
    )
    sales = _entity(doc, "Sales")
    assert sales["prefLabel"] == "Sales"
    assert _decision(doc, "entity", "Account")["identifier"] == "Sales"


def test_without_an_identifier_the_label_is_still_singularized():
    doc = mapping_to_csi(
        _config(), _schema(), label_policy="warn",
        curated_labels=_labels(entities=[{"name": "Account", "label": "Customers"}]),
    )
    assert _entity(doc, "Customer")["prefLabel"] == "Customers"


def test_an_identifier_lets_a_non_latin_label_name_a_concept():
    doc = mapping_to_csi(
        _config(), _schema(), label_policy="warn",
        curated_labels=_labels(entities=[{"name": "Account", "label": "顧客", "identifier": "Customer"}]),
    )
    assert _entity(doc, "Customer")["prefLabel"] == "顧客"


def test_an_identifier_in_the_wrong_shape_is_refused():
    doc = mapping_to_csi(
        _config(), _schema(), label_policy="warn",
        curated_labels=_labels(
            entities=[{"name": "Account", "label": "Sales", "identifier": "sales"}],
            properties=[{"entity": "Account", "name": "accountId", "label": "Id", "identifier": "AccountId"}],
        ),
    )
    assert _decision(doc, "entity", "Account")["resolution"] == "refused"
    assert "PascalCase" in _decision(doc, "entity", "Account")["reason"]
    prop = _decision(doc, "property", "accountId")
    assert prop["resolution"] == "refused" and "lowerCamel" in prop["reason"]


def test_an_identifier_still_goes_through_the_collision_checks():
    # Giving Account the name of the other entity is refused, identifier or not.
    doc = mapping_to_csi(
        _config(), _schema(), label_policy="warn",
        curated_labels=_labels(entities=[{"name": "Account", "label": "Contract", "identifier": "Contract"}]),
    )
    assert _decision(doc, "entity", "Account")["resolution"] == "refused"


def test_an_empty_identifier_is_a_shape_problem():
    from r2g.curated_labels import validate_curated_labels

    problems = validate_curated_labels(
        _labels(entities=[{"name": "Account", "label": "Sales", "identifier": " "}])
    )
    assert any("identifier must be a non-empty string" in p for p in problems)


# ── A label on a property that include/exclude removed ──────────────────────


def _filtered_config(**filters) -> MappingConfig:
    config = _config()
    accounts = config.collections["accounts"]
    accounts.field_mappings = {"cust_nm": "holderName"}
    for field, value in filters.items():
        setattr(accounts, field, value)
    return config


@pytest.mark.parametrize(
    "filters",
    [{"exclude_fields": ["cust_nm"]}, {"include_fields": ["account_id", "renewal_date"]}],
    ids=["exclude", "include"],
)
@pytest.mark.parametrize("with_schema", [True, False], ids=["schema", "no-schema"])
def test_label_on_a_filtered_rename_is_unmatched_and_recorded(filters, with_schema):
    entry = {"entity": "Account", "name": "holderName", "label": "Holder", "decidedBy": "steward"}
    doc = mapping_to_csi(
        _filtered_config(**filters),
        _schema() if with_schema else None,
        label_policy="warn",
        curated_labels=_labels(properties=[entry]),
    )
    validate_csi(doc)
    record = _record(doc, "holderName")
    assert record["resolution"] == "unmatched"
    assert record["decidedBy"] == "steward"
    assert "holderName" not in _prop_names(doc, "Account")
    assert "holder" not in _prop_names(doc, "Account")


@pytest.mark.parametrize("command", ["export-csi", "export-r2rml"])
def test_cli_refuses_a_label_on_a_filtered_rename_unless_allowed(tmp_path, command):
    from typer.testing import CliRunner

    from r2g.main import app

    labels = _labels(properties=[{"entity": "Account", "name": "holderName", "label": "Holder"}])
    config, schema, labels_path = _write_inputs(tmp_path, labels)
    with open(config, "w", encoding="utf-8") as handle:
        handle.write(
            "source_schema: crm\n"
            "collections:\n"
            "  accounts:\n"
            "    source_table: accounts\n"
            "    target_collection: accounts\n"
            "    field_mappings:\n"
            "      cust_nm: holderName\n"
            "    exclude_fields:\n"
            "      - cust_nm\n"
            "  contracts:\n"
            "    source_table: contracts\n"
            "    target_collection: contracts\n"
        )
    out = tmp_path / "out"
    args = [command, "-c", config, "-s", schema, "--curated-labels", labels_path, "-o", str(out)]

    result = CliRunner().invoke(app, args)
    assert result.exit_code == 1, result.output
    assert "unmatched" in result.output
    assert not out.exists()

    result = CliRunner().invoke(app, [*args, "--allow-unapplied-labels"])
    assert result.exit_code == 0, result.output
    assert "holderName" not in re.sub(r'"curatedLabels".*', "", out.read_text(encoding="utf-8"), flags=re.S)


def test_a_filtered_rename_no_longer_reserves_its_label_in_either_export():
    # The removed property no longer holds the predicate, so another property
    # may take that name, and both exporters agree because they share the plan.
    labels = _labels(properties=[{"entity": "Contract", "name": "amt", "label": "Holder Name"}])
    config = _filtered_config(exclude_fields=["cust_nm"])
    doc = mapping_to_csi(config, _schema(), label_policy="warn", curated_labels=labels)
    assert _record(doc, "amt")["resolution"] == "applied"
    assert "holderName" in _prop_names(doc, "Contract")
    assert "holderName" in _r2rml_predicates(mapping_to_r2rml(config, _schema(), curated_labels=labels))
