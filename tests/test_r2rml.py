"""Tests for the R2RML emitter (src/r2g/r2rml.py).

Parses the emitted Turtle with rdflib and asserts the R2RML triple structure —
so the tests fail on both syntax errors and wrong mappings, not just string
drift.
"""

from __future__ import annotations

import sys

import pytest

rdflib = pytest.importorskip("rdflib")
from rdflib import RDF, Namespace  # noqa: E402

from r2g.r2rml import (  # noqa: E402
    DEFAULT_CONCEPT_BASE,
    R2RMLError,
    _xsd_datatype,
    mapping_to_r2rml,
)
from r2g.types import (  # noqa: E402
    CollectionMapping,
    Column,
    EdgeDefinition,
    FieldExpression,
    MappingConfig,
    Schema,
    Table,
)

RR = Namespace("http://www.w3.org/ns/r2rml#")
CONCEPT = Namespace(DEFAULT_CONCEPT_BASE)
BASE = "http://test.example/m"


@pytest.fixture(autouse=True)
def _reset_structlog():
    """Restore structlog to real stderr after a CliRunner invocation."""
    yield
    import structlog

    structlog.reset_defaults()
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(0),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )


def _config() -> MappingConfig:
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
                    FieldExpression(target="total_cents", sources=["total"], expression="total*100"),
                ],
            ),
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
                from_collection="orders",
                to_collection="users",
                from_fields=["user_id"],
                to_fields=["id"],
            ),
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
                    Column(name="email", data_type="varchar", is_nullable=True),
                ],
                primary_key=["id"],
            ),
            "orders": Table(
                name="orders",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="user_id", data_type="integer"),
                    Column(name="total", data_type="numeric"),
                    Column(name="created", data_type="timestamp"),
                ],
                primary_key=["id"],
            ),
        }
    )


def _parse(ttl: str):
    g = rdflib.Graph()
    g.parse(data=ttl, format="turtle", publicID=BASE)
    return g


def _triples_maps(g) -> dict:
    """entity local-name -> TriplesMap node, keyed by subjectMap rr:class."""
    out = {}
    for tm in g.subjects(RDF.type, RR.TriplesMap):
        sm = g.value(tm, RR.subjectMap)
        cls = g.value(sm, RR["class"])
        out[str(cls).rsplit("#", 1)[-1]] = tm
    return out


def _poms(g, tm) -> dict:
    """predicate local-name -> objectMap node for a TriplesMap."""
    out = {}
    for pom in g.objects(tm, RR.predicateObjectMap):
        pred = g.value(pom, RR.predicate)
        out[str(pred).rsplit("#", 1)[-1]] = g.value(pom, RR.objectMap)
    return out


def test_parses_as_valid_turtle():
    g = _parse(mapping_to_r2rml(_config(), _schema()))
    assert len(g) > 0


def test_one_triplesmap_per_document_entity():
    g = _parse(mapping_to_r2rml(_config(), _schema()))
    tms = _triples_maps(g)
    # Join table 'placed' must NOT get a TriplesMap.
    assert set(tms) == {"User", "Order"}


def test_logical_table_names():
    g = _parse(mapping_to_r2rml(_config(), _schema()))
    tms = _triples_maps(g)
    user_lt = g.value(tms["User"], RR.logicalTable)
    assert str(g.value(user_lt, RR.tableName)) == "users"
    order_lt = g.value(tms["Order"], RR.logicalTable)
    assert str(g.value(order_lt, RR.tableName)) == "orders"


def test_subject_template_and_class():
    g = _parse(mapping_to_r2rml(_config(), _schema()))
    tms = _triples_maps(g)
    sm = g.value(tms["User"], RR.subjectMap)
    assert str(g.value(sm, RR["class"])) == str(CONCEPT.User)
    assert "{id}" in str(g.value(sm, RR.template))


def test_column_rename_maps_property_to_source_column():
    g = _parse(mapping_to_r2rml(_config(), _schema()))
    poms = _poms(g, _triples_maps(g)["User"])
    # conceptual property 'name' <- SQL column 'full_name'.
    assert str(g.value(poms["name"], RR.column)) == "full_name"
    # 'email' passes through unchanged.
    assert str(g.value(poms["email"], RR.column)) == "email"


def test_datatype_mapping():
    g = _parse(mapping_to_r2rml(_config(), _schema()))
    poms = _poms(g, _triples_maps(g)["Order"])
    assert str(g.value(poms["id"], RR.datatype)).endswith("integer")
    assert str(g.value(poms["total"], RR.datatype)).endswith("decimal")
    assert str(g.value(poms["created"], RR.datatype)).endswith("dateTime")
    # Text columns carry an explicit xsd:string (r2g-arango#24), so a missing
    # rr:datatype only ever means "r2g did not classify this column".
    user_poms = _poms(g, _triples_maps(g)["User"])
    assert str(g.value(user_poms["email"], RR.datatype)).endswith("#string")
    assert str(g.value(user_poms["name"], RR.datatype)).endswith("#string")


XSD = "http://www.w3.org/2001/XMLSchema#"


@pytest.mark.parametrize(
    ("sql_type", "expected"),
    [
        # Text, from each dialect r2g introspects (r2g-arango#24 table, row 1).
        ("varchar(40)", "xsd:string"),
        ("character varying", "xsd:string"),
        ("bpchar", "xsd:string"),
        ("character", "xsd:string"),
        ("text", "xsd:string"),
        ("VARCHAR(16777216)", "xsd:string"),
        ("TEXT", "xsd:string"),
        ("STRING", "xsd:string"),
        ("nvarchar", "xsd:string"),
        ("longtext", "xsd:string"),
        ("String", "xsd:string"),
        ("FixedString(16)", "xsd:string"),
        # Snowflake NUMBER/DECIMAL by scale; no scale -> decimal (row 2).
        ("NUMBER(38,0)", "xsd:integer"),
        ("number(10, 0)", "xsd:integer"),
        ("number(38)", "xsd:integer"),
        ("NUMBER(10,2)", "xsd:decimal"),
        ("number", "xsd:decimal"),
        ("DECIMAL(18,0)", "xsd:integer"),
        ("decimal(18,4)", "xsd:decimal"),
        ("numeric", "xsd:decimal"),
        ("numeric(12,2)", "xsd:decimal"),
        # ClickHouse wrappers are unwrapped before classifying (rows 3 and 4).
        ("Nullable(Int32)", "xsd:integer"),
        ("LowCardinality(String)", "xsd:string"),
        ("LowCardinality(Nullable(String))", "xsd:string"),
        ("Nullable(Float64)", "xsd:double"),
        ("Nullable(DateTime)", "xsd:dateTime"),
        # ClickHouse DecimalN(S): the one parameter is the scale.
        ("Decimal32(0)", "xsd:integer"),
        ("Decimal64(4)", "xsd:decimal"),
        ("Decimal(9, 2)", "xsd:decimal"),
        # Integers that were already typed stay typed; MySQL/SQL Server ones join.
        ("integer", "xsd:integer"),
        ("smallint", "xsd:integer"),
        ("bigint", "xsd:integer"),
        ("UInt64", "xsd:integer"),
        ("tinyint", "xsd:integer"),
        ("mediumint", "xsd:integer"),
        ("tinyint(1)", "xsd:integer"),
        ("int", "xsd:integer"),
        ("int unsigned", "xsd:integer"),
        ("Int256", "xsd:integer"),
        # Other classified types are unchanged.
        ("real", "xsd:double"),
        ("boolean", "xsd:boolean"),
        ("date", "xsd:date"),
        ("timestamp_ntz", "xsd:dateTime"),
        # Types r2g does not classify keep no datatype: "unknown" (row 5).
        ("uuid", None),
        ("json", None),
        ("jsonb", None),
        ("bytea", None),
        ("ARRAY", None),
        ("integer[]", None),
        ("Array(Int32)", None),
        ("variant", None),
        ("USER-DEFINED", None),
        # Start with "int" but are not integers.
        ("interval", None),
        ("int4range", None),
        ("int8range", None),
    ],
)
def test_xsd_datatype_classifies_each_sql_type(sql_type, expected):
    assert _xsd_datatype(sql_type) == expected


@pytest.mark.parametrize(
    ("sql_type", "expected_iri"),
    [
        ("varchar(40)", XSD + "string"),
        ("NUMBER(38,0)", XSD + "integer"),
        ("number", XSD + "decimal"),
        ("Nullable(Int32)", XSD + "integer"),
        ("uuid", None),
    ],
)
def test_emitted_object_map_carries_the_classified_datatype(sql_type, expected_iri):
    """End to end: the classification reaches the object map, and an
    unclassified column has no rr:datatype at all."""
    config = MappingConfig(
        source_schema="s",
        collections={
            "things": CollectionMapping(source_table="things", target_collection="Thing"),
        },
    )
    schema = Schema(
        tables={
            "things": Table(
                name="things",
                columns=[
                    Column(name="id", data_type="integer", is_primary_key=True),
                    Column(name="value", data_type=sql_type, is_nullable=True),
                ],
                primary_key=["id"],
            )
        }
    )
    g = _parse(mapping_to_r2rml(config, schema))
    declared = g.value(_poms(g, _triples_maps(g)["Thing"])["value"], RR.datatype)
    assert (str(declared) if declared is not None else None) == expected_iri


def test_fk_referencing_object_map():
    g = _parse(mapping_to_r2rml(_config(), _schema()))
    tms = _triples_maps(g)
    om = _poms(g, tms["Order"])["placedBy"]
    # The referencing object map points at the User TriplesMap...
    assert g.value(om, RR.parentTriplesMap) == tms["User"]
    # ...and joins child user_id (orders) to parent id (users).
    jc = g.value(om, RR.joinCondition)
    assert str(g.value(jc, RR.child)) == "user_id"
    assert str(g.value(jc, RR.parent)) == "id"


def test_missing_source_table_raises():
    schema = _schema()
    del schema.tables["users"]
    with pytest.raises(R2RMLError):
        mapping_to_r2rml(_config(), schema)


def test_no_primary_key_falls_back_to_all_columns():
    config = MappingConfig(
        collections={
            "t": CollectionMapping(source_table="t", target_collection="T"),
        },
    )
    schema = Schema(
        tables={
            "t": Table(
                name="t",
                columns=[
                    Column(name="a", data_type="text"),
                    Column(name="b", data_type="text"),
                ],
                primary_key=[],
            )
        }
    )
    ttl = mapping_to_r2rml(config, schema)
    g = _parse(ttl)
    sm = g.value(_triples_maps(g)["T"], RR.subjectMap)
    template = str(g.value(sm, RR.template))
    assert "{a}" in template and "{b}" in template
    assert "no primary key" in ttl  # the explanatory note


def test_table_outside_public_is_schema_qualified():
    # Ontop resolves a bare rr:tableName in the connection's first schema only, so
    # a mapping over tables in two schemas must name each one's schema.
    schema = _schema()
    schema.tables["users"].schema_name = "application"
    schema.tables["orders"].schema_name = "sales"
    g = _parse(mapping_to_r2rml(_config(), schema))
    tms = _triples_maps(g)
    names = {e: str(g.value(g.value(tms[e], RR.logicalTable), RR.tableName)) for e in ("User", "Order")}
    assert names == {"User": '"application"."users"', "Order": '"sales"."orders"'}


def test_table_in_public_keeps_its_bare_name():
    schema = _schema()
    schema.tables["users"].schema_name = "public"
    g = _parse(mapping_to_r2rml(_config(), schema))
    lt = g.value(_triples_maps(g)["User"], RR.logicalTable)
    assert str(g.value(lt, RR.tableName)) == "users"


def test_concept_base_default_matches_aql_leg():
    # The AQL leg (arango-sparql-py) synthesizes concepts under this namespace;
    # both legs must agree so a federated SPARQL query means the same thing.
    assert DEFAULT_CONCEPT_BASE == "urn:arango-sparql:concept#"


def test_join_table_edge_endpoint_is_skipped_with_note():
    config = _config()
    config.edges.append(
        EdgeDefinition(
            edge_collection="tagged",
            from_collection="user_orders",  # a join table, not a document entity
            to_collection="users",
            from_fields=["x"],
            to_fields=["id"],
        )
    )
    ttl = mapping_to_r2rml(config, _schema())
    assert "Skipped edges" in ttl and "tagged" in ttl
    # Still valid Turtle.
    assert len(_parse(ttl)) > 0


def test_export_r2rml_cli(tmp_path):
    from typer.testing import CliRunner

    from r2g.main import app

    schema_path = tmp_path / "schema.json"
    _schema().save_to_file(str(schema_path))
    config_path = tmp_path / "mapping.yaml"
    from r2g.config import ConfigManager

    ConfigManager.save_config(_config(), str(config_path))

    out = tmp_path / "m.ttl"
    result = CliRunner().invoke(
        app,
        ["export-r2rml", "--config", str(config_path), "--schema", str(schema_path), "-o", str(out)],
    )
    assert result.exit_code == 0, result.output
    g = _parse(out.read_text(encoding="utf-8"))
    assert set(_triples_maps(g)) == {"User", "Order"}


# --- names from a CSI (r2g-arango#17) ---------------------------------------------------------


def _colliding_config() -> MappingConfig:
    """Two entities sharing ``acctNo`` (Account's key, Contact's reference), ``name`` and
    ``created``, so every label policy has something to do."""
    return MappingConfig(
        source_schema="crm",
        collections={
            "accounts": CollectionMapping(source_table="accounts", target_collection="Account"),
            "contacts": CollectionMapping(source_table="contacts", target_collection="Contact"),
        },
        edges=[
            EdgeDefinition(
                edge_collection="works_at",
                from_collection="contacts",
                to_collection="accounts",
                from_fields=["acct_no"],
                to_fields=["acct_no"],
            ),
        ],
    )


def _colliding_schema() -> Schema:
    return Schema(
        tables={
            "accounts": Table(
                name="accounts",
                columns=[
                    Column(name="acct_no", data_type="integer", is_primary_key=True),
                    Column(name="name", data_type="text"),
                    Column(name="created", data_type="date"),
                ],
                primary_key=["acct_no"],
            ),
            "contacts": Table(
                name="contacts",
                columns=[
                    Column(name="contact_no", data_type="integer", is_primary_key=True),
                    Column(name="acct_no", data_type="integer"),
                    Column(name="name", data_type="text"),
                    Column(name="created", data_type="date"),
                ],
                primary_key=["contact_no"],
            ),
        }
    )


def _unmapped_csi_properties(csi: dict, ttl: str) -> list:
    """CSI properties with no predicate map on their entity's TriplesMap."""
    g = _parse(ttl)
    tms = _triples_maps(g)
    return sorted(
        f"{e['name']}.{p['name']}"
        for e in csi["conceptualModel"]["entities"]
        for p in e["properties"]
        if p["name"] not in _poms(g, tms[e["name"]])
    )


@pytest.mark.parametrize("policy", ["qualify", "roles", "warn", "off"])
def test_csi_names_map_every_csi_property(policy):
    from r2g.csi import mapping_to_csi

    config, schema = _colliding_config(), _colliding_schema()
    csi = mapping_to_csi(config, schema, label_policy=policy)
    assert _unmapped_csi_properties(csi, mapping_to_r2rml(config, schema, csi=csi)) == []


def test_without_csi_collided_labels_stay_bare():
    """The gap #17 reports, kept as the default: no CSI, no renames."""
    from r2g.csi import mapping_to_csi

    config, schema = _colliding_config(), _colliding_schema()
    csi = mapping_to_csi(config, schema)  # qualify
    assert "Account.accountName" in _unmapped_csi_properties(csi, mapping_to_r2rml(config, schema))


def test_csi_renames_only_the_column_predicates_it_names():
    from r2g.csi import mapping_to_csi

    config, schema = _colliding_config(), _colliding_schema()
    csi = mapping_to_csi(config, schema, label_policy="roles")
    g = _parse(mapping_to_r2rml(config, schema, csi=csi))
    tms = _triples_maps(g)
    account, contact = _poms(g, tms["Account"]), _poms(g, tms["Contact"])
    # roles: Account owns acctNo and keeps it; the referencing Contact is qualified.
    assert set(account) == {"acctNo", "accountName", "accountCreated"}
    assert set(contact) == {"contactNo", "contactAcctNo", "contactName", "contactCreated", "worksAt"}
    # Physical names and the relationship predicate are untouched.
    assert str(g.value(contact["contactAcctNo"], RR.column)) == "acct_no"
    assert str(g.value(account["accountName"], RR.column)) == "name"
    assert g.value(contact["worksAt"], RR.parentTriplesMap) is not None


def test_csi_without_renames_leaves_output_byte_identical():
    from r2g.csi import mapping_to_csi

    config, schema = _colliding_config(), _colliding_schema()
    for policy in ("warn", "off"):
        csi = mapping_to_csi(config, schema, label_policy=policy)
        assert mapping_to_r2rml(config, schema, csi=csi) == mapping_to_r2rml(config, schema)
    # And on the module's other fixture, which has no collision a CSI would rename.
    plain = {"provenance": {}}
    assert mapping_to_r2rml(_config(), _schema(), csi=plain) == mapping_to_r2rml(_config(), _schema())


def test_csi_names_with_curated_labels_match_the_csi():
    from r2g.csi import mapping_to_csi

    config, schema = _colliding_config(), _colliding_schema()
    labels = {
        "curatedLabelsVersion": "1",
        "entities": [{"name": "Contact", "label": "Person"}],
        "properties": [{"entity": "Contact", "name": "contactNo", "label": "Person Number"}],
    }
    csi = mapping_to_csi(config, schema, curated_labels=labels)
    ttl = mapping_to_r2rml(config, schema, curated_labels=labels, csi=csi)
    assert _unmapped_csi_properties(csi, ttl) == []
    g = _parse(ttl)
    assert {"personNumber", "personName"} <= set(_poms(g, _triples_maps(g)["Person"]))


def test_csi_from_another_mapping_is_refused():
    from r2g.csi import mapping_to_csi

    config, schema = _colliding_config(), _colliding_schema()
    labels = {"curatedLabelsVersion": "1", "entities": [{"name": "Contact", "label": "Person"}]}
    csi = mapping_to_csi(config, schema, curated_labels=labels)
    with pytest.raises(R2RMLError, match="Person"):
        mapping_to_r2rml(config, schema, csi=csi)  # curated labels not given to the R2RML


@pytest.mark.parametrize(
    "csi",
    [
        [],
        {"provenance": []},
        {"provenance": {"labelCollisions": {}}},
        {"provenance": {"labelCollisions": [{"label": 1, "renamedTo": {"Account": "x"}}]}},
        {"provenance": {"labelCollisions": [{"label": "name", "renamedTo": ["Account"]}]}},
        {"provenance": {"labelCollisions": [{"label": "name", "renamedTo": {"Account": ""}}]}},
        {"provenance": {"labelCollisions": [["name"]]}},
        {"provenance": {"labelCollisions": [{"label": "name", "renamedTo": {"Account": "a b"}}]}},
        {
            "provenance": {
                "labelCollisions": [
                    {"label": "name", "renamedTo": {"Account": "x> a rr:TriplesMap; <http://e/i"}}
                ]
            }
        },
    ],
)
def test_malformed_csi_is_refused(csi):
    with pytest.raises(R2RMLError):
        mapping_to_r2rml(_colliding_config(), _colliding_schema(), csi=csi)


def test_csi_renames_leave_a_relationship_named_like_a_collided_label():
    """An edge ``owner`` beside a collided ``owner`` column: only the column is renamed."""
    from r2g.csi import mapping_to_csi

    config = MappingConfig(
        source_schema="crm",
        collections={
            "accounts": CollectionMapping(source_table="accounts", target_collection="Account"),
            "contacts": CollectionMapping(source_table="contacts", target_collection="Contact"),
        },
        edges=[
            EdgeDefinition(
                edge_collection="owner",
                from_collection="contacts",
                to_collection="accounts",
                from_fields=["acct_no"],
                to_fields=["acct_no"],
            ),
        ],
    )
    schema = Schema(
        tables={
            "accounts": Table(
                name="accounts",
                columns=[
                    Column(name="acct_no", data_type="integer", is_primary_key=True),
                    Column(name="owner", data_type="text"),
                ],
                primary_key=["acct_no"],
            ),
            "contacts": Table(
                name="contacts",
                columns=[
                    Column(name="contact_no", data_type="integer", is_primary_key=True),
                    Column(name="acct_no", data_type="integer"),
                    Column(name="owner", data_type="text"),
                ],
                primary_key=["contact_no"],
            ),
        }
    )
    csi = mapping_to_csi(config, schema, label_policy="qualify")
    g = _parse(mapping_to_r2rml(config, schema, csi=csi))
    contact = _poms(g, _triples_maps(g)["Contact"])
    assert str(g.value(contact["contactOwner"], RR.column)) == "owner"
    assert g.value(contact["owner"], RR.parentTriplesMap) is not None


def test_export_r2rml_cli_csi(tmp_path):
    import json

    from typer.testing import CliRunner

    from r2g.config import ConfigManager
    from r2g.csi import mapping_to_csi
    from r2g.main import app

    config, schema = _colliding_config(), _colliding_schema()
    schema_path, config_path = tmp_path / "schema.json", tmp_path / "mapping.yaml"
    schema.save_to_file(str(schema_path))
    ConfigManager.save_config(config, str(config_path))
    csi = mapping_to_csi(config, schema)
    csi_path = tmp_path / "m.csi.json"
    csi_path.write_text(json.dumps(csi), encoding="utf-8")
    base = ["export-r2rml", "--config", str(config_path), "--schema", str(schema_path)]

    out = tmp_path / "m.ttl"
    result = CliRunner().invoke(app, [*base, "-o", str(out), "--csi", str(csi_path)])
    assert result.exit_code == 0, result.output
    assert _unmapped_csi_properties(csi, out.read_text(encoding="utf-8")) == []

    bad = tmp_path / "bad.json"
    bad.write_text("{not json", encoding="utf-8")
    result = CliRunner().invoke(app, [*base, "-o", str(tmp_path / "x.ttl"), "--csi", str(bad)])
    assert result.exit_code == 1
    assert not (tmp_path / "x.ttl").exists()
