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
    summarize_r2rml,
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


# --- summarize_r2rml: reading back what the emitter wrote ------------------------------------


def _by_rdflib(ttl: str) -> dict:
    """What rdflib, a full Turtle parser, reads from the document, in the summary's shape."""
    g = _parse(ttl)
    xsd = str(rdflib.XSD)
    tables = {}
    for tm in g.subjects(RDF.type, RR.TriplesMap):
        subject_map = g.value(tm, RR.subjectMap)
        columns, joins = [], []
        for pom in g.objects(tm, RR.predicateObjectMap):
            obj = g.value(pom, RR.objectMap)
            predicate = str(g.value(pom, RR.predicate))
            parent = g.value(obj, RR.parentTriplesMap)
            if parent is None:
                datatype = g.value(obj, RR.datatype)
                columns.append((str(g.value(obj, RR.column)), predicate,
                                None if datatype is None else "xsd:" + str(datatype)[len(xsd):]))
            else:
                joins.append((predicate, str(parent).rsplit("#", 1)[1], tuple(sorted(
                    (str(g.value(c, RR.child)), str(g.value(c, RR.parent)))
                    for c in g.objects(obj, RR.joinCondition)))))
        tables[str(tm).rsplit("#", 1)[1]] = (
            str(g.value(g.value(tm, RR.logicalTable), RR.tableName)),
            str(g.value(subject_map, RR.template)), str(g.value(subject_map, RR["class"])),
            sorted(columns, key=repr), sorted(joins, key=repr))
    return tables


def _by_summary(ttl: str) -> dict:
    return {
        t["subject"]: (
            t["table"], t["template"], t["class"],
            sorted(((c["column"], c["predicate"], c["datatype"]) for c in t["columns"]), key=repr),
            sorted(((j["predicate"], j["parent"], tuple(sorted(
                (c["child"], c["parent"]) for c in j["conditions"]))) for j in t["joins"]), key=repr))
        for t in summarize_r2rml(ttl)["tables"]
    }


def _awkward_schema() -> Schema:
    return Schema(tables={
        "t": Table(name='we"ird;t]', schema_name="sa#les", primary_key=["id", 'k;e"y'], columns=[
            Column(name="id", data_type="integer"),
            Column(name='k;e"y', data_type="text"),
            Column(name='a;b ]"c#d\\e', data_type="text"),
            Column(name="tab\tnew\nline\rcr", data_type="uuid"),
            Column(name="名前", data_type="timestamp"),
            Column(name="# not a comment", data_type="numeric"),
        ]),
        "p": Table(name="p", columns=[Column(name="x", data_type="text"),
                                      Column(name="y", data_type="text")]),
    })


def _awkward_config() -> MappingConfig:
    return MappingConfig(
        collections={n: CollectionMapping(source_table=n, target_collection=c)
                     for n, c in (("t", "Thing"), ("p", "Part"))},
        edges=[EdgeDefinition(edge_collection="has", from_collection="t", to_collection="p",
                              from_fields=["id", 'k;e"y'], to_fields=["x", "y"])],
    )


@pytest.mark.parametrize("config, schema, options", [
    (_config, _schema, {}),
    (_config, _schema, {"concept_base": "urn:no:hash:", "resource_base": "http://ex.org/r#"}),
    (_awkward_config, _awkward_schema, {}),
    (_awkward_config, _awkward_schema, {"concept_base": "https://ex.org/onto/"}),
    (_config, _schema, {"curated_labels": {"curatedLabelsVersion": "1", "entities": [
        {"name": "User", "label": "Customer"}]}}),
])
def test_the_summary_reads_what_rdflib_reads(config, schema, options):
    ttl = mapping_to_r2rml(config(), schema(), **options)
    assert _by_summary(ttl) == _by_rdflib(ttl) != {}


def test_the_summary_reads_the_committed_golden_document():
    from pathlib import Path

    ttl = (Path(__file__).parent / "fixtures" / "serialization_compat" /
           "reference.r2rml.ttl").read_text(encoding="utf-8")
    assert _by_summary(ttl) == _by_rdflib(ttl) != {}


def test_the_summary_keeps_physical_names_exactly():
    summary = summarize_r2rml(mapping_to_r2rml(_awkward_config(), _awkward_schema()))
    thing = next(t for t in summary["tables"] if t["subject"] == "Thing")
    assert thing["table"] == '"sa#les"."we""ird;t]"'
    assert {'a;b ]"c#d\\e', "tab\tnew\nline\rcr", "名前", "# not a comment"} <= {
        c["column"] for c in thing["columns"]}
    assert thing["joins"][0]["conditions"] == [{"child": "id", "parent": "x"},
                                               {"child": 'k;e"y', "parent": "y"}]


def test_the_summary_carries_what_a_federation_will_not_run():
    config = _config()
    config.edges.append(EdgeDefinition(edge_collection="via", from_collection="user_orders",
                                       to_collection="users", from_fields=["a"], to_fields=["b"]))
    schema = _schema()
    schema.tables["users"].primary_key = []
    notes = summarize_r2rml(mapping_to_r2rml(config, schema))["notes"]
    assert any("no primary key" in n for n in notes)
    assert any("computed properties omitted" in n and "total_cents" in n for n in notes)
    assert any(n.startswith("Skipped edges") for n in notes) and any("via" in n for n in notes)


def test_an_empty_document_has_no_tables():
    assert summarize_r2rml(mapping_to_r2rml(MappingConfig(), Schema())) == {"tables": [], "notes": []}


def _emitted() -> str:
    return mapping_to_r2rml(_config(), _schema())


@pytest.mark.parametrize("change", [
    lambda t: t.replace('rr:tableName "users"', "rr:tableName 'users'"),         # single quotes
    lambda t: t.replace('rr:tableName "users"', 'rr:tableName """users"""'),     # long string
    lambda t: t.replace('rr:tableName "users"', 'rr:tableName "us\\u0065rs"'),    # \\u escape
    lambda t: t.replace('rr:tableName "users"', 'rr:tableName "us\\xers"'),       # unknown escape
    lambda t: t.replace('rr:tableName "users"', 'rr:tableName "us\ners"'),       # raw newline
    lambda t: t.replace("@prefix rr: <http://www.w3.org/ns/r2rml#>",
                        "@prefix rr: <http://example.org/other#>"),               # prefix rebound
    lambda t: t.replace("@prefix xsd:", "@prefix ex: <urn:x> .\n@prefix xsd:"),   # extra prefix
    lambda t: t.replace("rr:logicalTable [", "rr:sqlQuery [", 1),                 # unknown term
    lambda t: t.replace("rr:datatype xsd:", "rr:language xsd:", 1),               # unknown term
    lambda t: t.replace("rr:datatype xsd:integer", "rr:datatype rdf:langString", 1),
    lambda t: t.replace("rr:subjectMap [", "rr:subjectMap ", 1),                  # unbalanced
    lambda t: t.replace("    ] .", "    ] ] .", 1),                               # unbalanced
    lambda t: "    ]\n".join(t.rsplit("    ] .\n", 1)),                            # no final .
    lambda t: t.replace("    ] .\n", '    ] "stray"\n', 1),                        # not a closing .
    lambda t: t + "<#User> a rr:TriplesMap .\n",                                  # incomplete
    lambda t: t + _emitted().split("@prefix xsd:")[1].split("\n", 1)[1],          # defined twice
    lambda t: t + "trailing",                                                     # trailing content
    lambda t: t.replace("rr:parentTriplesMap <#User>", "rr:parentTriplesMap <#Nobody>"),
    lambda t: t.replace("rr:parentTriplesMap <#User>", "rr:parentTriplesMap <http://x/User>"),
    lambda t: t.replace("<#User>\n", "<http://x/User>\n", 1),                     # outside document
    lambda t: t.replace("a rr:TriplesMap", "a rr:Other", 1),
    lambda t: t.replace('rr:tableName "users"', 'rr:tableName "users" , "x"'),    # object list
    lambda t: t.replace('rr:tableName "users"', 'rr:tableName "users" ; rr:tableName "x"'),
    lambda t: t.replace('rr:tableName "users"', "rr:tableName " + "[ rr:x " * 5000 + '"y"' + " ]" * 5000),
    lambda t: "",
])
def test_the_summary_refuses_anything_else(change):
    changed = change(_emitted())
    assert changed != _emitted()
    with pytest.raises(R2RMLError):
        summarize_r2rml(changed)


def test_a_refusal_names_the_line():
    ttl = _emitted().replace('rr:tableName "users"', "rr:tableName 'users'")
    line = next(i for i, text in enumerate(ttl.splitlines(), 1) if "'users'" in text)
    with pytest.raises(R2RMLError, match=f"line {line}:"):
        summarize_r2rml(ttl)
