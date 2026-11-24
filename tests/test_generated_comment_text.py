"""Names written into other generated files stay inside their comment or string.

The import script is not the only file r2g writes names into for a tool to read. An
R2RML mapping carries skipped edges and computed properties in ``#`` comment lines,
and a newline there would let a table name add a triples map of its own, with an
``rr:sqlQuery`` the mapping engine runs against the source. The Rego stub carries
edge and collection names in string literals, where a quote would end the string.
"""

from __future__ import annotations

import json

import pytest

rdflib = pytest.importorskip("rdflib")
from rdflib import Namespace  # noqa: E402

from r2g.governance import EntitlementReport, policy_rego  # noqa: E402
from r2g.r2rml import mapping_to_r2rml  # noqa: E402
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
INJECTED = ('jt\n<#Evil> a <http://www.w3.org/ns/r2rml#TriplesMap> ; '
            '<http://www.w3.org/ns/r2rml#logicalTable> [ '
            '<http://www.w3.org/ns/r2rml#sqlQuery> "DELETE FROM customers" ] .\n#')


def _mapping(edge_end: str, computed: str) -> tuple[MappingConfig, Schema]:
    config = MappingConfig(
        collections={
            "users": CollectionMapping(
                source_table="users", target_collection="User",
                field_expressions=[
                    FieldExpression(target=computed, sources=["id"], expression="@id * 2"),
                ],
            ),
        },
        # An endpoint that is not a mapped document entity: the edge is skipped and
        # named in a comment.
        edges=[EdgeDefinition(edge_collection="e", from_collection="users",
                              to_collection=edge_end, from_fields=["id"], to_fields=["id"])],
    )
    schema = Schema(tables={"users": Table(
        name="users", columns=[Column(name="id", data_type="integer", is_primary_key=True)],
        primary_key=["id"])})
    return config, schema


@pytest.mark.parametrize("where", ["skipped edge", "computed property"])
def test_a_name_with_a_newline_adds_nothing_to_an_r2rml_mapping(where):
    config, schema = _mapping(INJECTED if where == "skipped edge" else "orders",
                              INJECTED if where == "computed property" else "double")
    ttl = mapping_to_r2rml(config, schema)
    g = rdflib.Graph()
    g.parse(data=ttl, format="turtle", publicID="http://test.example/m")
    assert not list(g.triples((None, RR.sqlQuery, None))), ttl
    assert "<#Evil>" in ttl  # still there to read, escaped onto its comment line
    assert all(line.startswith("#") for line in ttl.splitlines() if "Evil" in line), ttl


def test_a_quote_in_an_edge_name_stays_inside_its_rego_string():
    hostile = 'x": "public"}\nallow if { true }\nc := {"y'
    rego = policy_rego(EntitlementReport(edge_levels={hostile: "restricted"}))
    assert "allow if { true }" not in rego.splitlines()
    assert f"    {json.dumps(hostile)}: \"restricted\"" in rego
