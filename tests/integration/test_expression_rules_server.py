"""docs/EXPRESSION-RULES.md, checked against r2g's ArangoDB route.

Each row goes through the same steps the streaming pipeline uses: its columns are
prepared by ``_json_safe`` (decimals become floats, dates become text), and the
expression is rendered by ``NodeTransformer.build_delegation_query``. The query is
read-only, so it runs in ``_system`` without creating anything.

``build_delegation_query`` reads only the transformer's delegated expressions. It
is called on a stand-in holding one expression, so an expression the local
evaluator can compile is still rendered the way delegation would render it.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from expression_rules_cases import CASES, REFUSED, Case, same

from r2g.expressions import extract_bind_references
from r2g.streaming.pipeline import _json_safe
from r2g.transformers.node_transformer import NodeTransformer
from r2g.types import FieldExpression

from .conftest import ARANGO_ENDPOINT, ARANGO_PASSWORD, ARANGO_USER, requires_arango

pytestmark = requires_arango

_ERROR = object()


@pytest.fixture(scope="module")
def system_db():
    from arango import ArangoClient

    client = ArangoClient(hosts=ARANGO_ENDPOINT)
    yield client.db("_system", username=ARANGO_USER, password=ARANGO_PASSWORD)
    client.close()


def _server(db, case: Case):
    query = NodeTransformer.build_delegation_query(
        SimpleNamespace(_delegated=[FieldExpression(target="v", expression=case.expr)])
    )
    row = {c: _json_safe(case.row.get(c)) for c in extract_bind_references(case.expr)}
    try:
        return next(db.aql.execute(query, bind_vars={"rows": [row]}))["v"]
    except Exception as exc:  # noqa: BLE001 - an engine error is an answer to record
        return (_ERROR, f"{type(exc).__name__}: {exc}")


def _params() -> list:
    out = []
    for case in CASES:
        if case.expected is REFUSED:
            continue
        marks = (
            [pytest.mark.xfail(strict=True, reason=f"KNOWN BREAK: {case.server_breaks}")]
            if case.server_breaks
            else []
        )
        out.append(pytest.param(case, id=case.id, marks=marks))
    return out


@pytest.mark.parametrize("case", _params())
def test_server_route_follows_the_rule(system_db, case: Case) -> None:
    actual = _server(system_db, case)
    assert same(actual, case.expected), f"{case.expr} with {case.row}: got {actual!r}, rule says {case.expected!r}"
