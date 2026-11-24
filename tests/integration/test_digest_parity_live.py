"""``MD5`` and ``SHA512`` evaluated by r2g give ArangoDB's digests.

The Studio's hash and tokenize masks are ``MD5(TO_STRING(@col))`` and
``SHA512(TO_STRING(@col))``. They used to run only in ArangoDB; r2g now evaluates them
itself, so a token written by r2g must equal the one ArangoDB would write for the same
text, or masked values stop joining across tools.
"""

from __future__ import annotations

import pytest

from r2g.expressions import compile_expression

from .conftest import ARANGO_ENDPOINT, ARANGO_PASSWORD, ARANGO_USER, requires_arango

VALUES = ["123-45-6789", 42, 42.0, 1.5, 0.1 + 0.2, True, "é ñ 漢字", "", 12345678901234567]


@requires_arango
@pytest.mark.integration
def test_digests_match_arangodb():
    from arango import ArangoClient

    db = ArangoClient(hosts=ARANGO_ENDPOINT).db("_system", username=ARANGO_USER,
                                                password=ARANGO_PASSWORD)
    theirs = list(db.aql.execute(
        "FOR v IN @vals RETURN [MD5(TO_STRING(v)), SHA512(TO_STRING(v))]",
        bind_vars={"vals": VALUES}))
    md5 = compile_expression("MD5(TO_STRING(@v))")
    sha = compile_expression("SHA512(TO_STRING(@v))")
    ours = [[md5.evaluate({"v": v}), sha.evaluate({"v": v})] for v in VALUES]
    assert ours == theirs
