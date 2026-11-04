"""The streaming path's up-front check of delegated expressions, against a real ArangoDB.

Before writing anything, the pipeline runs each expression it would send to ArangoDB
once over no rows (P5c.1.5). That must refuse what ArangoDB cannot evaluate, such as
``COALESCE`` and ``??``, and pass what it can. The queries are read-only, so they run
in ``_system`` without creating anything.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from r2g.connectors.arango_writer import ArangoWriter
from r2g.streaming.pipeline import DelegationError, StreamingPipeline
from r2g.types import (
    CollectionMapping,
    Column,
    FieldExpression,
    MappingConfig,
    Schema,
    Table,
)

from .conftest import ARANGO_ENDPOINT, ARANGO_PASSWORD, ARANGO_USER, requires_arango

pytestmark = [pytest.mark.integration, requires_arango]


def _check(expression: str) -> None:
    table = Table(name="people", columns=[
        Column(name="id", data_type="integer", is_primary_key=True),
        Column(name="name", data_type="text"),
        Column(name="nick", data_type="text"),
    ], primary_key=["id"])
    # SOUNDEX is not in r2g's evaluator, so each expression goes to ArangoDB.
    config = MappingConfig(collections={"people": CollectionMapping(
        source_table="people", target_collection="people",
        field_expressions=[FieldExpression(target="code", expression=expression)],
    )}, edges=[])
    writer = ArangoWriter(endpoint=ARANGO_ENDPOINT, database="_system",
                          username=ARANGO_USER, password=ARANGO_PASSWORD)
    writer.connect()
    try:
        # The check reads no source, so any connector will do.
        StreamingPipeline(source_connector=MagicMock(), arango_writer=writer,
                          schema=Schema(tables={"people": table}), config=config)._check_delegations()
    finally:
        writer.close()


def test_an_expression_arangodb_can_evaluate_passes() -> None:
    _check("SOUNDEX(@name)")


@pytest.mark.parametrize("expression", [
    "SOUNDEX(COALESCE(@nick, @name))",
    "SOUNDEX(@nick ?? @name)",
], ids=["COALESCE", "??"])
def test_an_expression_arangodb_rejects_is_refused_before_loading(expression: str) -> None:
    with pytest.raises(DelegationError, match="Collection 'people', field 'code'"):
        _check(expression)
