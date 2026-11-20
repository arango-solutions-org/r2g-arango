"""A fixed-point column with scale 0 holds whole numbers and loads as integers.

Every Snowflake integer column is ``NUMBER(38,0)``, which r2g's Snowflake
connector records as ``number(38,0)`` (r2g-arango#24). The type map called
every number/numeric/decimal a float, so the load converted ``7`` to ``7.0``
and ``9007199254740993`` to ``9007199254740992.0``: a double cannot hold a
whole number above 2**53, so its last digits were silently lost.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from r2g.config import pg_type_to_json_type
from r2g.connectors.snowflake import _column_type
from r2g.transformers.node_transformer import NodeTransformer
from r2g.types import CollectionMapping, Column, Table

ABOVE_2_53 = 2**53 + 1  # the first whole number a double cannot hold


@pytest.mark.parametrize(
    "declared",
    ["number(38,0)", "NUMBER(38, 0)", "numeric(18,0)", "decimal(10,0)", "numeric(10)",
     "number(9)", "decimal64(0)", "decimal(18, 0)"],
)
def test_a_fixed_point_type_with_scale_zero_is_an_integer(declared):
    assert pg_type_to_json_type(declared) == "integer"


@pytest.mark.parametrize(
    "declared",
    ["number(10,2)", "numeric(18,4)", "decimal(38,10)", "decimal64(4)",
     # No scale reported: the value may have a fraction, so it stays a float.
     # Bare "number" is also what RSA's Snowflake connector reports for
     # NUMBER(38,0); the Forge pins that gap (tests/test_forge_dialects.py).
     "number", "numeric", "decimal"],
)
def test_a_fixed_point_type_with_a_fraction_or_no_scale_stays_a_float(declared):
    assert pg_type_to_json_type(declared) == "float"


def test_snowflake_integer_columns_reach_the_load_as_integers():
    assert pg_type_to_json_type(_column_type("NUMBER", 38, 0)) == "integer"
    assert pg_type_to_json_type(_column_type("NUMBER", 10, 2)) == "float"


def _load(data_type: str, row: dict) -> dict:
    table = Table(
        name="accounts",
        columns=[Column(name="ID", data_type=data_type, is_nullable=False),
                 Column(name="VALUE", data_type=data_type, is_nullable=True)],
        primary_key=["ID"],
    )
    mapping = CollectionMapping(source_table="accounts", target_collection="accounts")
    return NodeTransformer(table, mapping).transform_row(row)


def test_a_snowflake_integer_loads_every_digit():
    doc = _load("number(38,0)", {"ID": 7, "VALUE": ABOVE_2_53})
    assert doc["ID"] == 7 and type(doc["ID"]) is int
    assert doc["VALUE"] == ABOVE_2_53 and type(doc["VALUE"]) is int
    assert doc["_key"] == "7"


def test_a_whole_decimal_value_loads_as_an_integer():
    # psycopg returns numeric values as Decimal; str(Decimal("1E+3")) is not int-parsable.
    doc = _load("numeric(20,0)", {"ID": Decimal(ABOVE_2_53), "VALUE": Decimal("1E+3")})
    assert doc["ID"] == ABOVE_2_53 and type(doc["ID"]) is int
    assert doc["VALUE"] == 1000 and type(doc["VALUE"]) is int


def test_a_column_with_a_fraction_still_loads_as_a_float():
    doc = _load("number(10,2)", {"ID": 1, "VALUE": Decimal("12.25")})
    assert doc["VALUE"] == 12.25 and type(doc["VALUE"]) is float
