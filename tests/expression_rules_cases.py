"""Worked examples for every rule in docs/EXPRESSION-RULES.md, and the record of
which r2g route breaks which rule today.

Each case states what the rulebook says the answer is. The answers come from the
rulebook, never from running an engine, so a route cannot pass by agreeing with
itself. Three routes are checked:

- ``check``: compiling the expression, which is where a mapping is checked. Only
  cases whose answer is ``REFUSED`` are checked here.
- ``local``: r2g's own Python evaluator, used on every load path when the
  expression compiles.
- ``server``: the ArangoDB delegation query, with rows prepared exactly as the
  streaming pipeline prepares them (tests/integration/test_expression_rules_server.py).

``local_breaks`` and ``server_breaks`` record, in words, how a route gets a case
wrong today. Those cases run as strict expected failures: when a fix makes one
pass, the test fails until its record here is removed. A case with no record
must follow the rule. test_expression_rules.py also checks that the rulebook's
"Changes r2g?" column agrees with ``local_breaks``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


class _Refused:
    def __repr__(self) -> str:
        return "REFUSED"


#: The expression must be refused when the mapping is checked.
REFUSED = _Refused()


@dataclass(frozen=True)
class Case:
    id: str
    rule: int
    expr: str
    row: dict[str, Any] = field(default_factory=dict)
    expected: Any = None
    local_breaks: str | None = None
    server_breaks: str | None = None


# How the routes break the rules, worded once so the records read the same.
_BLANK_AS_ZERO = "ArangoDB reads a blank as 0 in arithmetic"
_BLANK_SORTS_LOW = "ArangoDB sorts a blank below every number, so the comparison is true"
_NOT_CHECKED = "nothing checks expression types when a mapping is checked"
_NO_COALESCE = "ArangoDB has no COALESCE; the route sends it untranslated"

CASES: tuple[Case, ...] = (
    # 1. A blank in arithmetic gives a blank.
    Case("r1_add", 1, "@x + 1", {"x": None}, None, server_breaks=_BLANK_AS_ZERO),
    Case("r1_multiply", 1, "@x * 2", {"x": None}, None, server_breaks=_BLANK_AS_ZERO),
    # 2. A condition about a blank is never true.
    Case("r2_less_than", 2, '@s < 30 ? "yes" : "no"', {"s": None}, "no",
         server_breaks=_BLANK_SORTS_LOW),
    Case("r2_band", 2, '@s < 30 ? "low" : (@s < 70 ? "medium" : "high")', {"s": None}, "high",
         server_breaks=_BLANK_SORTS_LOW),
    Case("r2_not", 2, 'NOT (@s < 30) ? "yes" : "no"', {"s": None}, "no",
         local_breaks="a blank comparison counts as false, so NOT makes it true"),
    Case("r2_not_equal", 2, '@s != 5 ? "yes" : "no"', {"s": None}, "no",
         local_breaks="!= with a blank is true",
         server_breaks="!= with a blank is true"),
    Case("r2_is_blank", 2, '@s == null ? "yes" : "no"', {"s": None}, "yes"),
    Case("r2_comparison_value", 2, "@s < 30", {"s": None}, None,
         server_breaks=_BLANK_SORTS_LOW),
    # 3. Only true or false values can be used as conditions.
    Case("r3_text_literal_condition", 3, '"" ? "yes" : "no"', {}, REFUSED,
         local_breaks=_NOT_CHECKED),
    Case("r3_number_literal_condition", 3, '0 ? "yes" : "no"', {}, REFUSED,
         local_breaks=_NOT_CHECKED),
    Case("r3_empty_list_at_load", 3, '@l ? "yes" : "no"', {"l": []}, "no",
         server_breaks="ArangoDB counts an empty list as true"),
    Case("r3_list_at_load", 3, '@l ? "yes" : "no"', {"l": [1]}, "no",
         local_breaks="a list with items counts as true",
         server_breaks="a list with items counts as true"),
    Case("r3_text_at_load", 3, '@t ? "yes" : "no"', {"t": "x"}, "no",
         local_breaks="non-empty text counts as true",
         server_breaks="non-empty text counts as true"),
    Case("r3_boolean_column", 3, '@b ? "yes" : "no"', {"b": True}, "yes"),
    # 4. && and || give true, false or a blank.
    Case("r4_blank_or_false", 4, "@a || false", {"a": None}, None,
         local_breaks="a blank counts as false",
         server_breaks="ArangoDB returns the last operand, false"),
    Case("r4_blank_or_true", 4, "@a || true", {"a": None}, True),
    Case("r4_blank_and_true", 4, "@a && true", {"a": None}, None,
         local_breaks="a blank counts as false"),
    Case("r4_blank_and_false", 4, "@a && false", {"a": None}, False,
         server_breaks="ArangoDB returns the first operand, the blank"),
    Case("r4_not_blank_or_false", 4, 'NOT (@a || false) ? "yes" : "no"', {"a": None}, "no",
         local_breaks="a blank counts as false, so NOT makes it true",
         server_breaks="blank || false is false, so NOT makes it true"),
    Case("r4_text_operands_at_load", 4, "@a && @b", {"a": "y", "b": "x"}, None,
         local_breaks="non-empty text counts as true",
         server_breaks="ArangoDB returns an operand"),
    Case("r4_booleans", 4, "@a && @b", {"a": True, "b": True}, True),
    # 5. CONCAT treats a blank as empty text; CONCAT_SEPARATOR leaves it out.
    Case("r5_concat_blank", 5, 'CONCAT(UPPER(@first), " ", @last)',
         {"first": None, "last": "Smith"}, " Smith"),
    Case("r5_concat_separator_blank", 5, 'CONCAT_SEPARATOR("-", "a", @n, "b")', {"n": None}, "a-b",
         local_breaks="a blank becomes empty text, doubling the separator"),
    Case("r5_upper_blank", 5, "UPPER(@n)", {"n": None}, None,
         server_breaks="ArangoDB turns the blank into empty text"),
    Case("r5_length_blank", 5, "LENGTH(@n)", {"n": None}, None,
         server_breaks="ArangoDB gives 0"),
    # 6. + is for numbers only.
    Case("r6_text_literal_join", 6, 'UPPER(@first) + " " + @last', {}, REFUSED,
         local_breaks=_NOT_CHECKED),
    Case("r6_number_plus_text_literal", 6, '@n + "1"', {}, REFUSED,
         local_breaks=_NOT_CHECKED),
    Case("r6_text_at_load", 6, "@a + @b", {"a": "x", "b": 1}, None,
         local_breaks="+ joins the text",
         server_breaks="ArangoDB turns the text into 0 and adds"),
    Case("r6_numbers", 6, "@a + @b", {"a": 2, "b": 3}, 5),
    # 7. Text that isn't a number becomes a blank.
    Case("r7_not_a_number", 7, "TO_NUMBER(@x)", {"x": "12a"}, None,
         server_breaks="ArangoDB gives 0"),
    Case("r7_empty_text", 7, "TO_NUMBER(@x)", {"x": ""}, None,
         server_breaks="ArangoDB gives 0"),
    Case("r7_leading_zeros", 7, "TO_NUMBER(@x)", {"x": "00123"}, 123),
    # 8. Numbers are written as text in one fixed way.
    Case("r8_float_key", 8, 'CONCAT(@t, "_", @id)', {"t": 7, "id": 42.0}, "7_42"),
    Case("r8_decimal_one_place", 8, 'CONCAT(@t, "_", @id)', {"t": 7, "id": Decimal("42.0")}, "7_42"),
    Case("r8_decimal_two_places", 8, 'CONCAT(@t, "_", @id)', {"t": 7, "id": Decimal("42.00")}, "7_42"),
    Case("r8_decimal_fraction", 8, "TO_STRING(@x)", {"x": Decimal("1.50")}, "1.5"),
    Case("r8_shortest_fraction", 8, "TO_STRING(@x)", {"x": 0.1 + 0.2}, "0.30000000000000004"),
    Case("r8_large_number", 8, "TO_STRING(@x)", {"x": 1e20}, "1e+20"),
    Case("r8_round_decimal", 8, "TO_STRING(@x)", {"x": Decimal("100000000.00")}, "100000000",
         server_breaks="ArangoDB writes a whole number ending in eight zeros as 1e+8"),
    Case("r8_beyond_exact_whole", 8, "TO_STRING(@x)", {"x": 1e16}, "1e+16",
         server_breaks="ArangoDB writes 10000000000000000.0"),
    Case("r8_small_fraction", 8, "TO_STRING(@x)", {"x": 0.00001}, "1e-5",
         server_breaks="ArangoDB writes a fraction below 0.0001 in plain decimals, 0.00001"),
    # 9. Text positions start at 0 and count characters.
    Case("r9_substring", 9, "SUBSTRING(@p, 0, 3)", {"p": "Zürich"}, "Zür"),
    Case("r9_length", 9, "LENGTH(@p)", {"p": "Zürich"}, 6),
    # 10. Division gives the exact fraction; dividing by zero gives a blank.
    Case("r10_division", 10, "@a / @b", {"a": 7, "b": 2}, 3.5),
    Case("r10_divide_by_zero", 10, "@a / 0", {"a": 1}, None),
    Case("r10_remainder_by_zero", 10, "@a % 0", {"a": 7}, None),
    # 11. Text and numbers can't be compared with each other.
    Case("r11_text_below_number", 11, '"10" < 9', {}, REFUSED, local_breaks=_NOT_CHECKED),
    Case("r11_text_equals_number", 11, '"1" == 1', {}, REFUSED, local_breaks=_NOT_CHECKED),
    Case("r11_boolean_equals_number", 11, "true == 1", {}, REFUSED, local_breaks=_NOT_CHECKED),
    Case("r11_whole_and_fraction", 11, "@a == @b", {"a": 1, "b": 1.0}, True),
    Case("r11_text_vs_number_at_load", 11, '@a < @b ? "yes" : "no"', {"a": "10", "b": 9}, "no",
         local_breaks="the two are compared as text"),
    # 12. Empty text is not a blank.
    Case("r12_coalesce_keeps_empty_text", 12, "COALESCE(@a, @b)", {"a": "", "b": "z"}, "",
         server_breaks=_NO_COALESCE),
    Case("r12_empty_text_is_not_blank", 12, '@a == null ? "yes" : "no"', {"a": ""}, "no"),
    # 13. COALESCE and ?? both stay, and mean the same thing.
    Case("r13_coalesce", 13, "COALESCE(@a, @b)", {"a": None, "b": "z"}, "z",
         server_breaks=_NO_COALESCE),
    Case("r13_double_question_mark", 13, "@a ?? @b", {"a": None, "b": "z"}, "z",
         server_breaks="ArangoDB has no ?? operator; the route sends it untranslated"),
    # 14. Times are in UTC. No cases: the language has no date or time functions yet.
)

#: Rules with no cases, and why. A rule leaves this list when it gains a case.
RULES_WITHOUT_CASES = {14: "the expression language has no date or time functions yet"}


def same(actual: Any, expected: Any) -> bool:
    """Equal in value *and* kind, so ``True`` is not ``1`` and ``"1"`` is not ``1``.

    Whole numbers and fractions compare by value (``5 == 5.0``): a stored JSON
    document cannot tell them apart.
    """
    if expected is None or actual is None:
        return actual is None and expected is None
    if isinstance(expected, bool) or isinstance(actual, bool):
        return isinstance(actual, bool) and isinstance(expected, bool) and actual == expected
    numbers = (int, float, Decimal)
    if isinstance(expected, numbers) or isinstance(actual, numbers):
        return isinstance(actual, numbers) and isinstance(expected, numbers) and actual == expected
    if isinstance(expected, str) or isinstance(actual, str):
        return isinstance(actual, str) and isinstance(expected, str) and actual == expected
    return actual == expected
