"""docs/EXPRESSION-RULES.md, checked against the mapping check and r2g's own evaluator.

The ArangoDB route is checked in tests/integration/test_expression_rules_server.py
against the same cases. See tests/expression_rules_cases.py for how the cases and
the record of known breaks work.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from expression_rules_cases import CASES, REFUSED, RULES_WITHOUT_CASES, Case, same

from r2g.expressions import ExpressionError, compile_expression

RULEBOOK = Path(__file__).resolve().parents[1] / "docs" / "EXPRESSION-RULES.md"


def _params(cases: list[Case], breaks: str) -> list:
    out = []
    for case in cases:
        reason = getattr(case, breaks)
        marks = [pytest.mark.xfail(strict=True, reason=f"KNOWN BREAK: {reason}")] if reason else []
        out.append(pytest.param(case, id=case.id, marks=marks))
    return out


_REFUSED = [c for c in CASES if c.expected is REFUSED]
_VALUED = [c for c in CASES if c.expected is not REFUSED]


@pytest.mark.parametrize("case", _params(_REFUSED, "local_breaks"))
def test_check_refuses(case: Case) -> None:
    with pytest.raises(ExpressionError):
        compile_expression(case.expr)


@pytest.mark.parametrize("case", _params(_VALUED, "local_breaks"))
def test_local_evaluator_follows_the_rule(case: Case) -> None:
    actual = compile_expression(case.expr).evaluate(dict(case.row))
    assert same(actual, case.expected), f"{case.expr} with {case.row}: got {actual!r}, rule says {case.expected!r}"


# ── The record itself ───────────────────────────────────────────────────


def test_case_ids_are_unique() -> None:
    ids = [c.id for c in CASES]
    assert len(ids) == len(set(ids))


def test_every_rule_has_a_case_or_a_reason() -> None:
    covered = {c.rule for c in CASES}
    assert covered.isdisjoint(RULES_WITHOUT_CASES), "a rule with cases is still listed as having none"
    assert covered | set(RULES_WITHOUT_CASES) == set(_rulebook_changes_column())


def test_refused_cases_record_no_server_break() -> None:
    # A refused expression never reaches a route, so a server record would be dead.
    assert not [c.id for c in _REFUSED if c.server_breaks]


def _rulebook_changes_column() -> dict[int, bool]:
    """Rule number -> whether the rulebook's summary table says it changes r2g."""
    rows = re.findall(r"^\| (\d+) \|.*\| (\*\*Yes\*\*|No)[^|]*\|$", RULEBOOK.read_text(), re.MULTILINE)
    return {int(n): flag.startswith("**Yes") for n, flag in rows}


def test_rulebook_says_which_rules_change_r2g() -> None:
    """A rule changes r2g exactly when r2g's own evaluator or check breaks one of its cases."""
    column = _rulebook_changes_column()
    broken = {c.rule for c in CASES if c.local_breaks}
    wrong = {n: says for n, says in column.items() if says != (n in broken)}
    assert not wrong, (
        f"rulebook 'Changes r2g?' disagrees with the recorded breaks for rules {sorted(wrong)}; "
        "update the table or the record in tests/expression_rules_cases.py"
    )
