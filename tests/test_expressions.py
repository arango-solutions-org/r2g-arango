from __future__ import annotations

import pytest

from r2g.expressions import (
    SUPPORTED_FUNCTIONS,
    ExpressionError,
    compile_expression,
    evaluate,
    extract_bind_references,
    rewrite_bind_params,
)


class TestLiterals:
    def test_number_literal(self):
        assert evaluate("42", {}) == 42
        assert evaluate("3.14", {}) == 3.14

    def test_string_literal_double_and_single_quotes(self):
        assert evaluate('"hi"', {}) == "hi"
        assert evaluate("'hi'", {}) == "hi"

    def test_escape_sequences(self):
        assert evaluate(r'"a\tb\nc"', {}) == "a\tb\nc"

    def test_boolean_and_null(self):
        assert evaluate("true", {}) is True
        assert evaluate("false", {}) is False
        assert evaluate("null", {}) is None


class TestBindings:
    def test_bind_lookup(self):
        assert evaluate("@name", {"name": "Alice"}) == "Alice"

    def test_missing_binding_is_null(self):
        assert evaluate("@missing", {}) is None

    def test_bind_ref_with_underscore_and_digits(self):
        assert evaluate("@user_id_1", {"user_id_1": 7}) == 7


class TestArithmetic:
    def test_addition(self):
        assert evaluate("@a + @b", {"a": 3, "b": 4}) == 7

    def test_subtraction_and_unary_minus(self):
        assert evaluate("-@a + @b", {"a": 10, "b": 3}) == -7

    def test_multiplication_division(self):
        assert evaluate("@a * 2 / 4", {"a": 10}) == 5

    def test_modulo(self):
        assert evaluate("@a % 3", {"a": 10}) == 1

    def test_null_propagates_for_arithmetic(self):
        assert evaluate("@a + 1", {"a": None}) is None

    def test_division_by_zero_is_null(self):
        assert evaluate("@a / 0", {"a": 10}) is None

    def test_string_concat_via_plus(self):
        assert evaluate("@a + @b", {"a": "foo", "b": "bar"}) == "foobar"

    def test_mixed_string_plus_number_coerces_to_string(self):
        assert evaluate('"v=" + @a', {"a": 7}) == "v=7"


class TestComparison:
    def test_equality(self):
        assert evaluate("@a == 5", {"a": 5}) is True
        assert evaluate("@a != 5", {"a": 4}) is True

    def test_ordering(self):
        assert evaluate("@a < @b", {"a": 1, "b": 2}) is True
        assert evaluate("@a >= @b", {"a": 3, "b": 3}) is True

    def test_null_comparison(self):
        assert evaluate("@a == null", {"a": None}) is True
        assert evaluate("@a != null", {"a": 0}) is True
        assert evaluate("@a < 5", {"a": None}) is None


class TestLogical:
    def test_short_circuit_and(self):
        assert evaluate("@a && @b", {"a": True, "b": False}) is False

    def test_short_circuit_or(self):
        assert evaluate("@a || @b", {"a": False, "b": True}) is True

    def test_not(self):
        assert evaluate("NOT @a", {"a": False}) is True

    def test_case_insensitive_keywords(self):
        assert evaluate("@a and @b", {"a": 1, "b": 1}) is True
        assert evaluate("@a Or false", {"a": 1}) is True


class TestNullCoalesceAndTernary:
    def test_nullcoal_falls_through(self):
        assert evaluate('@a ?? "default"', {"a": None}) == "default"

    def test_nullcoal_keeps_value(self):
        assert evaluate('@a ?? "default"', {"a": "v"}) == "v"

    def test_nullcoal_chain(self):
        assert evaluate('@a ?? @b ?? "z"', {"a": None, "b": None}) == "z"

    def test_ternary(self):
        assert evaluate('@a > 0 ? "pos" : "neg"', {"a": 3}) == "pos"
        assert evaluate('@a > 0 ? "pos" : "neg"', {"a": -3}) == "neg"


class TestFunctions:
    def test_concat_with_nulls_becomes_empty(self):
        assert evaluate("CONCAT(@a, @b, @c)", {"a": "x", "b": None, "c": "y"}) == "xy"

    def test_concat_separator(self):
        assert (
            evaluate('CONCAT_SEPARATOR(", ", @a, @b, @c)', {"a": "x", "b": "y", "c": "z"})
            == "x, y, z"
        )

    def test_upper_lower_case_insensitive_name(self):
        assert evaluate("upper(@s)", {"s": "abc"}) == "ABC"
        assert evaluate("LOWER(@s)", {"s": "ABC"}) == "abc"

    def test_substring_two_and_three_args(self):
        assert evaluate("SUBSTRING(@s, 0, 3)", {"s": "abcdef"}) == "abc"
        assert evaluate("SUBSTRING(@s, 2)", {"s": "abcdef"}) == "cdef"

    def test_substring_null_input(self):
        assert evaluate("SUBSTRING(@s, 0, 3)", {"s": None}) is None

    def test_length(self):
        assert evaluate("LENGTH(@s)", {"s": "abc"}) == 3
        assert evaluate("LENGTH(@s)", {"s": None}) is None

    def test_trim_variants(self):
        assert evaluate("LTRIM(@s)", {"s": "  x"}) == "x"
        assert evaluate("RTRIM(@s)", {"s": "x  "}) == "x"
        assert evaluate("TRIM(@s)", {"s": "  x  "}) == "x"

    def test_contains(self):
        assert evaluate('CONTAINS(@s, "bc")', {"s": "abcdef"}) is True
        assert evaluate('CONTAINS(@s, "zz")', {"s": "abcdef"}) is False

    def test_coalesce(self):
        assert evaluate("COALESCE(@a, @b, @c)", {"a": None, "b": None, "c": 3}) == 3

    def test_to_number_to_string_to_bool(self):
        assert evaluate("TO_NUMBER(@s)", {"s": "12"}) == 12
        assert evaluate("TO_NUMBER(@s)", {"s": "1.5"}) == 1.5
        assert evaluate("TO_STRING(@n)", {"n": 12}) == "12"
        assert evaluate("TO_BOOL(@v)", {"v": ""}) is False
        assert evaluate("TO_BOOL(@v)", {"v": "x"}) is True

    def test_supported_functions_are_advertised(self):
        assert "CONCAT" in SUPPORTED_FUNCTIONS
        assert "SUBSTRING" in SUPPORTED_FUNCTIONS


class TestParseErrors:
    def test_unknown_function_is_rejected_at_compile_time(self):
        with pytest.raises(ExpressionError):
            compile_expression("FETCH_REMOTE(@a)")

    def test_bare_identifier_without_call_is_rejected(self):
        with pytest.raises(ExpressionError):
            compile_expression("name")

    def test_single_equals_is_rejected(self):
        with pytest.raises(ExpressionError):
            compile_expression("@a = 1")

    def test_unterminated_string(self):
        with pytest.raises(ExpressionError):
            compile_expression('"abc')

    def test_empty_expression(self):
        with pytest.raises(ExpressionError):
            compile_expression("   ")

    def test_trailing_tokens(self):
        with pytest.raises(ExpressionError):
            compile_expression("@a @b")


class TestBindParamHelpers:
    """Helpers used to push unsupported expressions down to ArangoDB (P5c.1.5)."""

    def test_extract_bind_references_distinct_in_order(self):
        assert extract_bind_references("CONCAT(@last, @first, @last)") == ["last", "first"]

    def test_extract_ignores_at_inside_strings(self):
        assert extract_bind_references('CONCAT(@user, "@example.com")') == ["user"]

    def test_extract_empty(self):
        assert extract_bind_references("") == []
        assert extract_bind_references("UPPER('x')") == []

    def test_rewrite_bind_params_to_row_refs(self):
        assert rewrite_bind_params("@first") == "row.`first`"

    def test_rewrite_preserves_surrounding_aql(self):
        # An expression outside the local subset (REGEX_REPLACE) still rewrites cleanly.
        out = rewrite_bind_params("REGEX_REPLACE(@name, '\\\\s+', '_')")
        assert out == "REGEX_REPLACE(row.`name`, '\\\\s+', '_')"

    def test_rewrite_does_not_touch_at_in_strings(self):
        out = rewrite_bind_params('CONCAT(@user, "@host")')
        assert out == 'CONCAT(row.`user`, "@host")'

    def test_rewrite_custom_var(self):
        assert rewrite_bind_params("@x + @y", var="doc") == "doc.`x` + doc.`y`"


class TestCompiledExpression:
    def test_references_collected_sorted(self):
        c = compile_expression('CONCAT(@last, ", ", @first)')
        assert c.references == ("first", "last")

    def test_reuses_compiled_form(self):
        c = compile_expression("UPPER(@s)")
        assert c.evaluate({"s": "ada"}) == "ADA"
        assert c.evaluate({"s": "lovelace"}) == "LOVELACE"



class TestDigests:
    """``MD5``/``SHA512``: the Studio's hash and tokenize masks, evaluated here."""

    @pytest.mark.parametrize("fn, algo", [("MD5", "md5"), ("SHA512", "sha512")])
    def test_lower_case_hex_of_the_text(self, fn, algo):
        import hashlib

        expr = compile_expression(f"{fn}(TO_STRING(@v))")
        assert expr.evaluate({"v": "123-45-6789"}) == hashlib.new(
            algo, b"123-45-6789").hexdigest()
        assert expr.evaluate({"v": 42.0}) == hashlib.new(algo, b"42").hexdigest()

    def test_a_blank_stays_blank(self):
        assert compile_expression("MD5(@v)").evaluate({"v": None}) is None

    def test_one_argument(self):
        with pytest.raises(ExpressionError):
            compile_expression("MD5(@a, @b)").evaluate({"a": "x", "b": "y"})
