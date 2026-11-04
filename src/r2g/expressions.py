"""Safe evaluator for an AQL-flavoured language of per-row field expressions.

Supports the language promised in PRD P5c.1.4. The syntax follows AQL but the
semantics do not: null handling, ``||``/``&&``, ``+`` on strings and
cross-type comparison all differ from ArangoDB, and ``??``/``COALESCE`` are
not AQL at all. See P5c.1.4 before changing behaviour toward AQL parity.

- Literals: numbers, strings ("..." or '...'), ``true``, ``false``, ``null``
- Bind parameters: ``@col_name`` refers to the source row value
- Arithmetic: ``+ - * / %`` (a ``null`` operand makes the result ``null``; AQL
  would treat it as ``0``)
- Comparison: ``== != < <= > >=``
- Logical: ``&& || NOT`` (case-insensitive ``AND`` / ``OR`` / ``NOT`` accepted too)
- Null coalescing: ``??``
- Ternary: ``cond ? a : b``
- Function calls (case-insensitive): ``CONCAT``, ``CONCAT_SEPARATOR``, ``UPPER``,
  ``LOWER``, ``SUBSTRING``, ``LENGTH``, ``LTRIM``, ``RTRIM``, ``TRIM``,
  ``TO_STRING``, ``TO_NUMBER``, ``TO_BOOL``, ``CONTAINS``, ``COALESCE``

Anything outside this subset raises :class:`ExpressionError` at compile time
so the streaming path can fall back to server-side delegation (per the PRD
path P5c.1.5). The evaluator is deterministic and never executes arbitrary
code: the parser only produces the AST node types handled by :meth:`eval_ast`,
and the function table is closed.

Usage::

    from r2g.expressions import compile_expression

    expr = compile_expression('CONCAT(UPPER(@first), " ", UPPER(@last))')
    result = expr.evaluate({"first": "ada", "last": "lovelace"})
    # 'ADA LOVELACE'
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable


class ExpressionError(Exception):
    """Raised for any parse-time or runtime problem in an expression."""


# ── Lexer ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Tok:
    kind: str
    value: Any
    pos: int


_SINGLE = {
    "(": "LPAREN", ")": "RPAREN", ",": "COMMA",
    "+": "PLUS", "-": "MINUS", "*": "STAR", "/": "SLASH", "%": "PERCENT",
    "?": "QMARK", ":": "COLON",
}

_KEYWORDS = {
    "true": ("BOOL", True),
    "false": ("BOOL", False),
    "null": ("NULL", None),
    "and": ("AND", None),
    "or": ("OR", None),
    "not": ("NOT", None),
}


def _tokenize(src: str) -> list[_Tok]:
    toks: list[_Tok] = []
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        if c.isspace():
            i += 1
            continue
        two = src[i:i + 2]
        if two == "==":
            toks.append(_Tok("EQ", "==", i))
            i += 2
            continue
        if two == "!=":
            toks.append(_Tok("NEQ", "!=", i))
            i += 2
            continue
        if two == "<=":
            toks.append(_Tok("LTE", "<=", i))
            i += 2
            continue
        if two == ">=":
            toks.append(_Tok("GTE", ">=", i))
            i += 2
            continue
        if two == "&&":
            toks.append(_Tok("AND", "&&", i))
            i += 2
            continue
        if two == "||":
            toks.append(_Tok("OR", "||", i))
            i += 2
            continue
        if two == "??":
            toks.append(_Tok("NULLCOAL", "??", i))
            i += 2
            continue
        if c in _SINGLE:
            toks.append(_Tok(_SINGLE[c], c, i))
            i += 1
            continue
        if c == "<":
            toks.append(_Tok("LT", "<", i))
            i += 1
            continue
        if c == ">":
            toks.append(_Tok("GT", ">", i))
            i += 1
            continue
        if c == "=":
            raise ExpressionError(f"single '=' is not allowed (use '==') at column {i}")
        if c == "!":
            raise ExpressionError(f"single '!' is not allowed (use '!=') at column {i}")
        if c == "@":
            j = i + 1
            if j >= n or not (src[j].isalpha() or src[j] == "_"):
                raise ExpressionError(f"expected bind-parameter name after '@' at column {i}")
            while j < n and (src[j].isalnum() or src[j] == "_"):
                j += 1
            toks.append(_Tok("BIND", src[i + 1:j], i))
            i = j
            continue
        if c == '"' or c == "'":
            quote = c
            j = i + 1
            out: list[str] = []
            while j < n and src[j] != quote:
                if src[j] == "\\" and j + 1 < n:
                    nxt = src[j + 1]
                    out.append({"n": "\n", "t": "\t", "r": "\r", "\\": "\\",
                                quote: quote}.get(nxt, nxt))
                    j += 2
                    continue
                out.append(src[j])
                j += 1
            if j >= n:
                raise ExpressionError(f"unterminated string starting at column {i}")
            toks.append(_Tok("STR", "".join(out), i))
            i = j + 1
            continue
        if c.isdigit() or (c == "." and i + 1 < n and src[i + 1].isdigit()):
            j = i
            has_dot = False
            while j < n and (src[j].isdigit() or (src[j] == "." and not has_dot)):
                if src[j] == ".":
                    has_dot = True
                j += 1
            raw = src[i:j]
            val: float | int = float(raw) if has_dot else int(raw)
            toks.append(_Tok("NUM", val, i))
            i = j
            continue
        if c.isalpha() or c == "_":
            j = i
            while j < n and (src[j].isalnum() or src[j] == "_"):
                j += 1
            name = src[i:j]
            low = name.lower()
            if low in _KEYWORDS:
                kw_kind, kw_val = _KEYWORDS[low]
                toks.append(_Tok(kw_kind, kw_val, i))
            else:
                toks.append(_Tok("IDENT", name, i))
            i = j
            continue
        raise ExpressionError(f"unexpected character {c!r} at column {i}")
    toks.append(_Tok("EOF", None, n))
    return toks


# ── Parser ───────────────────────────────────────────────────────────────


class _Parser:
    def __init__(self, toks: list[_Tok], src: str) -> None:
        self.toks = toks
        self.src = src
        self.i = 0

    def _peek(self) -> _Tok:
        return self.toks[self.i]

    def _advance(self) -> _Tok:
        t = self.toks[self.i]
        self.i += 1
        return t

    def _expect(self, kind: str) -> _Tok:
        t = self._peek()
        if t.kind != kind:
            raise ExpressionError(f"expected {kind} at column {t.pos}, got {t.kind}")
        return self._advance()

    def parse(self) -> tuple:
        node = self._ternary()
        if self._peek().kind != "EOF":
            t = self._peek()
            raise ExpressionError(f"unexpected token {t.kind} at column {t.pos}")
        return node

    def _ternary(self) -> tuple:
        cond = self._nullcoal()
        if self._peek().kind == "QMARK":
            self._advance()
            a = self._ternary()
            self._expect("COLON")
            b = self._ternary()
            return ("tern", cond, a, b)
        return cond

    def _nullcoal(self) -> tuple:
        left = self._or()
        while self._peek().kind == "NULLCOAL":
            self._advance()
            right = self._or()
            left = ("bin", "??", left, right)
        return left

    def _or(self) -> tuple:
        left = self._and()
        while self._peek().kind == "OR":
            self._advance()
            right = self._and()
            left = ("bin", "||", left, right)
        return left

    def _and(self) -> tuple:
        left = self._not()
        while self._peek().kind == "AND":
            self._advance()
            right = self._not()
            left = ("bin", "&&", left, right)
        return left

    def _not(self) -> tuple:
        if self._peek().kind == "NOT":
            self._advance()
            return ("unary", "not", self._not())
        return self._equality()

    def _equality(self) -> tuple:
        left = self._compare()
        while self._peek().kind in ("EQ", "NEQ"):
            op = self._advance().value
            right = self._compare()
            left = ("bin", op, left, right)
        return left

    def _compare(self) -> tuple:
        left = self._additive()
        while self._peek().kind in ("LT", "LTE", "GT", "GTE"):
            op = self._advance().value
            right = self._additive()
            left = ("bin", op, left, right)
        return left

    def _additive(self) -> tuple:
        left = self._term()
        while self._peek().kind in ("PLUS", "MINUS"):
            op = self._advance().value
            right = self._term()
            left = ("bin", op, left, right)
        return left

    def _term(self) -> tuple:
        left = self._unary()
        while self._peek().kind in ("STAR", "SLASH", "PERCENT"):
            op = self._advance().value
            right = self._unary()
            left = ("bin", op, left, right)
        return left

    def _unary(self) -> tuple:
        if self._peek().kind == "MINUS":
            self._advance()
            return ("unary", "-", self._unary())
        if self._peek().kind == "PLUS":
            self._advance()
            return self._unary()
        return self._primary()

    def _primary(self) -> tuple:
        t = self._peek()
        if t.kind == "NUM":
            self._advance()
            return ("num", t.value)
        if t.kind == "STR":
            self._advance()
            return ("str", t.value)
        if t.kind == "BOOL":
            self._advance()
            return ("bool", t.value)
        if t.kind == "NULL":
            self._advance()
            return ("null", None)
        if t.kind == "BIND":
            self._advance()
            return ("bind", t.value)
        if t.kind == "LPAREN":
            self._advance()
            e = self._ternary()
            self._expect("RPAREN")
            return e
        if t.kind == "IDENT":
            name = self._advance().value
            if self._peek().kind != "LPAREN":
                raise ExpressionError(
                    f"bare identifier {name!r} at column {t.pos}: "
                    "use @{name} to reference a column, or call as {name}(...)"
                )
            self._advance()
            args: list[tuple] = []
            if self._peek().kind != "RPAREN":
                args.append(self._ternary())
                while self._peek().kind == "COMMA":
                    self._advance()
                    args.append(self._ternary())
            self._expect("RPAREN")
            return ("call", name.upper(), args)
        raise ExpressionError(f"unexpected {t.kind} at column {t.pos}")


# ── Evaluator ────────────────────────────────────────────────────────────


def _to_string(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (float, Decimal)):
        return _number_text(float(v))
    return str(v)


def _number_text(x: float) -> str:
    """A number written as text, by rule 8 of docs/EXPRESSION-RULES.md: a whole number
    below 10**16 without a decimal point (``42``), any other in the fewest digits that read
    back as the same number (``0.30000000000000004``, ``1e+20``, ``1e-5``). A decimal is
    written as the floating-point number r2g stores for it, so ``42.00`` is ``42``."""
    if x.is_integer() and abs(x) < 1e16:
        return str(int(x))
    mantissa, _, exponent = repr(x).partition("e")
    return f"{mantissa}e{int(exponent):+d}" if exponent else mantissa


def _to_number(v: Any) -> float | int | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        return v
    s = str(v).strip()
    if s == "":
        return None
    try:
        if "." in s or "e" in s or "E" in s:
            return float(s)
        return int(s)
    except ValueError:
        try:
            return float(s)
        except ValueError:
            return None


def _to_bool(v: Any) -> bool:
    if v is None or v is False:
        return False
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v != ""
    return bool(v)


def _fn_concat(args: list[Any]) -> str:
    return "".join(_to_string(a) for a in args)


def _fn_concat_separator(args: list[Any]) -> str:
    if not args:
        raise ExpressionError("CONCAT_SEPARATOR requires at least one argument")
    sep = _to_string(args[0])
    return sep.join(_to_string(a) for a in args[1:])


def _fn_substring(args: list[Any]) -> str | None:
    if len(args) not in (2, 3):
        raise ExpressionError("SUBSTRING takes 2 or 3 arguments")
    s = args[0]
    if s is None:
        return None
    s = _to_string(s)
    start = int(_to_number(args[1]) or 0)
    if start < 0:
        start = max(len(s) + start, 0)
    if len(args) == 3:
        length = int(_to_number(args[2]) or 0)
        return s[start:start + length]
    return s[start:]


def _fn_length(args: list[Any]) -> int | None:
    if len(args) != 1:
        raise ExpressionError("LENGTH takes 1 argument")
    v = args[0]
    if v is None:
        return None
    if isinstance(v, (list, dict, str)):
        return len(v)
    return len(_to_string(v))


def _fn_contains(args: list[Any]) -> bool | None:
    if len(args) != 2:
        raise ExpressionError("CONTAINS takes 2 arguments")
    s, needle = args
    if s is None or needle is None:
        return None
    return _to_string(needle) in _to_string(s)


def _fn_coalesce(args: list[Any]) -> Any:
    for a in args:
        if a is not None:
            return a
    return None


_FUNCS: dict[str, Callable[[list[Any]], Any]] = {
    "CONCAT": _fn_concat,
    "CONCAT_SEPARATOR": _fn_concat_separator,
    "UPPER": lambda a: None if a[0] is None else _to_string(a[0]).upper(),
    "LOWER": lambda a: None if a[0] is None else _to_string(a[0]).lower(),
    "SUBSTRING": _fn_substring,
    "LENGTH": _fn_length,
    "LTRIM": lambda a: None if a[0] is None else _to_string(a[0]).lstrip(),
    "RTRIM": lambda a: None if a[0] is None else _to_string(a[0]).rstrip(),
    "TRIM": lambda a: None if a[0] is None else _to_string(a[0]).strip(),
    "TO_STRING": lambda a: None if a[0] is None else _to_string(a[0]),
    "TO_NUMBER": lambda a: _to_number(a[0]),
    "TO_BOOL": lambda a: _to_bool(a[0]),
    "CONTAINS": _fn_contains,
    "COALESCE": _fn_coalesce,
}


def _arith(op: str, a: Any, b: Any) -> Any:
    if a is None or b is None:
        return None
    if op == "+":
        if isinstance(a, str) or isinstance(b, str):
            return _to_string(a) + _to_string(b)
        return a + b
    na, nb = _to_number(a), _to_number(b)
    if na is None or nb is None:
        return None
    if op == "-":
        return na - nb
    if op == "*":
        return na * nb
    if op == "/":
        if nb == 0:
            return None
        return na / nb
    if op == "%":
        if nb == 0:
            return None
        return na % nb
    raise ExpressionError(f"unknown arithmetic operator {op!r}")


def _compare(op: str, a: Any, b: Any) -> Any:
    if a is None or b is None:
        if op == "==":
            return a is None and b is None
        if op == "!=":
            return not (a is None and b is None)
        return None
    try:
        if op == "==":
            return a == b
        if op == "!=":
            return a != b
        if op == "<":
            return a < b
        if op == "<=":
            return a <= b
        if op == ">":
            return a > b
        if op == ">=":
            return a >= b
    except TypeError:
        sa, sb = _to_string(a), _to_string(b)
        if op == "==":
            return sa == sb
        if op == "!=":
            return sa != sb
        if op == "<":
            return sa < sb
        if op == "<=":
            return sa <= sb
        if op == ">":
            return sa > sb
        if op == ">=":
            return sa >= sb
    raise ExpressionError(f"unknown comparison operator {op!r}")


def _eval(node: tuple, env: dict[str, Any]) -> Any:
    tag = node[0]
    if tag == "num" or tag == "str":
        return node[1]
    if tag == "bool":
        return node[1]
    if tag == "null":
        return None
    if tag == "bind":
        return env.get(node[1])
    if tag == "call":
        name, args = node[1], node[2]
        fn = _FUNCS.get(name)
        if fn is None:
            raise ExpressionError(f"unsupported function {name!r}")
        vals = [_eval(a, env) for a in args]
        return fn(vals)
    if tag == "unary":
        op = node[1]
        v = _eval(node[2], env)
        if op == "-":
            if v is None:
                return None
            n = _to_number(v)
            return -n if n is not None else None
        if op == "not":
            return not _to_bool(v)
        raise ExpressionError(f"unknown unary op {op!r}")
    if tag == "bin":
        op = node[1]
        # short-circuit logical
        if op == "&&":
            left = _eval(node[2], env)
            if not _to_bool(left):
                return False
            return _to_bool(_eval(node[3], env))
        if op == "||":
            left = _eval(node[2], env)
            if _to_bool(left):
                return True
            return _to_bool(_eval(node[3], env))
        if op == "??":
            left = _eval(node[2], env)
            if left is not None:
                return left
            return _eval(node[3], env)
        a = _eval(node[2], env)
        b = _eval(node[3], env)
        if op in ("+", "-", "*", "/", "%"):
            return _arith(op, a, b)
        if op in ("==", "!=", "<", "<=", ">", ">="):
            return _compare(op, a, b)
        raise ExpressionError(f"unknown binary op {op!r}")
    if tag == "tern":
        cond = _eval(node[1], env)
        return _eval(node[2], env) if _to_bool(cond) else _eval(node[3], env)
    raise ExpressionError(f"unknown ast node {tag!r}")


@dataclass
class CompiledExpression:
    """A parsed expression ready to evaluate against any row-env mapping."""

    source: str
    ast: tuple
    references: tuple[str, ...]

    def evaluate(self, env: dict[str, Any]) -> Any:
        return _eval(self.ast, env)


def _collect_bindings(node: tuple, out: set[str]) -> None:
    tag = node[0]
    if tag == "bind":
        out.add(node[1])
        return
    if tag in ("num", "str", "bool", "null"):
        return
    if tag == "call":
        for a in node[2]:
            _collect_bindings(a, out)
        return
    if tag == "unary":
        _collect_bindings(node[2], out)
        return
    if tag == "bin":
        _collect_bindings(node[2], out)
        _collect_bindings(node[3], out)
        return
    if tag == "tern":
        _collect_bindings(node[1], out)
        _collect_bindings(node[2], out)
        _collect_bindings(node[3], out)
        return


def compile_expression(expr: str) -> CompiledExpression:
    """Parse ``expr`` and return a reusable compiled expression.

    Raises :class:`ExpressionError` on any syntax / unsupported-function issue.
    """

    if expr is None or not expr.strip():
        raise ExpressionError("expression is empty")
    toks = _tokenize(expr)
    ast = _Parser(toks, expr).parse()
    _validate_functions(ast)
    refs: set[str] = set()
    _collect_bindings(ast, refs)
    return CompiledExpression(source=expr, ast=ast, references=tuple(sorted(refs)))


def _validate_functions(node: tuple) -> None:
    if node[0] == "call":
        if node[1] not in _FUNCS:
            raise ExpressionError(f"unsupported function {node[1]!r}")
        for a in node[2]:
            _validate_functions(a)
        return
    if node[0] in ("num", "str", "bool", "null", "bind"):
        return
    if node[0] == "unary":
        _validate_functions(node[2])
        return
    if node[0] == "bin":
        _validate_functions(node[2])
        _validate_functions(node[3])
        return
    if node[0] == "tern":
        _validate_functions(node[1])
        _validate_functions(node[2])
        _validate_functions(node[3])
        return


def evaluate(expr: str, env: dict[str, Any]) -> Any:
    """One-shot compile + evaluate convenience."""

    return compile_expression(expr).evaluate(env)


# ── Server-side (AQL) delegation helpers ─────────────────────────────────
#
# Expressions that fall outside the local subset (P5c.1.5) are pushed down to
# ArangoDB. The only rewriting the server query needs is turning our ``@col``
# bind-parameter syntax into a reference against the per-row FOR variable
# (e.g. ``row.`col```). Everything else is left verbatim so the full AQL
# function library is available server-side.


def _scan_bind_params(expr: str):
    """Yield ``(start, end, name)`` for each ``@name`` token outside strings.

    A quote-aware scan (single and double quotes, with backslash escapes) so
    that an ``@`` appearing inside a string literal is never treated as a
    bind parameter. Independent of the parser, so it works even for AQL that
    the local evaluator cannot compile.
    """
    i, n = 0, len(expr)
    while i < n:
        c = expr[i]
        if c == '"' or c == "'":
            quote = c
            j = i + 1
            while j < n and expr[j] != quote:
                if expr[j] == "\\" and j + 1 < n:
                    j += 2
                    continue
                j += 1
            i = j + 1
            continue
        if c == "@":
            j = i + 1
            if j < n and (expr[j].isalpha() or expr[j] == "_"):
                while j < n and (expr[j].isalnum() or expr[j] == "_"):
                    j += 1
                yield (i, j, expr[i + 1:j])
                i = j
                continue
        i += 1


def extract_bind_references(expr: str) -> list[str]:
    """Return the distinct bind-parameter names referenced by ``expr``.

    Works for any expression text, including AQL outside the local subset.
    Order-preserving and de-duplicated.
    """
    seen: dict[str, None] = {}
    for _start, _end, name in _scan_bind_params(expr or ""):
        seen.setdefault(name, None)
    return list(seen.keys())


def rewrite_bind_params(expr: str, var: str = "row") -> str:
    """Rewrite ``@col`` bind references to ``<var>.`col``` for server-side AQL.

    The attribute name is backtick-quoted so column names that collide with
    AQL keywords or contain unusual characters remain valid.
    """
    out: list[str] = []
    last = 0
    for start, end, name in _scan_bind_params(expr or ""):
        out.append(expr[last:start])
        out.append(f"{var}.`{name}`")
        last = end
    out.append(expr[last:])
    return "".join(out)


SUPPORTED_FUNCTIONS: tuple[str, ...] = tuple(sorted(_FUNCS.keys()))
