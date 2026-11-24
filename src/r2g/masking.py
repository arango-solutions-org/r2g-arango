"""Transform-at-load masking helpers (PRD Phase 9b, P9.6).

Masking reuses the existing :class:`~r2g.types.FieldExpression` engine rather
than introducing a parallel mechanism: a masked field is just a field expression
that overwrites the property with a de-identified value. Four deterministic
kinds are provided:

- ``hash``     – ``MD5(TO_STRING(@col))`` (irreversible digest)
- ``tokenize`` – ``SHA512(TO_STRING(@col))`` (stable token, joinable across rows)
- ``redact``   – constant ``"***"`` (value destroyed)
- ``nullify``  – ``null`` (value dropped but property kept)

``hash``/``tokenize`` use AQL functions outside the local expression subset, so
they are evaluated **server-side** by ArangoDB at load (the pipeline's existing
delegation path) and therefore work for every source type. ``redact``/``nullify``
compile locally.

Masking expressions are tagged with a sentinel in ``FieldExpression.description``
so the entitlement gate can recognise a field as masked (and therefore safe to
load above threshold) without guessing from the expression text.
"""

from __future__ import annotations

from typing import Optional

from r2g.types import FieldExpression

MASK_SENTINEL = "r2g:mask:"

# kind -> human description (also the order shown in the UI)
MASK_KINDS: dict[str, str] = {
    "hash": "Irreversible MD5 digest (not joinable)",
    "tokenize": "Stable SHA-512 token (joinable across rows)",
    "redact": 'Replace with a constant "***"',
    "nullify": "Drop the value (set to null)",
}


def make_mask_expression(column: str, kind: str) -> FieldExpression:
    """Build a :class:`FieldExpression` that masks ``column`` in place.

    ``column`` is both the source and the target property (masking overwrites
    the value). Raises :class:`ValueError` for an unknown ``kind``.
    """
    if kind not in MASK_KINDS:
        raise ValueError(
            f"Unknown mask kind '{kind}'. Expected one of: {', '.join(MASK_KINDS)}."
        )
    if kind == "redact":
        expression = '"***"'
    elif kind == "nullify":
        expression = "null"
    elif kind == "hash":
        expression = f"MD5(TO_STRING(@{column}))"
    else:  # tokenize
        expression = f"SHA512(TO_STRING(@{column}))"
    return FieldExpression(
        target=column,
        sources=[column],
        expression=expression,
        engine="aql",
        description=f"{MASK_SENTINEL}{kind}",
    )


def is_masking_expression(fx: FieldExpression) -> bool:
    """True when ``fx`` is a mask :func:`make_mask_expression` would write.

    The sentinel names the kind; the expression itself must be that kind's, over the
    one column it reads. The description is free text anyone editing the mapping can
    set, so a sentinel on any other expression (an identity copy, say) does not make
    the field masked: the gate would otherwise load it in clear.
    """
    kind = mask_kind_of(fx)
    if kind not in MASK_KINDS or fx.engine != "aql" or len(fx.sources) != 1:
        return False
    return fx.expression == make_mask_expression(fx.sources[0], kind).expression


def mask_kind_of(fx: FieldExpression) -> Optional[str]:
    """Return the mask kind for a masking expression, else ``None``."""
    desc = fx.description or ""
    if not desc.startswith(MASK_SENTINEL):
        return None
    return desc[len(MASK_SENTINEL):] or None


__all__ = [
    "MASK_KINDS",
    "MASK_SENTINEL",
    "make_mask_expression",
    "is_masking_expression",
    "mask_kind_of",
]
