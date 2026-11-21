"""ArangoDB document-key sanitization.

ArangoDB restricts the characters allowed in ``_key`` (and therefore in the
key portion of ``_from`` / ``_to``). Source primary-key values are not
guaranteed to satisfy those rules: a partitioned PostgreSQL table whose PK
includes a ``timestamp`` column (e.g. Pagila's ``payment``) yields keys
containing spaces and ``+``, which ArangoDB rejects with
``illegal document key``.

:func:`sanitize_key_component` maps an arbitrary value to a legal key fragment
by percent-encoding every disallowed byte. The transform is deterministic and
injective over arbitrary input strings (``%`` itself is always encoded, so
``"a b"`` and a literal ``"a%20b"`` cannot collide). Because the mapping is a
pure function of the value, a vertex ``_key`` and any ``_from`` / ``_to``
reference that points at it stay in agreement as long as both sides run their
PK / FK values through this function.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

# Characters ArangoDB permits in a document key (per the manual). ``%`` is
# permitted by ArangoDB but is deliberately excluded from the pass-through set
# because it is our escape character; encoding it keeps the mapping injective.
_PASSTHROUGH = frozenset(
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    "_-:.@()+,=;$!*'"
)


def sanitize_key_component(value: object) -> str:
    """Return *value* as a string safe to embed in an ArangoDB document key.

    Disallowed characters are percent-encoded byte-by-byte (UTF-8), so the
    result contains only characters ArangoDB accepts in ``_key``.
    """
    text = value if isinstance(value, str) else str(value)
    out: list[str] = []
    for ch in text:
        if ch in _PASSTHROUGH:
            out.append(ch)
        else:
            for byte in ch.encode("utf-8"):
                out.append(f"%{byte:02X}")
    return "".join(out)


#: A NULL inside a key built from every column. ``sanitize_key_component`` never
#: emits ``%n`` (it encodes ``%`` as ``%25``), so NULL and any value stay distinct.
_NULL_COMPONENT = "%null"
_MAX_ROW_KEY = 200


def row_key(table: Any, row: Mapping[str, Any], separator: str) -> str:
    """The document ``_key`` for *row*, shared by vertices and the edges that point at them.

    A table with a primary key is keyed by it, as before. A table without one is keyed
    by every column in schema order, so its rows get a deterministic key that its
    edges can reference (an auto-generated key gave those edges nothing to point at).
    """
    if table.primary_key:
        parts = []
        for col in table.primary_key:
            val = row.get(col)
            if val is None:
                raise ValueError(f"Row missing PK value for column '{col}': {row}")
            parts.append(sanitize_key_component(val))
        return separator.join(parts)
    # shortcut: identical rows of a key-less table share one key and collapse to one
    # document; key by a source row id instead if such a table must keep duplicates.
    key = separator.join(
        _NULL_COMPONENT if row.get(c.name) is None else sanitize_key_component(row[c.name])
        for c in table.columns
    )
    # ArangoDB caps _key at 254 bytes; leave room for an LPG label prefix.
    return key if len(key) <= _MAX_ROW_KEY else "h" + hashlib.sha256(key.encode()).hexdigest()
