"""Seeded synthesis as a stream (PLAN F-5, F-9, F-10, F-11).

:func:`stream_rows` yields ``(table, row)`` pairs in plan order, one row at a
time, from one ``random.Random(seed)``. It holds no rows: a foreign key needs
only its parent's row *count*, because a parent's ids are ``1..count`` by
construction, so the memory a stream needs does not grow with the rows it
yields. That is what lets the fabric generate 10^8-edge federations without
holding them (S5, ADR-0011 D9); :func:`~r2g.forge.core.synthesize_rows` is the
same stream collected into a dict, for the small shapes that want one.

Byte-compatible with the skeleton. With no knobs set, every draw is the one
the skeleton made, in the order it made it: ``rng.choice`` over the parent's
ids (``rng.choice(range(1, n + 1))`` consumes the generator exactly as
``rng.choice([1, …, n])`` did), then each property's value. A knob only adds
draws where it is set — a ``nullRate`` coin, a Zipf variate — so the rows of a
scale-free generation, and every suite committed from one, do not move.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Tuple

from .core import SURROGATE_KEY, ColumnPlan, ForgeError, SchemaPlan, TablePlan
from .scale import DEFAULT_RELATIONSHIP, ForgeScale, RelationshipScale

#: One streamed row: its canonical table name and its canonical columns.
StreamedRow = Tuple[str, Dict[str, Any]]


def _synthesize_value(rng: random.Random, json_type: str, prop: str) -> Any:
    if json_type == "integer":
        return rng.randrange(0, 100_000)
    if json_type == "float":
        return round(rng.uniform(0, 100_000), 6)
    if json_type == "boolean":
        return rng.random() < 0.5
    return f"{prop}-{rng.randrange(0, 100_000):05d}"


class ZipfSampler:
    """Ranks ``1..n`` with ``P(k) = k**-s / H(n, s)``, ``s > 0``, in O(1) memory.

    Rejection-inversion (Hörmann & Derflinger, "Rejection-inversion to generate
    variates from monotone discrete distributions", ACM TOMACS 6(3), 1996), as
    Apache Commons RNG's ``RejectionInversionZipfSampler`` implements it. An
    inverse-CDF table would hold ``n`` floats — 800 MB for 10^8 parents — which
    is exactly what streaming generation exists to avoid. Each sample draws
    ``rng.random()`` until accepted (about 1.1 draws on average for ``s`` near 1),
    so it is a pure function of the generator's state.
    """

    __slots__ = ("n", "s", "_h_x1", "_h_n", "_s_div")

    def __init__(self, n: int, s: float) -> None:
        if n < 1:
            raise ForgeError(f"a Zipf draw needs at least one rank, got n={n}")
        if not (math.isfinite(s) and s > 0):
            raise ForgeError(f"a Zipf exponent must be finite and > 0, got {s!r}")
        self.n = n
        self.s = s
        self._h_x1 = self._h_integral(1.5) - 1.0
        self._h_n = self._h_integral(n + 0.5)
        self._s_div = 2.0 - self._h_integral_inverse(self._h_integral(2.5) - self._h(2.0))

    def _h(self, x: float) -> float:
        return math.exp(-self.s * math.log(x))

    def _h_integral(self, x: float) -> float:
        log_x = math.log(x)
        return _helper2((1.0 - self.s) * log_x) * log_x

    def _h_integral_inverse(self, x: float) -> float:
        t = x * (1.0 - self.s)
        if t < -1.0:
            t = -1.0  # rounding guard, as in the reference implementation
        return math.exp(_helper1(t) * x)

    def sample(self, rng: random.Random) -> int:
        n = self.n
        while True:
            u = self._h_n + rng.random() * (self._h_x1 - self._h_n)
            x = self._h_integral_inverse(u)
            k = int(x + 0.5)
            if k < 1:
                k = 1
            elif k > n:
                k = n
            if k - x <= self._s_div or u >= self._h_integral(k + 0.5) - self._h(k):
                return k

    def probability(self, k: int) -> float:
        """The exact ``P(k)``; O(n), for tests and reports, not for sampling."""
        norm = math.fsum(i ** -self.s for i in range(1, self.n + 1))
        return float(k ** -self.s / norm)


def _helper1(x: float) -> float:
    """``log1p(x) / x``, continuous at 0."""
    if abs(x) > 1e-8:
        return math.log1p(x) / x
    return 1.0 - x * (0.5 - x * (1.0 / 3.0 - 0.25 * x))


def _helper2(x: float) -> float:
    """``expm1(x) / x``, continuous at 0."""
    if abs(x) > 1e-8:
        return math.expm1(x) / x
    return 1.0 + x * 0.5 * (1.0 + x * (1.0 / 3.0) * (1.0 + 0.25 * x))


def planned_row_counts(
    plan: SchemaPlan, rows_per_entity: int, scale: Optional[ForgeScale] = None
) -> Dict[str, int]:
    """Rows each table will get, by canonical table name, in plan order."""
    if isinstance(rows_per_entity, bool) or rows_per_entity < 1:
        raise ForgeError("rows_per_entity must be >= 1")
    scale = scale or ForgeScale()
    return {t.table: scale.rows_for(t.entity, rows_per_entity) for t in plan.tables}


@dataclass(frozen=True)
class _ForeignKeyDraw:
    """How one FK column of one table is filled."""

    column: str
    parent_rows: int
    self_link: bool
    knobs: RelationshipScale
    sampler: Optional[ZipfSampler]

    def draw(self, rng: random.Random, row_id: int) -> Optional[int]:
        if self.self_link:
            # A self-link points at an EARLIER row of its own table (PLAN F-9):
            # never itself, never a cycle, and every row it names is already
            # loaded when it is — so the rows load in order with the
            # constraint enforced. Row 1 is a root on every seed.
            candidates = row_id - 1
            if candidates < 1:
                return None
        else:
            candidates = self.parent_rows
        if self.knobs.null_rate and rng.random() < self.knobs.null_rate:
            return None
        if self.knobs.distribution == "zipf":
            assert self.knobs.exponent is not None
            sampler = self.sampler
            if sampler is None or sampler.n != candidates:
                sampler = ZipfSampler(candidates, self.knobs.exponent)
            return sampler.sample(rng)
        # The skeleton drew ``rng.choice(parent_ids)`` over ``[1, …, n]``; a
        # range is the same sequence to ``choice`` (one ``_randbelow(n)``), so
        # the draw is unchanged and holds no list.
        return rng.choice(range(1, candidates + 1))


def _foreign_key_draws(
    plan: SchemaPlan,
    table: TablePlan,
    counts: Dict[str, int],
    scale: ForgeScale,
) -> List[_ForeignKeyDraw]:
    edges = {(e.from_table, e.fk_column): e for e in plan.edges}
    tables = {t.table: t for t in plan.tables}
    draws: List[_ForeignKeyDraw] = []
    for fk in table.foreign_keys:
        if fk.references is None:
            continue
        parent = tables.get(fk.references)
        if parent is None:
            raise ForgeError(
                f"{table.table}.{fk.name} references {fk.references!r}, which the plan "
                "does not hold"
            )
        _require_key_reference(table, fk, parent)
        edge = edges.get((table.table, fk.name))
        knobs = scale.relationship(edge.relationship) if edge is not None else DEFAULT_RELATIONSHIP
        self_link = fk.references == table.table
        sampler = None
        if knobs.distribution == "zipf" and not self_link:
            assert knobs.exponent is not None
            sampler = ZipfSampler(counts[parent.table], knobs.exponent)
        draws.append(
            _ForeignKeyDraw(
                column=fk.name,
                parent_rows=counts[parent.table],
                self_link=self_link,
                knobs=knobs,
                sampler=sampler,
            )
        )
    return draws


def _require_key_reference(table: TablePlan, fk: ColumnPlan, parent: TablePlan) -> None:
    """A stream knows a parent's ids only as ``1..count`` — its primary key.

    The skeleton read the referenced column off the materialized parent rows,
    so an FK naming any other column failed with a KeyError. Refused by name
    instead: the stream would otherwise draw keys for a column it never filled.
    """
    referenced = fk.references_column or SURROGATE_KEY
    if referenced != parent.primary_key.name:
        raise ForgeError(
            f"{table.table}.{fk.name} references {parent.table}({referenced}), which is not "
            f"that table's primary key ({parent.primary_key.name}); the forge synthesizes "
            "foreign keys onto primary keys only"
        )


def stream_rows(
    plan: SchemaPlan,
    seed: int,
    rows_per_entity: int,
    scale: Optional[ForgeScale] = None,
) -> Iterator[StreamedRow]:
    """Yield every synthesized row as ``(table, row)``, in plan order.

    Deterministic: the same ``(plan, seed, rows_per_entity, scale)`` yields the
    same rows in the same order. Memory is bounded by the plan, not the rows —
    nothing yielded is kept. The arguments are checked when this is called,
    not when the first row is pulled, so a refused scale fails at generate
    time even for a caller that never iterates.
    """
    scale = scale or ForgeScale()
    counts = planned_row_counts(plan, rows_per_entity, scale)
    scale.check_against(plan)
    tables = [(t, _foreign_key_draws(plan, t, counts, scale)) for t in plan.tables]
    return _stream(tables, counts, seed)


def _stream(
    tables: List[Tuple[TablePlan, List[_ForeignKeyDraw]]],
    counts: Dict[str, int],
    seed: int,
) -> Iterator[StreamedRow]:
    rng = random.Random(seed)
    for table, draws in tables:
        # The key a row is stamped with comes from the plan, not the
        # SURROGATE_KEY literal: a plan whose key is named otherwise must load.
        key_column = table.primary_key.name
        properties = []
        for col in table.properties:
            assert col.prop is not None
            properties.append((col.name, col.json_type, col.prop))
        name = table.table
        for i in range(1, counts[name] + 1):
            row: Dict[str, Any] = {key_column: i}
            for draw in draws:
                row[draw.column] = draw.draw(rng, i)
            for column, json_type, prop in properties:
                row[column] = _synthesize_value(rng, json_type, prop)
            yield name, row


__all__ = ["StreamedRow", "ZipfSampler", "planned_row_counts", "stream_rows"]
