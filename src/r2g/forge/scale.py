"""Volume knobs for the Federation Forge (PLAN F-9/F-10, ADR-0006 D-1 ``scale``).

The skeleton gave every entity the same row count and drew every foreign key
uniformly from its parent's rows, so a hop had about one neighbour on average
and no parent was much busier than another. :class:`ForgeScale` is the knob
set the fabric's scale program turns, and the only one:

- ``rows`` — a row count per entity, overriding ``rows_per_entity``. The ratio
  of a child's rows to its parent's is a relationship's mean fan-out.
- ``relationships[<type>].distribution`` — how a child picks its parent:
  ``uniform`` (the skeleton's draw, the default) or ``zipf`` with an
  ``exponent`` ``s > 0``, under which the parent of rank ``k`` is picked with
  probability proportional to ``1 / k**s``. Rank ``k`` is parent ``id`` ``k``,
  so parent 1 is the busiest. The mean fan-out is unchanged; its spread is not.
- ``relationships[<type>].nullRate`` — the share of children with no parent at
  all (a NULL foreign key). A relationship with a positive rate gets a
  nullable foreign-key column, so the knob is part of the schema, not only of
  the data, and :func:`~r2g.forge.core.plan_schema` takes it.

Everything absent means "the skeleton's behaviour", and a scale with no knobs
set draws exactly the rows the skeleton drew for the same seed. That is what
keeps every committed suite generated before the knobs existed byte-identical.

Unknown entity or relationship names, a ``zipf`` without an exponent, an
exponent on ``uniform``, a rate outside ``[0, 1)`` and a row count below one
are refused by name at generate time (:meth:`ForgeScale.check_against`), never
quietly ignored: a knob that silently did nothing would make a benchmark
measure a shape nobody asked for.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .core import ForgeError

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .core import SchemaPlan

#: The fan-out distributions a relationship may declare.
DISTRIBUTIONS = ("uniform", "zipf")


class RelationshipScale(BaseModel):
    """The knobs on one relationship (keyed by its ``type`` in :class:`ForgeScale`)."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    distribution: Literal["uniform", "zipf"] = "uniform"
    exponent: Optional[float] = None
    null_rate: float = Field(0.0, alias="nullRate")

    def check(self, relationship: str) -> None:
        if self.distribution == "zipf":
            if self.exponent is None:
                raise ForgeError(
                    f"relationship {relationship!r}: distribution 'zipf' needs an "
                    "exponent > 0 (the skew: 0 would be uniform, 1 is classic Zipf)"
                )
            if not (math.isfinite(self.exponent) and self.exponent > 0):
                raise ForgeError(
                    f"relationship {relationship!r}: zipf exponent must be a finite "
                    f"number > 0, got {self.exponent!r}"
                )
        elif self.exponent is not None:
            raise ForgeError(
                f"relationship {relationship!r}: an exponent only applies to "
                f"distribution 'zipf', not {self.distribution!r}"
            )
        if not (math.isfinite(self.null_rate) and 0.0 <= self.null_rate < 1.0):
            raise ForgeError(
                f"relationship {relationship!r}: nullRate must be in [0, 1), got "
                f"{self.null_rate!r}"
            )

    def document(self) -> Dict[str, Any]:
        """The camelCase form, holding only the knobs that differ from the default."""
        out: Dict[str, Any] = {}
        if self.distribution != "uniform":
            out["distribution"] = self.distribution
        if self.exponent is not None:
            out["exponent"] = self.exponent
        if self.null_rate:
            out["nullRate"] = self.null_rate
        return out


class ForgeScale(BaseModel):
    """Per-entity row counts and per-relationship fan-out knobs.

    Build one from its JSON/YAML document with :meth:`from_document`; check it
    against the ontology it will drive with :meth:`check_against` (the
    synthesizer and :func:`~r2g.forge.core.plan_schema` both do).
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    rows: Dict[str, int] = Field(default_factory=dict)
    relationships: Dict[str, RelationshipScale] = Field(default_factory=dict)

    @classmethod
    def from_document(cls, document: Any) -> "ForgeScale":
        """Parse the camelCase document (``{"rows": …, "relationships": …}``).

        Malformed input is a :class:`~r2g.forge.core.ForgeError` naming what
        was wrong, never a pydantic traceback."""
        if document is None:
            return cls()
        if not isinstance(document, dict):
            raise ForgeError(f"a forge scale must be a mapping, got {type(document).__name__}")
        try:
            scale = cls.model_validate(document)
        except ValidationError as exc:
            raise ForgeError(f"malformed forge scale: {exc}") from exc
        for entity, count in scale.rows.items():
            if isinstance(count, bool) or count < 1:
                raise ForgeError(f"rows[{entity!r}] must be an integer >= 1, got {count!r}")
        for name, knobs in scale.relationships.items():
            knobs.check(name)
        return scale

    def document(self) -> Dict[str, Any]:
        """The camelCase form, holding only what is set (``{}`` for no knobs),
        in sorted key order so a descriptor that embeds it stays byte-stable."""
        out: Dict[str, Any] = {}
        if self.rows:
            out["rows"] = {k: self.rows[k] for k in sorted(self.rows)}
        relationships = {
            k: self.relationships[k].document()
            for k in sorted(self.relationships)
            if self.relationships[k].document()
        }
        if relationships:
            out["relationships"] = relationships
        return out

    @property
    def is_default(self) -> bool:
        return not self.document()

    def rows_for(self, entity: str, rows_per_entity: int) -> int:
        return self.rows.get(entity, rows_per_entity)

    def relationship(self, relationship: str) -> RelationshipScale:
        return self.relationships.get(relationship, DEFAULT_RELATIONSHIP)

    def check_against(self, plan: "SchemaPlan") -> None:
        """Refuse a knob that names no entity or relationship of ``plan``."""
        for entity, count in self.rows.items():
            if isinstance(count, bool) or count < 1:
                raise ForgeError(f"rows[{entity!r}] must be an integer >= 1, got {count!r}")
        entities = {t.entity for t in plan.tables}
        unknown = sorted(set(self.rows) - entities)
        if unknown:
            raise ForgeError(
                f"the scale sets rows for {unknown}, which the ontology does not declare "
                f"(entities: {sorted(entities)})"
            )
        relationships = {e.relationship for e in plan.edges}
        unknown = sorted(set(self.relationships) - relationships)
        if unknown:
            raise ForgeError(
                f"the scale sets knobs on relationships {unknown}, which the ontology "
                f"does not declare (relationships: {sorted(relationships)})"
            )
        for name, knobs in self.relationships.items():
            knobs.check(name)


DEFAULT_RELATIONSHIP = RelationshipScale.model_validate({})

#: The scale with no knobs set: the skeleton's behaviour.
DEFAULT_SCALE = ForgeScale()


__all__ = ["DEFAULT_RELATIONSHIP", "DEFAULT_SCALE", "DISTRIBUTIONS", "ForgeScale", "RelationshipScale"]
