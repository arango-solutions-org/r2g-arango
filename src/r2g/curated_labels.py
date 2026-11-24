"""Curated labels: a curator's names for concepts, applied at export (PRD P12.12).

Extraction names a concept after what stores it: ``cust_nm`` becomes ``custNm``.
A curator — working in ``arango-ontoextract``'s lexicon, or from a customer's
business glossary — decides it is really ``customerName``. That decision has to
survive regeneration, so it cannot live in r2g's output (regenerated on every
export) nor in AOE's extracted ontology (rewritten on every extraction). It lives
in a **curated-labels document** that r2g reads when it exports, and both
exporters apply it the same way.

The document::

    {
      "curatedLabelsVersion": "1",
      "producer": "arango-ontoextract",          # optional, recorded
      "ontology": "crm",                          # optional, recorded
      "entities":      [{"name": "Account", "label": "Customer", ...}],
      "properties":    [{"entity": "Account", "name": "custNm",
                         "label": "Customer Name", ...}],
      "relationships": [{"name": "placedBy", "label": "placed by", ...}]
    }

Every entry may also carry ``description``, ``altLabels`` (synonyms),
``decidedBy`` and ``decidedAt``.

**Keys are the names r2g generates.** ``name`` (and ``entity``) are r2g's own
conceptual names before any renaming: the class name
``entity_names_for_mapping`` gives each collection (P12.13),
``owl_property_name(stored field)``, ``owl_property_name(edge collection)``. A
CSI document exported with ``--label-policy warn`` (or ``off``) carries exactly
these names, so an ontology imported from it into AOE exports keys that match.
A key that matches nothing is reported ``unmatched``, never silently dropped.

**``label`` is the curator's words; the identifier is derived.** ``"Customer
Name"`` becomes ``customerName`` for a property or relationship and
``CustomerName`` for an entity (CC-12), because the name is an IRI local name a
query is written against. The words themselves are kept as ``prefLabel``, and
the generated name joins ``altLabels`` so a question that uses it still resolves.

**One decision, two exporters.** :func:`plan_curated_labels` decides every
rename and every refusal. ``mapping_to_csi`` and ``mapping_to_r2rml`` both build
the plan from the same mapping through :func:`r2g.csi.curated_plan_for_mapping`,
so the SQL leg and the graph leg can never disagree about a concept's name —
contextual-data-fabric refuses to build a catalog whose CSI names a property the
R2RML does not map.

**Accents are folded; anything else that cannot become an identifier is refused.**
Identifiers are ASCII (CC-12), so ``"Café owner"`` becomes ``cafeOwner``. A label
with letters that have no ASCII form (``ß``, ``名前``) or with no letters or digits
at all is refused, never silently shortened or written raw into an IRI.

**A rename is refused rather than traded for a new defect.** Applied before the
P12.1 label-collision check, a curated name must not:

- rename a declared cross-source join key (P6.7) — ``conceptualModel.joinKeys``
  and the bind-join both reference the bare label;
- take a property or relationship name any other property or relationship
  already holds, on any entity — the P12.1 invariant that no label is carried
  by two entities is the reason curation exists;
- take another entity's name.

A refused entry keeps its generated name and is recorded ``refused`` with the
reason; refusing one rename can free or occupy a name another entry wanted, so
refusal is repeated until nothing changes.
"""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set, Tuple

CURATED_LABELS_VERSION = "1"

_ENTRY_FIELDS = {"label", "identifier", "description", "altLabels", "decidedBy", "decidedAt"}

_IDENTIFIER = re.compile(r"[A-Za-z0-9_]+")
# CC-12 shapes a curator-given identifier must already have (r2g-arango#34).
_ENTITY_IDENTIFIER = re.compile(r"[A-Z][A-Za-z0-9]*")
_MEMBER_IDENTIFIER = re.compile(r"[a-z][A-Za-z0-9]*")


def derive_identifier(label: str, kind: str) -> Tuple[Optional[str], Optional[str]]:
    """``(identifier, None)`` for a curator's label, or ``(None, why not)``.

    ``kind`` is ``"entity"`` (PascalCase) or anything else (lowerCamel). Accents
    are folded first (NFKD, combining marks dropped) because the converter keeps
    only ASCII letters and digits and would otherwise drop the whole letter:
    ``"Café"`` must become ``Cafe``, not ``Caf``.
    """
    from .csi import owl_entity_name, owl_property_name

    folded = "".join(
        c for c in unicodedata.normalize("NFKD", label) if not unicodedata.combining(c)
    )
    foreign = list(dict.fromkeys(c for c in folded if c.isalnum() and not c.isascii()))
    if foreign:
        return None, (
            f"label has letters with no ASCII form ({''.join(foreign)}); identifiers are "
            "ASCII, so this label cannot name the concept without losing them"
        )
    identifier = owl_entity_name(folded) if kind == "entity" else owl_property_name(folded)
    if not _IDENTIFIER.fullmatch(identifier):
        return None, "label has no letters or digits to derive an identifier from"
    return identifier, None


def explicit_identifier(identifier: str, kind: str) -> Tuple[Optional[str], Optional[str]]:
    """``(identifier, None)`` for a curator's own identifier, or ``(None, why not)``.

    The curator's last word on a name (r2g-arango#34): used exactly as written,
    never singularized or re-cased, so ``Sales``, ``CustomerAnalytics`` or a name
    for a label in a non-Latin script come out as intended. It must already be a
    CC-12 identifier: PascalCase for an entity, lowerCamel for a property or
    relationship, ASCII letters and digits only.
    """
    pattern = _ENTITY_IDENTIFIER if kind == "entity" else _MEMBER_IDENTIFIER
    if pattern.fullmatch(identifier):
        return identifier, None
    shape = "PascalCase" if kind == "entity" else "lowerCamel"
    return None, (
        f"identifier {identifier!r} is not a CC-12 {shape} name "
        "(ASCII letters and digits, starting with a letter)"
    )


class CuratedLabelsError(ValueError):
    """The curated-labels document is not one this version can apply."""


def validate_curated_labels(doc: Any) -> List[str]:
    """Shape problems in a curated-labels document; empty when it is usable."""
    if not isinstance(doc, dict):
        return ["document must be a JSON object"]
    problems: List[str] = []
    version = doc.get("curatedLabelsVersion")
    if version != CURATED_LABELS_VERSION:
        problems.append(
            f"curatedLabelsVersion must be {CURATED_LABELS_VERSION!r} (got {version!r})"
        )
    required = {
        "entities": ("name",),
        "properties": ("entity", "name"),
        "relationships": ("name",),
    }
    for section, keys in required.items():
        entries = doc.get(section, [])
        if not isinstance(entries, list):
            problems.append(f"{section} must be a list")
            continue
        for i, entry in enumerate(entries):
            where = f"{section}[{i}]"
            if not isinstance(entry, dict):
                problems.append(f"{where} must be an object")
                continue
            for key in (*keys, "label"):
                value = entry.get(key)
                if not isinstance(value, str) or not value.strip():
                    problems.append(f"{where}.{key} must be a non-empty string")
            identifier = entry.get("identifier")
            if identifier is not None and (not isinstance(identifier, str) or not identifier.strip()):
                problems.append(f"{where}.identifier must be a non-empty string")
            alt = entry.get("altLabels", [])
            if not isinstance(alt, list) or not all(isinstance(a, str) for a in alt):
                problems.append(f"{where}.altLabels must be a list of strings")
            unknown = set(entry) - set(keys) - _ENTRY_FIELDS
            if unknown:
                problems.append(f"{where} has unknown field(s): {', '.join(sorted(unknown))}")
    return problems


def load_curated_labels(path: str | Path) -> Dict[str, Any]:
    """Read and validate a curated-labels document.

    Raises:
        CuratedLabelsError: unreadable JSON or a document with shape problems —
            a partly-understood file would apply some of a curator's decisions
            and silently not others.
    """
    try:
        doc = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CuratedLabelsError(f"cannot read curated labels {path}: {exc}") from exc
    problems = validate_curated_labels(doc)
    if problems:
        raise CuratedLabelsError(
            f"curated labels {path} is malformed:\n  - " + "\n  - ".join(problems)
        )
    return doc


@dataclass
class CuratedPlan:
    """Every rename and annotation the curated labels resolve to, plus the record.

    Lookups take r2g's *generated* names and return the name to emit, so an
    exporter can call them unconditionally: anything uncurated passes through.
    """

    entities: Dict[str, str] = field(default_factory=dict)
    properties: Dict[Tuple[str, str], str] = field(default_factory=dict)
    relationships: Dict[str, str] = field(default_factory=dict)
    #: ``("entity", e)`` / ``("property", e, p)`` / ``("relationship", r)`` ->
    #: ``{prefLabel, description?, altLabels}``, keyed by generated names.
    annotations: Dict[tuple, Dict[str, Any]] = field(default_factory=dict)
    records: List[Dict[str, Any]] = field(default_factory=list)
    source: Dict[str, Any] = field(default_factory=dict)

    def entity(self, name: str) -> str:
        return self.entities.get(name, name)

    def prop(self, entity: str, name: str) -> str:
        return self.properties.get((entity, name), name)

    def relationship(self, name: str) -> str:
        return self.relationships.get(name, name)

    @property
    def problems(self) -> List[Dict[str, Any]]:
        """Entries that were not applied: ``refused`` or ``unmatched``."""
        return [r for r in self.records if r["resolution"] in {"refused", "unmatched"}]


def _annotation(entry: Mapping[str, Any], generated: str, emitted: str) -> Dict[str, Any]:
    alt: List[str] = []
    for candidate in [*(entry.get("altLabels") or []), generated]:
        if candidate and candidate != emitted and candidate not in alt:
            alt.append(candidate)
    out: Dict[str, Any] = {"prefLabel": entry["label"]}
    if entry.get("description"):
        out["description"] = entry["description"]
    if alt:
        out["altLabels"] = alt
    return out


def _record(kind: str, entry: Mapping[str, Any], **extra: Any) -> Dict[str, Any]:
    rec: Dict[str, Any] = {"kind": kind}
    if kind == "property":
        rec["entity"] = entry["entity"]
    rec["name"] = entry["name"]
    rec["label"] = entry["label"]
    if entry.get("identifier"):
        rec["identifier"] = entry["identifier"]
    rec.update(extra)
    if entry.get("decidedBy"):
        rec["decidedBy"] = entry["decidedBy"]
    return rec


def plan_curated_labels(
    curated: Mapping[str, Any],
    *,
    entities: Mapping[str, Iterable[str]],
    relationships: Iterable[str],
    join_key_labels: Optional[Set[str]] = None,
    entity_join_key_labels: Optional[Mapping[str, Set[str]]] = None,
) -> CuratedPlan:
    """Resolve a curated-labels document against r2g's generated names.

    Args:
        curated: a document that passed :func:`validate_curated_labels`.
        entities: generated entity name -> its generated property names.
        relationships: generated relationship names.
        join_key_labels: generated property labels that are declared cross-source
            join keys (P6.7); never renamed, on any entity.
        entity_join_key_labels: generated entity name -> join-key labels bound on
            that entity only (a join table's, #88); never renamed on that entity,
            and no bar to renaming the same label on another.

    Pure and deterministic: entries are resolved in sorted order, so the result
    never depends on the order a curator's tool happened to write them.
    """
    from .csi import _qualified_label

    join_key_labels = join_key_labels or set()
    entity_join_key_labels = entity_join_key_labels or {}
    entity_props = {e: list(props) for e, props in entities.items()}
    relationship_names = sorted(set(relationships))
    plan = CuratedPlan(
        source={k: curated[k] for k in ("producer", "ontology") if curated.get(k)}
    )

    # ── Match every entry to a generated name; derive its target identifier. ──
    # (kind, key) -> (entry, target). Keys: ("entity", e) / ("property", e, p) /
    # ("relationship", r).
    wanted: Dict[tuple, Tuple[Mapping[str, Any], str]] = {}
    records: Dict[tuple, Dict[str, Any]] = {}
    unusable: Set[tuple] = set()  # claimed, but the label yields no identifier

    def _claim(key: tuple, kind: str, entry: Mapping[str, Any]) -> None:
        if key in wanted or key in unusable:
            # Two entries for one concept: neither can be the curator's decision.
            records[key] = _record(
                kind, entry, resolution="refused", reason="curated more than once in this document"
            )
            if key in wanted:
                wanted[key] = (wanted[key][0], key[-1])
            return
        if entry.get("identifier"):
            target, problem = explicit_identifier(entry["identifier"], kind)
        else:
            target, problem = derive_identifier(entry["label"], kind)
        if target is None:
            unusable.add(key)
            records[key] = _record(kind, entry, resolution="refused", reason=problem)
            return
        wanted[key] = (entry, target)

    for entry in sorted(curated.get("entities", []), key=lambda e: e["name"]):
        if entry["name"] not in entity_props:
            records[("entity", entry["name"])] = _record(
                "entity", entry, resolution="unmatched", reason="no entity with this generated name"
            )
            continue
        _claim(("entity", entry["name"]), "entity", entry)

    for entry in sorted(
        curated.get("properties", []), key=lambda e: (e["entity"], e["name"])
    ):
        key: Tuple[str, ...] = ("property", entry["entity"], entry["name"])
        if entry["entity"] not in entity_props:
            records[key] = _record(
                "property", entry, resolution="unmatched", reason="no entity with this generated name"
            )
            continue
        if entry["name"] not in entity_props[entry["entity"]]:
            reason = f"{entry['entity']} has no property with this generated name"
            # The likeliest cause: the key came from a CSI exported under
            # `qualify` / `roles`, which renamed a collided label.
            qualified_from = next(
                (
                    p
                    for p in entity_props[entry["entity"]]
                    for seam in (False, True)
                    if _qualified_label(entry["entity"], p, collapse_seam=seam) == entry["name"]
                ),
                None,
            )
            if qualified_from is not None:
                reason += (
                    f"; it is the name --label-policy qualify/roles gives "
                    f"{entry['entity']}.{qualified_from} — curate from a CSI exported "
                    f"with --label-policy warn, whose names are the generated ones"
                )
            records[key] = _record("property", entry, resolution="unmatched", reason=reason)
            continue
        if entry["name"] in join_key_labels or entry["name"] in entity_join_key_labels.get(
            entry["entity"], ()
        ):
            records[key] = _record(
                "property",
                entry,
                resolution="refused",
                reason=(
                    "declared cross-source join key (P6.7) — conceptualModel.joinKeys "
                    "and the bind-join reference the bare label"
                ),
            )
            continue
        _claim(key, "property", entry)

    for entry in sorted(curated.get("relationships", []), key=lambda e: e["name"]):
        key = ("relationship", entry["name"])
        if entry["name"] not in relationship_names:
            records[key] = _record(
                "relationship",
                entry,
                resolution="unmatched",
                reason="no relationship with this generated name",
            )
            continue
        _claim(key, "relationship", entry)

    # ── Refuse until stable: a refusal restores a generated name, which can ──
    # collide with another entry's target.
    accepted = {k for k in wanted if k not in records}
    while True:
        refusals: Dict[tuple, str] = {}

        def _emitted(key: tuple) -> str:
            return wanted[key][1] if key in accepted else key[-1]

        # Entity names: one namespace of their own.
        entity_names: Dict[str, List[tuple]] = {}
        for e in entity_props:
            entity_names.setdefault(_emitted(("entity", e)), []).append(("entity", e))
        # Property + relationship labels share the predicate namespace.
        predicate_holders: Dict[str, Set[tuple]] = {}
        for e, props in entity_props.items():
            for p in props:
                pkey: Tuple[str, ...] = ("property", e, p)
                predicate_holders.setdefault(_emitted(pkey), set()).add(pkey)
        for r in relationship_names:
            rkey: Tuple[str, ...] = ("relationship", r)
            predicate_holders.setdefault(_emitted(rkey), set()).add(rkey)

        for key in sorted(accepted):
            target = wanted[key][1]
            if key[0] == "entity":
                others = [k for k in entity_names.get(target, []) if k != key]
                if others:
                    refusals[key] = f"entity name {target!r} is already taken by {others[0][1]}"
                continue
            others = sorted(k for k in predicate_holders.get(target, set()) if k != key)
            if others:
                holder = (
                    f"{others[0][1]}.{others[0][2]}"
                    if others[0][0] == "property"
                    else f"relationship {others[0][1]}"
                )
                refusals[key] = f"label {target!r} is already carried by {holder}"
        if not refusals:
            break
        for key, reason in refusals.items():
            accepted.discard(key)
            kind = key[0]
            records[key] = _record(kind, wanted[key][0], resolution="refused", reason=reason)

    # ── Build the plan from what survived. ──
    for key in sorted(wanted):
        entry, target = wanted[key]
        kind = key[0]
        generated = key[-1]
        if key in accepted:
            if kind == "entity":
                plan.entities[generated] = target
            elif kind == "property":
                plan.properties[(key[1], generated)] = target
            else:
                plan.relationships[generated] = target
            plan.annotations[key] = _annotation(entry, generated, target)
            resolution = "applied" if target != generated else "unchanged"
            records[key] = _record(kind, entry, resolution=resolution, renamedTo=target)

    plan.records = [records[k] for k in sorted(records)]
    return plan
