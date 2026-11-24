"""Publisher-scoped mapping identities for forward CSI documents (r2g-arango#91).

A CSI document published under a publisher URI gives every mapping entry an id
under that URI, so a consumer can tell "the same mapping, exported again" from
"a different mapping that happens to share a name". The contract is
arangodb-schema-analyzer's (issue #42, released in 0.21.0): the id paths, the
``physicalMapping`` alias and the integrity rules below mirror
``schema_analyzer.csi._mint_mapping_ids`` / ``_identity_issues`` /
``_to_csi``'s publisher-URI checks. r2g does not depend on the analyzer at
runtime, so they are restated here; ``tests/test_csi_identity.py`` runs both
on the same documents and fails if the two ever disagree.

Ids are built from names, never positions: exporting the same mapping again,
or in another order, mints the same ids. They are as stable as the names they
are built from — a change to the schema, the label policy, the curated labels
or the conceptual naming rules can change a name and so its id. Carrying an id
across a rename is the analyzer's #52, not done here.
"""

from __future__ import annotations

import copy
import re
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

# A publisher URI or mapping id: absolute http(s), no whitespace, query or
# fragment — the CSI schema's ``$defs/MappingId``, and ASA's ``_MAPPING_ID_RE``.
MAPPING_ID_PATTERN = re.compile(r"^https?://[^\s?#]+$")

# The keys that make a document claim mapping identity.
_ID_KINDS = (("entities", "entity"), ("relationships", "relationship"))

# Marks a ``propertyIds`` key with no matching ``properties`` key.
_NO_PROPERTY = object()


class CsiIdentityError(ValueError):
    """A publisher URI or a document's mapping ids cannot be published as they are."""


def normalize_publisher_uri(value: Any, *, field: str = "publisher_uri") -> str:
    """``value`` without trailing ``/``, or :class:`CsiIdentityError` naming ``field``.

    Accepts exactly what the analyzer accepts: an absolute http(s) URI with no
    whitespace, query or fragment once trailing slashes are stripped.
    """
    if not isinstance(value, str) or not MAPPING_ID_PATTERN.match(value.rstrip("/")):
        raise CsiIdentityError(
            f"{field} must be an absolute http(s) URI without query or fragment: {value!r}"
        )
    return value.rstrip("/")


def _existing_publisher(document: Dict[str, Any]) -> Any:
    """``provenance.publisher.uri`` as the document carries it, or ``None``."""
    prov = document.get("provenance")
    publisher = prov.get("publisher") if isinstance(prov, dict) else None
    return publisher.get("uri") if isinstance(publisher, dict) else None


def resolve_publisher_uri(
    publisher_uri: Optional[str],
    *,
    existing: Any = None,
    field: str = "publisher_uri",
    existing_field: str = "provenance.publisher.uri",
) -> Optional[str]:
    """The one publisher URI a document is minted under, or ``None`` for none.

    Both the caller's ``publisher_uri`` and the URI the document ``existing``-ly
    carries are checked, so a malformed one is never hidden by the other, and
    two that disagree are refused: ids are kept, never re-minted under another
    publisher.
    """
    uris: Dict[str, str] = {}
    for name, value in ((field, publisher_uri), (existing_field, existing)):
        if value is not None:
            uris[name] = normalize_publisher_uri(value, field=name)
    if len(set(uris.values())) > 1:
        raise CsiIdentityError(
            f"{field} {uris[field]!r} differs from {existing_field} {uris[existing_field]!r}, "
            "under which this document's mapping ids were published; ids are kept, "
            "never re-minted under another publisher"
        )
    return next(iter(uris.values()), None)


def mint_mapping_ids(document: Dict[str, Any], base: str) -> Dict[str, Any]:
    """A copy of ``document`` with an id under ``base`` on every mapping entry.

    Mirrors ASA's ``_mint_mapping_ids``: an id already present is kept
    (``setdefault``), entities and relationships get
    ``<base>/mapping/<entity|relationship>/<name>``, each property
    ``<prefix>/property/<name>``, and a lifted ``reference`` on a conceptual
    relationship ``<base>/mapping/relationship/<type>/reference``. A mapping
    with no properties gets no ``propertyIds``. Names are percent-encoded with
    nothing left safe, so a ``/`` in a name cannot forge a path segment.
    """
    doc = copy.deepcopy(document)  # never mutate the caller's document

    def seg(name: str) -> str:
        return quote(name, safe="")

    physical = doc["arangoPhysicalMapping"]
    for kind, label in _ID_KINDS:
        specs = physical.get(kind)
        for name, spec in specs.items() if isinstance(specs, dict) else []:
            if not isinstance(spec, dict):
                continue
            prefix = f"{base}/mapping/{label}/{seg(name)}"
            spec.setdefault("id", prefix)
            props = spec.get("properties")
            ids = spec.setdefault("propertyIds", {}) if isinstance(props, dict) and props else None
            # A value already present, valid or not, is kept for identity_issues to judge.
            if isinstance(props, dict) and isinstance(ids, dict):
                for prop in props:
                    ids.setdefault(prop, f"{prefix}/property/{seg(prop)}")
    conceptual = doc.get("conceptualModel")
    for rel in conceptual.get("relationships") or [] if isinstance(conceptual, dict) else []:
        if isinstance(rel, dict) and isinstance(rel.get("reference"), dict):
            rel["reference"].setdefault(
                "id", f"{base}/mapping/relationship/{seg(str(rel.get('type')))}/reference"
            )
    return doc


def publish_identities(document: Dict[str, Any], publisher_uri: str) -> Dict[str, Any]:
    """``document`` published under ``publisher_uri``: ids minted, alias written, checked.

    The single identity path for :func:`r2g.csi.mapping_to_csi` and any caller
    holding a finished document. ``provenance.publisher.uri`` is set,
    ``physicalMapping`` becomes a deep copy of ``arangoPhysicalMapping`` (which
    stays where it is), and the result is refused with
    :class:`CsiIdentityError` if :func:`identity_issues` finds anything —
    ids from another publisher, a stray ``propertyIds`` key — rather than
    returned for a reader to refuse.
    """
    if publisher_uri is None:
        raise CsiIdentityError("publisher_uri is required to publish mapping ids")
    base = resolve_publisher_uri(publisher_uri, existing=_existing_publisher(document)) or publisher_uri
    doc = mint_mapping_ids(document, base)
    doc.setdefault("provenance", {})["publisher"] = {"uri": base}
    doc["physicalMapping"] = copy.deepcopy(doc["arangoPhysicalMapping"])
    require_identity_integrity(doc)
    return doc


def identity_issues(document: Dict[str, Any]) -> List[str]:
    """What JSON Schema cannot check about the alias and the mapping ids.

    Mirrors ASA's ``_identity_issues``, message for message:

    * ``physicalMapping``, when present beside ``arangoPhysicalMapping``, equals it.
    * Once any id-carrying key is present the set is complete — every entity and
      relationship mapping, every property (``propertyIds`` keys are exactly the
      ``properties`` keys) and every lifted ``reference`` — and every id is
      unique and lies under ``provenance.publisher.uri``. A document without
      ids claims no identity and is not checked further.
    """
    if not isinstance(document, dict):
        return []
    issues: List[str] = []
    physical = document.get("arangoPhysicalMapping")
    if "physicalMapping" in document and physical is not None and document["physicalMapping"] != physical:
        issues.append("physicalMapping differs from arangoPhysicalMapping")

    # (where, id): None when absent, _NO_PROPERTY for a stray propertyIds key.
    ids: List[Tuple[str, Any]] = []
    for kind, _ in _ID_KINDS:
        specs = physical.get(kind) if isinstance(physical, dict) else None
        for name, spec in specs.items() if isinstance(specs, dict) else []:
            if not isinstance(spec, dict):
                continue
            where = f"arangoPhysicalMapping.{kind}.{name}"
            ids.append((where, spec.get("id")))
            raw_props, raw_ids = spec.get("properties"), spec.get("propertyIds")
            props: Dict[str, Any] = raw_props if isinstance(raw_props, dict) else {}
            prop_ids: Dict[str, Any] = raw_ids if isinstance(raw_ids, dict) else {}
            ids.extend((f"{where}.propertyIds.{p}", prop_ids.get(p)) for p in props)
            ids.extend((f"{where}.propertyIds.{p}", _NO_PROPERTY) for p in prop_ids if p not in props)
    conceptual = document.get("conceptualModel")
    for rel in conceptual.get("relationships") or [] if isinstance(conceptual, dict) else []:
        if isinstance(rel, dict) and isinstance(rel.get("reference"), dict):
            ids.append(
                (f"conceptualModel.relationships.{rel.get('type')}.reference", rel["reference"].get("id"))
            )

    # Any id-carrying key present, even empty or stray, claims identity and is checked.
    if all(i is None for _, i in ids):
        return issues
    base = _existing_publisher(document)
    if not isinstance(base, str) or not base:
        issues.append("mapping ids are present but provenance.publisher.uri is missing")
    for where, mapping_id in ids:
        if mapping_id is _NO_PROPERTY:
            issues.append(f"{where} names no property in properties")
        elif not isinstance(mapping_id, str) or not mapping_id:
            issues.append(f"{where} has no mapping id")
        elif isinstance(base, str) and base and not mapping_id.startswith(base.rstrip("/") + "/"):
            issues.append(f"{where} id {mapping_id!r} is not under publisher {base!r}")
    counts = Counter(i for _, i in ids if isinstance(i, str) and i)
    issues.extend(f"duplicate mapping id {i!r}" for i, n in sorted(counts.items()) if n > 1)
    return issues


def carries_identity(document: Dict[str, Any]) -> bool:
    """Whether ``document`` holds anything :func:`identity_issues` would judge.

    A publisher, the ``physicalMapping`` alias, or any ``id`` / ``propertyIds``
    on a mapping entry or lifted reference.
    """
    if not isinstance(document, dict):
        return False
    if "physicalMapping" in document or _existing_publisher(document) is not None:
        return True
    physical = document.get("arangoPhysicalMapping")
    for kind, _ in _ID_KINDS:
        specs = physical.get(kind) if isinstance(physical, dict) else None
        for spec in specs.values() if isinstance(specs, dict) else []:
            if isinstance(spec, dict) and ("id" in spec or "propertyIds" in spec):
                return True
    conceptual = document.get("conceptualModel")
    return any(
        isinstance(rel, dict) and isinstance(rel.get("reference"), dict) and "id" in rel["reference"]
        for rel in (conceptual.get("relationships") or [] if isinstance(conceptual, dict) else [])
    )


def require_identity_integrity(document: Dict[str, Any]) -> None:
    """Raise :class:`CsiIdentityError` listing every :func:`identity_issues` entry."""
    issues = identity_issues(document)
    if issues:
        raise CsiIdentityError(
            "refusing a CSI document with invalid mapping ids:\n  - " + "\n  - ".join(issues)
        )


def refuse_collapsed_mappings(
    entity_sources: Dict[str, List[str]],
    relationship_sources: Dict[str, List[str]],
) -> None:
    """Refuse when distinct physical sources would share one conceptual name.

    ``entity_sources`` maps each final conceptual entity name (after naming and
    curated renames) to a description of every distinct physical source behind
    it; ``relationship_sources`` does the same for relationship types. The
    physical mapping is keyed by that name, so a second source silently
    overwrites the first and both would be published under one id. That is
    caught here, from the inputs, because nothing in the finished document
    still shows the source that was lost.
    """
    problems = [
        f"{kind} {name!r} would map {len(sources)} distinct physical sources: " + "; ".join(sources)
        for kind, by_name in (("entity", entity_sources), ("relationship", relationship_sources))
        for name, sources in sorted(by_name.items())
        if len(sources) > 1
    ]
    if problems:
        raise CsiIdentityError(
            "refusing to mint mapping ids: one conceptual name would identify several physical "
            "mappings, and all but one would be lost:\n  - "
            + "\n  - ".join(problems)
            + "\nRename one of each (a curated label, or the target collection) and export again."
        )
