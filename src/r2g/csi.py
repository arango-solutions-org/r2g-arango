"""Forward CSI v1 (Conceptual Schema Interchange) emitter.

r2g is the natural *forward* producer of ``CSI v1``: it knows both the source
relational schema *and* the ArangoDB collections it decided to create, so it can
emit a single document that pairs the conceptual model with its Arango physical
mapping. Downstream, the contextual-data-fabric M5 federated-query engine feeds
this bundle to the mapping adapters (CSI->R2RML for the Ontop relational leg,
CSI->MappingBundle for the ``arango-sparql-py`` AQL leg) so a conceptual SPARQL
query can be partitioned by source. See ``docs/PRD.md`` Phase 12 and
contextual-data-fabric ADR-0001 / implementation-plan WP-A1.

The reverse producer is ``arango-schema-analyzer`` (which reads an existing
Arango graph). Both write the *same* ``CSI v1`` contract — this module targets
``schemas/csi_v1.schema.json`` (a vendored copy of the analyzer's authoritative
schema; copy-now-converge-later).

The core entry point :func:`mapping_to_csi` is pure and deterministic (no
timestamps, no I/O) so its output is trivially testable; the CLI layer stamps
``generatedAt``.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from importlib import resources
from typing import Any, Dict, List, Optional, Tuple

from .csi_identities import (
    CsiIdentityError,  # noqa: F401  (re-exported: raised by mapping_to_csi)
    identity_issues,
    normalize_publisher_uri,
    publish_identities,
    refuse_collapsed_mappings,
)
from .types import (
    CollectionMapping,
    MappingConfig,
    Schema,
    Table,
)

CSI_VERSION = "1"
# CSI 1.1 adds ``path`` to a property mapping: a value inside a nested object,
# one name per level (arango-sparql-py#72). A document says "1.1" only when it
# uses ``path``, so a reader that accepts only "1" refuses just those documents
# and keeps reading every flat one. r2g writes flat documents today, so it
# writes "1"; ``csi_version_for`` makes that a rule, not a constant.
CSI_VERSION_NESTED = "1.1"
PRODUCER = "r2g"

# What a CSI property mapping (``arangoPhysicalMapping.entities.<E>.properties.
# <p>``) may hold under CSI 1.1: where the value is (``field`` — one literal
# name, a dot included — or ``path``), the descriptive extras producers write,
# or a key starting with ``x-``. Readers refuse anything else, because an
# ignored location key is how a reader answers from the wrong field.
CSI_PROPERTY_MAPPING_KEYS = frozenset(
    {
        "field",
        "path",
        "type",
        "indexed",
        "unique",
        "required",
        "role",
        "sentinelValues",
        "numericLike",
        "sampleValues",
    }
)
CSI_PROPERTY_EXTENSION_PREFIX = "x-"

# ArangoDB physical styles r2g emits, one pair per target layout (P13).
#
# ``pg`` — a table becomes its own document collection and an edge its own
# dedicated edge collection, so the type *is* the collection.
_ENTITY_STYLE = "COLLECTION"
_RELATIONSHIP_STYLE = "DEDICATED_COLLECTION"
# ``lpg`` — every type shares one collection and the type moves into a field,
# so the mapping has to name the carrier (``typeField``) and this entity's or
# relationship's value in it (``typeValue``) as well as the collection. Emitting
# the pg pair for an LPG target would describe collections that do not exist:
# a consumer would resolve ``Account`` to a collection named ``Account``, find
# nothing, and fail far from the cause.
_ENTITY_STYLE_LPG = "LABEL"
_RELATIONSHIP_STYLE_LPG = "GENERIC_WITH_TYPE"


def owl_entity_name(name: str) -> str:
    """Conceptual entity name per the fabric's CC-12 OWL convention.

    Singular PascalCase (``usage_metrics`` → ``UsageMetric``); the physical
    collection/table name is untouched — it lives in the physical mapping.
    """
    from .naming import convert_identifier, singularize, split_identifier

    words = split_identifier(name)
    if words:
        words[-1] = singularize(words[-1])
    return convert_identifier("_".join(words), "pascal") or name


def owl_property_name(name: str) -> str:
    """Conceptual property/relationship name per CC-12: lowerCamel."""
    from .naming import convert_identifier

    return convert_identifier(name, "camel") or name


def _qualified_label(entity_name: str, label: str, *, collapse_seam: bool = False) -> str:
    """``Contract`` + ``renewalDate`` → ``contractRenewalDate`` (CC-12 lowerCamel).

    With ``collapse_seam``, a word repeated across the join is dropped:
    ``FilmCategory`` + ``categoryId`` → ``filmCategoryId``, not
    ``filmCategoryCategoryId``. Only the ``roles`` policy collapses; the blunt
    ``qualify`` policy keeps its literal, wholly predictable concatenation.
    """
    from .naming import convert_identifier, split_identifier

    prefix = entity_name[:1].lower() + entity_name[1:]
    if collapse_seam:
        entity_words = split_identifier(entity_name)
        label_words = split_identifier(label)
        if (
            entity_words
            and len(label_words) > 1
            and entity_words[-1].lower() == label_words[0].lower()
        ):
            label_words = label_words[1:]
            return convert_identifier(
                "_".join(entity_words + label_words), "camel"
            ) or (prefix + label[:1].upper() + label[1:])
    return prefix + label[:1].upper() + label[1:]


def _structural_columns(config: MappingConfig, schema: Optional[Schema]):
    """``(pk_columns, fk_columns)`` as ``{(source_table, column)}``.

    A column is *structural* when the relational model — not the business
    domain — is the reason it exists: a primary key, a declared foreign key, or
    a column already carried by an edge in this mapping. Everything r2g needs to
    decide this is data it already holds, so the classification is mechanical
    rather than a heuristic that needs tuning.
    """
    pk_columns: set = set()
    fk_columns: set = set()
    if schema is not None:
        for table_name, table in schema.tables.items():
            for col in table.primary_key or []:
                pk_columns.add((table_name, col))
            for fk in table.foreign_keys or []:
                for col in fk.columns:
                    fk_columns.add((table_name, col))
    # Edges reference *source-table* names on both endpoints (see the resolver
    # in mapping_to_csi), so they key identically to the schema lookups above.
    for edge in config.edges:
        for col in edge.from_fields:
            fk_columns.add((edge.from_collection, col))
        for col in edge.to_fields:
            fk_columns.add((edge.to_collection, col))
    return pk_columns, fk_columns


def _property_roles(
    cm: CollectionMapping,
    entity: str,
    prop_names: List[str],
    pk_columns: set,
    fk_columns: set,
) -> Dict[tuple, str]:
    """``{(entity, label): "pk" | "fk" | "plain"}`` for one collection.

    Built here, at emit time, because this is the only point where a conceptual
    label's **source column** is still known. The physical mapping's ``field``
    is the *stored ArangoDB attribute*, which diverges from the source column
    whenever ``field_mappings`` renames one (pagila stores ``actorId`` for
    ``actor_id``), so classifying from ``field`` silently marks every renamed
    key as a plain attribute.
    """
    inverse = {target: source for source, target in cm.field_mappings.items()}
    computed = {fe.target for fe in cm.field_expressions}
    roles: Dict[tuple, str] = {}
    for prop in prop_names:
        label = owl_property_name(prop)
        if prop in computed:
            # A computed/fan-in property has no single source column, so it is
            # never a key however its inputs are shaped.
            roles[(entity, label)] = "plain"
            continue
        column = inverse.get(prop, prop)
        key = (cm.source_table, column)
        roles[(entity, label)] = (
            "pk" if key in pk_columns else "fk" if key in fk_columns else "plain"
        )
    return roles


def _resolve_label_collisions(
    conceptual_entities: List[Dict[str, Any]],
    physical_entities: Dict[str, Dict[str, Any]],
    policy: str,
    *,
    property_roles: Optional[Dict[tuple, str]] = None,
    join_key_labels: Optional[set] = None,
) -> List[Dict[str, Any]]:
    """Find (and optionally fix) attribute labels shared by two or more entities.

    A CSI document scopes each property to its entity, but consumers that derive
    a flat vocabulary from the labels — a generated question language, say —
    cannot recover that scoping. Two entities emitting ``renewalDate`` make every
    question mentioning it ambiguous *by construction*, and nothing downstream can
    undo it because the distinction was discarded here, at extraction time.

    ``policy``:
        ``"qualify"``
            Rename **every** occurrence to ``<entity><Label>``. Blunt and wholly
            predictable; the historical default.
        ``"roles"``
            Classify each collision by the *role* of the column behind it, then
            apply the remedy that fits (see below).
        ``"warn"``
            Record only. ``"off"`` skips the check entirely.

    ``join_key_labels`` names conceptual labels that are declared **cross-source
    join keys** (P6.7, emitted as ``conceptualModel.joinKeys``). A join key is a
    *deliberately* shared label — the federation spine — so it is exempt from
    every policy: kept bare on every entity that carries it, and recorded as a
    ``"join_key"`` collision so the sharing stays visible. Qualifying it would
    rename the very label ``joinKeys`` references, yielding a CSI whose join key
    points at a property no entity still holds.

    Under ``qualify`` every occurrence is renamed, not just the second one, for
    two reasons: the result cannot depend on mapping insertion order, and leaving
    one entity holding the bare label would preserve exactly the false confidence
    this fixes — a consumer asking for "renewal date" would still be silently
    routed to whichever entity happened to come first.

    ``roles`` splits collisions that are *not* the same kind of problem:

    - **semantic** (every occurrence a plain column — ``renewalDate``, ``city``):
      genuinely ambiguous business attributes. Qualified exactly as above.
    - **identity** (every occurrence a primary key — ``id``): not N ambiguous
      attributes but each entity's identity, already carried by ``_key`` in the
      physical mapping. Dropped as a conceptual attribute, which *removes* the
      ambiguity rather than renaming it.
    - **reference** (a foreign key is involved — ``accountId``): the entity that
      owns the key keeps the bare label; the entities that merely *reference* it
      are qualified. ``Account`` keeps ``accountId``; ``Contact`` becomes
      ``contactAccountId`` — never ``accountAccountId``, which reads as a
      business attribute rather than a join and stutters.

    Qualification is cascade-checked under every policy: a rename that would
    itself collide with another label (``Account.id`` → ``accountId``, already
    taken by ``Account.accountId``) is refused, and that group is recorded
    ``unresolved`` rather than traded for a fresh ambiguity. Returns one record
    per collision; the caller surfaces them. Mutates the entities in place.
    """
    if policy == "off":
        return []

    property_roles = property_roles or {}
    join_key_labels = join_key_labels or set()

    owners: Dict[str, List[str]] = {}
    for entity in conceptual_entities:
        for prop in entity["properties"]:
            owners.setdefault(prop["name"], []).append(entity["name"])

    colliding = {label: names for label, names in owners.items() if len(names) > 1}
    if not colliding:
        return []

    # Labels held by exactly one entity are already unambiguous and stay put, so
    # they permanently occupy their name; a qualified label may never take one.
    occupied = {label for label, names in owners.items() if len(names) == 1}

    records: List[Dict[str, Any]] = []
    renames: Dict[str, Dict[str, str]] = {}  # entity -> {old: new}
    drops: Dict[str, set] = {}  # entity -> {label, ...}

    def _apply(record, candidates, label, *, keeper=None):
        """Cascade-check ``candidates`` and either accept or refuse the group."""
        values = list(candidates.values())
        clashes = sorted(
            {c for c in values if c in occupied} | {c for c in values if values.count(c) > 1}
        )
        if clashes:
            record["resolution"] = "unresolved"
            record["reason"] = (
                "qualifying would collide with an existing label: " + ", ".join(clashes)
            )
            occupied.add(label)
            return
        record["resolution"] = "qualified"
        record["renamedTo"] = candidates
        if keeper is not None:
            record["owner"] = keeper
            occupied.add(label)  # the owner keeps it
        for entity_name, new_label in candidates.items():
            renames.setdefault(entity_name, {})[label] = new_label
            occupied.add(new_label)

    for label in sorted(colliding):
        entities = sorted(colliding[label])
        record: Dict[str, Any] = {"label": label, "entities": entities}

        if label in join_key_labels:
            # A declared cross-source join key (P6.7): the shared label IS the
            # federation spine that ``conceptualModel.joinKeys`` references, so it
            # must stay bare on every binding entity under every policy. Renaming
            # it would leave joinKeys pointing at a label no entity still holds.
            record["resolution"] = "join_key"
            record["detail"] = (
                "declared cross-source join key (P6.7) — kept bare on every "
                "entity so the federation join spine survives; the shared label "
                "is intentional, not an ambiguity"
            )
            records.append(record)
            occupied.add(label)
            continue

        if policy in {"warn", "reported"}:
            record["resolution"] = "reported"
            records.append(record)
            occupied.add(label)
            continue

        if policy == "qualify":
            _apply(record, {n: _qualified_label(n, label) for n in entities}, label)
            records.append(record)
            continue

        # ── policy == "roles" ──
        roles = {name: property_roles.get((name, label), "plain") for name in entities}
        kinds = set(roles.values())

        if kinds == {"plain"}:
            record["kind"] = "semantic"
            _apply(
                record,
                {n: _qualified_label(n, label, collapse_seam=True) for n in entities},
                label,
            )
            records.append(record)
            continue

        if kinds == {"pk"}:
            # Identity, not an attribute: ``_key`` already carries it, so the
            # collision disappears instead of being renamed into six variants.
            record["kind"] = "structural"
            record["resolution"] = "identity"
            record["detail"] = (
                "primary key on every entity carrying it — emitted as identity "
                "(_key in the physical mapping), not as a conceptual attribute"
            )
            for name in entities:
                drops.setdefault(name, set()).add(label)
            records.append(record)
            continue

        # A foreign key is involved. Whoever owns the key keeps the bare label.
        record["kind"] = "structural"
        keeper = _key_owner(label, entities, roles)
        if keeper is None:
            record["detail"] = "no entity in this document owns the key; qualifying all"
            _apply(
                record,
                {n: _qualified_label(n, label, collapse_seam=True) for n in entities},
                label,
            )
        else:
            _apply(
                record,
                {
                    n: _qualified_label(n, label, collapse_seam=True)
                    for n in entities
                    if n != keeper
                },
                label,
                keeper=keeper,
            )
        records.append(record)

    for entity in conceptual_entities:
        name = entity["name"]
        mapping = renames.get(name)
        dropped = drops.get(name, set())
        if not mapping and not dropped:
            continue
        mapping = mapping or {}
        entity["properties"] = [
            {**prop, "name": mapping.get(prop["name"], prop["name"])}
            for prop in entity["properties"]
            if prop["name"] not in dropped
        ]
        physical = physical_entities.get(name)
        if physical is not None:
            # Rebuild rather than mutate so property order is preserved; the
            # value (the physical field) is untouched, so the conceptual rename
            # never breaks the mapping back to the stored column.
            physical["properties"] = {
                mapping.get(prop_name, prop_name): spec
                for prop_name, spec in physical["properties"].items()
                if prop_name not in dropped
            }

    return records


def _apply_curated_plan(
    plan,
    conceptual_entities: List[Dict[str, Any]],
    physical_entities: Dict[str, Dict[str, Any]],
    property_roles: Dict[tuple, str],
    entity_name_by_collection: Dict[str, str],
) -> None:
    """Rename entities and properties to their curated names, in place (P12.12).

    Conceptual names only: each physical property spec — the stored ``field`` —
    moves to its new conceptual key untouched, so a curated name still resolves
    to the column that holds the data. Property roles are re-keyed so the
    ``roles`` collision policy still knows which renamed label is a key.
    """
    for entity in conceptual_entities:
        generated = entity["name"]
        new_name = plan.entity(generated)
        entity.update(plan.annotations.get(("entity", generated), {}))
        if new_name != generated:
            entity["name"] = new_name
            # labels[0] is the entity name (P13.13); extra LPG labels follow.
            entity["labels"] = [new_name] + [
                lbl for lbl in entity.get("labels", [])[1:] if lbl != new_name
            ]
            physical_entities[new_name] = physical_entities.pop(generated)
            for collection, name in list(entity_name_by_collection.items()):
                if name == generated:
                    entity_name_by_collection[collection] = new_name
        renamed_props: Dict[str, str] = {}
        properties = []
        for prop in entity["properties"]:
            new_prop = plan.prop(generated, prop["name"])
            annotated = {
                **prop,
                "name": new_prop,
                **plan.annotations.get(("property", generated, prop["name"]), {}),
            }
            properties.append(annotated)
            renamed_props[prop["name"]] = new_prop
            role = property_roles.pop((generated, prop["name"]), None)
            if role is not None:
                property_roles[(new_name, new_prop)] = role
        entity["properties"] = properties
        physical = physical_entities.get(new_name)
        if physical is not None:
            physical["properties"] = {
                renamed_props.get(name, name): spec
                for name, spec in physical["properties"].items()
            }


def _key_owner(label: str, entities: List[str], roles: Dict[str, str]) -> Optional[str]:
    """The entity a shared key *belongs to*, or ``None``.

    Preference: the entity holding it as a primary key, else the entity whose
    name matches the key's entity prefix (``accountId`` → ``Account``). Both
    tiers are resolved in sorted order so the answer never depends on mapping
    insertion order. Reuses the P6.7 hub heuristics rather than restating them.
    """
    from r2g.xsk import _table_matches_entity, entity_of

    pk_holders = sorted(n for n in entities if roles.get(n) == "pk")
    if pk_holders:
        return pk_holders[0]
    prefix = entity_of(label.lower())
    if not prefix:
        return None
    named = sorted(n for n in entities if _table_matches_entity(n, prefix))
    return named[0] if named else None


def _entity_property_names(cm: CollectionMapping, table: Optional[Table]) -> List[str]:
    """Ordered, de-duplicated target-property names for one collection.

    Prefers the explicit mapping (``field_mappings`` values + ``field_expressions``
    targets); falls back to the source table's columns (honouring
    include/exclude) when the mapping doesn't rename anything.
    """
    names: List[str] = []
    seen: set[str] = set()

    def _add(name: str) -> None:
        if name and name not in seen:
            seen.add(name)
            names.append(name)

    # Explicit field renames: value is the target property name.
    for target in cm.field_mappings.values():
        _add(target)
    # Computed / fan-in properties.
    for fe in cm.field_expressions:
        _add(fe.target)

    # Fall back to the physical columns when we have the schema and no explicit
    # rename covered a column.
    if table is not None:
        include = set(cm.include_fields) if cm.include_fields is not None else None
        exclude = set(cm.exclude_fields)
        for col in table.columns:
            if include is not None and col.name not in include:
                continue
            if col.name in exclude:
                continue
            _add(cm.field_mappings.get(col.name, col.name))

    return names


def _join_key_labels(config: MappingConfig) -> set:
    """Generated conceptual labels that are declared cross-source join keys (P6.7)."""
    collection_by_source_table = {cm.source_table: cm for cm in config.collections.values()}
    labels: set = set()
    for sk in config.shared_keys:
        for b in sk.bindings:
            cm_b = collection_by_source_table.get(b.table)
            if cm_b is None or cm_b.collection_type == "edge" or cm_b.is_join_table:
                continue
            stored = cm_b.field_mappings.get(b.column, b.column)
            labels.add(owl_property_name(stored))
    return labels


_KEY_SUFFIX = re.compile(r"(?i)(?:_id|id)$")


def entity_names_for_mapping(
    config: MappingConfig, schema: Optional[Schema]
) -> Tuple[Dict[str, str], List[Dict[str, Any]]]:
    """Conceptual class name per collection, and the foreign-key evidence used (P12.13).

    The name is :func:`owl_entity_name` of the target collection, unless the
    schema's own foreign keys say otherwise. A column that names one row of the
    table it references with a singular of its own (``plans.basis_id`` ->
    ``bases``) is the schema designer's word for that row, so when every such
    column agrees on a singular that differs from the singularizer's, that
    singular is used (``bases`` -> ``Basis``, where the singularizer gives
    ``Base``).

    Only declared or curated single-column keys count: keys r2g inferred were
    matched *using* the singularizer, so they are not independent evidence. A
    column whose other words differ from the table's (``ship_via``,
    ``reports_to``, ``manager_id``) names a role, not the table, and is ignored,
    as is any table whose collection the mapping renamed or that shares its
    collection with another table.

    A column that only repeats its table's name (``goods_id`` -> ``goods``,
    ``customers_id`` -> ``customers``) is not evidence: many schemas name every
    key ``<table>_id``, and nothing in one table tells the two apart. Name such a
    table with a curated ``identifier`` instead (P12.12). Returns
    ``(names, evidence)``: ``names`` maps every entity collection to its class
    name; ``evidence`` records each name the foreign keys decided.
    """
    from relational_schema_analyzer.overlay import INFERRED_CONSTRAINT_PREFIX

    from .naming import convert_identifier, pluralize, singularize, split_identifier

    def _words(name: str) -> List[str]:
        return [w.lower() for w in split_identifier(name)]

    referencing: Dict[str, List[Tuple[str, str]]] = {}
    if schema is not None:
        for table_name, table in schema.tables.items():
            for fk in table.foreign_keys:
                if len(fk.columns) != 1:
                    continue
                if (fk.constraint_name or "").startswith(INFERRED_CONSTRAINT_PREFIX):
                    continue
                referencing.setdefault(fk.foreign_table, []).append((table_name, fk.columns[0]))

    entity_maps = [
        cm for cm in config.collections.values() if cm.collection_type != "edge" and not cm.is_join_table
    ]
    sharing = Counter(cm.target_collection for cm in entity_maps)
    names: Dict[str, str] = {}
    evidence: List[Dict[str, Any]] = []
    for cm in entity_maps:
        generated = owl_entity_name(cm.target_collection)
        names[cm.target_collection] = generated
        if sharing[cm.target_collection] > 1:
            continue
        table_words = _words(cm.source_table)
        if not table_words or _words(cm.target_collection) != table_words:
            continue
        head, last = table_words[:-1], table_words[-1]
        said: Dict[str, List[str]] = {}
        for ref_table, column in referencing.get(cm.source_table, []):
            match = _KEY_SUFFIX.search(column)
            if match is None or match.start() == 0:
                continue
            prefix_words = _words(column[: match.start()].rstrip("_"))
            if not prefix_words or prefix_words[:-1] != head:
                continue
            word = prefix_words[-1]
            if word != last and (singularize(last).lower() == word or pluralize(word).lower() == last):
                said.setdefault(word, []).append(f"{ref_table}.{column}")
        if len(said) != 1:
            continue
        ((word, columns),) = said.items()
        named = convert_identifier("_".join(head + [word]), "pascal") or generated
        if named != generated:
            names[cm.target_collection] = named
            evidence.append(
                {
                    "collection": cm.target_collection,
                    "name": named,
                    "singularizerName": generated,
                    "rule": "foreign-key",
                    "foreignKeys": sorted(columns),
                }
            )
    return names, evidence


def curated_plan_for_mapping(
    config: MappingConfig,
    schema: Optional[Schema],
    curated: Dict[str, Any],
):
    """The :class:`~r2g.curated_labels.CuratedPlan` for one mapping (P12.12).

    The single place both exporters get their curated names from: the entity and
    property names are the ones :func:`mapping_to_csi` generates, so a CSI
    document and the R2RML exported from the same mapping and labels agree.
    """
    from .curated_labels import plan_curated_labels

    tables = schema.tables if schema is not None else {}
    names, _ = entity_names_for_mapping(config, schema)
    entities: Dict[str, List[str]] = {}
    for cm in config.collections.values():
        if cm.collection_type == "edge" or cm.is_join_table:
            continue
        entities[names[cm.target_collection]] = [
            owl_property_name(p)
            for p in _entity_property_names(cm, tables.get(cm.source_table))
        ]
    return plan_curated_labels(
        curated,
        entities=entities,
        relationships=[owl_property_name(e.edge_collection) for e in config.edges],
        join_key_labels=_join_key_labels(config),
    )


def csi_version_for(physical_entities: Dict[str, Any]) -> str:
    """The ``csiVersion`` a document with these physical entities must declare.

    ``"1.1"`` when some property mapping uses ``path``, ``"1"`` otherwise — so
    a later change that emits ``path`` (embedding a table's columns as a nested
    object) cannot keep the old version by accident.
    """
    for spec in physical_entities.values():
        properties = spec.get("properties") if isinstance(spec, dict) else None
        for prop in (properties or {}).values():
            if isinstance(prop, dict) and "path" in prop:
                return CSI_VERSION_NESTED
    return CSI_VERSION


def mapping_to_csi(
    config: MappingConfig,
    schema: Optional[Schema] = None,
    *,
    source_type: str = "relational",
    source_ref: str = "",
    source_fingerprint: Optional[str] = None,
    producer_version: Optional[str] = None,
    generated_at: Optional[str] = None,
    confidence: Optional[float] = None,
    label_policy: str = "qualify",
    transaction_time: Optional[str] = None,
    valid_time: Optional[Dict[str, Any]] = None,
    valid_time_source: Optional[str] = None,
    predecessor_fingerprint: Optional[str] = None,
    curated_labels: Optional[Dict[str, Any]] = None,
    publisher_uri: Optional[str] = None,
) -> Dict[str, Any]:
    """Emit a forward ``CSI v1`` document from an r2g :class:`MappingConfig`.

    Args:
        config: the r2g mapping (source tables -> collections, FK/join edges).
        schema: optional source :class:`Schema`; when given, entity property
            lists are enriched from the physical columns.
        source_type: ``provenance.source.kind`` (e.g. ``"postgresql"``,
            ``"mysql"``, ``"mssql"``, ``"snowflake"``, ``"csv"``).
        source_ref: ``provenance.source.ref`` — a human pointer to the source
            (database/schema name). Defaults to ``config.source_schema``.
        source_fingerprint: optional content hash of the source schema.
        producer_version: ``provenance.producerVersion``; defaults to the
            installed r2g version.
        generated_at: optional ISO-8601 stamp for ``provenance.generatedAt``
            (kept out of the pure path so callers control determinism).
        confidence: optional 0..1 confidence; r2g's mapping is a deterministic
            mechanical translation, so callers typically leave this unset.
        label_policy: what to do when two entities emit the same attribute
            label — ``"qualify"`` (default) renames every occurrence to
            ``<entity><Label>``; ``"roles"`` classifies each collision by the
            role of the column behind it and applies the fitting remedy
            (semantic → qualify, primary key → emit as identity, foreign key →
            the owning entity keeps the bare label); ``"warn"`` records them
            untouched; ``"off"`` skips the check. Whatever the policy, every
            collision is recorded in ``provenance.labelCollisions``; see
            :func:`_resolve_label_collisions`.
        curated_labels: an optional curated-labels document
            (:mod:`r2g.curated_labels`, P12.12). Its renames are applied
            *before* the label-collision check, carry ``prefLabel`` /
            ``description`` / ``altLabels`` onto the renamed concept, and are
            recorded in ``provenance.curatedLabels``. Physical fields are never
            renamed.
        publisher_uri: optional publisher namespace (r2g-arango#91). With it,
            every mapping entry gets a deterministic id under it, the mapping is
            also written as ``physicalMapping`` and the URI is recorded as
            ``provenance.publisher.uri`` (see :mod:`r2g.csi_identities`). Ids
            are minted last, from the final names. Two physical sources that
            would share one conceptual name are refused rather than published
            under one id. Without it the document is exactly what it was before.

    Returns:
        A ``CSI v1``-valid ``dict`` (validate with :func:`validate_csi`).

    Raises:
        CsiIdentityError: ``publisher_uri`` is malformed, or would identify
            two physical mappings by one id.
    """
    if producer_version is None:
        from . import __version__ as producer_version  # local import: avoid cycle
    if publisher_uri is not None:
        # Fail fast on a malformed URI, before any work is done.
        normalize_publisher_uri(publisher_uri)

    tables = schema.tables if schema is not None else {}
    # The physical layout this mapping targets (P13). When set, entities and
    # relationships are described by LABEL / GENERIC_WITH_TYPE rather than by a
    # collection per type — see the style constants above.
    lpg = config.lpg if config.graph_layout == "lpg" else None

    # --- Conceptual + physical entities: one per document collection. ---
    conceptual_entities: List[Dict[str, Any]] = []
    physical_entities: Dict[str, Dict[str, Any]] = {}
    entity_name_by_collection: Dict[str, str] = {}
    generated_names, entity_naming = entity_names_for_mapping(config, schema)
    # {(entity, label): "pk"|"fk"|"plain"} — the structural role of the column
    # behind each conceptual property, used by the "roles" label policy.
    pk_columns, fk_columns = _structural_columns(config, schema)
    property_roles: Dict[tuple, str] = {}
    # Curated labels (P12.12) are planned here, from the inputs alone, so a
    # publisher export can check the final names below before building anything.
    plan = (
        curated_plan_for_mapping(config, schema, curated_labels)
        if curated_labels is not None
        else None
    )
    if publisher_uri is not None:
        # The physical mappings below are keyed by conceptual name, so a second
        # source with the same name overwrites the first. Legacy exports keep
        # that behaviour; a publisher export would mint one id for both.
        refuse_collapsed_mappings(*_mapping_sources(config, generated_names, plan))
    for cm in config.collections.values():
        if cm.collection_type == "edge" or cm.is_join_table:
            # Join tables / edge collections are relationships, not entities.
            continue
        # Conceptual name follows CC-12 (singular PascalCase), or the schema's own
        # foreign keys (P12.13); the physical collection name is preserved in the
        # physical mapping.
        name = generated_names[cm.target_collection]
        entity_name_by_collection[cm.target_collection] = name
        prop_names = _entity_property_names(cm, tables.get(cm.source_table))
        property_roles.update(_property_roles(cm, name, prop_names, pk_columns, fk_columns))
        # Conceptual labels: the entity name, plus any additional LPG labels the
        # mapping declares (P13.13), so a consumer sees the full set a node
        # answers to. The *physical* `typeValue` below stays the primary label —
        # the CSI schema allows only one there, and the primary is what the
        # `_key` namespace and the entity's identity are built on.
        entity_labels = [name]
        for extra in cm.extra_labels:
            candidate = owl_entity_name(extra)
            if candidate not in entity_labels:
                entity_labels.append(candidate)
        conceptual_entities.append(
            {
                "name": name,
                "labels": entity_labels,
                "properties": [{"name": owl_property_name(p)} for p in prop_names],
            }
        )
        entity_physical: Dict[str, Any] = {
            "style": _ENTITY_STYLE,
            "collectionName": cm.target_collection,
            # Conceptual property → stored field, so the AQL/SQL legs resolve
            # OWL-style names back to physical attributes (CC-12).
            "properties": {owl_property_name(p): {"field": p} for p in prop_names},
        }
        if lpg is not None:
            # The label the loader actually writes is the *physical* target
            # collection name, not the conceptual entity name — those diverge
            # under a naming convention (`accounts` → `Account`), and this
            # mapping must describe what is stored.
            entity_physical.update(
                style=_ENTITY_STYLE_LPG,
                collectionName=lpg.node_collection,
                typeField=lpg.label_field,
                typeValue=cm.target_collection,
            )
        physical_entities[name] = entity_physical

    # Source-table → collection, needed both for the join-key label set here and
    # the join-key bindings emitted further below.
    collection_by_source_table = {
        cm.source_table: cm for cm in config.collections.values()
    }
    # Conceptual labels that are declared cross-source join keys (P6.7). A join
    # key is deliberately shared across entities, so the collision resolver must
    # keep it bare on every binding entity — qualifying it would rename the very
    # spine that ``conceptualModel.joinKeys`` (built below) references.
    join_key_labels = _join_key_labels(config)

    # Curated labels (P12.12) go first, so the collision check below sees the
    # names a curator chose — a curated rename can itself resolve a collision.
    if plan is not None:
        _apply_curated_plan(
            plan, conceptual_entities, physical_entities, property_roles, entity_name_by_collection
        )

    # Resolve attribute labels shared by two or more entities *before* anything
    # downstream sees them: this is the only place with the whole document in
    # view, so a collision left here is unrecoverable for every consumer.
    label_collisions = _resolve_label_collisions(
        conceptual_entities,
        physical_entities,
        label_policy,
        property_roles=property_roles,
        join_key_labels=join_key_labels,
    )

    # --- Conceptual + physical relationships: one per edge definition. ---
    # EdgeDefinition.from_collection / to_collection reference *source-table*
    # names; resolve them to the collection names that actually hold the data.
    target_by_source = {
        cm.source_table: cm.target_collection for cm in config.collections.values()
    }
    # ``collection_by_source_table`` (source table → CollectionMapping) is built
    # above, before label resolution, and reused for the join-key bindings below.
    conceptual_relationships: List[Dict[str, Any]] = []
    physical_relationships: Dict[str, Dict[str, Any]] = {}
    for edge in config.edges:
        generated_type = owl_property_name(edge.edge_collection)
        rel_type = plan.relationship(generated_type) if plan is not None else generated_type
        from_coll = target_by_source.get(edge.from_collection, edge.from_collection)
        to_coll = target_by_source.get(edge.to_collection, edge.to_collection)
        relationship: Dict[str, Any] = {
            "type": rel_type,
            "fromEntity": entity_name_by_collection.get(from_coll, owl_entity_name(from_coll)),
            "toEntity": entity_name_by_collection.get(to_coll, owl_entity_name(to_coll)),
        }
        if plan is not None:
            relationship.update(plan.annotations.get(("relationship", generated_type), {}))
        conceptual_relationships.append(relationship)
        # NB: relationships must NOT carry ``collectionName`` (CSI schema
        # forbids it) — only ``edgeCollectionName``.
        rel_physical: Dict[str, Any] = {
            "style": _RELATIONSHIP_STYLE,
            "edgeCollectionName": edge.edge_collection,
        }
        if lpg is not None:
            rel_physical.update(
                style=_RELATIONSHIP_STYLE_LPG,
                edgeCollectionName=lpg.edge_collection,
                typeField=lpg.type_field,
                typeValue=edge.edge_collection,
            )
        physical_relationships[rel_type] = rel_physical

    # --- Declared cross-source join keys (PRD P6.7). ---
    # These are NOT relationships: a project maps one source, so a key shared
    # with another source has no edge collection to name. They are emitted as a
    # separate conceptual block so the federated executor can bind-join on them
    # without hand-authored config. The CSI v1 schema allows additional
    # properties on ``conceptualModel``, so this is additive and stays valid.
    join_keys: List[Dict[str, Any]] = []
    for sk in config.shared_keys:
        hub: Dict[str, Any] = {"kind": sk.hub_kind, "concept": sk.concept}
        if sk.hub_kind == "entity":
            hub.update({"source": sk.hub_source, "table": sk.hub_table, "column": sk.hub_column})
        bindings: List[Dict[str, Any]] = []
        for b in sk.bindings:
            collection = target_by_source.get(b.table)
            binding: Dict[str, Any] = {
                "source": b.source,
                "table": b.table,
                # The SQL column, for the relational leg.
                "column": b.column,
            }
            # Only tables mapped by *this* project resolve to a conceptual
            # entity; the other sources' tables are named but unresolved here.
            if collection is not None:
                binding["entity"] = entity_name_by_collection.get(
                    collection, owl_entity_name(collection)
                )
                # ``field`` is the *stored ArangoDB attribute*, matching the
                # meaning it carries everywhere else in this document — so it
                # must go through field_mappings. Emitting the source column
                # here would hand the federated executor an attribute that does
                # not exist on the documents it bind-joins.
                cm_for_table = collection_by_source_table.get(b.table)
                binding["field"] = (
                    cm_for_table.field_mappings.get(b.column, b.column)
                    if cm_for_table is not None
                    else b.column
                )
            bindings.append(binding)
        join_keys.append(
            {
                "key": sk.key,
                "concept": sk.concept,
                "hub": hub,
                "bindings": bindings,
                "confidence": sk.confidence,
                "method": sk.method,
            }
        )

    source: Dict[str, Any] = {
        "kind": source_type or "relational",
        "ref": source_ref or config.source_schema,
        "fingerprint": source_fingerprint,
    }
    provenance: Dict[str, Any] = {
        "producer": PRODUCER,
        "producerVersion": producer_version,
        "direction": "forward",
        "source": source,
        "generatedAt": generated_at,
    }
    if confidence is not None:
        provenance["confidence"] = confidence
    # Bitemporal stamping (CSI v1.1) — pass valid/transaction time through so the downstream
    # temporal store (contextual-data-fabric) gets both clocks from the forward producer too.
    # Converged with arango-schema-analyzer §3.13.5 and the RSA twin; kept out of the pure
    # path like generatedAt, so the CLI/caller (which knows the source's valid time) supplies them.
    if transaction_time is not None:
        provenance["transactionTime"] = transaction_time
    if valid_time is not None:
        provenance["validTime"] = valid_time
    if valid_time_source is not None:
        provenance["validTimeSource"] = valid_time_source
    if predecessor_fingerprint is not None:
        provenance["predecessorFingerprint"] = predecessor_fingerprint
    # Recorded even when every collision was auto-qualified: a consumer (or a
    # curator) must be able to see that two entities meant the same word, and
    # which rename resolved it. Omitted entirely when there were none, so
    # collision-free documents keep their historical bytes.
    if label_collisions:
        provenance["labelCollisions"] = label_collisions
    # Every entry of a supplied curated-labels document, applied or not: a
    # refused or unmatched decision must be as visible as an applied one.
    if entity_naming:
        # Class names the schema's own foreign keys decided (P12.13).
        provenance["entityNaming"] = entity_naming
    if plan is not None:
        provenance["curatedLabels"] = plan.records
        if plan.source:
            provenance["curatedLabelsSource"] = plan.source

    conceptual_model: Dict[str, Any] = {
        "entities": conceptual_entities,
        "relationships": conceptual_relationships,
    }
    # Omitted entirely when no join key has been accepted, so mappings without
    # P6.7 keys emit byte-identical CSI to before.
    if join_keys:
        conceptual_model["joinKeys"] = join_keys

    document: Dict[str, Any] = {
        "csiVersion": csi_version_for(physical_entities),
        "conceptualModel": conceptual_model,
        "arangoPhysicalMapping": {
            "entities": physical_entities,
            "relationships": physical_relationships,
        },
        "provenance": provenance,
    }
    # Last, so ids are minted from the final names: after conceptual naming,
    # curated labels, label-collision renames and the physical mapping.
    if publisher_uri is not None:
        document = publish_identities(document, publisher_uri)
    return document


def _mapping_sources(
    config: MappingConfig, generated_names: Dict[str, str], plan
) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
    """Each final conceptual entity name and relationship type -> its distinct physical sources.

    Names are resolved exactly as :func:`mapping_to_csi` resolves them —
    generated, then curated — so :func:`refuse_collapsed_mappings` sees the keys
    the physical mapping will be built under.
    """
    entities: Dict[str, List[str]] = {}
    for cm in config.collections.values():
        if cm.collection_type == "edge" or cm.is_join_table:
            continue
        name = generated_names[cm.target_collection]
        final = plan.entity(name) if plan is not None else name
        source = f"table {cm.source_table!r} -> collection {cm.target_collection!r}"
        if source not in entities.setdefault(final, []):
            entities[final].append(source)
    relationships: Dict[str, List[str]] = {}
    for edge in config.edges:
        generated_type = owl_property_name(edge.edge_collection)
        final = plan.relationship(generated_type) if plan is not None else generated_type
        source = (
            f"edge collection {edge.edge_collection!r} "
            f"({edge.from_collection}.{','.join(edge.from_fields)} -> "
            f"{edge.to_collection}.{','.join(edge.to_fields)})"
        )
        if source not in relationships.setdefault(final, []):
            relationships[final].append(source)
    return entities, relationships


def csi_schema() -> Dict[str, Any]:
    """Load the ``CSI v1`` JSON Schema.

    ``arango-schema-analyzer`` owns the authoritative copy
    (``schema_analyzer/csi/v1/csi.schema.json``). When that package is importable
    its schema is used, so r2g can never validate against a stale copy; otherwise
    the vendored copy in ``r2g.schemas`` is the fallback (r2g does not depend on
    the analyzer at runtime). ``tests/test_csi.py`` asserts the two agree.
    """
    try:
        text = (
            resources.files("schema_analyzer.csi.v1")
            .joinpath("csi.schema.json")
            .read_text(encoding="utf-8")
        )
    except (ModuleNotFoundError, FileNotFoundError, TypeError):
        text = (
            resources.files("r2g.schemas")
            .joinpath("csi_v1.schema.json")
            .read_text(encoding="utf-8")
        )
    return json.loads(text)


def _unique_constraint_errors(document: Dict[str, Any]) -> List[str]:
    """Shape and reference faults in every entity's ``uniqueConstraints``.

    The CSI schema cannot catch these. An entity object is declared
    ``additionalProperties: true`` (detection enrichments ride along as optional
    additive fields), so ``uniqueConstraints`` arrives as an unrecognised field
    and passes untouched however it is shaped. That schema is the analyzer's and
    r2g only vendors it — ``tests/test_csi.py`` asserts the copy does not drift —
    so r2g cannot add the field to it unilaterally. The check lives here instead,
    and runs against any CSI document r2g validates no matter who wrote it.

    Silence is the whole problem: a flat list or a misspelled property name is
    not a document that fails, it is a document whose declared uniqueness is
    wrong, and the first symptom is a downstream join that groups incorrectly —
    arbitrarily far from the typo.

    ``uniqueConstraints`` is a list of key *sets*, each set itself a list, because
    one key can span several properties and an entity can have several
    independent keys. Order carries no meaning anywhere. Names are conceptual
    property names (the entity's own ``properties``), never physical columns.

    An **absent** field means nobody declared uniqueness; an **empty** list means
    the entity was checked and genuinely has none. Both are legal, and they are
    not the same claim — neither is reported here.

    Returns:
        Human-readable faults, empty when every declaration is well-formed.
    """
    faults: List[str] = []
    conceptual = document.get("conceptualModel")
    if not isinstance(conceptual, dict):
        return faults
    entities = conceptual.get("entities")
    if not isinstance(entities, list):
        return faults

    for position, entity in enumerate(entities):
        if not isinstance(entity, dict) or "uniqueConstraints" not in entity:
            continue
        name = entity.get("name")
        where = name if isinstance(name, str) and name else f"entities[{position}]"
        declared = entity["uniqueConstraints"]

        if not isinstance(declared, list):
            faults.append(f"{where}: uniqueConstraints must be a list of key sets, got {type(declared).__name__}")
            continue

        # Only resolvable when the entity lists its properties; an entity that
        # omits them is under-specified, not wrong, and reporting every name as
        # unknown would bury the real faults.
        properties = entity.get("properties")
        known: Optional[set] = None
        if isinstance(properties, list):
            known = {
                prop["name"] for prop in properties if isinstance(prop, dict) and isinstance(prop.get("name"), str)
            }

        for index, key_set in enumerate(declared):
            at = f"{where}.uniqueConstraints[{index}]"
            if isinstance(key_set, str):
                # The single most likely mistake, so it names its own fix.
                faults.append(
                    f"{at} is the bare name {key_set!r}; every key set is itself "
                    f'a list, so a one-property key is [["{key_set}"]]'
                )
                continue
            if not isinstance(key_set, list):
                faults.append(f"{at} must be a list of property names, got {type(key_set).__name__}")
                continue
            if not key_set:
                faults.append(f"{at} is an empty key set; drop it or name its properties")
                continue

            seen: set = set()
            for member in key_set:
                if not isinstance(member, str) or not member:
                    faults.append(f"{at} contains {member!r}, which is not a property name")
                    continue
                if member in seen:
                    faults.append(f"{at} names {member!r} twice")
                seen.add(member)
                if known is not None and member not in known:
                    faults.append(
                        f"{at} names {member!r}, which is not a property of {where}; "
                        f"use conceptual property names, not source column names"
                    )

    return faults


def validate_csi(document: Dict[str, Any]) -> None:
    """Validate ``document`` against the ``CSI v1`` schema (see :func:`csi_schema`).

    Also enforces the ``uniqueConstraints`` rules the schema itself cannot express
    (see :func:`_unique_constraint_errors`), and the mapping-id and
    ``physicalMapping`` alias rules (:func:`r2g.csi_identities.identity_issues`),
    which find nothing in a document that carries no ids — as the analyzer's
    ``validate_csi`` does.

    Raises:
        jsonschema.ValidationError: if the document is not CSI-valid.
    """
    import jsonschema  # lazy: keep the pure emitter import-light

    jsonschema.validate(instance=document, schema=csi_schema())

    faults = _unique_constraint_errors(document)
    if faults:
        raise jsonschema.ValidationError("uniqueConstraints is malformed:\n  - " + "\n  - ".join(faults))

    issues = identity_issues(document)
    if issues:
        raise jsonschema.ValidationError("mapping identity is invalid:\n  - " + "\n  - ".join(issues))
