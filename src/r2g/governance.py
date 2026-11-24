"""Entitlement report, load-time gate, and lineage manifest (PRD Phase 9b).

Lane discipline (see ``docs/internal/PLAN-classification-entitlement.md``): r2g
**advises and emits**; the serving layer **enforces**. This module turns the
9a classification carrier + mosaic recompute into:

- an **entitlement report** over a mapping + annotated schema — every target
  property, its contributing source columns, its mosaic-recomputed sensitivity
  level, and whether it is masked;
- a **threshold gate** that, by default, excludes above-threshold *unmasked*
  fields from a load (overridable with ``allow_sensitive``); and
- a **lineage manifest** — the auditable record of which classified source
  columns crossed the boundary, at what level, and how they were handled.

Everything here is pure (no I/O except the explicit manifest writer) and is
unit-tested against hand-built ``Schema`` / ``MappingConfig`` objects.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field

from r2g.classification import (
    PUBLIC,
    SENSITIVITY_ORDER,
    exceeds_threshold,
    max_sensitivity,
    recompute_mosaic,
    sensitivity_rank,
    tier_of,
)
from r2g.expressions import extract_bind_references
from r2g.masking import is_masking_expression, mask_kind_of
from r2g.types import Classification, MappingConfig, Schema

DEFAULT_THRESHOLD = "confidential"


class EntitlementField(BaseModel):
    """One target property's governance posture in a mapping."""

    source_table: str
    target_collection: str
    target_property: str
    # Contributing source columns (one for identity maps, several for fan-in).
    source_columns: list[str] = Field(default_factory=list)
    level: str = PUBLIC
    tags: list[str] = Field(default_factory=list)
    tier: Optional[str] = None
    masked: bool = False
    mask_kind: Optional[str] = None
    # Set by the gate: the field was excluded from the load.
    excluded: bool = False


class KeyExposure(BaseModel):
    """A sensitive column a load would write into a document or edge key.

    The gate removes properties, not keys: ``_key``, ``_from`` and ``_to`` are built
    from the row's values, so a sensitive primary key, any column of a table without
    one, or a sensitive foreign key that an edge follows reaches the graph whatever the
    gate excludes. The remedy is a reviewed key overlay naming a key that is not
    sensitive, or leaving the table or edge out.
    """

    collection: str  # the collection or edge collection whose keys carry it
    source_table: str
    column: str
    level: str


class EntitlementReport(BaseModel):
    """Pre-load governance report for a project's mapping."""

    project: Optional[str] = None
    threshold: str = DEFAULT_THRESHOLD
    fields: list[EntitlementField] = Field(default_factory=list)
    collection_levels: dict[str, str] = Field(default_factory=dict)
    edge_levels: dict[str, str] = Field(default_factory=dict)
    # Sensitive columns that would be written into keys, which the gate cannot remove.
    key_exposures: list[KeyExposure] = Field(default_factory=list)

    @property
    def above_threshold(self) -> list[EntitlementField]:
        """Above-threshold fields that are NOT masked (the ones the gate acts on)."""
        return [
            f
            for f in self.fields
            if not f.masked and exceeds_threshold(f.level, self.threshold)
        ]

    @property
    def masked_fields(self) -> list[EntitlementField]:
        return [f for f in self.fields if f.masked]

    def summary(self) -> dict[str, int]:
        return {
            "total_fields": len(self.fields),
            "above_threshold": len(self.above_threshold),
            "masked": len(self.masked_fields),
        }


def _aggregate(classifications: list[Classification]) -> tuple[list[str], Optional[str]]:
    """Union of tags + first tier across several classifications."""
    tags: list[str] = []
    tier: Optional[str] = None
    for clf in classifications:
        for t in clf.tags:
            if t not in tags:
                tags.append(t)
        if tier is None and clf.tier:
            tier = clf.tier
    return tags, tier


def build_entitlement_report(
    config: MappingConfig,
    schema: Schema,
    *,
    threshold: str = DEFAULT_THRESHOLD,
    project: Optional[str] = None,
    tag_levels: Optional[dict[str, str]] = None,
) -> EntitlementReport:
    """Compute the entitlement report for a mapping over an annotated schema.

    Mirrors the mosaic recompute's notion of a "property": a fan-in
    ``FieldExpression`` aggregates the level + tags of its source columns; plain
    kept columns map 1:1. Masking is detected from the field-expression sentinel
    (a masked field is reported but excluded from the gate's above-threshold set).
    """
    mosaic = recompute_mosaic(config, schema, tag_levels=tag_levels)
    report = EntitlementReport(
        project=project,
        threshold=threshold,
        collection_levels=dict(mosaic.collections),
        edge_levels=dict(mosaic.edges),
    )

    for cm in config.collections.values():
        table = schema.tables.get(cm.source_table)
        if table is None:
            continue
        clf_by_col: dict[str, Classification] = {
            c.name: (c.classification or Classification()) for c in table.columns
        }

        # Every expression for each target property: a mapping may hold more than one,
        # and the transformer writes the last, so a target is rated by all of them.
        by_target: dict[str, list] = {}
        for fx in cm.field_expressions:
            by_target.setdefault(fx.target, []).append(fx)
        # A target counts as masked only when each of its expressions is a mask.
        mask_by_target: dict[str, Optional[str]] = {
            target: mask_kind_of(fxs[-1])
            for target, fxs in by_target.items()
            if all(is_masking_expression(fx) for fx in fxs)
        }
        # Every column a target reads: the ones its expressions declare, and every column
        # they reference, which the transformer reads from the row whether declared or not.
        expr_sources: dict[str, list[str]] = {}
        for target, fxs in by_target.items():
            cols: list[str] = []
            for fx in fxs:
                cols += fx.sources or ([fx.target] if fx.target in clf_by_col else [])
                cols += extract_bind_references(fx.expression)
            expr_sources[target] = list(dict.fromkeys(cols))

        # Determine kept source columns (include/exclude lists).
        kept = [c.name for c in table.columns]
        if cm.include_fields is not None:
            inc = set(cm.include_fields)
            kept = [c for c in kept if c in inc]
        if cm.exclude_fields:
            exc = set(cm.exclude_fields)
            kept = [c for c in kept if c not in exc]

        # Expression (incl. fan-in) target properties.
        seen_targets: set[str] = set()
        for fx in (fxs[-1] for fxs in by_target.values()):
            sources = expr_sources.get(fx.target, [])
            clfs = [clf_by_col.get(s, Classification()) for s in sources]
            level = max_sensitivity(tier_of(c, tag_levels=tag_levels) for c in clfs)
            tags, tier = _aggregate(clfs)
            report.fields.append(
                EntitlementField(
                    source_table=cm.source_table,
                    target_collection=cm.target_collection,
                    target_property=fx.target,
                    source_columns=list(sources),
                    level=level,
                    tags=tags,
                    tier=tier,
                    masked=fx.target in mask_by_target,
                    mask_kind=mask_by_target.get(fx.target),
                )
            )
            seen_targets.add(fx.target)

        # Plain kept columns not already covered by an expression target.
        for col in kept:
            target_prop = cm.field_mappings.get(col, col)
            if target_prop in seen_targets:
                continue
            clf = clf_by_col.get(col, Classification())
            report.fields.append(
                EntitlementField(
                    source_table=cm.source_table,
                    target_collection=cm.target_collection,
                    target_property=target_prop,
                    source_columns=[col],
                    level=tier_of(clf, tag_levels=tag_levels),
                    tags=list(clf.tags),
                    tier=clf.tier,
                    masked=target_prop in mask_by_target,
                    mask_kind=mask_by_target.get(target_prop),
                )
            )

    report.key_exposures = key_exposures(config, schema, threshold=threshold,
                                         tag_levels=tag_levels)
    return report


def key_exposures(
    config: MappingConfig,
    schema: Schema,
    *,
    threshold: str = DEFAULT_THRESHOLD,
    tag_levels: Optional[dict[str, str]] = None,
    include: Optional[list[str]] = None,
    exclude: Optional[list[str]] = None,
) -> list[KeyExposure]:
    """Every column at or above ``threshold`` that a load of ``config`` would write into
    a key, for the tables the load writes (filtered by source table, as a load filters
    them: an empty ``include`` means every table, as it does to the pipeline).

    A document's ``_key`` is its table's key (:func:`r2g.keys.row_key`: the primary key,
    or every column of a table without one). An edge carries its starting table's key in
    ``_from`` and its own ``_key``, whether or not that table is mapped as documents, and
    the values of its ``from_fields`` in ``_to``; those are the key of the row it points
    at, so each is rated at the higher of its own level and the referenced column's.
    """
    inc = set(include) if include else None
    exc = set(exclude or ())

    def loads(table: str) -> bool:
        return (inc is None or table in inc) and table not in exc

    def level_of(table_name: str, column: str) -> str:
        table = schema.tables.get(table_name)
        col = next((c for c in table.columns if c.name == column), None) if table else None
        clf = (col.classification if col is not None else None) or Classification()
        return tier_of(clf, tag_levels=tag_levels)

    def key_of(table_name: str) -> list[str]:
        table = schema.tables[table_name]
        return list(table.primary_key) or [c.name for c in table.columns]

    out: list[KeyExposure] = []

    def note(where: str, table_name: str, column: str, level: str) -> None:
        if exceeds_threshold(level, threshold):
            out.append(KeyExposure(collection=where, source_table=table_name,
                                   column=column, level=level))

    for cm in config.collections.values():
        # Only document collections are written per row; an edge-typed mapping writes
        # nothing itself (its relationships are the edges below).
        if (cm.collection_type == "document" and cm.source_table in schema.tables
                and loads(cm.source_table)):
            for col in key_of(cm.source_table):
                note(cm.target_collection, cm.source_table, col,
                     level_of(cm.source_table, col))
    for edge in config.edges:
        start = edge.from_collection
        if start not in schema.tables or not loads(start):
            continue
        for col in key_of(start):  # _from, and the edge's _key
            note(edge.edge_collection, start, col, level_of(start, col))
        for i, col in enumerate(edge.from_fields):  # _to, and the edge's _key
            pointed = edge.to_fields[i] if i < len(edge.to_fields) else None
            levels = [level_of(start, col)]
            if pointed is not None:
                levels.append(level_of(edge.to_collection, pointed))
            note(edge.edge_collection, start, col, max_sensitivity(levels))
    return out


def apply_sensitivity_gate(
    config: MappingConfig,
    report: EntitlementReport,
    *,
    allow_sensitive: bool = False,
) -> tuple[MappingConfig, list[EntitlementField]]:
    """Return a copy of ``config`` with above-threshold unmasked fields excluded.

    The gate is the migration's refusal to *silently* launder sensitive data: by
    default the contributing source columns of every above-threshold, unmasked
    field are added to that collection's ``exclude_fields`` for the run, and the
    field's expression, if it has one, is removed. Passing
    ``allow_sensitive=True`` is the explicit opt-out (config returned unchanged).

    Returns ``(gated_config, excluded_fields)`` where each excluded field has
    ``excluded=True`` set on the returned copies. ``report`` is not mutated.
    """
    if allow_sensitive:
        return config, []

    gated = config.model_copy(deep=True)
    excluded: list[EntitlementField] = []
    for field in report.above_threshold:
        # The field's own collection: the one that reads its table and writes it.
        # (Validation refuses two document collections on one table, which a load
        # could not honour.)
        for cm in gated.collections.values():
            if (cm.source_table != field.source_table
                    or cm.target_collection != field.target_collection):
                continue
            for col in field.source_columns:
                if col not in cm.exclude_fields:
                    cm.exclude_fields.append(col)
            # An expression reads its columns from the row, excluded or not, so the
            # field's expression goes too; otherwise it would load what it computes.
            cm.field_expressions = [fx for fx in cm.field_expressions
                                    if fx.target != field.target_property]
        marked = field.model_copy(update={"excluded": True})
        excluded.append(marked)
    return gated, excluded


def lineage_manifest(report: EntitlementReport) -> dict:
    """Build the machine-readable lineage manifest for a report.

    The auditable record of what crossed the relational→graph boundary: each
    target property, its source columns, classification, mosaic level, and how
    it was handled (masked / excluded / loaded).
    """
    entries = []
    for f in report.fields:
        if f.masked:
            handling = f"masked:{f.mask_kind}" if f.mask_kind else "masked"
        elif f.excluded:
            handling = "excluded"
        elif exceeds_threshold(f.level, report.threshold):
            handling = "loaded-above-threshold"
        else:
            handling = "loaded"
        entries.append(
            {
                "source": [f"{f.source_table}.{c}" for c in f.source_columns],
                "target": f"{f.target_collection}.{f.target_property}",
                "level": f.level,
                "tags": f.tags,
                "tier": f.tier,
                "handling": handling,
            }
        )
    return {
        "project": report.project,
        "threshold": report.threshold,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "collection_levels": report.collection_levels,
        "edge_levels": report.edge_levels,
        "summary": report.summary(),
        "fields": entries,
    }


def write_lineage_manifest(report: EntitlementReport, out_dir: str | Path) -> Path:
    """Write ``lineage.json`` under ``<out_dir>/governance/`` and return its path."""
    gov_dir = Path(out_dir) / "governance"
    gov_dir.mkdir(parents=True, exist_ok=True)
    path = gov_dir / "lineage.json"
    path.write_text(json.dumps(lineage_manifest(report), indent=2), encoding="utf-8")
    return path


# ── Phase 9c: enable enforcement — emit, don't enforce ────────────────────
#
# r2g never sits in the query-time authz path. On a governed load it *emits*
# the metadata a serving layer needs to enforce: a canonical classification
# manifest, a suggested ArangoDB collection-RBAC grant table, an optional
# OPA/Rego policy stub, and (optionally) a per-tier physical-layout
# recommendation. The operator / IdP / proxy applies them.

ADVISORY_NOTE = (
    "Advisory only. r2g emits this artifact at migration time; it does NOT "
    "enforce access. The serving layer (ArangoDB RBAC, an OPA/proxy, or your "
    "IdP) is responsible for enforcement. Levels are mosaic-recomputed "
    "(max-of-contributors) and may exceed any single source column's level."
)


def _all_entity_levels(report: EntitlementReport) -> dict[str, str]:
    """Entity→level map keyed by **target** (ArangoDB) collection / edge names.

    The mosaic levels carried on the report are keyed by *source* table name;
    governance artifacts must reference the real target collection names a
    serving layer grants on. Document-collection levels are recomputed as the
    max over their fields' levels (equivalent to the mosaic collection level);
    edge levels are already keyed by the (target) edge-collection name.
    """
    levels: dict[str, str] = {}
    for f in report.fields:
        cur = levels.get(f.target_collection, PUBLIC)
        levels[f.target_collection] = max_sensitivity([cur, f.level])
    for ename, lvl in report.edge_levels.items():
        levels[ename] = lvl
    return levels


def classification_manifest(
    report: EntitlementReport,
    *,
    owners: Optional[list[str]] = None,
    synced_at: Optional[str] = None,
) -> dict:
    """Canonical per-collection/edge/field classification + lineage.

    A superset of the lineage manifest: adds the data owners and the
    classification-sync timestamp, and groups fields under their target
    collection so a serving layer can build access rules per collection.
    """
    base = lineage_manifest(report)
    target_levels = _all_entity_levels(report)
    collections: dict[str, dict] = {}
    for f in report.fields:
        coll = collections.setdefault(
            f.target_collection,
            {
                "level": target_levels.get(f.target_collection, PUBLIC),
                "fields": [],
            },
        )
        coll["fields"].append(
            {
                "property": f.target_property,
                "level": f.level,
                "tags": f.tags,
                "tier": f.tier,
                "masked": f.masked,
                "mask_kind": f.mask_kind,
                "excluded": f.excluded,
                "sources": [f"{f.source_table}.{c}" for c in f.source_columns],
            }
        )
    return {
        "kind": "r2g.classification-manifest/v1",
        "note": ADVISORY_NOTE,
        "project": report.project,
        "generated_at": base["generated_at"],
        "classifications_synced_at": synced_at,
        "owners": owners or [],
        "threshold": report.threshold,
        "summary": report.summary(),
        "collection_levels": report.collection_levels,
        "edge_levels": report.edge_levels,
        "collections": collections,
        "lineage": base["fields"],
    }


def suggested_rbac(
    report: EntitlementReport,
    *,
    database: Optional[str] = None,
) -> dict:
    """Suggested ArangoDB collection-level grants, by clearance (cumulative).

    Emits one read role per clearance level present in the graph. A clearance
    grants read (``ro``) on every collection at or below that level — a
    ``confidential``-cleared principal reads public/internal/confidential
    collections but NOT restricted ones. This is a *recommendation* an operator
    applies via ArangoDB Enterprise collection access or an IdP group mapping;
    r2g never creates users or grants.
    """
    levels = _all_entity_levels(report)
    roles = []
    prev_readable: Optional[list[str]] = None
    for clearance in SENSITIVITY_ORDER:
        readable = sorted(
            c
            for c, lvl in levels.items()
            if sensitivity_rank(lvl) <= sensitivity_rank(clearance)
        )
        # Skip empty clearances and ones that read exactly what a lower
        # clearance already reads (no collection exists at this tier).
        if not readable or readable == prev_readable:
            continue
        roles.append(
            {
                "role": f"r2g_clearance_{clearance}",
                "clearance": clearance,
                "access": "ro",
                "collections": readable,
            }
        )
        prev_readable = readable
    return {
        "kind": "r2g.suggested-rbac/v1",
        "note": ADVISORY_NOTE,
        "project": report.project,
        "database": database,
        "collection_levels": levels,
        "roles": roles,
    }


def policy_rego(report: EntitlementReport, *, package: str = "r2g.authz") -> str:
    """An OPA/Rego policy stub keyed on collection level vs. principal clearance.

    Default-deny; allow a read when the principal's clearance rank is >= the
    collection's mosaic level rank. Emitted as a templated string (no ``opa``
    dependency). Advisory — wire it into an app/proxy to enforce.
    """
    levels = _all_entity_levels(report)
    rank_lines = ",\n".join(
        f'    "{lvl}": {sensitivity_rank(lvl)}' for lvl in SENSITIVITY_ORDER
    )
    if levels:
        coll_lines = ",\n".join(
            # JSON string escaping is Rego's own: a quote or newline in a name stays
            # inside its string.
            f"    {json.dumps(c)}: {json.dumps(lvl)}" for c, lvl in sorted(levels.items())
        )
    else:
        coll_lines = ""
    return (
        f"# Generated by r2g (PRD Phase 9c) — ADVISORY policy stub.\n"
        f"# r2g emits; the serving layer enforces. Default-deny.\n"
        f"package {package}\n\n"
        f"import future.keywords.in\n\n"
        f"sensitivity_rank := {{\n{rank_lines}\n}}\n\n"
        f"# Mosaic-recomputed level per target collection / edge.\n"
        f"collection_level := {{\n{coll_lines}\n}}\n\n"
        f"default allow := false\n\n"
        f"# Allow read when the principal's clearance dominates the collection level.\n"
        f"allow if {{\n"
        f"    input.action == \"read\"\n"
        f"    level := collection_level[input.collection]\n"
        f"    sensitivity_rank[input.principal.clearance] >= sensitivity_rank[level]\n"
        f"}}\n\n"
        f"# Unknown collections are denied by default (no entry == not allowed).\n"
    )


def tier_layout_recommendation(
    report: EntitlementReport,
    *,
    database: Optional[str] = None,
) -> dict:
    """Recommend a per-tier physical layout so coarse collection-RBAC can bite.

    Groups collections/edges by mosaic tier and suggests a database (or
    collection-prefix) per tier. A *recommendation*, not a generated layout —
    generating the layout is a larger mapping/load change deferred past V1.
    """
    levels = _all_entity_levels(report)
    tiers: dict[str, dict] = {}
    for coll, lvl in sorted(levels.items()):
        tier = tiers.setdefault(
            lvl,
            {
                "collections": [],
                "suggested_database": f"{database}_{lvl}" if database else None,
                "suggested_prefix": f"{lvl}_",
            },
        )
        tier["collections"].append(coll)
    return {
        "kind": "r2g.tier-layout/v1",
        "note": (
            "Advisory layout recommendation. Separating tiers into distinct "
            "databases/graphs (or collection prefixes) lets ArangoDB's "
            "collection-level RBAC enforce coarse tier boundaries. r2g "
            "recommends; it does not generate the layout."
        ),
        "project": report.project,
        "database": database,
        "strategy": "separate-database" if database else "collection-prefix",
        "tiers": tiers,
    }


def write_governance_artifacts(
    report: EntitlementReport,
    out_dir: str | Path,
    *,
    owners: Optional[list[str]] = None,
    database: Optional[str] = None,
    tier_layout: bool = False,
    emit_rego: bool = True,
    synced_at: Optional[str] = None,
) -> dict[str, Path]:
    """Emit the Phase 9c governance artifacts under ``<out_dir>/governance/``.

    Always writes the lineage manifest, the classification manifest, and the
    suggested-RBAC table; writes ``policy.rego`` when ``emit_rego`` and the
    tier-layout recommendation when ``tier_layout``. Returns the written paths
    keyed by artifact name.
    """
    gov_dir = Path(out_dir) / "governance"
    gov_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, Path] = {}

    def _write_json(name: str, payload: dict) -> None:
        p = gov_dir / name
        p.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        written[name] = p

    _write_json("lineage.json", lineage_manifest(report))
    _write_json(
        "classification-manifest.json",
        classification_manifest(report, owners=owners, synced_at=synced_at),
    )
    _write_json("suggested-rbac.json", suggested_rbac(report, database=database))
    if emit_rego:
        rego_path = gov_dir / "policy.rego"
        rego_path.write_text(policy_rego(report), encoding="utf-8")
        written["policy.rego"] = rego_path
    if tier_layout:
        _write_json(
            "tier-layout.json",
            tier_layout_recommendation(report, database=database),
        )
    return written


__all__ = [
    "ADVISORY_NOTE",
    "DEFAULT_THRESHOLD",
    "EntitlementField",
    "EntitlementReport",
    "build_entitlement_report",
    "apply_sensitivity_gate",
    "lineage_manifest",
    "write_lineage_manifest",
    "classification_manifest",
    "suggested_rbac",
    "policy_rego",
    "tier_layout_recommendation",
    "write_governance_artifacts",
]
