# Curated ontology approval bundles

This workflow turns a reviewed physical ontology and one exact relational-schema
snapshot into an inspectable, secret-free package for an ArangoDB operator. It
does not give r2g permission to run the package on the operator's behalf.

**Status (2026-11-20).** Steps 1–7 work today: import, reconciliation against a
pinned snapshot, project creation and explicit rebase (PRD P6.11–P6.13, P6.15).
The package itself — the ZIP, its content root, checksums, approval record,
extraction plan and guarded import (P6.14) — is **not built yet**. Today step 8
opens the existing `arangoimport` script preview for the saved mapping; there
is no download, and steps 9 onward and the sections *Approval boundary* and
*Runtime safety* describe the design the package will follow.

## What must be ready

- A registered Snowflake source. Enable **Include views** before taking the
  snapshot when the ontology maps views or materialized views.
- A reviewed RSA key overlay when Snowflake does not declare every primary,
  unique, and foreign key used by the mapping. Pending review candidates are
  not executable keys.
- The ontology's root document and every locally imported ontology document.
  r2g never fetches an `owl:imports` URL.
- A registered ArangoDB target that uses a dedicated database, not `_system`.

## Studio sequence

1. In `/workspace`, right-click **Sources**, add the Snowflake source, and take
   a snapshot.
2. Right-click the source and attach the reviewed key overlay. Take a new
   snapshot after changing keys; an old snapshot remains immutable.
3. Right-click **Ontologies**, choose **Import ontology**, and upload the root
   document plus its local imports. Supply the equivalent Turtle or RDF/XML
   closure when both serializations are governed artifacts.
4. Right-click the imported ontology and validate it against the exact source
   snapshot. Resolve every missing table/view, column, object-kind mismatch,
   ambiguous identity, and relationship blocker.
5. Add or select the ArangoDB target. Its endpoint, database, and username are
   copied into the project for audit; credentials remain in the encrypted
   catalog and never enter an approval package.
6. Create the project from the ontology. r2g pins the reconciled snapshot and
   compiles ontology terms, field renames, collection identity, and
   relationships into the mapping.
7. Review and save the mapping. Use **Rebase to latest snapshot** only when the
   source schema has intentionally changed; rebasing reruns reconciliation and
   invalidates approval of the old inputs.
8. Open **View arangoimport script bundle**, inspect the content root and
   provenance, then download the ZIP. The downloaded package is built from the
   saved mapping, not an unsaved browser draft.
9. Give the ZIP to the approving operator. The operator verifies its checksums
   and content root, records approval for that exact root, supplies runtime
   credentials through environment variables, extracts Snowflake rows, runs
   the guarded import, and runs verification.

## Approval boundary (planned, P6.14)

The ZIP contains scripts and immutable metadata, not source rows or secrets.
Its content root binds:

- the ontology closure and canonical mapping;
- the reviewed key overlay;
- the pinned schema snapshot and view policy;
- the saved r2g mapping;
- non-secret Snowflake and ArangoDB identities;
- generated extraction, import, graph, and verification scripts; and
- the bundle/tool policy versions.

Changing any bound input creates a different content root. An approval record
for the old root must not authorize the new package.

## Runtime safety (planned, P6.14)

Extraction uses explicit quoted projections rather than `SELECT *`. It writes
to a temporary runtime directory, records query IDs, headers, row counts,
bytes, and SHA-256 values, and publishes only after every object succeeds.

Before import, the package checks the approval record, runtime source and target
identity, expected file set, headers, and checksums. The import stops on an
`arangoimport` warning, rejected row, or failed command. Graph setup compares
the requested definition with the existing graph and never drops loaded
collections.

Keep the review ZIP separate from the runtime artifact directory. Runtime CSV
files may contain regulated data and must follow the organization's normal
retention, encryption, and deletion policy.
