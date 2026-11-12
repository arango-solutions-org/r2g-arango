from __future__ import annotations

import json
import os
import shlex
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from r2g.config import pg_type_to_json_type
from r2g.log import get_logger
from r2g.security import scrub_dsn_credentials
from r2g.types import CollectionMapping, MappingConfig, Schema

logger = get_logger(__name__)

_VALID_ON_DUPLICATE = frozenset({"error", "update", "replace", "ignore"})


#: How the password is referenced in every generated command.
#:
#: ArangoDB's client tools resolve a startup option wrapped in at signs from
#: the environment (``--server.password @ARANGO_PASSWORD@``). Their own help
#: text gives the reason: it "helps to expose the password less, like to the
#: process list". A plain ``"$ARANGO_PASSWORD"`` is expanded by the shell
#: before exec, so the secret is visible in ``ps`` to every user on the host
#: for as long as the import runs.
PASSWORD_ENV_REF = "@ARANGO_PASSWORD@"


#: Shell preamble that turns a silently-discarded row into a failed script.
#:
#: arangoimport discards a row whose raw CSV token is not a legal JSON value
#: for the type it was told to use -- an empty field in a column typed
#: ``number`` is the common case -- and **exits 0 anyway**, so ``set -e`` does
#: not fire and the script reports success with rows missing. Its
#: ``--max-errors`` option is not a usable guard: measured on ArangoDB 3.12.4,
#: fifty discarded rows still exit 0 whenever at least one row loaded, and it
#: only returns non-zero when every row failed.
#:
#: The summary arangoimport prints is the reliable signal, so each import runs
#: through this wrapper, which fails the script when the count is not zero.
#: A missing summary also fails: the wrapper fails closed, never open.
#:
#: One kind of rejection is the correct outcome: a CSV-direct edge whose
#: foreign key is NULL. ``_to`` is built from the FK column plus
#: ``--to-collection-prefix``, so a NULL arrives as the bare prefix
#: (``customers/``) and the server rejects it. There is nothing for that edge
#: to point to, so skipping it loses no data; failing on it would stop every
#: load of a schema with an optional FK. (JSONL mode drops those edges before
#: writing the file, so it never reaches arangoimport.)
#:
#: arangoimport logs at most 20 rejected documents however many there are
#: (measured on 3.12.11: 150 rejections, 20 logged), so the rejections cannot
#: be classified one by one. The ``null-fk-ok`` policy therefore accepts them
#: only when the generator knows the FK column is nullable, *and* every
#: rejection that was logged is a NULL-FK one. Any logged rejection of another
#: kind fails the script. Edge rows carry nothing but the two endpoints (every
#: other column is removed and none gets number or boolean coercion), so the
#: only other way to reject one is an FK value that is not a legal document
#: key. With an enforced FK that value is also the parent's primary key, whose
#: document import already failed under ``strict``. With an unenforced or
#: inferred FK, an orphan value of that kind falling beyond the first 20 logged
#: rejections is counted as a NULL FK: that is this guard's known limit.
IMPORT_GUARD = r'''
# arangoimport exits 0 even when it discards rows, so `set -e` cannot catch a
# partial load. Its printed summary is the reliable signal; check that instead.
#   run_import <label> strict|null-fk-ok <command...>
run_import() {
  local label="$1" policy="$2"; shift 2
  local output
  if ! output="$("$@" 2>&1)"; then
    printf '%s\n' "$output" >&2
    echo "FAILED: $label (arangoimport returned non-zero)" >&2
    exit 1
  fi
  printf '%s\n' "$output"
  local errors logged null_fk
  errors="$(printf '%s\n' "$output" | sed -n 's|^warnings/errors: *\([0-9][0-9]*\).*|\1|p' | tail -1)"
  if [ -z "$errors" ]; then
    echo "FAILED: $label (no 'warnings/errors:' summary; cannot confirm every row loaded)" >&2
    exit 1
  fi
  [ "$errors" -eq 0 ] && return 0
  # An edge whose foreign key is NULL reaches the server as the bare prefix
  # ("customers/") and is rejected; it has nothing to point to, so skipping it
  # is correct. Accept that only for a nullable FK, and only if every logged
  # rejection is of that kind (arangoimport logs at most 20 of them).
  logged="$(printf '%s\n' "$output" | grep -c 'offending ' || true)"
  null_fk="$(printf '%s\n' "$output" | grep -cE 'offending document: .*"_(from|to)":"[^"/]+/"[,}]' || true)"
  if [ "$policy" = "null-fk-ok" ] && [ "$logged" -gt 0 ] && [ "$null_fk" -eq "$logged" ]; then
    echo "NOTE: $label skipped $errors edge(s) whose foreign key is NULL (nothing to link to)."
    return 0
  fi
  echo "FAILED: $label discarded $errors row(s); the load is incomplete." >&2
  echo "        arangoimport reports this as a warning and exits 0, so this" >&2
  echo "        check exists to stop the script rather than report success." >&2
  exit 1
}
'''


def _guarded(command: str, label: str, *, null_fk_ok: bool = False) -> str:
    """Wrap an arangoimport invocation so a discarded row fails the script.

    ``command`` is a backslash-continued ``arangoimport ...`` string, so
    prefixing the shell function, its label and its policy is all that is
    required. ``null_fk_ok`` is for a CSV-direct edge import whose FK column
    is nullable; see ``IMPORT_GUARD``.
    """
    policy = "null-fk-ok" if null_fk_ok else "strict"
    return f"run_import {shlex.quote(label)} {policy} {command}"


def _bash_env_default(var_name: str, value: str) -> str:
    """A line that sets ``var_name`` to ``value`` unless the caller set it.

    The default is a plain assignment, where ``shlex.quote`` means what it
    says. Inside ``"${VAR:-...}"`` bash keeps single quotes as part of the
    value and still expands ``$``, so a default containing a space, ``!`` or
    ``$`` used to arrive quoted, or abort the script under ``set -u``.
    """
    if value == "":
        return f'{var_name}="${{{var_name}:-}}"'
    return f'[ -n "${{{var_name}:-}}" ] || {var_name}={shlex.quote(value)}'


def _collection_mapping_for_ref(
    config: MappingConfig,
    name: str,
) -> CollectionMapping | None:
    for key, mapping in config.collections.items():
        if name in (key, mapping.source_table, mapping.target_collection):
            return mapping
    return None


def _source_table_for_ref(config: MappingConfig, name: str) -> str:
    mapping = _collection_mapping_for_ref(config, name)
    return mapping.source_table if mapping is not None else name


def _target_collection_for_ref(config: MappingConfig, name: str) -> str:
    mapping = _collection_mapping_for_ref(config, name)
    return mapping.target_collection if mapping is not None else name


def _script_environment(
    *,
    endpoint: str,
    database: str,
    username: str,
    password: str,
    secret_safe: bool,
) -> list[str]:
    return [
        _bash_env_default(
            "ARANGO_ENDPOINT",
            scrub_dsn_credentials(endpoint) if secret_safe else endpoint,
        ),
        _bash_env_default("ARANGO_DB", database),
        _bash_env_default("ARANGO_USER", username),
        # Exported because the commands read it as @ARANGO_PASSWORD@, which the
        # client tool resolves from its own environment. A plain assignment is
        # a shell variable only: a password baked into the script would never
        # reach arangoimport, and the import would fail with HTTP 401.
        _bash_env_default("ARANGO_PASSWORD", "" if secret_safe else password),
        "export ARANGO_PASSWORD",
    ]


def _build_jsonl_import_command(
    *,
    endpoint: str,
    database: str,
    username: str,
    password: str,
    file_path: str,
    collection_name: str,
    collection_type: str,
    create_collection: bool,
    create_collection_type: str,
    on_duplicate: str,
    overwrite: bool,
    threads: int,
    environment_credentials: bool,
) -> str:
    resolved_type = (
        create_collection_type
        or ("edge" if collection_type == "edge" else "document")
    )
    # The password is always passed as @ARANGO_PASSWORD@, never as a shell
    # expansion or a literal. ArangoDB's client tools resolve a startup option
    # wrapped in at signs from the environment themselves, so the value never
    # reaches the process list where any other user on the host can read it.
    # The ARANGO_PASSWORD export at the top of the script supplies it in both
    # credential modes.
    server_values = (
        ('"$ARANGO_ENDPOINT"', '"$ARANGO_DB"', '"$ARANGO_USER"', PASSWORD_ENV_REF)
        if environment_credentials
        else (
            *(shlex.quote(value) for value in (endpoint, database, username)),
            PASSWORD_ENV_REF,
        )
    )
    parts: list[str] = [
        "arangoimport",
        "--server.endpoint",
        server_values[0],
        "--server.database",
        server_values[1],
        "--server.username",
        server_values[2],
        "--server.password",
        server_values[3],
        "--file",
        shlex.quote(file_path),
        "--type",
        "jsonl",
        "--collection",
        shlex.quote(collection_name),
        "--create-collection",
        "true" if create_collection else "false",
        "--create-collection-type",
        resolved_type,
        "--on-duplicate",
        shlex.quote(on_duplicate),
        "--threads",
        str(threads),
    ]
    if overwrite:
        parts.append("--overwrite")
    return " ".join(parts)


def _build_graph_creation_arangosh(
    config: MappingConfig,
    graph_name: str,
) -> list[str]:
    """Build an idempotent, shell-safe named-graph creation command."""
    rel_parts: list[str] = []
    for edge in config.edges:
        edge_collection = json.dumps(edge.edge_collection)
        from_collection = json.dumps(
            _target_collection_for_ref(config, edge.from_collection)
        )
        to_collection = json.dumps(
            _target_collection_for_ref(config, edge.to_collection)
        )
        rel_parts.append(
            "  graph._relation("
            f"{edge_collection}, [{from_collection}], [{to_collection}])"
        )

    js_lines = [
        'var graph = require("@arangodb/general-graph");',
        f"try {{ graph._drop({json.dumps(graph_name)}, true); }} catch(e) {{}}",
        "var edgeDefs = [",
        ",\n".join(rel_parts),
        "];",
        f"graph._create({json.dumps(graph_name)}, edgeDefs);",
        f"print({json.dumps(f'Named graph {graph_name} created.')});",
    ]
    js_code = " ".join(js_lines)
    return [
        "echo " + shlex.quote(f"Creating named graph {graph_name}..."),
        (
            "arangosh"
            ' --server.endpoint "$ARANGO_ENDPOINT"'
            ' --server.database "$ARANGO_DB"'
            ' --server.username "$ARANGO_USER"'
            f" --server.password {PASSWORD_ENV_REF}"
            f" --javascript.execute-string {shlex.quote(js_code)}"
        ),
    ]


def _render_graph_shell(
    config: MappingConfig,
    graph_name: str,
    *,
    endpoint: str,
    database: str,
    username: str,
    password: str,
    secret_safe: bool,
) -> str:
    lines = [
        "#!/usr/bin/env bash",
        "# Generated by R2G for arangosh",
        "set -euo pipefail",
        "",
        *_script_environment(
            endpoint=endpoint,
            database=database,
            username=username,
            password=password,
            secret_safe=secret_safe,
        ),
        "",
        *_build_graph_creation_arangosh(config, graph_name),
    ]
    return "\n".join(lines).rstrip() + "\n"


@dataclass(frozen=True)
class ImportCommandSpec:
    """One generated command tied to a validated mapping object."""

    mapping_id: str
    kind: Literal["document", "edge"]
    source_table: str
    target_collection: str
    command: str
    edge_collection: str | None = None
    from_collection: str | None = None
    to_collection: str | None = None
    from_fields: tuple[str, ...] = ()
    to_fields: tuple[str, ...] = ()

    def model_dump(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class ImportPlan:
    """Structured, in-memory batch artifact shared by CLI and Studio."""

    mode: Literal["jsonl", "csv"]
    data_dir: str
    script: str
    documents: tuple[ImportCommandSpec, ...]
    edges: tuple[ImportCommandSpec, ...]
    graph_script: str
    warnings: tuple[str, ...] = field(default_factory=tuple)

    def model_dump(self) -> dict:
        """Pydantic-like serializer used by the FastAPI response layer."""
        return asdict(self)


class ArangoImportGenerator:
    """Generates arangoimport shell scripts for loading JSONL data into ArangoDB."""

    def __init__(
        self,
        config: MappingConfig,
        endpoint: str = "http://localhost:8529",
        database: str = "_system",
        username: str = "root",
        password: str = "",
        data_dir: str = "./output",
        on_duplicate: str = "replace",
    ) -> None:
        if on_duplicate not in _VALID_ON_DUPLICATE:
            raise ValueError(
                f"on_duplicate must be one of {sorted(_VALID_ON_DUPLICATE)}, got {on_duplicate!r}"
            )
        self.config = config
        self.endpoint = endpoint
        self.database = database
        self.username = username
        self.password = password
        self.data_dir = data_dir
        self.on_duplicate = on_duplicate

    def build_document_specs(
        self,
        *,
        overwrite: bool = False,
    ) -> tuple[ImportCommandSpec, ...]:
        """Return env-based JSONL commands with stable mapping identities."""
        specs: list[ImportCommandSpec] = []
        for key, mapping in self.config.collections.items():
            if mapping.collection_type != "document":
                continue
            file_path = (
                f"{self.data_dir.rstrip('/')}/"
                f"{mapping.target_collection}.jsonl"
            )
            specs.append(
                ImportCommandSpec(
                    mapping_id=key,
                    kind="document",
                    source_table=mapping.source_table,
                    target_collection=mapping.target_collection,
                    command=self._build_import_command_for_script(
                        mapping.target_collection,
                        file_path,
                        collection_type="document",
                        create_collection_type="document",
                        overwrite=overwrite,
                    ),
                )
            )
        return tuple(specs)

    def build_edge_specs(
        self,
        *,
        overwrite: bool = False,
    ) -> tuple[ImportCommandSpec, ...]:
        """Return env-based JSONL edge commands with mapping metadata."""
        specs: list[ImportCommandSpec] = []
        for edge in self.config.edges:
            file_path = (
                f"{self.data_dir.rstrip('/')}/{edge.edge_collection}.jsonl"
            )
            specs.append(
                ImportCommandSpec(
                    mapping_id=edge.edge_collection,
                    kind="edge",
                    source_table=_source_table_for_ref(
                        self.config,
                        edge.from_collection
                    ),
                    target_collection=edge.edge_collection,
                    edge_collection=edge.edge_collection,
                    from_collection=_target_collection_for_ref(
                        self.config,
                        edge.from_collection
                    ),
                    to_collection=_target_collection_for_ref(
                        self.config,
                        edge.to_collection
                    ),
                    from_fields=tuple(edge.from_fields),
                    to_fields=tuple(edge.to_fields),
                    command=self._build_import_command_for_script(
                        edge.edge_collection,
                        file_path,
                        collection_type="edge",
                        create_collection_type="edge",
                        overwrite=overwrite,
                    ),
                )
            )
        return tuple(specs)

    def _build_import_command(
        self,
        collection_name: str,
        file_path: str,
        collection_type: str = "document",
        create_collection: bool = True,
        create_collection_type: str = "",
        overwrite: bool = False,
        threads: int = 4,
    ) -> str:
        """Build a single arangoimport command string."""
        return _build_jsonl_import_command(
            endpoint=self.endpoint,
            database=self.database,
            username=self.username,
            password=self.password,
            file_path=file_path,
            collection_name=collection_name,
            collection_type=collection_type,
            create_collection=create_collection,
            create_collection_type=create_collection_type,
            on_duplicate=self.on_duplicate,
            overwrite=overwrite,
            threads=threads,
            environment_credentials=False,
        )

    def _build_import_command_for_script(
        self,
        collection_name: str,
        file_path: str,
        collection_type: str = "document",
        create_collection: bool = True,
        create_collection_type: str = "",
        overwrite: bool = False,
        threads: int = 4,
    ) -> str:
        return _build_jsonl_import_command(
            endpoint=self.endpoint,
            database=self.database,
            username=self.username,
            password=self.password,
            file_path=file_path,
            collection_name=collection_name,
            collection_type=collection_type,
            create_collection=create_collection,
            create_collection_type=create_collection_type,
            on_duplicate=self.on_duplicate,
            overwrite=overwrite,
            threads=threads,
            environment_credentials=True,
        )

    def generate_document_commands(self) -> list[str]:
        """Generate import commands for all document collections."""
        base = Path(self.data_dir)
        commands: list[str] = []
        for mapping in self.config.collections.values():
            if mapping.collection_type != "document":
                continue
            file_path = str(base / f"{mapping.target_collection}.jsonl")
            commands.append(
                self._build_import_command(
                    mapping.target_collection,
                    file_path,
                    collection_type="document",
                    create_collection_type="document",
                )
            )
        return commands

    def generate_edge_commands(self) -> list[str]:
        """Generate import commands for all edge collections."""
        base = Path(self.data_dir)
        commands: list[str] = []
        for edge in self.config.edges:
            file_path = str(base / f"{edge.edge_collection}.jsonl")
            commands.append(
                self._build_import_command(
                    edge.edge_collection,
                    file_path,
                    collection_type="edge",
                    create_collection_type="edge",
                )
            )
        return commands

    def render_script(
        self,
        *,
        overwrite_on_initial: bool = True,
        graph_name: str | None = None,
        secret_safe: bool = False,
        generated_at: str | None = None,
    ) -> str:
        """Render a bash script without writing it to disk.

        ``secret_safe`` removes the password default while retaining the
        ``$ARANGO_PASSWORD`` runtime reference used by every command.
        """
        documents = self.build_document_specs(overwrite=overwrite_on_initial)
        edges = self.build_edge_specs(overwrite=overwrite_on_initial)
        return self._render_script(
            documents,
            edges,
            graph_name=graph_name,
            secret_safe=secret_safe,
            generated_at=generated_at,
        )

    def _render_script(
        self,
        documents: tuple[ImportCommandSpec, ...],
        edges: tuple[ImportCommandSpec, ...],
        *,
        graph_name: str | None,
        secret_safe: bool,
        generated_at: str | None = None,
    ) -> str:
        """Render a JSONL script from already-built command specifications."""
        generated_at = generated_at or datetime.now(timezone.utc).isoformat()
        lines: list[str] = [
            "#!/usr/bin/env bash",
            "# Generated by R2G ArangoImportGenerator",
            f"# {generated_at}",
            "set -euo pipefail",
            IMPORT_GUARD,
            "",
            *_script_environment(
                endpoint=self.endpoint,
                database=self.database,
                username=self.username,
                password=self.password,
                secret_safe=secret_safe,
            ),
            "",
        ]
        for spec in documents:
            lines.append(
                f"# {spec.source_table} -> {spec.target_collection}"
            )
            lines.append(
                "echo "
                + shlex.quote(
                    f"Importing document collection {spec.target_collection}..."
                )
            )
            lines.append(_guarded(spec.command, spec.target_collection))
            lines.append("")
        for spec in edges:
            lines.append(
                f"# {spec.source_table} -> {spec.edge_collection} "
                f"({', '.join(spec.from_fields)} -> {spec.to_collection})"
            )
            lines.append(
                "echo "
                + shlex.quote(
                    f"Importing edge collection {spec.edge_collection}..."
                )
            )
            lines.append(_guarded(spec.command, spec.target_collection))
            lines.append("")
        if graph_name:
            lines.extend(["# === Named graph ===", ""])
            lines.extend(
                _build_graph_creation_arangosh(self.config, graph_name)
            )
            lines.append("")
        return "\n".join(lines).rstrip() + "\n"

    def build_plan(
        self,
        *,
        graph_name: str = "r2g_graph",
        overwrite_on_initial: bool = True,
        secret_safe: bool = True,
    ) -> ImportPlan:
        """Build a structured JSONL import artifact for CLI/UI consumers."""
        if self.config.graph_layout == "lpg":
            raise ValueError(
                "arangoimport preview does not support LPG layout; use the "
                "Studio streaming Load path for this mapping"
            )
        documents = self.build_document_specs(overwrite=overwrite_on_initial)
        edges = self.build_edge_specs(overwrite=overwrite_on_initial)
        return ImportPlan(
            mode="jsonl",
            data_dir=self.data_dir,
            script=self._render_script(
                documents,
                edges,
                graph_name=graph_name,
                secret_safe=secret_safe,
            ),
            documents=documents,
            edges=edges,
            graph_script=_render_graph_shell(
                self.config,
                graph_name,
                endpoint=self.endpoint,
                database=self.database,
                username=self.username,
                password=self.password,
                secret_safe=secret_safe,
            ),
        )

    def generate_script(
        self, output_path: str, overwrite_on_initial: bool = True
    ) -> str:
        """Render, write, and make executable an arangoimport bash script."""
        content = self.render_script(
            overwrite_on_initial=overwrite_on_initial,
            secret_safe=False,
        )
        out = Path(output_path)
        try:
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(content, encoding="utf-8")
            os.chmod(out, 0o755)
        except OSError as e:
            logger.exception(
                "failed_to_write_arangoimport_script",
                output_path=str(out),
                error=str(e),
            )
            raise
        return content

    def generate_create_graph_aql(self, graph_name: str = "r2g_graph") -> str:
        """Generate arangosh JavaScript to create a named graph from edge definitions."""
        lines = [
            "// Generated for arangosh: create named graph from R2G edge definitions",
            'var graph = require("@arangodb/general-graph");',
            "var edgeDefinitions = [",
        ]
        rel_parts: list[str] = []
        for edge in self.config.edges:
            ec = json.dumps(edge.edge_collection)
            fc = json.dumps(
                _target_collection_for_ref(self.config, edge.from_collection)
            )
            tc = json.dumps(
                _target_collection_for_ref(self.config, edge.to_collection)
            )
            rel_parts.append(f"  graph._relation({ec}, [{fc}], [{tc}])")
        lines.append(",\n".join(rel_parts))
        lines.append("];")
        lines.append(f"graph._create({json.dumps(graph_name)}, edgeDefinitions);")
        return "\n".join(lines) + "\n"


_PG_TO_ARANGO_DATATYPE = {
    "integer": "number",
    "float": "number",
    "boolean": "boolean",
}


class CsvDirectUnsupportedError(ValueError):
    """A mapping uses a feature the CSV-direct import cannot reproduce."""


class CsvImportGenerator:
    """Generates arangoimport commands that work directly on PG CSV dumps.

    Uses --type csv with --translate, --datatype, --from-collection-prefix,
    --to-collection-prefix, and --remove-attribute to let arangoimport handle
    key remapping and edge projection natively -- no intermediate JSONL needed.
    """

    def __init__(
        self,
        config: MappingConfig,
        schema: Schema,
        endpoint: str = "http://localhost:8529",
        database: str = "_system",
        username: str = "root",
        password: str = "",
        data_dir: str = "./dumps",
        on_duplicate: str = "replace",
    ) -> None:
        if on_duplicate not in _VALID_ON_DUPLICATE:
            raise ValueError(
                f"on_duplicate must be one of {sorted(_VALID_ON_DUPLICATE)}, got {on_duplicate!r}"
            )
        self.config = config
        self.schema = schema
        self.endpoint = endpoint
        self.database = database
        self.username = username
        self.password = password
        self.data_dir = data_dir
        self.on_duplicate = on_duplicate

    def _datatype_flags(
        self, table_name: str, exclude_cols: set[str] | None = None,
    ) -> list[str]:
        """Build --datatype flags from the schema's column types.

        Columns in *exclude_cols* are skipped -- use this for PK/FK columns
        that will become _key, _from, or _to (always strings in ArangoDB).

        Nullable columns are also skipped for number/boolean because
        arangoimport cannot coerce an empty CSV value to those types.

        .. warning::

           That nullable skip is a partial guard against a silent data-loss
           bug, not a complete one. arangoimport substitutes the raw CSV token
           into its JSON request body, so a token that is not a legal JSON
           value for the requested type makes the body malformed. It then
           fails to parse its own payload, **drops that row**, and **exits 0**
           -- so a generated script running under ``set -euo pipefail``
           continues and reports success with a row missing.

           Verified against ArangoDB 3.12.4 and 3.12.11. An empty field in a
           column typed ``number`` produces ``["2",]`` and loses the row; so
           does a zero-padded value such as ``00042``, which is not a legal
           JSON number. Three rows in, two stored, exit status 0 in both cases.

           The exposure here is a column the schema declares NOT NULL whose
           dump nonetheless contains an empty field, or whose declared type
           does not match the data. arangoimport logs only a WARNING for it.
           The generated script's ``run_import`` wrapper (``IMPORT_GUARD``)
           works around this: it reads the printed summary and fails the
           script, so the load stops instead of reporting success. It cannot
           recover the row; the data or the declared type has to be fixed.

           An alternative loader was measured on the same input (arangox at
           d060f6a): it keeps the row, storing an empty value as ``null`` with
           no error even when the column is NOT NULL, and stores ``00042`` as
           ``42``. It rejects a genuinely unconvertible value such as ``abc``
           with an error naming the column, and loads nothing.
        """
        if table_name not in self.schema.tables:
            return []
        skip = exclude_cols or set()
        flags: list[str] = []
        for col in self.schema.tables[table_name].columns:
            if col.name in skip:
                continue
            json_type = pg_type_to_json_type(col.data_type)
            arango_dt = _PG_TO_ARANGO_DATATYPE.get(json_type)
            if arango_dt and not (col.is_nullable and arango_dt in ("number", "boolean")):
                flags.extend(["--datatype", shlex.quote(f"{col.name}={arango_dt}")])
        return flags

    def _fk_is_nullable(self, table_name: str, fk_cols: tuple[str, ...]) -> bool:
        """Whether a NULL is legal in any of an edge's FK columns.

        Decides the guard policy for that edge import: only a nullable FK can
        legitimately produce rejected (bare-prefix) edges.
        """
        table = self.schema.tables.get(table_name)
        if table is None:
            return False
        return any(
            col.is_nullable for col in table.columns if col.name in fk_cols
        )

    def _csv_file_path(self, table_name: str) -> str:
        return f"{self.data_dir.rstrip('/')}/{table_name}.csv"

    def _server_flags_for_script(self) -> list[str]:
        return [
            "--server.endpoint", '"$ARANGO_ENDPOINT"',
            "--server.database", '"$ARANGO_DB"',
            "--server.username", '"$ARANGO_USER"',
            "--server.password", PASSWORD_ENV_REF,
        ]

    def _build_doc_command(
        self,
        table_name: str,
        target_collection: str,
        *,
        field_mappings: dict[str, str] | None = None,
        overwrite: bool = True,
    ) -> str:
        """Build arangoimport command for a document collection from a CSV dump."""
        table = self.schema.tables.get(table_name)
        pk_cols = table.primary_key if table else []
        renames = {
            source: target
            for source, target in (field_mappings or {}).items()
            if source != target
        }

        parts: list[str] = [
            "arangoimport",
            *self._server_flags_for_script(),
            "--file", shlex.quote(self._csv_file_path(table_name)),
            "--type", "csv",
            "--collection", shlex.quote(target_collection),
            "--create-collection", "true",
            "--create-collection-type", "document",
            "--on-duplicate", shlex.quote(self.on_duplicate),
        ]

        # PK → _key: force string so auto-detection doesn't treat numeric IDs as
        # numbers. When a single PK is also mapped to a regular target property,
        # merge it into _key instead of translating it away; arangoimport applies
        # the merge before header translations and therefore preserves both.
        if len(pk_cols) == 1:
            if pk_cols[0] in renames:
                parts.extend([
                    "--merge-attributes",
                    shlex.quote(f"_key=[{pk_cols[0]}]"),
                ])
            else:
                parts.extend(["--translate", shlex.quote(f"{pk_cols[0]}=_key")])
            parts.extend(["--datatype", shlex.quote(f"{pk_cols[0]}=string")])
        elif len(pk_cols) > 1:
            merge_expr = "[" + "]_[".join(pk_cols) + "]"
            parts.extend([
                "--merge-attributes", shlex.quote(f"_key={merge_expr}"),
            ])
            for pk in pk_cols:
                parts.extend(["--datatype", shlex.quote(f"{pk}=string")])

        for source, target in renames.items():
            parts.extend([
                "--translate",
                shlex.quote(f"{source}={target}"),
            ])

        parts.extend(self._datatype_flags(table_name, exclude_cols=set(pk_cols)))
        parts.extend(["--overwrite", "true" if overwrite else "false"])
        return " \\\n    ".join(parts)

    def _build_edge_command(
        self,
        table_name: str,
        edge_collection: str,
        from_collection: str,
        to_collection: str,
        from_fields: list[str],
        *,
        overwrite: bool = True,
    ) -> str:
        """Build arangoimport command for an edge collection from a CSV dump.

        Uses the source table's PK as _from and the FK column(s) as _to,
        with collection prefixes. All non-structural columns are removed
        so the edge is a clean relationship.

        For composite FK edges, ``--merge-attributes`` is used to construct
        ``_to`` from multiple columns.
        """
        table = self.schema.tables.get(table_name)
        pk_cols = table.primary_key if table else []
        all_cols = [c.name for c in table.columns] if table else []

        parts: list[str] = [
            "arangoimport",
            *self._server_flags_for_script(),
            "--file", shlex.quote(self._csv_file_path(table_name)),
            "--type", "csv",
            "--collection", shlex.quote(edge_collection),
            "--create-collection", "true",
            "--create-collection-type", "edge",
            "--on-duplicate", shlex.quote(self.on_duplicate),
        ]

        # PK → _from: force string so _id references resolve correctly
        if len(pk_cols) == 1:
            parts.extend(["--translate", shlex.quote(f"{pk_cols[0]}=_from")])
            parts.extend(["--datatype", shlex.quote(f"{pk_cols[0]}=string")])
        elif len(pk_cols) > 1:
            merge_expr = "[" + "]_[".join(pk_cols) + "]"
            parts.extend([
                "--merge-attributes", shlex.quote(f"_from={merge_expr}"),
            ])
            for pk in pk_cols:
                parts.extend(["--datatype", shlex.quote(f"{pk}=string")])

        # FK → _to: single column uses --translate, composite uses --merge-attributes
        if len(from_fields) == 1:
            parts.extend(["--translate", shlex.quote(f"{from_fields[0]}=_to")])
            parts.extend(["--datatype", shlex.quote(f"{from_fields[0]}=string")])
        else:
            merge_expr = "[" + "]_[".join(from_fields) + "]"
            parts.extend([
                "--merge-attributes", shlex.quote(f"_to={merge_expr}"),
            ])
            for ff in from_fields:
                parts.extend(["--datatype", shlex.quote(f"{ff}=string")])

        parts.extend([
            "--from-collection-prefix", shlex.quote(f"{from_collection}/"),
            "--to-collection-prefix", shlex.quote(f"{to_collection}/"),
        ])

        keep: set[str] = {"_from", "_to"}
        keep.update(from_fields)
        if len(pk_cols) == 1:
            keep.add(pk_cols[0])
        for col_name in all_cols:
            if col_name not in keep and col_name not in pk_cols:
                parts.extend(["--remove-attribute", shlex.quote(col_name)])

        parts.extend(["--overwrite", "true" if overwrite else "false"])
        return " \\\n    ".join(parts)

    def build_document_specs(
        self,
        *,
        overwrite: bool = True,
    ) -> tuple[ImportCommandSpec, ...]:
        """Return direct-CSV document commands with stable identities."""
        specs: list[ImportCommandSpec] = []
        for key, mapping in self.config.collections.items():
            if mapping.collection_type != "document":
                continue
            specs.append(
                ImportCommandSpec(
                    mapping_id=key,
                    kind="document",
                    source_table=mapping.source_table,
                    target_collection=mapping.target_collection,
                    command=self._build_doc_command(
                        mapping.source_table,
                        mapping.target_collection,
                        field_mappings=mapping.field_mappings,
                        overwrite=overwrite,
                    ),
                )
            )
        return tuple(specs)

    def build_edge_specs(
        self,
        *,
        overwrite: bool = True,
    ) -> tuple[ImportCommandSpec, ...]:
        """Return direct-CSV edge commands with stable mapping metadata."""
        specs: list[ImportCommandSpec] = []
        for edge in self.config.edges:
            source_table = _source_table_for_ref(
                self.config,
                edge.from_collection
            )
            from_collection = _target_collection_for_ref(
                self.config,
                edge.from_collection
            )
            to_collection = _target_collection_for_ref(
                self.config,
                edge.to_collection
            )
            specs.append(
                ImportCommandSpec(
                    mapping_id=edge.edge_collection,
                    kind="edge",
                    source_table=source_table,
                    target_collection=edge.edge_collection,
                    edge_collection=edge.edge_collection,
                    from_collection=from_collection,
                    to_collection=to_collection,
                    from_fields=tuple(edge.from_fields),
                    to_fields=tuple(edge.to_fields),
                    command=self._build_edge_command(
                        table_name=source_table,
                        edge_collection=edge.edge_collection,
                        from_collection=from_collection,
                        to_collection=to_collection,
                        from_fields=edge.from_fields,
                        overwrite=overwrite,
                    ),
                )
            )
        return tuple(specs)

    def _refuse_unfaithful_mapping(self) -> None:
        """Refuse a mapping this CSV-direct script cannot reproduce faithfully.

        ``arangoimport --type csv`` loads every CSV column as it stands, so a
        field expression (a mask included), an include or exclude filter, or a
        mapping-level type override would be silently ignored: a masked or
        excluded column would load in clear, and a computed field would be
        missing. Renames via ``field_mappings`` are reproduced with
        ``--translate``, and an identity expression that leaves a column as it
        is changes nothing, so neither is refused.
        """
        problems: list[str] = []
        for key, mapping in self.config.collections.items():
            features: list[str] = []
            changed = sorted(
                fx.target
                for fx in mapping.field_expressions
                if not (fx.is_identity and fx.sources in ([], [fx.target]))
            )
            if changed:
                features.append(f"field expressions ({', '.join(changed)})")
            if mapping.include_fields is not None:
                features.append("an include-field filter")
            if mapping.exclude_fields:
                features.append(
                    f"excluded fields ({', '.join(sorted(mapping.exclude_fields))})"
                )
            if features:
                problems.append(f"{key}: {'; '.join(features)}")
        if self.config.type_overrides:
            problems.append(
                "mapping-level type overrides ("
                + ", ".join(sorted(self.config.type_overrides))
                + ")"
            )
        if problems:
            raise CsvDirectUnsupportedError(
                "CSV-direct import cannot reproduce this mapping, and would load "
                "these columns as they stand in the CSV: "
                + " | ".join(problems)
                + ". Use JSONL mode instead (transform-all, then generate-import)."
            )

    def _build_graph_creation_arangosh(self, graph_name: str) -> list[str]:
        """Build arangosh command to create a named graph."""
        return _build_graph_creation_arangosh(self.config, graph_name)

    def render_script(
        self,
        *,
        overwrite_on_initial: bool = True,
        graph_name: str | None = None,
        secret_safe: bool = False,
        generated_at: str | None = None,
    ) -> str:
        """Render direct-CSV commands without writing a script."""
        self._refuse_unfaithful_mapping()
        documents = self.build_document_specs(overwrite=overwrite_on_initial)
        edges = self.build_edge_specs(overwrite=overwrite_on_initial)
        return self._render_script(
            documents,
            edges,
            graph_name=graph_name,
            secret_safe=secret_safe,
            generated_at=generated_at,
        )

    def _render_script(
        self,
        documents: tuple[ImportCommandSpec, ...],
        edges: tuple[ImportCommandSpec, ...],
        *,
        graph_name: str | None,
        secret_safe: bool,
        generated_at: str | None = None,
    ) -> str:
        """Render a CSV script from already-built command specifications."""
        generated_at = generated_at or datetime.now(timezone.utc).isoformat()
        lines: list[str] = [
            "#!/usr/bin/env bash",
            "# Generated by R2G CsvImportGenerator",
            f"# {generated_at}",
            "# Imports PG CSV dumps directly via arangoimport --type csv",
            "# No intermediate JSONL transformation required",
            "set -euo pipefail",
            IMPORT_GUARD,
            "",
            *_script_environment(
                endpoint=self.endpoint,
                database=self.database,
                username=self.username,
                password=self.password,
                secret_safe=secret_safe,
            ),
            "",
        ]
        lines.extend(["", "# === Document collections ===", ""])
        for spec in documents:
            msg = (
                f"Importing document collection {spec.target_collection} "
                f"from {spec.source_table}.csv..."
            )
            lines.append("echo " + shlex.quote(msg))
            lines.append(_guarded(spec.command, spec.target_collection))
            lines.append("")

        lines.append("# === Edge collections ===")
        lines.append("")
        for spec in edges:
            lines.append(
                "echo " + shlex.quote(
                    f"Importing edge collection {spec.edge_collection} "
                    f"({spec.from_collection} -> {spec.to_collection}) "
                    f"from {spec.source_table}.csv..."
                )
            )
            lines.append(
                _guarded(
                    spec.command,
                    spec.target_collection,
                    null_fk_ok=self._fk_is_nullable(
                        spec.source_table, spec.from_fields
                    ),
                )
            )
            lines.append("")
        if graph_name:
            lines.append("# === Named graph ===")
            lines.append("")
            lines.extend(self._build_graph_creation_arangosh(graph_name))
            lines.append("")
        return "\n".join(lines).rstrip() + "\n"

    def build_plan(
        self,
        *,
        graph_name: str = "r2g_graph",
        overwrite_on_initial: bool = True,
        secret_safe: bool = True,
    ) -> ImportPlan:
        """Build a structured direct-CSV artifact for CLI/UI consumers."""
        if self.config.graph_layout == "lpg":
            raise ValueError(
                "arangoimport preview does not support LPG layout; use the "
                "Studio streaming Load path for this mapping"
            )
        self._refuse_unfaithful_mapping()
        documents = self.build_document_specs(overwrite=overwrite_on_initial)
        edges = self.build_edge_specs(overwrite=overwrite_on_initial)
        return ImportPlan(
            mode="csv",
            data_dir=self.data_dir,
            script=self._render_script(
                documents,
                edges,
                graph_name=graph_name,
                secret_safe=secret_safe,
            ),
            documents=documents,
            edges=edges,
            graph_script=_render_graph_shell(
                self.config,
                graph_name,
                endpoint=self.endpoint,
                database=self.database,
                username=self.username,
                password=self.password,
                secret_safe=secret_safe,
            ),
        )

    def generate_csv_script(
        self,
        output_path: str,
        overwrite_on_initial: bool = True,
        graph_name: str | None = None,
    ) -> str:
        """Render, write, and make executable a direct-CSV import script."""
        content = self.render_script(
            overwrite_on_initial=overwrite_on_initial,
            graph_name=graph_name,
            secret_safe=False,
        )
        out = Path(output_path)
        try:
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(content, encoding="utf-8")
            os.chmod(out, 0o755)
        except OSError as e:
            logger.exception(
                "failed_to_write_csv_import_script",
                output_path=str(out),
                error=str(e),
            )
            raise
        return content
