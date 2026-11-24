"""At-rest encryption for credentials stored in the R2G catalog.

Implements an opinionated, minimal envelope:

- A symmetric key is loaded, in this order:
    1. ``R2G_SECRET_KEY`` env var (a urlsafe base64 Fernet key, 44 chars)
    2. ``<catalog_dir>/secret.key`` on disk (0600)
    3. auto-generated and written to that path the first time we need it
- Sensitive catalog fields (``SourceConfig.connection_string``,
  ``TargetConfig.password``) are stored as ``enc:v1:<base64>`` and
  decrypted lazily when the catalog is read. Plaintext values are still
  accepted on read for backwards compatibility, but any catalog write
  re-emits them encrypted.
- ``redact_for_display`` masks everything but a trailing hint for API
  responses and logs.

The encryption is strictly at-rest protection against someone who gets
a copy of ``catalog.json`` (e.g. via a disk snapshot or an accidental
git commit). Anyone with access to the key file or to the running
process still has the plaintext.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote, quote_plus, unquote, urlencode, urlparse, urlsplit, urlunparse

from cryptography.fernet import Fernet, InvalidToken

from r2g.log import get_logger

logger = get_logger(__name__)

ENC_PREFIX = "enc:v1:"
SECRET_ENV = "R2G_SECRET_KEY"
SECRET_FILENAME = "secret.key"


class SecretKeyError(Exception):
    """Raised when the secret key is malformed or cannot be loaded/created."""


def _load_key_from_env() -> bytes | None:
    raw = os.environ.get(SECRET_ENV)
    if not raw:
        return None
    key = raw.strip().encode("ascii")
    try:
        Fernet(key)
    except (ValueError, TypeError) as err:  # noqa: F841
        raise SecretKeyError(
            f"{SECRET_ENV} is set but is not a valid Fernet key "
            "(must be a 32-byte urlsafe base64 string)"
        ) from err
    return key


def _read_or_create_key_file(path: Path) -> bytes:
    """Read ``path`` or create it with a fresh Fernet key (mode 0600)."""

    if path.exists():
        raw = path.read_bytes().strip()
        try:
            Fernet(raw)
        except (ValueError, TypeError) as err:
            raise SecretKeyError(f"secret key at {path} is not a valid Fernet key") from err
        return raw

    path.parent.mkdir(parents=True, exist_ok=True)
    key = Fernet.generate_key()
    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(fd, key)
    finally:
        os.close(fd)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass
    logger.info("secret_key_created", path=str(path))
    return key


def load_secret_key(catalog_dir: Path) -> bytes:
    """Load the active secret key, creating one under ``catalog_dir`` if needed."""

    env_key = _load_key_from_env()
    if env_key is not None:
        return env_key
    return _read_or_create_key_file(catalog_dir / SECRET_FILENAME)


class CredentialCipher:
    """Thin wrapper around Fernet that tags its ciphertexts with ``enc:v1:``."""

    def __init__(self, key: bytes) -> None:
        self._fernet = Fernet(key)

    def encrypt(self, plaintext: str) -> str:
        if plaintext is None or plaintext == "":
            return ""
        if plaintext.startswith(ENC_PREFIX):
            return plaintext
        token = self._fernet.encrypt(plaintext.encode("utf-8")).decode("ascii")
        return f"{ENC_PREFIX}{token}"

    def decrypt(self, value: str) -> str:
        if value is None or value == "":
            return ""
        if not value.startswith(ENC_PREFIX):
            return value
        token = value[len(ENC_PREFIX):].encode("ascii")
        try:
            return self._fernet.decrypt(token).decode("utf-8")
        except InvalidToken as err:
            raise SecretKeyError(
                "failed to decrypt secret; the R2G_SECRET_KEY / secret.key "
                "does not match the one that produced this ciphertext"
            ) from err

    def is_encrypted(self, value: str) -> bool:
        return isinstance(value, str) and value.startswith(ENC_PREFIX)


def redact_for_display(value: str, keep: int = 3) -> str:
    """Return a short hint-only form of a secret suitable for UI display.

    Produces an empty string for empty input, the placeholder ``"***"``
    for very short values, and ``"***" + last ``keep`` characters`` for
    anything longer. This is intentionally one-way.
    """

    if not value:
        return ""
    if value.startswith(ENC_PREFIX):
        return "***"
    if len(value) <= keep:
        return "***"
    return "***" + value[-keep:]


# Matches credentials embedded in a DSN anywhere within a larger string, e.g.
# "could not connect to postgresql://user:secret@host:5432/db" in an error message.
_DSN_CRED_RE = re.compile(r"([A-Za-z][\w+.\-]*://)[^\s:/@]+:[^\s:/@]+@")
_SENSITIVE_DSN_QUERY_KEYS = frozenset(
    {
        "access_token",
        "oauth_token",
        "password",
        "private_key",
        "private_key_file",
        "private_key_file_pwd",
        "token",
    }
)


def scrub_dsn_credentials(text: str) -> str:
    """Replace ``scheme://user:pass@`` credential pairs anywhere in ``text``.

    Used to sanitize free-form strings (log fields, error messages) that may
    have a connection string embedded in them. Leaves the rest of the text
    intact so the message is still useful.
    """

    if not text:
        return text
    return _DSN_CRED_RE.sub(r"\1***:***@", text)


# ── Connection errors (#118) ─────────────────────────────────────────────────
#
# A driver's error text goes into r2g's message, and from there into logs, CLI output, the
# UI and MCP responses, and any printed traceback. Whether a driver repeats the password or
# token it was given depends on the driver and the failure (Snowflake's was caught doing
# it), so connectors treat driver text as untrusted. Mirrors relational-schema-analyzer
# 0.16's ``redact`` module (same names), so r2g can import that instead once it requires
# RSA 0.16 or later.

#: Parameter names whose value is a credential, matched case-insensitively.
SENSITIVE_CONNECT_KEYS = _SENSITIVE_DSN_QUERY_KEYS | frozenset(
    {
        "passwd",
        "pwd",
        "passcode",
        "motherduck_token",
        "private_key_passphrase",
        "passphrase",
        "client_secret",
        "secret",
        "api_key",
    }
)

#: What a masked credential reads as.
SECRET_MASK = "***"

# libpq's key=value form: ``host=db password=s3cret`` or ``password='s3 cret'``.
_KEYWORD_VALUE = re.compile(r"(\w+)\s*=\s*('(?:[^'\\]|\\.)*'|\S+)")


def _sensitive_key(name: str) -> bool:
    return name.lower() in SENSITIVE_CONNECT_KEYS


def _with_encodings(value: str) -> list[str]:
    """The value as given, decoded and percent-encoded: a driver may echo any form."""
    if not value:
        return []
    decoded = unquote(value)
    return [value, decoded, quote(decoded, safe=""), quote_plus(decoded, safe="")]


def secrets_in(*sources: Any) -> list[str]:
    """Every credential in ``sources``, longest first.

    A source is a connection string (URL or libpq ``key=value`` form), a mapping of
    connection parameters, or ``None``. Anything else is ignored.
    """
    found: set[str] = set()
    for source in sources:
        if isinstance(source, Mapping):
            for key, value in source.items():
                if isinstance(key, str) and _sensitive_key(key) and value not in (None, ""):
                    found.update(_with_encodings(str(value)))
        elif isinstance(source, str) and source:
            found.update(_secrets_in_string(source))
    return sorted((s for s in found if s), key=len, reverse=True)


def _secrets_in_string(text: str) -> set[str]:
    found: set[str] = set()
    if "://" in text:
        try:
            password = urlsplit(text).password
        except ValueError:
            password = None  # a malformed URL still gets the scans below
        if password:
            found.update(_with_encodings(password))
    if "?" in text:  # a query string, with or without a scheme
        for name, value in parse_qsl(text.split("?", 1)[1], keep_blank_values=False):
            if _sensitive_key(name):
                found.update(_with_encodings(value))
    for name, value in _KEYWORD_VALUE.findall(text):
        if _sensitive_key(name):
            if len(value) >= 2 and value[0] == value[-1] == "'":
                value = value[1:-1].replace("\\'", "'").replace("\\\\", "\\")
            # Unquoted, the value may run into a query string's next parameter: mask both.
            found.update(_with_encodings(value))
            found.update(_with_encodings(value.split("&", 1)[0]))
    return found


def redact(text: str, secrets: Iterable[str]) -> str:
    """``text`` with every secret masked (longest first) and any ``user:pass@`` scrubbed."""
    for secret in sorted(set(secrets), key=len, reverse=True):
        if secret:
            text = text.replace(secret, SECRET_MASK)
    return scrub_dsn_credentials(text)


def connection_error(what: str, err: BaseException, secrets: Iterable[str]) -> RuntimeError:
    """The exception to raise **``from None``** when a connection fails.

    ``"<what>: <DriverError>: <driver text, secrets masked>"``. ``from None`` matters: a
    chained ``__cause__`` is the driver's original exception, and any traceback prints it
    unmasked.
    """
    return RuntimeError(f"{what}: {type(err).__name__}: {redact(str(err), secrets)}")


def redact_source_dump(dump: dict) -> dict:
    """Return a copy of a serialized ``SourceConfig`` with its DSN masked."""
    out = dict(dump)
    out["connection_string"] = redact_connection_string(out.get("connection_string") or "")
    return out


def redact_target_dump(dump: dict) -> dict:
    """Return a copy of a serialized ``TargetConfig`` with its password masked."""
    out = dict(dump)
    out["password"] = redact_for_display(out.get("password") or "")
    return out


def redact_connection_string(url: str) -> str:
    """Redact credentials and sensitive query parameters in a DSN.

    Leaves host, port, database, and username visible so operators can
    still tell sources apart. Snowflake key paths/passphrases and common token
    fields are masked even when the netloc itself has no password.
    """

    if not url:
        return ""
    try:
        parsed = urlparse(url)
    except ValueError:
        return redact_for_display(url, keep=6)
    if not parsed.scheme:
        return redact_for_display(url, keep=6)
    query = urlencode(
        [
            (
                key,
                "***"
                if key.lower() in _SENSITIVE_DSN_QUERY_KEYS
                else value,
            )
            for key, value in parse_qsl(
                parsed.query, keep_blank_values=True
            )
        ],
        doseq=True,
    )
    new_netloc = parsed.netloc
    if "@" in parsed.netloc:
        user_info, _, host = parsed.netloc.rpartition("@")
        if ":" in user_info:
            username, _ = user_info.split(":", 1)
            new_netloc = f"{username}:***@{host}"
    elif not parsed.netloc:
        return redact_for_display(url, keep=6)
    return urlunparse(
        parsed._replace(netloc=new_netloc, query=query)
    )
