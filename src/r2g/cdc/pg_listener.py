"""PostgreSQL logical replication listener.

Manages replication slots, polls for changes via
``pg_logical_slot_get_changes``, parses them through the appropriate
output plugin parser, and feeds the resulting ChangeEvents into a
CDCHandler.

Two output plugins are supported:

- ``test_decoding`` -- built-in to PostgreSQL, no extensions required.
- ``wal2json`` -- requires the wal2json extension to be installed.

The listener runs a polling loop that sleeps when no changes are
available and processes changes in transaction-grouped batches when
they arrive.
"""

from __future__ import annotations

import signal
import time
from typing import Any

import psycopg
from psycopg.rows import tuple_row

from r2g.cdc.handler import CDCHandler
from r2g.cdc.models import ChangeEvent
from r2g.cdc.parsers import TestDecodingParser, Wal2JsonParser
from r2g.log import get_logger
from r2g.security import connection_error, secrets_in

logger = get_logger(__name__)

SUPPORTED_PLUGINS = ("test_decoding", "wal2json")


class PGReplicationListener:
    """Polls a PostgreSQL logical replication slot for change events.

    Usage::

        listener = PGReplicationListener(pg_conn, handler, ...)
        listener.setup()       # create slot if needed
        listener.run()         # blocks until stop() or SIGINT
        listener.teardown()    # optional cleanup
    """

    def __init__(
        self,
        pg_conn_string: str,
        handler: CDCHandler,
        slot_name: str = "r2g_slot",
        plugin: str = "test_decoding",
        poll_interval: float = 1.0,
        batch_size: int = 1000,
    ) -> None:
        if plugin not in SUPPORTED_PLUGINS:
            raise ValueError(
                f"Unsupported plugin '{plugin}'; choose from {SUPPORTED_PLUGINS}"
            )
        self.pg_conn_string = pg_conn_string
        self.handler = handler
        self.slot_name = slot_name
        self.plugin = plugin
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self._running = False
        self._conn: psycopg.Connection | None = None

        self._td_parser = TestDecodingParser()
        self._wj_parser = Wal2JsonParser()

    # ------------------------------------------------------------------
    # Slot management
    # ------------------------------------------------------------------

    def _connect(self) -> psycopg.Connection:
        if self._conn is None or self._conn.closed:
            try:
                self._conn = psycopg.connect(
                    self.pg_conn_string,
                    autocommit=True,
                    row_factory=tuple_row,
                )
            except Exception as err:
                raise connection_error(
                    "Failed to connect to PostgreSQL", err, secrets_in(self.pg_conn_string)
                ) from None
        return self._conn

    def slot_exists(self) -> bool:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s",
                (self.slot_name,),
            )
            return cur.fetchone() is not None

    def create_slot(self) -> bool:
        """Create the replication slot.  Returns True if newly created."""
        if self.slot_exists():
            logger.info("replication_slot_exists", slot=self.slot_name)
            return False
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_create_logical_replication_slot(%s, %s)",
                (self.slot_name, self.plugin),
            )
        logger.info(
            "replication_slot_created",
            slot=self.slot_name,
            plugin=self.plugin,
        )
        return True

    def drop_slot(self) -> bool:
        """Drop the replication slot.  Returns True if dropped."""
        if not self.slot_exists():
            logger.info("replication_slot_not_found", slot=self.slot_name)
            return False
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_drop_replication_slot(%s)",
                (self.slot_name,),
            )
        logger.info("replication_slot_dropped", slot=self.slot_name)
        return True

    def slot_status(self) -> dict[str, Any] | None:
        """Return metadata about the replication slot, or None."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT slot_name, plugin, slot_type, active,
                       restart_lsn, confirmed_flush_lsn
                FROM pg_replication_slots
                WHERE slot_name = %s
                """,
                (self.slot_name,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return {
                "slot_name": row[0],
                "plugin": row[1],
                "slot_type": row[2],
                "active": row[3],
                "restart_lsn": str(row[4]) if row[4] else None,
                "confirmed_flush_lsn": str(row[5]) if row[5] else None,
            }

    # ------------------------------------------------------------------
    # Setup / teardown helpers
    # ------------------------------------------------------------------

    def setup(self) -> dict[str, Any]:
        """Create the slot if it doesn't exist.  Returns slot status."""
        self.create_slot()
        status = self.slot_status()
        logger.info("cdc_setup_complete", status=status)
        return status or {}

    def teardown(self, drop_slot: bool = True) -> None:
        """Drop the slot and close the connection."""
        if drop_slot:
            self.drop_slot()
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Polling
    # ------------------------------------------------------------------

    def poll_once(self) -> list[ChangeEvent]:
        """Consume pending changes and return parsed ChangeEvents."""
        conn = self._connect()
        events: list[ChangeEvent] = []

        with conn.cursor() as cur:
            cur.execute(
                "SELECT lsn, xid, data "
                "FROM pg_logical_slot_get_changes(%s, NULL, %s)",
                (self.slot_name, self.batch_size),
            )
            rows = cur.fetchall()

        for lsn_raw, xid, data in rows:
            lsn_str = str(lsn_raw) if lsn_raw is not None else None

            if self.plugin == "test_decoding":
                evt = self._td_parser.parse_message(data, lsn=lsn_str, xid=xid)
                if evt is not None:
                    events.append(evt)
            else:
                events.extend(self._wj_parser.parse_message(data, lsn=lsn_str))

        return events

    # ------------------------------------------------------------------
    # Continuous loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start the continuous polling loop.  Blocks until stopped.

        Handles SIGINT / SIGTERM for graceful shutdown.
        """
        self._running = True
        prev_sigint = signal.getsignal(signal.SIGINT)
        prev_sigterm = signal.getsignal(signal.SIGTERM)

        def _handle_signal(signum: int, _frame: Any) -> None:
            logger.info("cdc_signal_received", signal=signum)
            self._running = False

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        logger.info(
            "cdc_listener_started",
            slot=self.slot_name,
            plugin=self.plugin,
            poll_interval=self.poll_interval,
            batch_size=self.batch_size,
        )

        try:
            while self._running:
                try:
                    events = self.poll_once()
                    if events:
                        groups = self.handler.group_by_transaction(events)
                        for group in groups:
                            if len(group) == 1:
                                self.handler.handle_event(group[0])
                            else:
                                self.handler.handle_transaction(group)
                        logger.info(
                            "cdc_poll_batch",
                            events=len(events),
                            applied=self.handler.stats.deltas_applied,
                            failed=self.handler.stats.deltas_failed,
                            last_lsn=self.handler.stats.last_lsn,
                        )
                    else:
                        time.sleep(self.poll_interval)
                except Exception as exc:
                    if not self._running:
                        break
                    logger.error("cdc_poll_error", error=str(exc))
                    time.sleep(self.poll_interval)
        finally:
            signal.signal(signal.SIGINT, prev_sigint)
            signal.signal(signal.SIGTERM, prev_sigterm)
            logger.info(
                "cdc_listener_stopped",
                stats=self.handler.stats.as_dict(),
            )

    def stop(self) -> None:
        """Signal the polling loop to exit."""
        self._running = False
