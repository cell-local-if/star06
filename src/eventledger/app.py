"""Append-only event ledger: the baseline service.

Public contract is README.md; this module is the entry point the acceptance scripts import.
Deliberately small but real: a working HTTP surface, optimistic concurrency, and sqlite persistence.
"""
from __future__ import annotations

import dataclasses
import json
import re
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import unquote

EVENT_TYPES = {"OrderPlaced", "OrderCancelled", "LineItemAdded", "NoteRecorded"}

# Canonical decimal non-negative integer: "0", "1", "10" — no sign, whitespace,
# decimal point or exponent. Anything else is invalid_request. ASCII digits
# only: str.isdigit would admit Unicode digits that int() rejects.
_AT_PATTERN = re.compile(r"[0-9]+")


def parse_at_version(query: str) -> int | None:
    """Parse the ``at`` parameter out of a raw query string.

    Returns None when ``at`` is absent (latest-projection behaviour). An empty
    value, a repeated ``at``, or any value that is not a decimal non-negative
    integer is InvalidRequest: no sign, whitespace, decimal point, exponent or
    non-ASCII digits.
    """
    values: list[str] = []
    for chunk in query.split("&"):
        if not chunk:
            continue
        key, separator, value = chunk.partition("=")
        if unquote(key) == "at":
            # A bare "?at" (no '=') counts as an empty value as well.
            values.append(unquote(value) if separator else "")
    if not values:
        return None
    if len(values) > 1:
        raise InvalidRequest("query parameter at must appear exactly once")
    raw = values[0]
    if not _AT_PATTERN.fullmatch(raw):
        raise InvalidRequest("query parameter at must be a decimal non-negative integer")
    return int(raw)


def parse_audit_query(query: str) -> tuple[int, int]:
    """Parse ``GET /events`` parameters out of a raw query string.

    Returns ``(after, limit)``; ``after`` defaults to 0, ``limit`` to 100.
    Every rule is strict: only the keys ``after`` and ``limit`` are allowed
    (each at most once), and every value must be a decimal non-negative
    integer in ASCII digits. Booleans, floats, whitespace, signs, scientific
    notation, non-ASCII digits, empty values and any unknown key are all
    InvalidRequest. ``limit`` must additionally lie in 1..1000.
    """
    after_text: str | None = None
    limit_text: str | None = None
    for chunk in query.split("&"):
        if not chunk:
            continue
        key, separator, value = chunk.partition("=")
        name = unquote(key)
        # A bare "?after" (no '=') counts as an empty value, just like "after=".
        text = unquote(value) if separator else ""
        if name == "after":
            if after_text is not None:
                raise InvalidRequest("query parameter after must appear exactly once")
            after_text = text
        elif name == "limit":
            if limit_text is not None:
                raise InvalidRequest("query parameter limit must appear exactly once")
            limit_text = text
        else:
            raise InvalidRequest(f"unknown query parameter: {name}")
    if after_text is None:
        after = 0
    else:
        if not _AT_PATTERN.fullmatch(after_text):
            raise InvalidRequest("query parameter after must be a decimal non-negative integer")
        after = int(after_text)
    if limit_text is None:
        limit = 100
    else:
        if not _AT_PATTERN.fullmatch(limit_text):
            raise InvalidRequest("query parameter limit must be a decimal non-negative integer")
        limit = int(limit_text)
        if not 1 <= limit <= 1000:
            raise InvalidRequest("query parameter limit must be between 1 and 1000")
    return after, limit


class LedgerError(Exception):
    """Base class so callers can translate to HTTP without inspecting messages."""

    code = "internal_error"
    status = 500


class InvalidRequest(LedgerError):
    code, status = "invalid_request", 400


class StreamNotFound(LedgerError):
    code, status = "not_found", 404


class VersionConflict(LedgerError):
    code, status = "version_conflict", 409


class IdempotencyConflict(LedgerError):
    code, status = "idempotency_conflict", 409


@dataclass(frozen=True)
class Event:
    stream_id: str
    version: int
    event_id: str
    type: str
    payload: dict[str, Any]
    cursor: int = 0

    def as_json(self) -> dict[str, Any]:
        return {"stream_id": self.stream_id, "version": self.version, "event_id": self.event_id,
                "type": self.type, "payload": self.payload}

    def as_audit_json(self) -> dict[str, Any]:
        return {"cursor": self.cursor, "stream_id": self.stream_id, "version": self.version,
                "event_id": self.event_id, "type": self.type, "payload": self.payload}


def validate_command_id(command_id: Any) -> str:
    if not isinstance(command_id, str) or not command_id or len(command_id) > 200:
        raise InvalidRequest("command_id must be a non-empty string of at most 200 characters")
    return command_id


def request_fingerprint(stream_id: str, events: list[dict[str, Any]], expected_version: int) -> str:
    """Canonical form of a command request.

    JSON with sorted keys: payloads that differ only in key order are the same command,
    while event order, values, stream_id and expected_version all participate.
    """
    return json.dumps({"stream_id": stream_id, "expected_version": expected_version, "events": events},
                      sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def transaction_fingerprint(streams: list[tuple[str, list[dict[str, Any]], int]]) -> str:
    """Canonical form of a cross-stream transaction request.

    Same rules as ``request_fingerprint``, lifted to the ordered streams array:
    stream order and event order are significant, payload key order is not.
    """
    return json.dumps({"streams": [{"stream_id": stream_id, "expected_version": expected_version,
                                    "events": events}
                                   for stream_id, events, expected_version in streams]},
                      sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def validate_transaction(streams: Any) -> list[tuple[str, list[dict[str, Any]], int]]:
    """Validate the whole ``streams`` array of a transaction before anything is written.

    Every item reuses the single-stream rules (stream_id shape, event types and
    shapes, expected_version semantics, unknown-field rejection); additionally
    the array must be non-empty and no stream_id may repeat — one stream can
    only appear once per transaction, otherwise its versions would not be
    consecutive within the batch.
    """
    if not isinstance(streams, list) or not streams:
        raise InvalidRequest("streams must be a non-empty array")
    cleaned: list[tuple[str, list[dict[str, Any]], int]] = []
    seen: set[str] = set()
    for index, item in enumerate(streams):
        if not isinstance(item, dict):
            raise InvalidRequest(f"streams[{index}] must be an object")
        extra = set(item) - {"stream_id", "events", "expected_version"}
        if extra:
            raise InvalidRequest(f"streams[{index}] has unknown fields: {sorted(extra)}")
        stream_id = item.get("stream_id")
        events = validate_append(stream_id, item.get("events"), item.get("expected_version"))
        if stream_id in seen:
            raise InvalidRequest(f"streams[{index}] duplicates stream_id {stream_id!r}")
        seen.add(stream_id)
        cleaned.append((stream_id, events, item["expected_version"]))
    return cleaned


def validate_append(stream_id: str, events: Any, expected_version: Any) -> list[dict[str, Any]]:
    if not isinstance(stream_id, str) or not stream_id or len(stream_id) > 200:
        raise InvalidRequest("stream_id must be a non-empty string of at most 200 characters")
    if not isinstance(events, list) or not events:
        raise InvalidRequest("events must be a non-empty array")
    if len(events) > 100:
        raise InvalidRequest("at most 100 events may be appended at once")
    if not isinstance(expected_version, int) or isinstance(expected_version, bool) or expected_version < 0:
        raise InvalidRequest("expected_version must be a non-negative integer")
    cleaned: list[dict[str, Any]] = []
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            raise InvalidRequest(f"events[{index}] must be an object")
        extra = set(event) - {"type", "payload"}
        if extra:
            raise InvalidRequest(f"events[{index}] has unknown fields: {sorted(extra)}")
        kind = event.get("type")
        if kind not in EVENT_TYPES:
            raise InvalidRequest(f"events[{index}].type must be one of {sorted(EVENT_TYPES)}")
        payload = event.get("payload", {})
        if not isinstance(payload, dict):
            raise InvalidRequest(f"events[{index}].payload must be an object")
        cleaned.append({"type": kind, "payload": payload})
    return cleaned


class Ledger:
    """One sqlite file per ledger; every write is a transaction, every read is ordered by version."""

    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(self.path, check_same_thread=False, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA busy_timeout=5000")
        self._db.execute("""CREATE TABLE IF NOT EXISTS events (
            stream_id TEXT NOT NULL, version INTEGER NOT NULL, event_id TEXT NOT NULL,
            type TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (stream_id, version))""")
        self._db.execute("""CREATE TABLE IF NOT EXISTS commands (
            command_id TEXT PRIMARY KEY, stream_id TEXT NOT NULL, fingerprint TEXT NOT NULL,
            request TEXT NOT NULL, response TEXT NOT NULL)""")
        # Cross-stream transactions have their own idempotency namespace: one row per
        # committed transaction command, holding the canonical fingerprint and the
        # full first response (including the generated transaction_id).
        self._db.execute("""CREATE TABLE IF NOT EXISTS transactions (
            command_id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL,
            request TEXT NOT NULL, response TEXT NOT NULL)""")
        self._db.execute("""CREATE TABLE IF NOT EXISTS snapshots (
            stream_id TEXT NOT NULL, version INTEGER NOT NULL, state TEXT NOT NULL,
            PRIMARY KEY (stream_id, version))""")
        # Global allocation state. The single row (id=1) holds the high-water mark;
        # cursors are taken under BEGIN IMMEDIATE so commits are globally ordered.
        self._db.execute("""CREATE TABLE IF NOT EXISTS event_sequence (
            id INTEGER PRIMARY KEY CHECK (id = 1), next_cursor INTEGER NOT NULL)""")
        self._db.commit()
        self._migrate_global_cursors()

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def _migrate_global_cursors(self) -> None:
        """Upgrade a pre-audit ledger to globally ordered cursors.

        Legacy rows have no ``cursor`` column. They are numbered 1..N in
        ``rowid`` order — the order in which appends physically wrote them,
        which preserves the relative order of prior writes — and the sequence
        high-water mark is set to N. The whole migration is one transaction:
        on any failure it rolls back, leaving the old facts untouched and the
        file still openable by the old code, rather than a half-built order.
        Concurrent openers serialize on SQLite's write lock; the schema/count
        double-check makes exactly one of them perform the upgrade.
        """
        with self._lock:
            columns = {row[1] for row in self._db.execute("PRAGMA table_info(events)")}
            if "cursor" in columns:
                return
            try:
                self._db.execute("BEGIN IMMEDIATE")
                # Another process may have finished the migration while we waited
                # for the write lock; re-check inside the transaction.
                columns = {row[1] for row in self._db.execute("PRAGMA table_info(events)")}
                if "cursor" in columns:
                    self._db.execute("COMMIT")
                    return
                self._db.execute("ALTER TABLE events ADD COLUMN cursor INTEGER")
                self._db.execute(
                    "UPDATE events SET cursor = (SELECT COUNT(*) FROM events AS prior "
                    "WHERE prior.rowid <= events.rowid)")
                self._db.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS events_cursor_idx ON events(cursor)")
                total = int(self._db.execute("SELECT COUNT(*) FROM events").fetchone()[0])
                self._db.execute("INSERT INTO event_sequence (id, next_cursor) VALUES (1, ?)",
                                 (total,))
                self._verify_global_cursors()
                self._db.execute("COMMIT")
            except LedgerError:
                self._rollback_quietly()
                raise
            except Exception as error:
                # The DDL above is transactional, so this undoes the added column/index too:
                # no half order survives, and the facts are byte-for-byte untouched.
                self._rollback_quietly()
                raise LedgerError("could not complete global cursor migration") from error

    def _rollback_quietly(self) -> None:
        try:
            self._db.execute("ROLLBACK")
        except sqlite3.Error:
            pass

    def _verify_global_cursors(self) -> None:
        """Fail loudly rather than serve a non-dense, non-stable ordering."""
        with self._lock:
            total = int(self._db.execute("SELECT COUNT(*) FROM events").fetchone()[0])
            nulls = int(self._db.execute(
                "SELECT COUNT(*) FROM events WHERE cursor IS NULL").fetchone()[0])
            out_of_range = int(self._db.execute(
                "SELECT COUNT(*) FROM events WHERE cursor NOT BETWEEN 1 AND ?",
                (total,)).fetchone()[0])
            distinct = int(self._db.execute(
                "SELECT COUNT(DISTINCT cursor) FROM events").fetchone()[0])
            if nulls or out_of_range or distinct != total:
                raise LedgerError("global cursor migration did not produce a dense 1..N order")
            row = self._db.execute("SELECT next_cursor FROM event_sequence WHERE id = 1").fetchone()
            if row is None or int(row[0]) != total:
                raise LedgerError("global cursor sequence is out of step with the events")

    def version(self, stream_id: str) -> int:
        with self._lock:
            row = self._db.execute("SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?", (stream_id,)).fetchone()
        return int(row[0])

    def append(self, stream_id: str, events: Any, expected_version: Any,
               command_id: Any = None) -> list[Event]:
        cleaned = validate_append(stream_id, events, expected_version)
        if command_id is not None:
            command_id = validate_command_id(command_id)
        fingerprint = request_fingerprint(stream_id, cleaned, expected_version)
        with self._lock:
            # BEGIN IMMEDIATE takes the write lock up front: two concurrent retries of the
            # same command serialize in SQLite itself, so only one batch is ever committed.
            self._db.execute("BEGIN IMMEDIATE")
            try:
                if command_id is not None:
                    repeat = self._lookup_command(command_id, fingerprint)
                    if repeat is not None:
                        self._db.execute("COMMIT")
                        return repeat
                current = int(self._db.execute(
                    "SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?",
                    (stream_id,)).fetchone()[0])
                if current != expected_version:
                    raise VersionConflict(f"stream {stream_id!r} is at version {current}, not {expected_version}")
                # Global cursors for the whole batch are reserved here, inside the
                # same transaction that holds the events: they become visible
                # atomically with commit, and a rollback returns them to the pool.
                base_cursor = int(self._db.execute(
                    "SELECT next_cursor FROM event_sequence WHERE id = 1").fetchone()[0])
                written: list[Event] = []
                for offset, event in enumerate(cleaned, start=1):
                    event_id = str(uuid.uuid4())
                    cursor = base_cursor + offset
                    self._db.execute(
                        "INSERT INTO events (stream_id, version, event_id, type, payload, cursor) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (stream_id, current + offset, event_id, event["type"],
                         json.dumps(event["payload"]), cursor))
                    written.append(Event(stream_id, current + offset, event_id, event["type"],
                                         event["payload"], cursor))
                self._db.execute("UPDATE event_sequence SET next_cursor = ? WHERE id = 1",
                                 (base_cursor + len(cleaned),))
                if command_id is not None:
                    # Written in the same transaction as the events, only after every event row
                    # succeeded: a rollback leaves neither partial events nor a hitable record,
                    # and the PRIMARY KEY makes a double commit impossible from another process.
                    response = {"version": written[-1].version, "events": [e.as_json() for e in written]}
                    self._db.execute(
                        "INSERT INTO commands (command_id, stream_id, fingerprint, request, response) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (command_id, stream_id, fingerprint,
                         json.dumps({"stream_id": stream_id, "events": events,
                                     "expected_version": expected_version}, ensure_ascii=False),
                         json.dumps(response, ensure_ascii=False)))
                self._db.execute("COMMIT")
            except sqlite3.IntegrityError:
                # Another process won a write race between our version check and commit.
                self._db.execute("ROLLBACK")
                self._db.execute("BEGIN IMMEDIATE")
                try:
                    if command_id is not None:
                        repeat = self._lookup_command(command_id, fingerprint)
                        if repeat is not None:
                            return repeat
                        # Same command_id but mismatched fingerprint raises inside _lookup_command;
                        # without a command_id the collision was on (stream_id, version).
                    current = int(self._db.execute(
                        "SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?",
                        (stream_id,)).fetchone()[0])
                    raise VersionConflict(f"stream {stream_id!r} is at version {current}, not {expected_version}")
                finally:
                    self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        return written

    def _lookup_command(self, command_id: str, fingerprint: str) -> list[Event] | None:
        """Return the stored response for a committed command, or None.

        Raises IdempotencyConflict when the command_id already succeeded with a different
        request — this takes priority over version_conflict for retries.
        """
        row = self._db.execute("SELECT fingerprint, response FROM commands WHERE command_id = ?",
                               (command_id,)).fetchone()
        if row is None:
            return None
        if row[0] != fingerprint:
            raise IdempotencyConflict(f"command {command_id!r} was already committed with a different request")
        stored = [Event(**event) for event in json.loads(row[1])["events"]]
        # The persisted response is the public per-stream shape (no cursor), but the
        # objects returned to callers carry their real global cursors; hydrate them
        # from the committed rows rather than re-announcing cursor 0.
        if stored:
            cursor_by_version = dict(self._db.execute(
                "SELECT version, cursor FROM events WHERE stream_id = ?",
                (stored[0].stream_id,)).fetchall())
            stored = [dataclasses.replace(event, cursor=int(cursor_by_version[event.version]))
                      for event in stored]
        return stored

    def append_transaction(self, command_id: Any, streams: Any) -> dict[str, Any]:
        """Commit one atomic batch across several streams; return the public response body.

        The whole request is validated before anything is written. All events,
        the transaction idempotency record and the reserved global cursors commit
        in a single SQLite transaction, so a crash or conflict can never leave
        some streams written and others not. Cursors are handed out in streams
        array order, giving each stream a contiguous run of versions exactly as
        if its events had been appended one batch at a time in that order.
        """
        command_id = validate_command_id(command_id)
        cleaned = validate_transaction(streams)
        fingerprint = transaction_fingerprint(cleaned)
        with self._lock:
            # BEGIN IMMEDIATE serializes concurrent transactions (and single-stream
            # appends) in SQLite itself: only one batch per command_id ever commits.
            self._db.execute("BEGIN IMMEDIATE")
            try:
                repeat = self._lookup_transaction(command_id, fingerprint)
                if repeat is not None:
                    self._db.execute("COMMIT")
                    return repeat
                # Every expected_version is checked, in array order, before the
                # first insert: the first conflicting stream wins the error and
                # no stream is written at all.
                for stream_id, _events, expected_version in cleaned:
                    current = int(self._db.execute(
                        "SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?",
                        (stream_id,)).fetchone()[0])
                    if current != expected_version:
                        raise VersionConflict(
                            f"stream {stream_id!r} is at version {current}, not {expected_version}")
                base_cursor = int(self._db.execute(
                    "SELECT next_cursor FROM event_sequence WHERE id = 1").fetchone()[0])
                cursor = base_cursor
                total = 0
                response_streams: list[dict[str, Any]] = []
                for stream_id, events, expected_version in cleaned:
                    written: list[Event] = []
                    for event in events:
                        cursor += 1
                        expected_version += 1
                        event_id = str(uuid.uuid4())
                        self._db.execute(
                            "INSERT INTO events (stream_id, version, event_id, type, payload, cursor) "
                            "VALUES (?, ?, ?, ?, ?, ?)",
                            (stream_id, expected_version, event_id, event["type"],
                             json.dumps(event["payload"]), cursor))
                        written.append(Event(stream_id, expected_version, event_id,
                                             event["type"], event["payload"], cursor))
                    total += len(written)
                    response_streams.append({"stream_id": stream_id, "version": written[-1].version,
                                             "events": [e.as_json() for e in written]})
                self._db.execute("UPDATE event_sequence SET next_cursor = ? WHERE id = 1",
                                 (base_cursor + total,))
                response = {"transaction_id": str(uuid.uuid4()), "streams": response_streams}
                # The idempotency record lands in the same transaction as the events:
                # a rollback leaves neither partial events nor a hitable record.
                self._db.execute(
                    "INSERT INTO transactions (command_id, fingerprint, request, response) "
                    "VALUES (?, ?, ?, ?)",
                    (command_id, fingerprint,
                     json.dumps({"streams": streams}, ensure_ascii=False),
                     json.dumps(response, ensure_ascii=False)))
                self._db.execute("COMMIT")
            except sqlite3.IntegrityError:
                # Another process won a write race between our version checks and commit.
                self._db.execute("ROLLBACK")
                self._db.execute("BEGIN IMMEDIATE")
                try:
                    repeat = self._lookup_transaction(command_id, fingerprint)
                    if repeat is not None:
                        return repeat
                    for stream_id, _events, expected_version in cleaned:
                        current = int(self._db.execute(
                            "SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?",
                            (stream_id,)).fetchone()[0])
                        if current != expected_version:
                            raise VersionConflict(
                                f"stream {stream_id!r} is at version {current}, not {expected_version}")
                    raise  # pragma: no cover - a constraint other than these two raced us
                finally:
                    self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        return response

    def _lookup_transaction(self, command_id: str, fingerprint: str) -> dict[str, Any] | None:
        """Return the stored first response for a committed transaction, or None.

        Raises IdempotencyConflict when the command_id already committed with a
        semantically different streams array — this takes priority over
        version_conflict for retries, exactly as in the single-stream path.
        """
        row = self._db.execute("SELECT fingerprint, response FROM transactions WHERE command_id = ?",
                               (command_id,)).fetchone()
        if row is None:
            return None
        if row[0] != fingerprint:
            raise IdempotencyConflict(f"command {command_id!r} was already committed with a different request")
        return json.loads(row[1])

    def read(self, stream_id: str, since: int = 0) -> list[Event]:
        with self._lock:
            rows = self._db.execute("SELECT stream_id, version, event_id, type, payload FROM events "
                                    "WHERE stream_id = ? AND version > ? ORDER BY version", (stream_id, since)).fetchall()
        return [Event(row[0], int(row[1]), row[2], row[3], json.loads(row[4])) for row in rows]

    def read_at(self, stream_id: str, at: int) -> list[Event]:
        """Replay slice 1..at against one consistent version boundary.

        The boundary check and the event fetch run under the same lock that
        serializes appends, so a concurrent commit can never land between the
        "what is current" and "read up to at" steps. Versions are contiguous, so
        when at <= current the last replayed version is exactly at (empty for
        at=0, which still requires the stream to exist). at past the current
        version — or a stream that was never written — is not_found.
        """
        with self._lock:
            current = int(self._db.execute(
                "SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?",
                (stream_id,)).fetchone()[0])
            if current == 0 or at > current:
                raise StreamNotFound(
                    f"stream {stream_id!r} has no version {at}")
            rows = self._db.execute(
                "SELECT stream_id, version, event_id, type, payload FROM events "
                "WHERE stream_id = ? AND version <= ? ORDER BY version",
                (stream_id, at)).fetchall()
        return [Event(row[0], int(row[1]), row[2], row[3], json.loads(row[4])) for row in rows]

    def read_global(self, after: int, limit: int) -> tuple[list[Event], bool]:
        """Read a page of the global audit trail after cursor ``after``.

        Returns ``(events, has_more)`` where events are in strictly increasing
        cursor order and at most ``limit`` rows. The count and the fetch run
        under the same lock that serializes appends, so a single response sees
        one commit boundary: events that commit after the boundary check are
        not counted or returned, and are left for the next page. ``has_more``
        reports whether any committed event sits beyond ``after`` past the
        returned page; an empty page beyond the head therefore reports False.
        """
        with self._lock:
            # One read transaction pins one WAL snapshot: even another process
            # committing between the two SELECTs cannot push a boundary-spanning
            # page to us. The Python lock additionally serializes local appends.
            self._db.execute("BEGIN")
            try:
                total = int(self._db.execute(
                    "SELECT COUNT(*) FROM events WHERE cursor > ?", (after,)).fetchone()[0])
                rows = self._db.execute(
                    "SELECT stream_id, version, event_id, type, payload, cursor FROM events "
                    "WHERE cursor > ? ORDER BY cursor LIMIT ?", (after, limit)).fetchall()
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise
        events = [Event(row[0], int(row[1]), row[2], row[3], json.loads(row[4]), int(row[5]))
                  for row in rows]
        return events, total > len(rows)

    def streams(self) -> list[str]:
        with self._lock:
            rows = self._db.execute("SELECT DISTINCT stream_id FROM events ORDER BY stream_id").fetchall()
        return [row[0] for row in rows]

    def snapshot(self, stream_id: str, at_version: Any) -> tuple[dict[str, Any], bool]:
        """固化流在 ``at_version`` 的确定性投影；返回 ``(state, created)``.

        快照只是事件的派生数据：状态由重放 1..at_version 得到，与
        ``GET /streams/{stream_id}?at=N`` 逐字段一致。版本检查、重放与固化
        落在同一个事务里（与追加共用同一把锁），所以并发追加前后的事件
        不会混入同一快照。重复固化同一 (stream_id, version) 命中已存快照，
        返回 ``created=False`` 且不产生任何写入。
        """
        if not isinstance(at_version, int) or isinstance(at_version, bool) or at_version < 0:
            raise InvalidRequest("at_version must be a non-negative integer")
        with self._lock:
            # BEGIN IMMEDIATE：并发的相同快照请求在 SQLite 内串行化，
            # 只有一个事务能插入 (stream_id, version) 这一行。
            self._db.execute("BEGIN IMMEDIATE")
            try:
                current = int(self._db.execute(
                    "SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?",
                    (stream_id,)).fetchone()[0])
                if current == 0 or at_version > current:
                    raise StreamNotFound(f"stream {stream_id!r} has no version {at_version}")
                row = self._db.execute(
                    "SELECT state FROM snapshots WHERE stream_id = ? AND version = ?",
                    (stream_id, at_version)).fetchone()
                if row is not None:
                    self._db.execute("COMMIT")
                    return json.loads(row[0]), False
                rows = self._db.execute(
                    "SELECT stream_id, version, event_id, type, payload FROM events "
                    "WHERE stream_id = ? AND version <= ? ORDER BY version",
                    (stream_id, at_version)).fetchall()
                events = [Event(r[0], int(r[1]), r[2], r[3], json.loads(r[4])) for r in rows]
                state = replay(events)
                self._db.execute("INSERT INTO snapshots (stream_id, version, state) VALUES (?, ?, ?)",
                                 (stream_id, at_version, json.dumps(state, ensure_ascii=False)))
                self._db.execute("COMMIT")
                return state, True
            except sqlite3.IntegrityError:
                # 另一进程抢先固化了同一 (stream_id, version)：状态是确定性重放，
                # 已存内容与我们算出的必然相同，直接按“已存在”返回。
                self._db.execute("ROLLBACK")
                row = self._db.execute(
                    "SELECT state FROM snapshots WHERE stream_id = ? AND version = ?",
                    (stream_id, at_version)).fetchone()
                if row is None:  # pragma: no cover - 主键冲突只可能来自快照表
                    raise
                return json.loads(row[0]), False
            except BaseException:
                self._db.execute("ROLLBACK")
                raise


def replay(events: Iterable[Event]) -> dict[str, Any]:
    """Deterministic projection: the same events always produce the same state."""
    state: dict[str, Any] = {"status": "unknown", "lines": [], "notes": [], "cancelled": False}
    for event in events:
        if event.type == "OrderPlaced":
            state["status"] = "placed"
        elif event.type == "LineItemAdded":
            state["lines"] = state["lines"] + [dict(event.payload)]
        elif event.type == "NoteRecorded":
            state["notes"] = state["notes"] + [event.payload.get("text", "")]
        elif event.type == "OrderCancelled":
            state["cancelled"] = True
            state["status"] = "cancelled"
    return state


def make_handler(ledger: Ledger) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "event-ledger/0.1"
        protocol_version = "HTTP/1.1"

        def log_message(self, *args: Any) -> None:  # keep the acceptance output readable
            return

        def _send(self, status: int, body: dict[str, Any]) -> None:
            raw = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _read_json(self) -> Any:
            length = self.headers.get("Content-Length")
            if length is None:
                raise InvalidRequest("Content-Length is required")
            try:
                size = int(length)
            except ValueError as error:
                raise InvalidRequest("Content-Length must be an integer") from error
            if size < 0 or size > 1_048_576:
                raise InvalidRequest("Content-Length must be between 0 and 1 MiB")
            if size == 0:
                return None
            try:
                return json.loads(self.rfile.read(size).decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise InvalidRequest("body must be valid UTF-8 JSON") from error

        def do_GET(self) -> None:  # noqa: N802 - http.server API
            try:
                parts = [p for p in self.path.split("?")[0].split("/") if p]
                if parts == ["health"]:
                    return self._send(200, {"status": "ok"})
                if len(parts) == 1 and parts[0] == "streams":
                    return self._send(200, {"streams": ledger.streams()})
                if len(parts) == 3 and parts[0] == "streams" and parts[2] == "events":
                    since = 0
                    query = self.path.split("?", 1)[1] if "?" in self.path else ""
                    for chunk in query.split("&"):
                        if chunk.startswith("since="):
                            since = int(chunk[6:] or 0)
                    events = ledger.read(parts[1], since)
                    if not events and ledger.version(parts[1]) == 0 and since == 0:
                        raise StreamNotFound(f"stream {parts[1]!r} does not exist")
                    return self._send(200, {"events": [e.as_json() for e in events]})
                if len(parts) == 2 and parts[0] == "streams":
                    query = self.path.split("?", 1)[1] if "?" in self.path else ""
                    at = parse_at_version(query)
                    if at is None:
                        events = ledger.read(parts[1])
                        if not events and ledger.version(parts[1]) == 0:
                            raise StreamNotFound(f"stream {parts[1]!r} does not exist")
                        return self._send(200, {"state": replay(events), "version": ledger.version(parts[1])})
                    # at is validated before the existence check; read_at enforces
                    # the consistent version boundary and the not-found cases.
                    events = ledger.read_at(parts[1], at)
                    return self._send(200, {"state": replay(events), "version": at})
                if parts == ["events"]:
                    query = self.path.split("?", 1)[1] if "?" in self.path else ""
                    after, limit = parse_audit_query(query)
                    events, has_more = ledger.read_global(after, limit)
                    next_cursor = events[-1].cursor if events else after
                    return self._send(200, {"events": [e.as_audit_json() for e in events],
                                            "next_cursor": next_cursor, "has_more": has_more})
                return self._send(404, {"error": {"code": "not_found"}})
            except LedgerError as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}})
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

        def do_POST(self) -> None:  # noqa: N802 - http.server API
            try:
                parts = [p for p in self.path.split("?")[0].split("/") if p]
                if len(parts) == 3 and parts[0] == "streams" and parts[2] == "events":
                    body = self._read_json()
                    if not isinstance(body, dict):
                        raise InvalidRequest("body must be a JSON object")
                    written = ledger.append(parts[1], body.get("events"), body.get("expected_version"),
                                            body.get("command_id"))
                    return self._send(201, {"version": written[-1].version,
                                            "events": [e.as_json() for e in written]})
                if len(parts) == 3 and parts[0] == "streams" and parts[2] == "snapshots":
                    body = self._read_json()
                    if not isinstance(body, dict):
                        raise InvalidRequest("body must be a JSON object")
                    extra = set(body) - {"at_version"}
                    if extra:
                        raise InvalidRequest(f"body has unknown fields: {sorted(extra)}")
                    if "at_version" not in body:
                        raise InvalidRequest("at_version is required")
                    state, created = ledger.snapshot(parts[1], body["at_version"])
                    status = 201 if created else 200
                    return self._send(status, {"stream_id": parts[1], "version": body["at_version"],
                                               "state": state})
                if len(parts) == 2 and parts[0] == "transactions":
                    body = self._read_json()
                    if not isinstance(body, dict):
                        raise InvalidRequest("body must be a JSON object")
                    extra = set(body) - {"streams"}
                    if extra:
                        raise InvalidRequest(f"body has unknown fields: {sorted(extra)}")
                    if "streams" not in body:
                        raise InvalidRequest("streams is required")
                    response = ledger.append_transaction(parts[1], body["streams"])
                    return self._send(201, response)
                return self._send(404, {"error": {"code": "not_found"}})
            except LedgerError as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}})
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

    return Handler


def serve(host: str = "127.0.0.1", port: int = 18891, db: str = ":memory:") -> ThreadingHTTPServer:
    ledger = Ledger(db)
    httpd = ThreadingHTTPServer((host, port), make_handler(ledger))
    httpd.ledger = ledger  # type: ignore[attr-defined]
    return httpd


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="append-only event ledger")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18891)
    parser.add_argument("--db", default="ledger.sqlite")
    args = parser.parse_args()
    server = serve(args.host, args.port, args.db)
    print(f"event ledger listening on http://{args.host}:{args.port}", flush=True)
    server.serve_forever()
