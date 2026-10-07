"""Read-only database verification entry point: ``python -m eventledger.verify <db>``.

Diagnostic only: it never starts the service and never writes, migrates or
journal-mode-switches the inspected file. Success prints
``{"ok":true,"events":N,"max_cursor":N}`` and exits 0; corrupted facts print
``{"ok":false,"errors":[{"code":...,"detail":...}]}`` and exit 1; unusable
paths/databases/schemas and argument errors print ``database_unreadable`` /
``usage_error`` and exit 2.

What counts as the truth:

* events   — known ``type``, JSON-object payload, per-stream contiguous
  versions from 1, non-empty globally-unique ``event_id``, dense global
  cursors 1..N, and the ``event_sequence`` high-water mark in step with N;
* snapshots — every row points at a real (stream, version) and its ``state``
  is field-for-field equal to ``replay(events[1..version])``;
* commands  — successful append/transaction records only. The fingerprint
  must equal the canonical form of the persisted request, and the persisted
  response must match the events actually on file (and never carry a
  cursor). Transactions are checked in ``streams`` order over contiguous
  per-stream version ranges.
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from collections import defaultdict
from typing import Any

from .app import (
    EVENT_TYPES,
    Event,
    replay,
    request_fingerprint,
    transaction_fingerprint,
    validate_append,
    validate_transaction,
)

REQUIRED_EVENT_COLUMNS = {"stream_id", "version", "event_id", "type", "payload", "cursor"}
REQUIRED_COMMAND_COLUMNS = {"command_id", "stream_id", "fingerprint", "request", "response", "kind"}
REQUIRED_SNAPSHOT_COLUMNS = {"stream_id", "version", "state"}
REQUIRED_SEQUENCE_COLUMNS = {"id", "next_cursor"}

PUBLIC_EVENT_KEYS = {"stream_id", "version", "event_id", "type", "payload"}


class VerificationError(Exception):
    """Operational failure (exit 2), as opposed to a corrupted fact (exit 1)."""

    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


def _quote_uri(path: str) -> str:
    # The path may itself contain '?' or '#', which URI syntax treats
    # specially; percent-encode them (and '%') so the path stays literal.
    return path.replace("%", "%25").replace("?", "%3F").replace("#", "%23")


def _open_read_only(path: str) -> sqlite3.Connection:
    """Open the file strictly read-only and leave the schema untouched.

    Two modes, chosen by whether a hot WAL is present:

    * no ``<db>-wal`` — the main file is fully checkpointed (a cleanly closed
      ledger). Open with ``immutable=1``: no ``-wal``/``-shm`` side file and no
      shared-memory state is created at all, so a read cannot leave litter or
      switch a rollback-journal file into WAL mode.
    * a ``<db>-wal`` already exists — a writer is or was recently active and may
      hold uncheckpointed facts. Open with ``mode=ro`` so SQLite coordinates on
      that existing WAL and every read is an atomic snapshot. The WAL is not
      created here (it is already there); at most a shared-memory ``-shm`` map
      is attached to read it, which is required to observe a live ledger.

    Both modes forbid writes at the OS/SQLite level, so the verifier cannot
    migrate a pre-cursor file, create a temporary table or record anything.
    """
    quoted = _quote_uri(path)
    if os.path.exists(path + "-journal"):
        # A hot rollback journal means a non-WAL writer crashed mid-transaction;
        # reading before recovery would inspect uncommitted/torn pages. The
        # ledger itself always runs WAL, so this is not a clean ledger file.
        raise VerificationError(
            "database_unreadable",
            "database has a hot rollback journal; recover it with the ledger service first")
    wal = path + "-wal"
    # A non-empty WAL may hold committed-but-uncheckpointed frames, so open in
    # mode=ro and let SQLite coordinate on it. A missing or zero-length WAL
    # means the main file is fully checkpointed: immutable reads it without
    # creating any -wal/-shm side file at all.
    hot_wal = os.path.exists(wal) and os.path.getsize(wal) > 0
    uri = f"file:{quoted}?mode=ro" if hot_wal else f"file:{quoted}?immutable=1"
    return sqlite3.connect(uri, uri=True, isolation_level=None)


def _table_columns(db: sqlite3.Connection, table: str) -> set[str] | None:
    rows = db.execute(f"PRAGMA table_info({table})").fetchall()
    if not rows:
        return None
    return {row[1] for row in rows}


def _require_schema(db: sqlite3.Connection) -> None:
    """Fail database_unreadable on a missing table/column or old-format file."""
    expected = {
        "events": REQUIRED_EVENT_COLUMNS,
        "commands": REQUIRED_COMMAND_COLUMNS,
        "snapshots": REQUIRED_SNAPSHOT_COLUMNS,
        "event_sequence": REQUIRED_SEQUENCE_COLUMNS,
    }
    for table, required in expected.items():
        columns = _table_columns(db, table)
        if columns is None:
            raise VerificationError("database_unreadable", f"missing table: {table}")
        missing = required - columns
        if missing:
            # Pre-cursor events or pre-kind commands land here: they are
            # old-format files and must be reported, never upgraded.
            raise VerificationError(
                "database_unreadable",
                f"table {table} is missing columns: {sorted(missing)}")


def _is_positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _verify_events(rows: list[tuple], errors: list[tuple[str, str]]) -> tuple[
        dict[str, dict[int, Event]], set[tuple[str, int]]]:
    """Check event shapes, per-stream versions, event_ids and global cursors.

    Returns ``(by_stream, tainted)``: events indexed by stream/version, and the
    (stream_id, version) pairs whose content facts are bad. Positional facts
    (stream_id/version/cursor) drive the density checks independently of the
    content facts, so a broken payload never masquerades as a cursor gap.
    Derived facts (snapshots, command responses) that rest on a tainted event
    are skipped rather than compared against a corrupted basis.
    """
    by_stream: dict[str, dict[int, Event]] = defaultdict(dict)
    tainted: set[tuple[str, int]] = set()
    seen_event_ids: set[str] = set()
    positional: list[tuple[str, int, int]] = []
    indexed: list[tuple] = []

    for row in rows:
        stream_id, version, event_id, event_type, payload_text, cursor = row
        label = f"event stream_id={stream_id!r} version={version!r}"
        position_ok = isinstance(stream_id, str) and bool(stream_id) \
            and _is_positive_int(version) and _is_positive_int(cursor)
        content_bad = not position_ok
        if not isinstance(stream_id, str) or not stream_id:
            errors.append(("invalid_event", f"{label}: stream_id must be a non-empty string"))
        if not _is_positive_int(version):
            errors.append(("invalid_event", f"{label}: version must be a positive integer"))
        if not isinstance(event_id, str) or not event_id:
            errors.append(("invalid_event", f"{label}: event_id must be a non-empty string"))
            content_bad = True
        elif event_id in seen_event_ids:
            errors.append(("invalid_event",
                           f"event_id {event_id!r} is used by more than one event"))
            content_bad = True
        else:
            seen_event_ids.add(event_id)
        if event_type not in EVENT_TYPES:
            errors.append(("invalid_event",
                           f"{label}: type {event_type!r} is not one of {sorted(EVENT_TYPES)}"))
            content_bad = True
        payload: Any = None
        try:
            payload = json.loads(payload_text)
        except (TypeError, ValueError):
            errors.append(("invalid_event", f"{label}: payload is not valid JSON"))
            content_bad = True
        else:
            if not isinstance(payload, dict):
                errors.append(("invalid_event", f"{label}: payload must be a JSON object"))
                content_bad = True
        if not _is_positive_int(cursor):
            errors.append(("cursor_gap",
                           f"{label}: cursor {cursor!r} is not a positive integer"))
        if position_ok:
            positional.append((stream_id, version, cursor))
            # Every position-valid row is indexed so existence checks work. A
            # non-object payload gets a placeholder; such rows are tainted and
            # never enter replay() or a value comparison.
            safe_payload = payload if isinstance(payload, dict) else {}
            indexed.append((stream_id, version, event_id if isinstance(event_id, str) else "",
                            event_type, safe_payload, cursor))
            if content_bad:
                tainted.add((stream_id, version))

    # Per-stream versions start at 1 and stay dense; duplicate versions are
    # reported as gaps as well. Walking the real versions reports every hole.
    versions_by_stream: dict[str, list[int]] = defaultdict(list)
    for stream_id, version, _cursor in positional:
        versions_by_stream[stream_id].append(version)
    for stream_id in sorted(versions_by_stream):
        seen: set[int] = set()
        expected = 1
        for version in sorted(versions_by_stream[stream_id]):
            if version in seen or version != expected:
                errors.append(("version_gap",
                               f"stream {stream_id!r}: expected version {expected}, found {version}"))
                if version in seen:
                    tainted.add((stream_id, version))
            seen.add(version)
            expected = version + 1

    # Global cursors: dense 1..N over every row, globally unique.
    total = len(rows)
    cursors = [cursor for _s, _v, cursor in positional]
    cursor_set = set(cursors)
    for cursor in range(1, total + 1):
        if cursor not in cursor_set:
            errors.append(("cursor_gap", f"global cursor {cursor} is missing"))
    for cursor in sorted(cursor_set):
        if not 1 <= cursor <= total:
            errors.append(("cursor_gap", f"global cursor {cursor} is outside 1..{total}"))
    if len(cursor_set) != len(cursors):
        errors.append(("cursor_gap", "global cursors are not unique"))
        seen_cursor: set[int] = set()
        for stream_id, version, cursor in positional:
            if cursor in seen_cursor:
                tainted.add((stream_id, version))
            seen_cursor.add(cursor)

    for stream_id, version, event_id, event_type, payload, cursor in indexed:
        by_stream[stream_id][version] = Event(
            stream_id, version, event_id, event_type, payload, cursor)
    return by_stream, tainted


def _verify_sequence(db: sqlite3.Connection, total_events: int, errors: list[tuple[str, str]]) -> None:
    """The single high-water row must be in step with the allocated cursors.

    The service stores the last allocated cursor in ``next_cursor`` (0 on a
    fresh ledger, N after N events; the next append allocates N+1 from it),
    so the persisted value must equal the event count / max cursor.
    """
    rows = db.execute("SELECT id, next_cursor FROM event_sequence").fetchall()
    if len(rows) != 1:
        errors.append(("sequence_mismatch",
                       f"event_sequence must hold exactly one row, found {len(rows)}"))
        return
    row_id, next_cursor = rows[0]
    if row_id != 1:
        errors.append(("sequence_mismatch", f"event_sequence row id must be 1, found {row_id!r}"))
    if not isinstance(next_cursor, int) or isinstance(next_cursor, bool) \
            or next_cursor != total_events:
        errors.append(("sequence_mismatch",
                       f"event_sequence.next_cursor is {next_cursor!r}, "
                       f"expected the high-water mark {total_events}"))


def _replay_until(stream: dict[int, Event], version: int) -> dict[str, Any]:
    return replay([stream[v] for v in range(1, version + 1)])


def _range_clean(stream: dict[int, Event], tainted: set[tuple[str, int]],
                 stream_id: str, first: int, last: int) -> bool:
    """True when versions first..last all exist and rest on sound event facts."""
    return all(v in stream and (stream_id, v) not in tainted
               for v in range(first, last + 1))


def _verify_snapshots(db: sqlite3.Connection,
                      by_stream: dict[str, dict[int, Event]],
                      tainted: set[tuple[str, int]],
                      errors: list[tuple[str, str]]) -> None:
    rows = db.execute("SELECT stream_id, version, state FROM snapshots").fetchall()
    for stream_id, version, state_text in rows:
        label = f"snapshot stream_id={stream_id!r} version={version!r}"
        if not isinstance(stream_id, str) or not stream_id or not isinstance(version, int) \
                or isinstance(version, bool) or version < 0:
            errors.append(("invalid_snapshot", f"{label}: invalid stream_id or version"))
            continue
        stream = by_stream.get(stream_id, {})
        # Version 0 is a legal snapshot of an existing (event-bearing) stream;
        # any other target must name a version the stream actually has.
        if version == 0:
            if not stream:
                errors.append(("invalid_snapshot",
                               f"{label}: points at a stream with no events"))
                continue
            expected_state = replay([])
        elif version in stream:
            if not _range_clean(stream, tainted, stream_id, 1, version):
                # A missing or corrupted event makes replay(events[1..version])
                # uncomputable; the underlying version_gap/invalid_event is the
                # root error, so do not pile a snapshot verdict on top.
                continue
            expected_state = _replay_until(stream, version)
        else:
            errors.append(("invalid_snapshot",
                           f"{label}: points at a version that does not exist"))
            continue
        try:
            stored_state = json.loads(state_text)
        except (TypeError, ValueError):
            errors.append(("invalid_snapshot", f"{label}: state is not valid JSON"))
            continue
        if stored_state != expected_state:
            errors.append(("snapshot_mismatch",
                           f"{label}: stored state does not match replay(events[1..{version}])"))


def _canonical_fingerprint(kind: str, request: Any) -> str | None:
    """Canonicalise the persisted request exactly like the write path.

    The request was validated before it was first persisted, so None means
    the stored facts themselves are corrupt.
    """
    if kind == "append":
        cleaned = validate_append(request["stream_id"], request["events"],
                                  request["expected_version"])
        return request_fingerprint(request["stream_id"], cleaned, request["expected_version"])
    if kind == "transaction":
        return transaction_fingerprint(validate_transaction(request["streams"]))
    return None


def _event_shape_problems(event_json: Any) -> list[str]:
    """Structural response facts that hold regardless of the event on file."""
    if not isinstance(event_json, dict):
        return ["event is not a JSON object"]
    problems: list[str] = []
    if set(event_json) != PUBLIC_EVENT_KEYS:
        problems.append(f"event keys are {sorted(set(event_json))}, "
                        f"expected {sorted(PUBLIC_EVENT_KEYS)} (cursor is not persisted)")
    return problems


def _event_value_problems(event_json: Any, actual: Event) -> list[str]:
    return [f"field {field} disagrees with the committed event"
            for field, value in actual.as_json().items()
            if event_json.get(field) != value]


def _verify_append_response(command_id: str, stream_id: Any, response: Any,
                            request: Any,
                            by_stream: dict[str, dict[int, Event]],
                            tainted: set[tuple[str, int]],
                            errors: list[tuple[str, str]]) -> None:
    label = f"command {command_id!r}"
    if not isinstance(response, dict) or set(response) != {"version", "events"}:
        errors.append(("command_response_mismatch",
                       f"{label}: response must be an object with exactly version and events"))
        return
    events = response["events"]
    last_version = response["version"]
    if not isinstance(events, list) or not events:
        errors.append(("command_response_mismatch",
                       f"{label}: response.events must be a non-empty array"))
        return
    if not _is_positive_int(last_version) or last_version < len(events):
        errors.append(("command_response_mismatch",
                       f"{label}: response.version must be the last written version"))
        return
    if not isinstance(stream_id, str) or not stream_id:
        errors.append(("invalid_command", f"{label}: stream_id must be a non-empty string"))
        return
    # A successful append wrote exactly expected_version+1..+len: the response
    # interval must line up with the persisted request's optimistic version.
    expected_version = request.get("expected_version") if isinstance(request, dict) else None
    if not isinstance(expected_version, int) or isinstance(expected_version, bool) \
            or expected_version < 0 or last_version != expected_version + len(events):
        errors.append(("command_response_mismatch",
                       f"{label}: response version interval does not match "
                       "the persisted expected_version"))
        return
    stream = by_stream.get(stream_id)
    if stream is None:
        errors.append(("command_response_mismatch",
                       f"{label}: stream {stream_id!r} has no events"))
        return
    expected_versions = list(range(last_version - len(events) + 1, last_version + 1))
    for index, (version, event_json) in enumerate(zip(expected_versions, events)):
        actual = stream.get(version)
        if actual is None:
            errors.append(("command_response_mismatch",
                           f"{label}: response references missing version {version}"))
            continue
        for problem in _event_shape_problems(event_json):
            errors.append(("command_response_mismatch",
                           f"{label}: response.events[{index}] at version {version}: {problem}"))
        if (stream_id, version) in tainted:
            # The event on file is itself corrupt (already reported); its value
            # agreement with the response cannot be judged from broken facts.
            continue
        if isinstance(event_json, dict):
            for problem in _event_value_problems(event_json, actual):
                errors.append(("command_response_mismatch",
                               f"{label}: response.events[{index}] at version {version}: {problem}"))


def _verify_transaction_response(command_id: str, response: Any, request: Any,
                                 by_stream: dict[str, dict[int, Event]],
                                 tainted: set[tuple[str, int]],
                                 errors: list[tuple[str, str]]) -> None:
    label = f"command {command_id!r}"
    if not isinstance(response, dict) or set(response) != {"transaction_id", "streams"}:
        errors.append(("command_response_mismatch",
                       f"{label}: response must be an object with exactly transaction_id and streams"))
        return
    if response.get("transaction_id") != command_id:
        errors.append(("command_response_mismatch",
                       f"{label}: response.transaction_id must equal the command_id"))
    streams_response = response.get("streams")
    streams_request = request.get("streams")
    if not isinstance(streams_response, list) or not isinstance(streams_request, list) \
            or len(streams_response) != len(streams_request):
        errors.append(("command_response_mismatch",
                       f"{label}: response.streams must mirror request.streams in order"))
        return
    for index, (wanted, result) in enumerate(zip(streams_request, streams_response)):
        entry_label = f"{label} streams[{index}]"
        if not isinstance(wanted, dict) or not isinstance(result, dict) \
                or set(result) != {"stream_id", "version", "events"}:
            errors.append(("command_response_mismatch", f"{entry_label}: malformed entry"))
            continue
        stream_id = wanted.get("stream_id")
        if result.get("stream_id") != stream_id:
            errors.append(("command_response_mismatch",
                           f"{entry_label}: stream_id disagrees with the request order"))
        stream = by_stream.get(stream_id) if isinstance(stream_id, str) else None
        if stream is None:
            errors.append(("command_response_mismatch",
                           f"{entry_label}: stream {stream_id!r} has no events"))
            continue
        wanted_events = wanted.get("events")
        result_events = result.get("events")
        if not isinstance(wanted_events, list) or not isinstance(result_events, list) \
                or not wanted_events or len(wanted_events) != len(result_events):
            errors.append(("command_response_mismatch",
                           f"{entry_label}: event count disagrees with the request"))
            continue
        last_version = result.get("version")
        count = len(result_events)
        if not _is_positive_int(last_version) or last_version < count:
            errors.append(("command_response_mismatch",
                           f"{entry_label}: version must be the last written version"))
            continue
        # The written interval for this stream starts right after its
        # expected_version; check that the response lands on that interval.
        expected_version = wanted.get("expected_version")
        if not isinstance(expected_version, int) or isinstance(expected_version, bool) \
                or expected_version < 0 or last_version != expected_version + count:
            errors.append(("command_response_mismatch",
                           f"{entry_label}: version interval does not match "
                           "the persisted expected_version"))
            continue
        expected_versions = list(range(last_version - count + 1, last_version + 1))
        for offset, (version, event_json) in enumerate(zip(expected_versions, result_events)):
            sub_label = f"{entry_label} events[{offset}]"
            actual = stream.get(version)
            if actual is None:
                errors.append(("command_response_mismatch",
                               f"{sub_label}: references missing version {version}"))
                continue
            for problem in _event_shape_problems(event_json):
                errors.append(("command_response_mismatch", f"{sub_label}: {problem}"))
            if (stream_id, version) in tainted:
                continue
            # Agreement with the request is already covered by the fingerprint;
            # here the response must name the events that actually landed.
            if isinstance(event_json, dict):
                for problem in _event_value_problems(event_json, actual):
                    errors.append(("command_response_mismatch", f"{sub_label}: {problem}"))


def _verify_commands(db: sqlite3.Connection,
                     by_stream: dict[str, dict[int, Event]],
                     tainted: set[tuple[str, int]],
                     errors: list[tuple[str, str]]) -> None:
    rows = db.execute(
        "SELECT command_id, stream_id, fingerprint, request, response, kind FROM commands"
    ).fetchall()
    seen_command_ids: set[str] = set()
    for command_id, stream_id, fingerprint, request_text, response_text, kind in rows:
        label = f"command {command_id!r}"
        if not isinstance(command_id, str) or not command_id:
            errors.append(("invalid_command",
                           f"command_id must be a non-empty string: {command_id!r}"))
            continue
        if command_id in seen_command_ids:
            errors.append(("invalid_command", f"{label}: command_id is not unique"))
        seen_command_ids.add(command_id)
        if kind not in ("append", "transaction"):
            errors.append(("invalid_command", f"{label}: unknown kind {kind!r}"))
            continue
        try:
            request = json.loads(request_text)
            response = json.loads(response_text)
        except (TypeError, ValueError):
            errors.append(("invalid_command", f"{label}: request/response is not valid JSON"))
            continue
        if not isinstance(request, dict) or not isinstance(response, dict):
            errors.append(("invalid_command", f"{label}: request/response must be JSON objects"))
            continue
        try:
            expected_fingerprint = _canonical_fingerprint(kind, request)
        except Exception:
            errors.append(("invalid_command",
                           f"{label}: persisted request is not a valid {kind} command"))
            continue
        if not isinstance(fingerprint, str) or fingerprint != expected_fingerprint:
            errors.append(("invalid_command",
                           f"{label}: fingerprint does not match the canonical persisted request"))
            continue
        if kind == "append":
            if isinstance(request.get("stream_id"), str) and request["stream_id"] != stream_id:
                errors.append(("invalid_command",
                               f"{label}: commands.stream_id {stream_id!r} disagrees with "
                               f"the persisted request stream_id {request['stream_id']!r}"))
                continue
            _verify_append_response(command_id, stream_id, response, request,
                                    by_stream, tainted, errors)
        else:
            # The stream_id column records the first stream (NOT NULL); verify it.
            first_stream = None
            request_streams = request.get("streams")
            if isinstance(request_streams, list) and request_streams \
                    and isinstance(request_streams[0], dict):
                first_stream = request_streams[0].get("stream_id")
            if isinstance(first_stream, str) and stream_id != first_stream:
                errors.append(("invalid_command",
                               f"{label}: commands.stream_id {stream_id!r} disagrees with the "
                               f"first request stream {first_stream!r}"))
                continue
            _verify_transaction_response(command_id, response, request, by_stream, tainted, errors)


def verify_database(path: str) -> dict[str, Any]:
    """Verify one SQLite file. Returns the public result object."""
    db = _open_read_only(path)
    try:
        # One read transaction pins one snapshot for every check below, exactly
        # like the service's own reads: a writer committing while the verifier
        # runs can never make the events, commands and sequence SELECTs land on
        # different commit boundaries (which would look like torn facts).
        db.execute("BEGIN")
        try:
            _require_schema(db)
            errors: list[tuple[str, str]] = []
            event_rows = db.execute(
                "SELECT stream_id, version, event_id, type, payload, cursor FROM events").fetchall()
            by_stream, tainted = _verify_events(event_rows, errors)
            _verify_sequence(db, len(event_rows), errors)
            _verify_snapshots(db, by_stream, tainted, errors)
            _verify_commands(db, by_stream, tainted, errors)
        except BaseException:
            db.execute("ROLLBACK")
            raise
        else:
            db.execute("COMMIT")
    finally:
        db.close()
    if errors:
        return {"ok": False, "errors": [
            {"code": code, "detail": detail}
            for code, detail in sorted(errors, key=lambda item: (item[0], item[1]))]}
    max_cursor = max((event.cursor for stream in by_stream.values() for event in stream.values()),
                     default=0)
    return {"ok": True, "events": len(event_rows), "max_cursor": max_cursor}


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        print(json.dumps({"ok": False, "error": {
            "code": "usage_error",
            "detail": "usage: python -m eventledger.verify <db>"}}, separators=(",", ":")))
        return 2
    try:
        result = verify_database(args[0])
    except VerificationError as error:
        print(json.dumps({"ok": False, "error": {"code": error.code, "detail": error.detail}},
                         separators=(",", ":")))
        return 2
    except (OSError, sqlite3.Error) as error:
        print(json.dumps({"ok": False, "error": {
            "code": "database_unreadable", "detail": str(error)}}, separators=(",", ":")))
        return 2
    print(json.dumps(result, separators=(",", ":"), ensure_ascii=False))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
