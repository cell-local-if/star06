"""Read-only database verification: ``python -m eventledger.verify <db>``.

Diagnostic only. Opens the given SQLite file read-only, never starts the
service and never migrates or writes anything (no WAL, no temp tables).

Exit codes:
  0  every recorded fact is consistent:
     {"ok": true, "events": N, "max_cursor": N}
  1  the file is a current-schema ledger but some fact is corrupted:
     {"ok": false, "errors": [{"code": "...", "detail": "..."}, ...]}
  2  the file cannot be read as a current ledger, or the arguments are wrong:
     {"ok": false, "errors": [{"code": "database_unreadable"|"usage_error", ...}]}
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from collections import defaultdict
from typing import Any
from urllib.parse import quote

from .app import (EVENT_TYPES, Event, InvalidRequest, replay, request_fingerprint,
                  transaction_fingerprint, validate_append, validate_transaction)

_PUBLIC_EVENT_FIELDS = {"stream_id", "version", "event_id", "type", "payload"}


class VerifyUnreadable(Exception):
    """The path is not a readable, current-schema ledger file (exit code 2)."""


def _print_failure(code: str, detail: str) -> None:
    print(json.dumps({"ok": False, "errors": [{"code": code, "detail": detail}]},
                     ensure_ascii=False, separators=(",", ":")))


def _open_readonly(path: str) -> sqlite3.Connection:
    """Open the file strictly read-only without creating any side files.

    When no ``-wal`` companion exists the ledger is quiescent (the service
    checkpoints and removes the WAL on its last close), so ``immutable=1`` is
    safe and guarantees SQLite creates neither a WAL nor a shared-memory file.
    When a ``-wal`` file is already present, plain ``mode=ro`` reads committed
    WAL frames as well; a read-only connection never creates the WAL itself.
    """
    target = f"file:{quote(path)}"
    if not os.path.exists(path + "-wal"):
        target += "?immutable=1"
    else:
        target += "?mode=ro"
    conn = sqlite3.connect(target, uri=True)
    # Tolerate a writer checkpointing a live file; this pragma writes nothing.
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _table_names(db: sqlite3.Connection) -> set[str]:
    return {row[0] for row in db.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')")}


def _columns(db: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in db.execute(f"PRAGMA table_info({table})")}


def verify(path: str) -> int:
    """Check one ledger file; return the process exit code."""
    try:
        db = _open_readonly(path)
    except sqlite3.Error as error:
        _print_failure("database_unreadable", f"cannot open sqlite database: {error}")
        return 2
    try:
        # An explicit deferred read transaction pins one consistent snapshot
        # for the whole check; it is released at close and, being read-only,
        # creates no journal or WAL.
        db.execute("BEGIN")
        try:
            tables = _table_names(db)
            required = {"events", "commands", "snapshots", "event_sequence"}
            if not required <= tables:
                raise VerifyUnreadable(
                    f"ledger tables missing: {', '.join(sorted(required - tables))}")
            # Current schema only. Pre-cursor ledgers (events without the
            # cursor column) and pre-kind ledgers (commands without kind)
            # are reported unreadable rather than upgraded.
            if "cursor" not in _columns(db, "events"):
                raise VerifyUnreadable(
                    "events table lacks the global cursor column; legacy files are not upgraded")
            if "kind" not in _columns(db, "commands"):
                raise VerifyUnreadable(
                    "commands table lacks the kind column; legacy files are not upgraded")
            errors = _collect_errors(db)
            total = int(db.execute("SELECT COUNT(*) FROM events").fetchone()[0])
            max_cursor = int(db.execute(
                "SELECT COALESCE(MAX(cursor), 0) FROM events").fetchone()[0])
        except VerifyUnreadable:
            raise
        except sqlite3.Error as error:
            raise VerifyUnreadable(f"sqlite error while reading database: {error}") from error
    except VerifyUnreadable as error:
        _print_failure("database_unreadable", str(error))
        return 2
    finally:
        db.close()
    if errors:
        errors.sort(key=lambda item: (item["code"], item["detail"]))
        print(json.dumps({"ok": False, "errors": errors},
                         ensure_ascii=False, separators=(",", ":")))
        return 1
    print(json.dumps({"ok": True, "events": total, "max_cursor": max_cursor},
                     ensure_ascii=False, separators=(",", ":")))
    return 0


def _err(errors: list[dict[str, str]], code: str, detail: str) -> None:
    errors.append({"code": code, "detail": detail})


def _load_events(db: sqlite3.Connection, errors: list[dict[str, str]]) -> list[dict[str, Any]]:
    """Read every event in physical insertion order.

    Each raw row is validated on its own (type set, JSON-object payload,
    non-empty globally-unique event_id); version/cursor continuity is checked
    afterwards so one corrupt row does not hide the remaining facts.
    """
    raw_rows = db.execute(
        "SELECT stream_id, version, event_id, type, payload, cursor, rowid "
        "FROM events ORDER BY rowid").fetchall()
    events: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for stream_id, version, event_id, kind, payload_text, cursor, rowid in raw_rows:
        loc = f"event rowid={rowid} stream_id={stream_id!r} version={version}"
        if not isinstance(stream_id, str) or not stream_id:
            _err(errors, "invalid_event", f"{loc}: stream_id must be a non-empty string")
        if not isinstance(version, int) or isinstance(version, bool) or version < 1:
            _err(errors, "invalid_event", f"{loc}: version must be a positive integer")
        if not isinstance(event_id, str) or not event_id:
            _err(errors, "invalid_event", f"{loc}: event_id must be a non-empty string")
        elif event_id in seen_ids:
            _err(errors, "invalid_event", f"{loc}: event_id {event_id!r} is not globally unique")
        else:
            seen_ids.add(event_id)
        if kind not in EVENT_TYPES:
            _err(errors, "invalid_event", f"{loc}: unknown event type {kind!r}")
        try:
            payload = json.loads(payload_text)
        except (TypeError, json.JSONDecodeError) as error:
            _err(errors, "invalid_event", f"{loc}: payload is not valid JSON: {error}")
            payload = None
        else:
            if not isinstance(payload, dict):
                _err(errors, "invalid_event", f"{loc}: payload must be a JSON object")
        if not isinstance(cursor, int) or isinstance(cursor, bool):
            _err(errors, "invalid_event", f"{loc}: cursor must be an integer")
        events.append({"stream_id": stream_id, "version": version, "event_id": event_id,
                       "type": kind, "payload": payload, "cursor": cursor, "rowid": rowid})
    return events


def _check_versions(events: list[dict[str, Any]], errors: list[dict[str, str]]) -> None:
    """Each stream's versions must be exactly 1..M (the primary key forbids duplicates)."""
    by_stream: dict[str, list[int]] = defaultdict(list)
    for event in events:
        if isinstance(event["stream_id"], str) and event["stream_id"] \
                and isinstance(event["version"], int) and not isinstance(event["version"], bool):
            by_stream[event["stream_id"]].append(event["version"])
    for stream_id in sorted(by_stream):
        versions = sorted(by_stream[stream_id])
        expected = list(range(1, len(versions) + 1))
        if versions != expected:
            _err(errors, "version_gap",
                 f"stream {stream_id!r}: versions {versions} are not the dense "
                 f"sequence 1..{len(versions)}")


def _check_cursors(events: list[dict[str, Any]], errors: list[dict[str, str]]) -> None:
    """Global cursors must be the dense permutation 1..N with no gaps or repeats."""
    cursors = sorted(event["cursor"] for event in events
                     if isinstance(event["cursor"], int) and not isinstance(event["cursor"], bool))
    total = len(events)
    expected = list(range(1, total + 1))
    if cursors != expected:
        _err(errors, "cursor_gap",
             f"cursors {cursors} are not the dense global sequence 1..{total}")


def _check_sequence(db: sqlite3.Connection, total: int, errors: list[dict[str, str]]) -> None:
    """The event_sequence high-water mark must be N, so the next cursor is N+1."""
    row = db.execute("SELECT next_cursor FROM event_sequence WHERE id = 1").fetchone()
    if row is None:
        _err(errors, "sequence_mismatch", "event_sequence has no high-water row (id = 1)")
        return
    next_cursor = row[0]
    if not isinstance(next_cursor, int) or next_cursor != total:
        _err(errors, "sequence_mismatch",
             f"event_sequence next_cursor is {next_cursor!r}, expected {total} "
             f"(high-water mark {total}, next cursor {total + 1})")


def _check_snapshots(db: sqlite3.Connection, events_by_stream: dict[str, list[dict[str, Any]]],
                     errors: list[dict[str, str]]) -> None:
    """Snapshots are derived facts: real version, state field-equal to replay(1..v)."""
    for stream_id, version, state_text, rowid in db.execute(
            "SELECT stream_id, version, state, rowid FROM snapshots ORDER BY rowid"):
        loc = f"snapshot rowid={rowid} stream_id={stream_id!r} version={version}"
        stream_events = events_by_stream.get(stream_id, [])
        max_version = max((event["version"] for event in stream_events
                           if isinstance(event["version"], int)), default=0)
        # Version 0 (initial projection) is a legal snapshot for an existing
        # stream; snapshots never count as events themselves.
        if not isinstance(stream_id, str) or not stream_id or not stream_events \
                or not isinstance(version, int) or isinstance(version, bool) \
                or not 0 <= version <= max_version:
            _err(errors, "invalid_snapshot",
                 f"{loc}: points at a version that does not exist in the stream")
            continue
        try:
            stored_state = json.loads(state_text)
        except (TypeError, json.JSONDecodeError) as error:
            _err(errors, "invalid_snapshot", f"{loc}: state is not valid JSON: {error}")
            continue
        if not isinstance(stored_state, dict):
            _err(errors, "invalid_snapshot", f"{loc}: state must be a JSON object")
            continue
        prefix = sorted((event for event in stream_events if event["version"] <= version),
                        key=lambda event: event["version"])
        if any(event["payload"] is None for event in prefix):
            # Replaying needs every payload; the payload fault was already
            # reported as invalid_event, so skip the derived comparison.
            continue
        projected = replay([Event(event["stream_id"], event["version"], event["event_id"],
                                  event["type"], event["payload"]) for event in prefix])
        if stored_state != projected:
            _err(errors, "snapshot_mismatch",
                 f"{loc}: stored state {json.dumps(stored_state, ensure_ascii=False)} "
                 f"does not equal replay(events[1..{version}]) "
                 f"{json.dumps(projected, ensure_ascii=False)}")


def _response_event_matches(loc: str, claimed: Any, landed: dict[str, Any],
                            errors: list[dict[str, str]]) -> None:
    """A response event must equal the landed event and carry no cursor field."""
    if not isinstance(claimed, dict):
        _err(errors, "command_response_mismatch", f"{loc}: response event is not an object")
        return
    if set(claimed) != _PUBLIC_EVENT_FIELDS:
        _err(errors, "command_response_mismatch",
             f"{loc}: response event fields are {sorted(claimed)} "
             "(must be exactly stream_id, version, event_id, type, payload; no cursor)")
        return
    mismatched = claimed["stream_id"] != landed["stream_id"] \
        or claimed["version"] != landed["version"] \
        or claimed["event_id"] != landed["event_id"] \
        or claimed["type"] != landed["type"]
    # A landed event with an unparseable payload was already reported as
    # invalid_event; compare the payload only when it is available.
    if landed["payload"] is not None and claimed["payload"] != landed["payload"]:
        mismatched = True
    if mismatched:
        _err(errors, "command_response_mismatch",
             f"{loc}: response event {json.dumps(claimed, ensure_ascii=False)} "
             f"does not match the landed event {json.dumps(landed, ensure_ascii=False)}")


def _check_append_command(loc: str, stream_id: str, fingerprint: str, request: Any,
                          response: Any, events_by_stream: dict[str, list[dict[str, Any]]],
                          events_by_key: dict[tuple[str, int], dict[str, Any]],
                          errors: list[dict[str, str]]) -> None:
    if not isinstance(request, dict) \
            or not {"stream_id", "events", "expected_version"} <= set(request):
        _err(errors, "invalid_command", f"{loc}: stored request is not the append request shape")
        return
    if request["stream_id"] != stream_id:
        _err(errors, "invalid_command",
             f"{loc}: request stream_id {request['stream_id']!r} does not match the command row "
             f"stream_id {stream_id!r}")
        return
    # The writer fingerprints the *cleaned* request (missing payload defaults to
    # {}, types validated), not the raw JSON; clean the same way before
    # recomputing. A request that cannot be cleaned could never have committed.
    try:
        cleaned_events = validate_append(
            request["stream_id"], request["events"], request["expected_version"])
    except InvalidRequest as error:
        _err(errors, "invalid_command", f"{loc}: stored request fails append validation: {error}")
        return
    expected_fingerprint = request_fingerprint(request["stream_id"], cleaned_events,
                                               request["expected_version"])
    if fingerprint != expected_fingerprint:
        _err(errors, "invalid_command",
             f"{loc}: fingerprint {fingerprint!r} does not equal the canonical request value "
             f"{expected_fingerprint!r}")
    if not isinstance(response, dict) or not {"version", "events"} <= set(response):
        _err(errors, "invalid_command", f"{loc}: stored response is not the append response shape")
        return
    request_events = request["events"]
    response_events = response["events"]
    end_version = response["version"]
    if not isinstance(request_events, list) or not isinstance(response_events, list) \
            or not response_events or len(request_events) != len(response_events):
        _err(errors, "invalid_command",
             f"{loc}: request and response event lists differ in shape or length")
        return
    if not isinstance(end_version, int) or isinstance(end_version, bool) or end_version < 1:
        _err(errors, "command_response_mismatch",
             f"{loc}: response version {end_version!r} is not a positive integer")
        return
    expected_version = request["expected_version"]
    if isinstance(expected_version, int) and not isinstance(expected_version, bool) \
            and end_version != expected_version + len(response_events):
        _err(errors, "command_response_mismatch",
             f"{loc}: response version {end_version} does not close the appended interval "
             f"starting after expected_version {expected_version}")
    stream_versions = {event["version"] for event in events_by_stream.get(stream_id, [])
                       if isinstance(event["version"], int)}
    start_version = end_version - len(response_events) + 1
    for offset, claimed in enumerate(response_events):
        version = start_version + offset
        vloc = f"{loc} version={version}"
        if version not in stream_versions:
            _err(errors, "command_response_mismatch",
                 f"{vloc}: no committed event exists at that version")
            continue
        _response_event_matches(vloc, claimed, events_by_key[(stream_id, version)], errors)


def _check_transaction_command(loc: str, command_id: str, row_stream_id: str,
                               fingerprint: str, request: Any, response: Any,
                               events_by_stream: dict[str, list[dict[str, Any]]],
                               events_by_key: dict[tuple[str, int], dict[str, Any]],
                               errors: list[dict[str, str]]) -> None:
    if not isinstance(request, dict) or not isinstance(request.get("streams"), list) \
            or not request["streams"]:
        _err(errors, "invalid_command",
             f"{loc}: stored request is not the transaction request shape")
        return
    # Clean exactly as the writer did before fingerprinting (per-item single
    # stream validation; payload defaults; duplicate streams rejected).
    try:
        cleaned_streams = validate_transaction(request["streams"])
    except InvalidRequest as error:
        _err(errors, "invalid_command", f"{loc}: stored request fails transaction validation: {error}")
        return
    expected_fingerprint = transaction_fingerprint(cleaned_streams)
    if fingerprint != expected_fingerprint:
        _err(errors, "invalid_command",
             f"{loc}: fingerprint {fingerprint!r} does not equal the canonical request value "
             f"{expected_fingerprint!r}")
    # The commands.stream_id column names the transaction's first stream.
    first_stream = request["streams"][0].get("stream_id")
    if first_stream != row_stream_id:
        _err(errors, "invalid_command",
             f"{loc}: first request stream_id {first_stream!r} does not match the command row "
             f"stream_id {row_stream_id!r}")
        return
    if not isinstance(response, dict) or response.get("transaction_id") != command_id \
            or not isinstance(response.get("streams"), list):
        _err(errors, "invalid_command",
             f"{loc}: stored response is not the transaction response shape "
             "(transaction_id must equal the command_id)")
        return
    request_streams = request["streams"]
    response_streams = response["streams"]
    if len(request_streams) != len(response_streams):
        _err(errors, "command_response_mismatch",
             f"{loc}: request lists {len(request_streams)} streams but response lists "
             f"{len(response_streams)}")
        return
    # The whole batch must occupy one contiguous global cursor range, handed
    # out in streams order; gather the landed cursors and check afterwards.
    batch_cursors: list[tuple[str, int]] = []
    for index, (req_item, res_item) in enumerate(zip(request_streams, response_streams)):
        sloc = f"{loc} streams[{index}]"
        if not isinstance(req_item, dict) or not isinstance(res_item, dict) \
                or req_item.get("stream_id") != res_item.get("stream_id"):
            _err(errors, "command_response_mismatch",
                 f"{sloc}: request/response stream entries disagree")
            continue
        stream_id = req_item["stream_id"]
        request_events = req_item.get("events")
        response_events = res_item.get("events")
        end_version = res_item.get("version")
        if not isinstance(request_events, list) or not isinstance(response_events, list) \
                or not response_events or len(request_events) != len(response_events):
            _err(errors, "command_response_mismatch",
                 f"{sloc}: request/response event lists differ in shape or length")
            continue
        if not isinstance(end_version, int) or isinstance(end_version, bool) \
                or not isinstance(req_item.get("expected_version"), int) \
                or isinstance(req_item.get("expected_version"), bool) \
                or end_version != req_item["expected_version"] + len(response_events):
            _err(errors, "command_response_mismatch",
                 f"{sloc}: response version {end_version!r} does not close the version interval "
                 f"{req_item.get('expected_version')!r}..+{len(response_events)}")
            continue
        stream_versions = {event["version"] for event in events_by_stream.get(stream_id, [])
                           if isinstance(event["version"], int)}
        start_version = end_version - len(response_events) + 1
        for offset, claimed in enumerate(response_events):
            version = start_version + offset
            vloc = f"{sloc} version={version}"
            if version not in stream_versions:
                _err(errors, "command_response_mismatch",
                     f"{vloc}: no committed event exists at that version")
                continue
            landed = events_by_key[(stream_id, version)]
            _response_event_matches(vloc, claimed, landed, errors)
            if isinstance(landed["cursor"], int) and not isinstance(landed["cursor"], bool):
                batch_cursors.append((vloc, landed["cursor"]))
    # In streams order the landed cursors must be one strictly consecutive
    # run: the writer reserves a single contiguous global range per batch.
    for index, (vloc, cursor) in enumerate(batch_cursors):
        if index == 0:
            continue
        if cursor != batch_cursors[index - 1][1] + 1:
            _err(errors, "command_response_mismatch",
                 f"{vloc}: landed cursor {cursor} breaks the contiguous batch order; "
                 f"expected {batch_cursors[index - 1][1] + 1}")


def _check_commands(db: sqlite3.Connection, events_by_stream: dict[str, list[dict[str, Any]]],
                    events_by_key: dict[tuple[str, int], dict[str, Any]],
                    errors: list[dict[str, str]]) -> None:
    """Every committed command record must match the events it claims landed."""
    for command_id, stream_id, fingerprint, request_text, response_text, kind, rowid in db.execute(
            "SELECT command_id, stream_id, fingerprint, request, response, kind, rowid "
            "FROM commands ORDER BY rowid"):
        loc = f"command rowid={rowid} command_id={command_id!r}"
        try:
            request = json.loads(request_text)
            response = json.loads(response_text)
        except (TypeError, json.JSONDecodeError) as error:
            _err(errors, "invalid_command",
                 f"{loc}: stored request/response is not valid JSON: {error}")
            continue
        if kind == "append":
            _check_append_command(loc, stream_id, fingerprint, request, response,
                                  events_by_stream, events_by_key, errors)
        elif kind == "transaction":
            _check_transaction_command(loc, command_id, stream_id, fingerprint, request, response,
                                       events_by_stream, events_by_key, errors)
        else:
            _err(errors, "invalid_command", f"{loc}: unknown kind {kind!r}")


def _collect_errors(db: sqlite3.Connection) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    events = _load_events(db, errors)
    _check_versions(events, errors)
    _check_cursors(events, errors)
    _check_sequence(db, len(events), errors)
    events_by_stream: dict[str, list[dict[str, Any]]] = defaultdict(list)
    events_by_key: dict[tuple[str, int], dict[str, Any]] = {}
    for event in events:
        if isinstance(event["stream_id"], str) and event["stream_id"]:
            events_by_stream[event["stream_id"]].append(event)
            if isinstance(event["version"], int) and not isinstance(event["version"], bool):
                events_by_key[(event["stream_id"], event["version"])] = event
    _check_snapshots(db, events_by_stream, errors)
    _check_commands(db, events_by_stream, events_by_key, errors)
    return errors


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1 or not args[0]:
        _print_failure("usage_error", "usage: python -m eventledger.verify <db>")
        return 2
    return verify(args[0])


if __name__ == "__main__":
    sys.exit(main())
