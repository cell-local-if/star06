"""Append-only event ledger: the baseline service.

Public contract is README.md; this module is the entry point the acceptance scripts import.
Deliberately small but real: a working HTTP surface, optimistic concurrency, and sqlite persistence.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterable

EVENT_TYPES = {"OrderPlaced", "OrderCancelled", "LineItemAdded", "NoteRecorded"}


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

    def as_json(self) -> dict[str, Any]:
        return {"stream_id": self.stream_id, "version": self.version, "event_id": self.event_id,
                "type": self.type, "payload": self.payload}


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
        self._db = sqlite3.connect(self.path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("""CREATE TABLE IF NOT EXISTS events (
            stream_id TEXT NOT NULL, version INTEGER NOT NULL, event_id TEXT NOT NULL,
            type TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (stream_id, version))""")
        self._db.execute("""CREATE TABLE IF NOT EXISTS commands (
            command_id TEXT PRIMARY KEY, stream_id TEXT NOT NULL, expected_version INTEGER NOT NULL,
            events_canonical TEXT NOT NULL, response_json TEXT NOT NULL)""")
        self._db.commit()

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def version(self, stream_id: str) -> int:
        with self._lock:
            row = self._db.execute("SELECT COALESCE(MAX(version), 0) FROM events WHERE stream_id = ?", (stream_id,)).fetchone()
        return int(row[0])

    def append(self, stream_id: str, events: Any, expected_version: Any,
               command_id: Any = None) -> list[Event]:
        if command_id is not None and (not isinstance(command_id, str)
                                       or not command_id or len(command_id) > 200):
            raise InvalidRequest("command_id must be a non-empty string of at most 200 characters")
        cleaned = validate_append(stream_id, events, expected_version)
        # Canonical form: key order in payloads is insignificant, array order is not.
        canonical = json.dumps(cleaned, sort_keys=True, separators=(",", ":"))
        with self._lock:
            if command_id is not None:
                row = self._db.execute(
                    "SELECT stream_id, expected_version, events_canonical, response_json "
                    "FROM commands WHERE command_id = ?", (command_id,)).fetchone()
                if row is not None:
                    if row[0] == stream_id and int(row[1]) == expected_version and row[2] == canonical:
                        stored = json.loads(row[3])
                        return [Event(e["stream_id"], int(e["version"]), e["event_id"],
                                      e["type"], e["payload"]) for e in stored["events"]]
                    raise IdempotencyConflict(
                        f"command_id {command_id!r} was already recorded with a different request")
            current = self.version(stream_id)
            if current != expected_version:
                raise VersionConflict(f"stream {stream_id!r} is at version {current}, not {expected_version}")
            written: list[Event] = []
            try:
                for offset, event in enumerate(cleaned, start=1):
                    event_id = str(uuid.uuid4())
                    self._db.execute("INSERT INTO events VALUES (?, ?, ?, ?, ?)",
                                     (stream_id, current + offset, event_id, event["type"], json.dumps(event["payload"])))
                    written.append(Event(stream_id, current + offset, event_id, event["type"], event["payload"]))
                if command_id is not None:
                    response = {"version": written[-1].version, "events": [e.as_json() for e in written]}
                    self._db.execute("INSERT INTO commands VALUES (?, ?, ?, ?, ?)",
                                     (command_id, stream_id, expected_version, canonical, json.dumps(response)))
                self._db.commit()
            except Exception:
                self._db.rollback()
                raise
        return written

    def read(self, stream_id: str, since: int = 0) -> list[Event]:
        with self._lock:
            rows = self._db.execute("SELECT stream_id, version, event_id, type, payload FROM events "
                                    "WHERE stream_id = ? AND version > ? ORDER BY version", (stream_id, since)).fetchall()
        return [Event(row[0], int(row[1]), row[2], row[3], json.loads(row[4])) for row in rows]

    def streams(self) -> list[str]:
        with self._lock:
            rows = self._db.execute("SELECT DISTINCT stream_id FROM events ORDER BY stream_id").fetchall()
        return [row[0] for row in rows]


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
                    events = ledger.read(parts[1])
                    if not events and ledger.version(parts[1]) == 0:
                        raise StreamNotFound(f"stream {parts[1]!r} does not exist")
                    return self._send(200, {"state": replay(events), "version": ledger.version(parts[1])})
                return self._send(404, {"error": {"code": "not_found"}})
            except LedgerError as error:
                return self._send(error.status, {"error": {"code": error.code, "message": str(error)}})
            except Exception:
                return self._send(500, {"error": {"code": "internal_error"}})

        def do_POST(self) -> None:  # noqa: N802 - http.server API
            try:
                parts = [p for p in self.path.split("?")[0].split("/") if p]
                if len(parts) != 3 or parts[0] != "streams" or parts[2] != "events":
                    return self._send(404, {"error": {"code": "not_found"}})
                body = self._read_json()
                if not isinstance(body, dict):
                    raise InvalidRequest("body must be a JSON object")
                written = ledger.append(parts[1], body.get("events"), body.get("expected_version"),
                                        body.get("command_id"))
                return self._send(201, {"version": written[-1].version, "events": [e.as_json() for e in written]})
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
