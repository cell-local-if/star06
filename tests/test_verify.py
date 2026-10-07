"""Read-only verifier: ``python -m eventledger.verify <db>``.

The verifier is a standalone diagnostic entry point, so these tests drive it
exactly as documented: as a subprocess, asserting exit code plus stdout JSON.
It must never start the service, migrate a legacy file or write anything.
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import eventledger
from eventledger import Ledger

SRC = str(Path(eventledger.__file__).resolve().parent.parent)
ENV = {**os.environ, "PYTHONPATH": SRC + (os.pathsep + os.environ["PYTHONPATH"]
                                          if os.environ.get("PYTHONPATH") else "")}


def build_ledger(path: Path) -> None:
    """A healthy ledger that exercises every fact kind the verifier checks."""
    led = Ledger(str(path))
    led.append("order-1", [{"type": "OrderPlaced", "payload": {"total": 10}},
                           {"type": "LineItemAdded", "payload": {"sku": "a"}}], 0, command_id="cmd-1")
    led.append("order-1", [{"type": "NoteRecorded", "payload": {"text": "hi"}}], 2, command_id="cmd-2")
    led.transaction("tx-1", [
        {"stream_id": "order-2", "events": [{"type": "OrderPlaced", "payload": {}}],
         "expected_version": 0},
        {"stream_id": "order-1", "events": [{"type": "OrderCancelled", "payload": {}}],
         "expected_version": 3},
    ])
    # An append without command_id lands events but no command record.
    led.append("order-2", [{"type": "LineItemAdded", "payload": {"sku": "b"}}], 1)
    led.snapshot("order-1", 3)
    led.snapshot("order-2", 0)
    led.close()


class VerifyCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def verify(self, path: Path) -> tuple[int, dict, str]:
        proc = subprocess.run([sys.executable, "-m", "eventledger.verify", str(path)],
                              capture_output=True, text=True, env=ENV)
        return proc.returncode, json.loads(proc.stdout), proc.stderr

    def fresh(self, name: str = "ledger.sqlite") -> Path:
        path = self.dir / name
        build_ledger(path)
        return path

    def mutate(self, name: str, statements: list[str] | str,
               params: tuple = ()) -> Path:
        path = self.fresh(name)
        db = sqlite3.connect(str(path))
        if isinstance(statements, str):
            statements = [statements]
        for statement in statements:
            db.execute(statement, params)
        db.commit()
        db.close()
        return path

    def mutate_response(self, name: str, command_id: str, change) -> Path:
        path = self.fresh(name)
        db = sqlite3.connect(str(path))
        response = json.loads(db.execute(
            "SELECT response FROM commands WHERE command_id = ?", (command_id,)).fetchone()[0])
        change(response)
        db.execute("UPDATE commands SET response = ? WHERE command_id = ?",
                   (json.dumps(response), command_id))
        db.commit()
        db.close()
        return path

    def codes(self, body: dict) -> set[str]:
        return {error["code"] for error in body["errors"]}

    # --- success -----------------------------------------------------------

    def test_healthy_ledger_reports_counts(self) -> None:
        code, body, err = self.verify(self.fresh())
        self.assertEqual((code, err), (0, ""))
        self.assertEqual(body, {"ok": True, "events": 6, "max_cursor": 6})

    def test_empty_ledger_reports_zero(self) -> None:
        path = self.dir / "empty.sqlite"
        led = Ledger(str(path))
        led.close()
        code, body, err = self.verify(path)
        self.assertEqual((code, err), (0, ""))
        self.assertEqual(body, {"ok": True, "events": 0, "max_cursor": 0})

    def test_verifier_creates_no_side_files(self) -> None:
        path = self.fresh()
        before = {p.name for p in self.dir.iterdir()}
        self.verify(path)
        self.verify(path)
        self.assertEqual(before, {p.name for p in self.dir.iterdir()})

    def test_verifier_does_not_modify_bytes(self) -> None:
        path = self.fresh()
        digest = path.read_bytes()
        self.verify(path)
        self.assertEqual(digest, path.read_bytes())

    # --- usage / unreadable (exit 2) --------------------------------------

    def test_no_or_extra_args_are_usage_error(self) -> None:
        proc = subprocess.run([sys.executable, "-m", "eventledger.verify"],
                              capture_output=True, text=True, env=ENV)
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(json.loads(proc.stdout)["errors"][0]["code"], "usage_error")
        proc = subprocess.run([sys.executable, "-m", "eventledger.verify", "a", "b"],
                              capture_output=True, text=True, env=ENV)
        self.assertEqual(proc.returncode, 2)
        self.assertEqual(json.loads(proc.stdout)["errors"][0]["code"], "usage_error")

    def test_missing_file_is_unreadable(self) -> None:
        code, body, err = self.verify(self.dir / "nope.sqlite")
        self.assertEqual((code, err), (2, ""))
        self.assertEqual(body["errors"][0]["code"], "database_unreadable")

    def test_non_sqlite_file_is_unreadable(self) -> None:
        path = self.dir / "junk"
        path.write_text("not a database")
        code, body, err = self.verify(path)
        self.assertEqual((code, err), (2, ""))
        self.assertEqual(body["errors"][0]["code"], "database_unreadable")

    def test_legacy_cursor_schema_is_unreadable_and_not_upgraded(self) -> None:
        path = self.dir / "legacy.sqlite"
        db = sqlite3.connect(str(path))
        db.executescript(
            "CREATE TABLE events (stream_id TEXT NOT NULL, version INTEGER NOT NULL, "
            "event_id TEXT NOT NULL, type TEXT NOT NULL, payload TEXT NOT NULL, "
            "PRIMARY KEY (stream_id, version));"
            "CREATE TABLE commands (command_id TEXT PRIMARY KEY, stream_id TEXT NOT NULL, "
            "fingerprint TEXT NOT NULL, request TEXT NOT NULL, response TEXT NOT NULL, "
            "kind TEXT NOT NULL DEFAULT 'append');"
            "CREATE TABLE snapshots (stream_id TEXT NOT NULL, version INTEGER NOT NULL, "
            "state TEXT NOT NULL, PRIMARY KEY (stream_id, version));"
            "CREATE TABLE event_sequence (id INTEGER PRIMARY KEY CHECK (id=1), "
            "next_cursor INTEGER NOT NULL);"
            "INSERT INTO events VALUES ('s1', 1, 'e1', 'OrderPlaced', '{}');"
            "INSERT INTO event_sequence VALUES (1, 1);")
        db.commit()
        db.close()
        size = path.stat().st_size
        code, body, _ = self.verify(path)
        self.assertEqual(code, 2)
        self.assertEqual(body["errors"][0]["code"], "database_unreadable")
        # Never upgraded: the cursor column is still absent and size unchanged.
        self.assertEqual(path.stat().st_size, size)
        columns = {row[1] for row in sqlite3.connect(str(path)).execute(
            "PRAGMA table_info(events)")}
        self.assertNotIn("cursor", columns)

    def test_legacy_kind_schema_is_unreadable(self) -> None:
        path = self.dir / "legacy_kind.sqlite"
        db = sqlite3.connect(str(path))
        db.executescript(
            "CREATE TABLE events (stream_id TEXT NOT NULL, version INTEGER NOT NULL, "
            "event_id TEXT NOT NULL, type TEXT NOT NULL, payload TEXT NOT NULL, "
            "cursor INTEGER NOT NULL, PRIMARY KEY (stream_id, version));"
            "CREATE TABLE commands (command_id TEXT PRIMARY KEY, stream_id TEXT NOT NULL, "
            "fingerprint TEXT NOT NULL, request TEXT NOT NULL, response TEXT NOT NULL);"
            "CREATE TABLE snapshots (stream_id TEXT NOT NULL, version INTEGER NOT NULL, "
            "state TEXT NOT NULL, PRIMARY KEY (stream_id, version));"
            "CREATE TABLE event_sequence (id INTEGER PRIMARY KEY CHECK (id=1), "
            "next_cursor INTEGER NOT NULL);")
        db.commit()
        db.close()
        code, body, _ = self.verify(path)
        self.assertEqual(code, 2)
        self.assertEqual(body["errors"][0]["code"], "database_unreadable")

    # --- corrupted facts (exit 1) -----------------------------------------

    def assert_corrupt(self, path: Path, expected: str) -> None:
        code, body, err = self.verify(path)
        self.assertEqual((code, err), (1, ""))
        self.assertFalse(body["ok"])
        self.assertIn(expected, self.codes(body))
        # Errors are stably sorted by (code, detail).
        details = [(e["code"], e["detail"]) for e in body["errors"]]
        self.assertEqual(details, sorted(details))

    def test_unknown_event_type(self) -> None:
        self.assert_corrupt(self.mutate("t.sqlite", "UPDATE events SET type='Bogus' WHERE cursor=6"),
                            "invalid_event")

    def test_non_object_payload(self) -> None:
        self.assert_corrupt(
            self.mutate("t.sqlite", "UPDATE events SET payload='5' WHERE cursor=1"),
            "invalid_event")

    def test_invalid_json_payload(self) -> None:
        self.assert_corrupt(
            self.mutate("t.sqlite", "UPDATE events SET payload='{bad' WHERE cursor=1"),
            "invalid_event")

    def test_duplicate_event_id(self) -> None:
        self.assert_corrupt(
            self.mutate("t.sqlite",
                        "UPDATE events SET event_id=(SELECT event_id FROM events WHERE cursor=1) "
                        "WHERE cursor=2"),
            "invalid_event")

    def test_empty_event_id(self) -> None:
        self.assert_corrupt(
            self.mutate("t.sqlite", "UPDATE events SET event_id='' WHERE cursor=1"),
            "invalid_event")

    def test_version_gap(self) -> None:
        path = self.mutate("t.sqlite", [
            "DELETE FROM commands", "DELETE FROM snapshots",
            "DELETE FROM events WHERE cursor=2",
            "UPDATE events SET cursor=cursor-1 WHERE cursor>2",
            "UPDATE event_sequence SET next_cursor=5 WHERE id=1"])
        self.assert_corrupt(path, "version_gap")

    def test_cursor_gap(self) -> None:
        self.assert_corrupt(
            self.mutate("t.sqlite", "UPDATE events SET cursor=7 WHERE cursor=6"),
            "cursor_gap")

    def test_sequence_mismatch(self) -> None:
        self.assert_corrupt(
            self.mutate("t.sqlite", "UPDATE event_sequence SET next_cursor=7 WHERE id=1"),
            "sequence_mismatch")

    def test_invalid_snapshot_state_json(self) -> None:
        self.assert_corrupt(
            self.mutate("t.sqlite", "UPDATE snapshots SET state='nope' WHERE stream_id='order-1'"),
            "invalid_snapshot")

    def test_snapshot_points_at_missing_version(self) -> None:
        self.assert_corrupt(
            self.mutate("t.sqlite",
                        "UPDATE snapshots SET version=99 WHERE stream_id='order-1'"),
            "invalid_snapshot")

    def test_snapshot_for_unknown_stream(self) -> None:
        self.assert_corrupt(
            self.mutate("t.sqlite",
                        "INSERT INTO snapshots (stream_id, version, state) VALUES ('ghost', 1, '{}')"),
            "invalid_snapshot")

    def test_snapshot_state_mismatch(self) -> None:
        wrong = json.dumps({"status": "cancelled", "lines": [], "notes": [], "cancelled": True})
        self.assert_corrupt(
            self.mutate("t.sqlite",
                        "UPDATE snapshots SET state=? WHERE stream_id='order-1' AND version=3",
                        (wrong,)),
            "snapshot_mismatch")

    def test_append_fingerprint_mismatch(self) -> None:
        self.assert_corrupt(
            self.mutate("t.sqlite", "UPDATE commands SET fingerprint='x' WHERE command_id='cmd-1'"),
            "invalid_command")

    def test_transaction_fingerprint_mismatch_on_reordered_streams(self) -> None:
        path = self.fresh("t.sqlite")
        db = sqlite3.connect(str(path))
        request = json.loads(db.execute(
            "SELECT request FROM commands WHERE command_id='tx-1'").fetchone()[0])
        request["streams"] = [request["streams"][1], request["streams"][0]]
        db.execute("UPDATE commands SET request=? WHERE command_id='tx-1'",
                   (json.dumps(request),))
        db.commit()
        db.close()
        self.assert_corrupt(path, "invalid_command")

    def test_unknown_command_kind(self) -> None:
        self.assert_corrupt(
            self.mutate("t.sqlite", "UPDATE commands SET kind='bogus' WHERE command_id='cmd-1'"),
            "invalid_command")

    def test_command_request_not_json(self) -> None:
        self.assert_corrupt(
            self.mutate("t.sqlite", "UPDATE commands SET request='nope' WHERE command_id='cmd-1'"),
            "invalid_command")

    def test_response_version_mismatch(self) -> None:
        path = self.mutate_response(
            "t.sqlite", "cmd-1", lambda response: response.__setitem__("version", 9))
        self.assert_corrupt(path, "command_response_mismatch")

    def test_response_event_id_mismatch(self) -> None:
        path = self.mutate_response(
            "t.sqlite", "cmd-2", lambda response: response["events"][0].__setitem__(
                "event_id", "fake"))
        self.assert_corrupt(path, "command_response_mismatch")

    def test_response_payload_mismatch(self) -> None:
        path = self.mutate_response(
            "t.sqlite", "cmd-2", lambda response: response["events"][0].__setitem__(
                "payload", {"text": "changed"}))
        self.assert_corrupt(path, "command_response_mismatch")

    def test_response_must_not_carry_cursor(self) -> None:
        path = self.mutate_response(
            "t.sqlite", "cmd-2", lambda response: response["events"][0].__setitem__(
                "cursor", 3))
        code, body, err = self.verify(path)
        self.assertEqual((code, err), (1, ""))
        self.assertTrue(any("no cursor" in e["detail"] for e in body["errors"]))

    def test_transaction_response_stream_order_mismatch(self) -> None:
        def swap(response: dict) -> None:
            response["streams"][0], response["streams"][1] = (
                response["streams"][1], response["streams"][0])
        self.assert_corrupt(self.mutate_response("t.sqlite", "tx-1", swap),
                            "command_response_mismatch")

    def test_omitted_payload_is_still_a_valid_command(self) -> None:
        # The writer fills a missing payload with {}; the verifier must clean
        # the persisted request the same way instead of flagging a false error.
        path = self.dir / "omit.sqlite"
        led = Ledger(str(path))
        led.append("s1", [{"type": "OrderPlaced"}], 0, command_id="c1")
        led.close()
        code, body, err = self.verify(path)
        self.assertEqual((code, err), (0, ""), body)
        self.assertTrue(body["ok"])


if __name__ == "__main__":
    unittest.main()
