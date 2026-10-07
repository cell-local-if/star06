"""Tests for the read-only verification entry point: ``python -m eventledger.verify <db>``.

The verifier diagnoses a SQLite file without starting the service or writing
anything; these tests cover the success/failure JSON contract, every fact
error code, the exit-code split (0 ok / 1 corrupt / 2 unusable or bad usage),
and the read-only/no-upgrade guarantees.
"""
from __future__ import annotations

import glob
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from eventledger import Ledger
from eventledger.verify import main, verify_database


def corrupt(path: str, *statements: str, drop_cursor_index: bool = False) -> None:
    """Mutate a closed ledger's file with raw SQL to simulate fact corruption."""
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    if drop_cursor_index:
        db.execute("DROP INDEX events_cursor_idx")
    for statement in statements:
        db.execute(statement)
    db.commit()
    db.close()


def ledger_event_json(path: str, stream_id: str) -> list[dict]:
    """Read a stream's persisted public event shapes straight from the file."""
    ledger = Ledger(path)
    try:
        return [event.as_json() for event in ledger.read(stream_id)]
    finally:
        ledger.close()


class VerifierFixture(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "ledger.sqlite")
        ledger = Ledger(self.path)
        ledger.append("order-1", [
            {"type": "OrderPlaced", "payload": {"total": 10}},
            {"type": "NoteRecorded", "payload": {"text": "hi"}},
        ], 0, command_id="cmd-1")
        ledger.transaction("tx-1", [{
            "stream_id": "order-2",
            "events": [{"type": "OrderPlaced", "payload": {}},
                       {"type": "LineItemAdded", "payload": {"sku": "x"}}],
            "expected_version": 0,
        }])
        ledger.snapshot("order-1", 2)
        ledger.snapshot("order-2", 0)
        ledger.close()

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def run_cli(self, *args: str) -> tuple[int, dict]:
        env = dict(os.environ, PYTHONPATH=os.path.join(os.path.dirname(__file__), "..", "src"))
        completed = subprocess.run(
            [sys.executable, "-m", "eventledger.verify", *args],
            capture_output=True, text=True, env=env)
        return completed.returncode, json.loads(completed.stdout)


class HealthyDatabaseTests(VerifierFixture):
    def test_healthy_database_reports_counts(self) -> None:
        code, body = self.run_cli(self.path)
        self.assertEqual((code, body), (0, {"ok": True, "events": 4, "max_cursor": 4}))

    def test_python_api_matches_cli(self) -> None:
        self.assertEqual(verify_database(self.path),
                         {"ok": True, "events": 4, "max_cursor": 4})

    def test_fresh_empty_database_is_consistent(self) -> None:
        empty = str(Path(self.tmp.name) / "empty.sqlite")
        Ledger(empty).close()
        code, body = self.run_cli(empty)
        self.assertEqual((code, body), (0, {"ok": True, "events": 0, "max_cursor": 0}))

    def test_main_returns_exit_codes(self) -> None:
        import contextlib
        import io
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(main([self.path]), 0)
            self.assertEqual(main(), 2)
            self.assertEqual(main([self.path, self.path]), 2)
            self.assertEqual(main([str(Path(self.tmp.name) / "missing.sqlite")]), 2)


class UsageAndUnreadableTests(VerifierFixture):
    def test_no_args_is_usage_error(self) -> None:
        code, body = self.run_cli()
        self.assertEqual(code, 2)
        self.assertEqual(body["error"]["code"], "usage_error")

    def test_too_many_args_is_usage_error(self) -> None:
        code, body = self.run_cli(self.path, self.path)
        self.assertEqual(code, 2)
        self.assertEqual(body["error"]["code"], "usage_error")

    def test_missing_path_is_database_unreadable(self) -> None:
        code, body = self.run_cli(str(Path(self.tmp.name) / "nope.sqlite"))
        self.assertEqual(code, 2)
        self.assertEqual(body["error"]["code"], "database_unreadable")

    def test_non_sqlite_file_is_database_unreadable(self) -> None:
        bogus = str(Path(self.tmp.name) / "bogus.sqlite")
        Path(bogus).write_text("not a database")
        code, body = self.run_cli(bogus)
        self.assertEqual(code, 2)
        self.assertEqual(body["error"]["code"], "database_unreadable")

    def test_pre_cursor_legacy_file_is_unreadable_and_not_upgraded(self) -> None:
        legacy = str(Path(self.tmp.name) / "legacy.sqlite")
        db = sqlite3.connect(legacy)
        db.execute("CREATE TABLE events (stream_id TEXT, version INTEGER, event_id TEXT, "
                   "type TEXT, payload TEXT, PRIMARY KEY (stream_id, version))")
        db.execute("INSERT INTO events VALUES ('s', 1, 'e', 'OrderPlaced', '{}')")
        db.execute("CREATE TABLE commands (command_id TEXT PRIMARY KEY, stream_id TEXT, "
                   "fingerprint TEXT, request TEXT, response TEXT)")
        db.execute("CREATE TABLE snapshots (stream_id TEXT, version INTEGER, state TEXT, "
                   "PRIMARY KEY (stream_id, version))")
        db.commit()
        db.close()
        before = Path(legacy).read_bytes()
        code, body = self.run_cli(legacy)
        self.assertEqual(code, 2)
        self.assertEqual(body["error"]["code"], "database_unreadable")
        # The verifier must never upgrade the file: bytes unchanged, no cursor
        # column and no event_sequence table afterwards.
        self.assertEqual(Path(legacy).read_bytes(), before)
        db = sqlite3.connect(legacy)
        columns = {row[1] for row in db.execute("PRAGMA table_info(events)")}
        tables = {row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        db.close()
        self.assertNotIn("cursor", columns)
        self.assertNotIn("event_sequence", tables)

    def test_pre_kind_legacy_file_is_unreadable(self) -> None:
        half = str(Path(self.tmp.name) / "half.sqlite")
        db = sqlite3.connect(half)
        db.execute("CREATE TABLE events (stream_id TEXT, version INTEGER, event_id TEXT, "
                   "type TEXT, payload TEXT, cursor INTEGER)")
        db.execute("CREATE TABLE commands (command_id TEXT PRIMARY KEY, stream_id TEXT, "
                   "fingerprint TEXT, request TEXT, response TEXT)")
        db.execute("CREATE TABLE snapshots (stream_id TEXT, version INTEGER, state TEXT)")
        db.execute("CREATE TABLE event_sequence (id INTEGER PRIMARY KEY CHECK (id = 1), "
                   "next_cursor INTEGER NOT NULL)")
        db.commit()
        db.close()
        code, body = self.run_cli(half)
        self.assertEqual(code, 2)
        self.assertEqual(body["error"]["code"], "database_unreadable")
        self.assertIn("kind", body["error"]["detail"])


class EventFactTests(VerifierFixture):
    def error_codes(self, *statements: str, drop_cursor_index: bool = False) -> set[str]:
        corrupt(self.path, *statements, drop_cursor_index=drop_cursor_index)
        code, body = self.run_cli(self.path)
        self.assertEqual(code, 1)
        self.assertFalse(body["ok"])
        return {error["code"] for error in body["errors"]}

    def test_unknown_type(self) -> None:
        self.assertEqual(
            self.error_codes("UPDATE events SET type='Nope' WHERE cursor=1"),
            {"invalid_event"})

    def test_non_object_payload(self) -> None:
        self.assertEqual(
            self.error_codes("UPDATE events SET payload='[1, 2]' WHERE cursor=1"),
            {"invalid_event"})

    def test_unparseable_payload(self) -> None:
        self.assertEqual(
            self.error_codes("UPDATE events SET payload='{bad' WHERE cursor=1"),
            {"invalid_event"})

    def test_empty_event_id(self) -> None:
        self.assertEqual(
            self.error_codes("UPDATE events SET event_id='' WHERE cursor=1"),
            {"invalid_event"})

    def test_duplicate_event_id(self) -> None:
        self.assertEqual(
            self.error_codes(
                "UPDATE events SET event_id=(SELECT event_id FROM events WHERE cursor=1) "
                "WHERE cursor=2"),
            {"invalid_event"})

    def test_version_gap(self) -> None:
        ledger = Ledger(self.path)
        ledger.append("g", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="g1")
        ledger.append("g", [{"type": "NoteRecorded", "payload": {"text": "m"}}], 1,
                      command_id="g2")
        ledger.append("g", [{"type": "OrderCancelled", "payload": {}}], 2, command_id="g3")
        ledger.close()
        self.assertIn(
            "version_gap",
            self.error_codes("DELETE FROM events WHERE stream_id='g' AND version=2",
                             "DELETE FROM commands WHERE command_id='g2'"))

    def test_cursor_gap(self) -> None:
        self.assertEqual(
            self.error_codes("UPDATE events SET cursor=99 WHERE cursor=2"),
            {"cursor_gap"})

    def test_duplicate_cursor(self) -> None:
        self.assertEqual(
            self.error_codes("UPDATE events SET cursor=1 WHERE cursor=2",
                             drop_cursor_index=True),
            {"cursor_gap"})

    def test_sequence_high_water_mismatch(self) -> None:
        self.assertEqual(
            self.error_codes("UPDATE event_sequence SET next_cursor=99"),
            {"sequence_mismatch"})


class SnapshotFactTests(VerifierFixture):
    def error_codes(self, *statements: str) -> set[str]:
        corrupt(self.path, *statements)
        code, body = self.run_cli(self.path)
        self.assertEqual(code, 1)
        return {error["code"] for error in body["errors"]}

    def test_snapshot_points_at_missing_version(self) -> None:
        self.assertEqual(
            self.error_codes("UPDATE snapshots SET version=99"),
            {"invalid_snapshot"})

    def test_snapshot_points_at_unknown_stream(self) -> None:
        self.assertEqual(
            self.error_codes("UPDATE snapshots SET stream_id='ghost' WHERE version=2"),
            {"invalid_snapshot"})

    def test_snapshot_state_is_not_json(self) -> None:
        self.assertEqual(
            self.error_codes("UPDATE snapshots SET state='{bad'"),
            {"invalid_snapshot"})

    def test_snapshot_state_disagrees_with_replay(self) -> None:
        wrong = json.dumps({"status": "cancelled", "lines": [], "notes": [],
                            "cancelled": True})
        self.assertEqual(
            self.error_codes(f"UPDATE snapshots SET state='{wrong}' WHERE version=2"),
            {"snapshot_mismatch"})

    def test_version_zero_snapshot_is_replay_of_empty_prefix(self) -> None:
        # The healthy fixture already contains a version-0 snapshot; a wrong
        # state on it must be snapshot_mismatch.
        wrong = json.dumps({"status": "placed", "lines": [], "notes": [],
                            "cancelled": False})
        self.assertEqual(
            self.error_codes(f"UPDATE snapshots SET state='{wrong}' WHERE version=0"),
            {"snapshot_mismatch"})


class CommandFactTests(VerifierFixture):
    def error_codes(self, command_id: str, statement: str) -> set[str]:
        corrupt(self.path, f"UPDATE commands SET {statement} WHERE command_id='{command_id}'")
        code, body = self.run_cli(self.path)
        self.assertEqual(code, 1)
        return {error["code"] for error in body["errors"]}

    def test_unknown_kind(self) -> None:
        self.assertEqual(self.error_codes("cmd-1", "kind='weird'"), {"invalid_command"})

    def test_fingerprint_mismatch(self) -> None:
        self.assertEqual(self.error_codes("cmd-1", "fingerprint='wrong'"),
                         {"invalid_command"})

    def test_unparseable_persisted_request(self) -> None:
        self.assertEqual(self.error_codes("cmd-1", "request='{bad'"),
                         {"invalid_command"})

    def test_transaction_request_reordered_changes_fingerprint(self) -> None:
        reordered = json.dumps({"streams": [{
            "stream_id": "order-2",
            "events": [{"type": "LineItemAdded", "payload": {"sku": "x"}},
                       {"type": "OrderPlaced", "payload": {}}],
            "expected_version": 0,
        }]})
        self.assertEqual(
            self.error_codes("tx-1", f"request='{reordered}'"),
            {"invalid_command"})

    def test_response_event_id_mismatch(self) -> None:
        self.assertEqual(
            self.error_codes("cmd-1",
                             "response=JSON_SET(response,'$.events[0].event_id','fake')"),
            {"command_response_mismatch"})

    def test_response_payload_mismatch(self) -> None:
        self.assertEqual(
            self.error_codes("cmd-1",
                             "response=JSON_SET(response,'$.events[0].payload','{}')"),
            {"command_response_mismatch"})

    def test_response_version_mismatch(self) -> None:
        self.assertEqual(
            self.error_codes("cmd-1", "response=JSON_SET(response,'$.version',99)"),
            {"command_response_mismatch"})

    def test_response_interval_must_follow_expected_version(self) -> None:
        # Build a stream with 3 events, then retarget the command response at
        # versions 2..3 (real on-file events) while expected_version stays 0:
        # every named event matches, but the interval must still start at 1.
        path = str(Path(self.tmp.name) / "interval.sqlite")
        ledger = Ledger(path)
        ledger.append("s", [
            {"type": "OrderPlaced", "payload": {}},
            {"type": "NoteRecorded", "payload": {"text": "a"}},
            {"type": "NoteRecorded", "payload": {"text": "b"}},
        ], 0, command_id="c")
        ledger.close()
        events = ledger_event_json(path, "s")
        db = sqlite3.connect(path)
        db.execute("PRAGMA journal_mode=DELETE")
        tampered = {"version": 3, "events": events[1:]}  # claims versions 2..3
        db.execute("UPDATE commands SET response=? WHERE command_id='c'",
                   (json.dumps(tampered),))
        db.commit()
        db.close()
        env = dict(os.environ, PYTHONPATH=os.path.join(os.path.dirname(__file__), "..", "src"))
        completed = subprocess.run([sys.executable, "-m", "eventledger.verify", path],
                                   capture_output=True, text=True, env=env)
        body = json.loads(completed.stdout)
        self.assertEqual(completed.returncode, 1)
        self.assertEqual({e["code"] for e in body["errors"]},
                         {"command_response_mismatch"})

    def test_commands_stream_id_must_match_request(self) -> None:
        self.assertEqual(
            self.error_codes("cmd-1", "stream_id='order-2'"),
            {"invalid_command"})

    def test_response_event_must_not_carry_cursor(self) -> None:
        self.assertEqual(
            self.error_codes("cmd-1",
                             "response=JSON_SET(response,'$.events[0].cursor',1)"),
            {"command_response_mismatch"})

    def test_transaction_stream_version_mismatch(self) -> None:
        self.assertEqual(
            self.error_codes("tx-1",
                             "response=JSON_SET(response,'$.streams[0].version',99)"),
            {"command_response_mismatch"})

    def test_transaction_event_must_not_carry_cursor(self) -> None:
        self.assertEqual(
            self.error_codes("tx-1",
                             "response=JSON_SET(response,'$.streams[0].events[0].cursor',3)"),
            {"command_response_mismatch"})


class ErrorOrderingTests(VerifierFixture):
    def test_errors_sort_by_code_then_detail(self) -> None:
        corrupt(self.path,
                "UPDATE events SET type='Nope' WHERE cursor=1",
                "UPDATE event_sequence SET next_cursor=99")
        code, body = self.run_cli(self.path)
        self.assertEqual(code, 1)
        errors = body["errors"]
        keys = [(error["code"], error["detail"]) for error in errors]
        self.assertEqual(keys, sorted(keys))
        self.assertEqual([error["code"] for error in errors],
                         ["invalid_event", "sequence_mismatch"])


class ReadOnlyGuaranteeTests(VerifierFixture):
    def test_verify_changes_no_bytes_and_creates_no_side_files(self) -> None:
        # The fixture's Ledger is closed, so its WAL is fully checkpointed:
        # verification must be byte-for-byte side-effect free.
        db = sqlite3.connect(self.path)
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        db.execute("PRAGMA journal_mode=DELETE")
        db.commit()
        db.close()
        for side in glob.glob(self.path + "-*"):
            os.remove(side)
        before = Path(self.path).read_bytes()
        code, body = self.run_cli(self.path)
        self.assertEqual((code, body["ok"]), (0, True))
        self.assertEqual(Path(self.path).read_bytes(), before)
        self.assertEqual(sorted(glob.glob(self.path + "*")), [self.path])

    def test_verify_is_consistent_against_a_live_writer(self) -> None:
        live = str(Path(self.tmp.name) / "live.sqlite")
        ledger = Ledger(live)
        ledger.append("live", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="seed")
        stop = False

        def writer() -> None:
            version = 0
            while not stop:
                version += 1
                try:
                    ledger.append(
                        "live",
                        [{"type": "NoteRecorded", "payload": {"text": str(version)}}],
                        version - 1, command_id=f"live-{version}")
                except Exception:
                    pass
                time.sleep(0.001)

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            for _ in range(25):
                result = verify_database(live)
                self.assertTrue(result["ok"], result)
                self.assertEqual(result["events"], result["max_cursor"])
        finally:
            stop = True
            thread.join()
            ledger.close()


if __name__ == "__main__":
    unittest.main()
