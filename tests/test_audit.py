"""Tests for the read-only global audit entry point (GET /events).

Covers strict query parsing, dense strictly-increasing global cursors,
pagination stability under concurrent cross-stream appends, exclusion of
snapshots/idempotency records, persistence across restart, and the one-time
migration that numbers pre-audit events by their historical write order.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from eventledger import (  # noqa: F401
    InvalidRequest,
    Ledger,
    LedgerError,
    parse_audit_query,
    replay,
    serve,
)


class ParseAuditQueryTests(unittest.TestCase):
    def test_defaults(self) -> None:
        self.assertEqual(parse_audit_query(""), (0, 100))
        self.assertEqual(parse_audit_query("after=5"), (5, 100))
        self.assertEqual(parse_audit_query("limit=20"), (0, 20))
        self.assertEqual(parse_audit_query("after=5&limit=20"), (5, 20))
        # Parameter order is irrelevant.
        self.assertEqual(parse_audit_query("limit=20&after=5"), (5, 20))

    def test_plain_and_encoded_decimals_including_leading_zeros(self) -> None:
        self.assertEqual(parse_audit_query("after=0"), (0, 100))
        self.assertEqual(parse_audit_query("after=00"), (0, 100))
        self.assertEqual(parse_audit_query("after=012"), (12, 100))
        self.assertEqual(parse_audit_query("after=%31"), (1, 100))
        self.assertEqual(parse_audit_query("limit=0001"), (0, 1))
        self.assertEqual(parse_audit_query("limit=1000"), (0, 1000))

    def test_invalid_after_shapes(self) -> None:
        bad = ["after=", "after", "after=+1", "after=-1", "after=-0", "after=1.0",
               "after=1e1", "after=1E1", "after=0x1", "after=foo", "after=true",
               "after=false", "after=%201", "after=1%20", "after=%2B1", "after=%2D1",
               "after=%D9%A1", "after=1&after=2", "after=2&after=2"]
        for query in bad:
            with self.subTest(query=query):
                with self.assertRaises(InvalidRequest):
                    parse_audit_query(query)

    def test_invalid_limit_shapes(self) -> None:
        bad = ["limit=", "limit", "limit=0", "limit=+1", "limit=-1", "limit=1.0",
               "limit=1e2", "limit=0x1", "limit=abc", "limit=true", "limit=false",
               "limit=%202", "limit=2%20", "limit=%2B2", "limit=%D9%A2",
               "limit=2&limit=3", "limit=1001", "limit=01001"]
        for query in bad:
            with self.subTest(query=query):
                with self.assertRaises(InvalidRequest):
                    parse_audit_query(query)

    def test_unknown_parameter_is_rejected(self) -> None:
        for query in ["foo=1", "after=1&foo=2", "limit=2&since=3", "events=1",
                      "cursor=1", "AFTER=1", "After=1"]:
            with self.subTest(query=query):
                with self.assertRaises(InvalidRequest):
                    parse_audit_query(query)

    def test_empty_chunks_are_ignored_but_unknown_keys_are_not(self) -> None:
        self.assertEqual(parse_audit_query("&"), (0, 100))
        self.assertEqual(parse_audit_query("after=1&"), (1, 100))
        with self.assertRaises(InvalidRequest):
            parse_audit_query("after=1&foo=")


class GlobalCursorUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger(":memory:")

    def tearDown(self) -> None:
        self.ledger.close()

    def test_cursors_are_dense_across_streams_and_batches(self) -> None:
        self.ledger.append("a", [{"type": "OrderPlaced", "payload": {}},
                                 {"type": "NoteRecorded", "payload": {"text": "x"}}], 0)
        self.ledger.append("b", [{"type": "LineItemAdded", "payload": {"sku": "s"}}], 0)
        self.ledger.append("a", [{"type": "OrderCancelled", "payload": {}}], 2)
        events, has_more = self.ledger.read_global(0, 100)
        self.assertEqual([(e.stream_id, e.version, e.cursor) for e in events],
                         [("a", 1, 1), ("a", 2, 2), ("b", 1, 3), ("a", 3, 4)])
        self.assertFalse(has_more)
        # A batch's events take consecutive cursors and appear together.
        self.assertEqual([e.cursor for e in events if e.stream_id == "a"], [1, 2, 4])

    def test_audit_json_leads_with_cursor(self) -> None:
        self.ledger.append("a", [{"type": "OrderPlaced", "payload": {"total": 1}}], 0)
        events, _ = self.ledger.read_global(0, 10)
        self.assertEqual(list(events[0].as_audit_json().keys()),
                         ["cursor", "stream_id", "version", "event_id", "type", "payload"])
        # The per-stream view keeps its original, cursor-less shape.
        self.assertEqual(list(events[0].as_json().keys()),
                         ["stream_id", "version", "event_id", "type", "payload"])

    def test_paging_after_limit_and_has_more(self) -> None:
        self.ledger.append("a", [{"type": "OrderPlaced", "payload": {}}], 0)
        self.ledger.append("b", [{"type": "LineItemAdded", "payload": {}}], 0)
        self.ledger.append("c", [{"type": "NoteRecorded", "payload": {}}], 0)
        page, more = self.ledger.read_global(0, 2)
        self.assertEqual([e.cursor for e in page], [1, 2])
        self.assertTrue(more)
        page, more = self.ledger.read_global(2, 2)
        self.assertEqual([e.cursor for e in page], [3])
        # Partial last page: nothing beyond it.
        self.assertFalse(more)
        page, more = self.ledger.read_global(3, 2)
        self.assertEqual(page, [])
        self.assertFalse(more)

    def test_after_beyond_head_is_empty_with_has_more_false(self) -> None:
        self.ledger.append("a", [{"type": "OrderPlaced", "payload": {}}], 0)
        page, more = self.ledger.read_global(999, 10)
        self.assertEqual(page, [])
        self.assertFalse(more)

    def test_empty_ledger(self) -> None:
        page, more = self.ledger.read_global(0, 100)
        self.assertEqual(page, [])
        self.assertFalse(more)

    def test_conflicts_and_retries_burn_no_cursors(self) -> None:
        self.ledger.append("a", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="c1")
        # Idempotent retry: same response, no new cursor.
        retry = self.ledger.append("a", [{"type": "OrderPlaced", "payload": {}}], 0,
                                   command_id="c1")
        self.assertEqual([e.cursor for e in retry], [1])
        # Version conflict on another stream allocates nothing.
        with self.assertRaises(Exception):
            self.ledger.append("a", [{"type": "OrderCancelled", "payload": {}}], 0)
        events, _ = self.ledger.read_global(0, 100)
        self.assertEqual([e.cursor for e in events], [1])
        # The next genuine write continues at 2.
        nxt = self.ledger.append("b", [{"type": "OrderPlaced", "payload": {}}], 0)
        self.assertEqual(nxt[0].cursor, 2)

    def test_snapshots_and_commands_are_never_audited(self) -> None:
        self.ledger.append("a", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="cmd")
        self.ledger.snapshot("a", 1)
        # Retry touches the commands table only.
        self.ledger.append("a", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="cmd")
        events, _ = self.ledger.read_global(0, 100)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].cursor, 1)

    def test_per_stream_reads_and_replay_are_unchanged(self) -> None:
        batch = [
            {"type": "OrderPlaced", "payload": {"total": 10}},
            {"type": "LineItemAdded", "payload": {"sku": "a"}},
            {"type": "NoteRecorded", "payload": {"text": "hi"}},
            {"type": "OrderCancelled", "payload": {}},
        ]
        self.ledger.append("a", batch, 0)
        self.ledger.append("b", batch[:2], 0)
        self.assertEqual([e.version for e in self.ledger.read("a")], [1, 2, 3, 4])
        self.assertEqual(replay(self.ledger.read("a")),
                         {"status": "cancelled", "lines": [{"sku": "a"}],
                          "notes": ["hi"], "cancelled": True})
        self.assertEqual(replay(self.ledger.read_at("a", 2)),
                         {"status": "placed", "lines": [{"sku": "a"}],
                          "notes": [], "cancelled": False})

    def test_concurrent_appends_yield_one_dense_global_order(self) -> None:
        stream_count, batch_size = 8, 25

        def worker(worker_id: int) -> None:
            stream = f"s-{worker_id}"
            for _ in range(batch_size):
                # Single-event batches from many threads interleave aggressively;
                # expected_version is always 0 only for the first, so re-read on conflict.
                for expected in range(100):
                    current = self.ledger.version(stream)
                    try:
                        self.ledger.append(
                            stream, [{"type": "NoteRecorded", "payload": {"n": current}}], current)
                        break
                    except Exception:
                        continue

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(worker, range(stream_count)))
        events, more = self.ledger.read_global(0, 1000)
        total = stream_count * batch_size
        self.assertFalse(more)
        self.assertEqual([e.cursor for e in events], list(range(1, total + 1)))
        self.assertEqual(len({(e.stream_id, e.version) for e in events}), total)
        # Within each stream the global walk still sees contiguous versions.
        for worker_id in range(stream_count):
            versions = [e.version for e in events if e.stream_id == f"s-{worker_id}"]
            self.assertEqual(versions, list(range(1, batch_size + 1)))


def build_legacy_db(path: str, rows: list[tuple]) -> None:
    """Create a database in the pre-audit schema and write rows in order."""
    db = sqlite3.connect(path)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("""CREATE TABLE events (
        stream_id TEXT NOT NULL, version INTEGER NOT NULL, event_id TEXT NOT NULL,
        type TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (stream_id, version))""")
    db.execute("""CREATE TABLE commands (
        command_id TEXT PRIMARY KEY, stream_id TEXT NOT NULL, fingerprint TEXT NOT NULL,
        request TEXT NOT NULL, response TEXT NOT NULL)""")
    db.execute("""CREATE TABLE snapshots (
        stream_id TEXT NOT NULL, version INTEGER NOT NULL, state TEXT NOT NULL,
        PRIMARY KEY (stream_id, version))""")
    for row in rows:
        db.execute("INSERT INTO events VALUES (?, ?, ?, ?, ?)", row)
    db.commit()
    db.close()


LEGACY_ROWS = [
    ("a", 1, "id-a1", "OrderPlaced", json.dumps({"total": 10})),
    ("b", 1, "id-b1", "NoteRecorded", json.dumps({"text": "first"})),
    ("a", 2, "id-a2", "LineItemAdded", json.dumps({"sku": "x"})),
    ("b", 2, "id-b2", "NoteRecorded", json.dumps({"text": "second"})),
    ("c", 1, "id-c1", "OrderCancelled", json.dumps({})),
]


class MigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "ledger.sqlite")
        build_legacy_db(self.path, LEGACY_ROWS)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_numbers_existing_events_by_write_order_without_touching_facts(self) -> None:
        ledger = Ledger(self.path)
        events, more = ledger.read_global(0, 100)
        self.assertFalse(more)
        # rowid order == historical insertion order, interleaved across streams.
        self.assertEqual([(e.stream_id, e.version, e.cursor) for e in events],
                         [("a", 1, 1), ("b", 1, 2), ("a", 2, 3), ("b", 2, 4), ("c", 1, 5)])
        for event, (sid, version, event_id, kind, payload) in zip(events, LEGACY_ROWS):
            self.assertEqual((event.stream_id, event.version, event.event_id, event.type),
                             (sid, version, event_id, kind))
            self.assertEqual(event.payload, json.loads(payload))
        # Per-stream reads and deterministic replay are byte-for-byte unchanged.
        self.assertEqual([e.event_id for e in ledger.read("a")], ["id-a1", "id-a2"])
        self.assertEqual([e.version for e in ledger.read("b")], [1, 2])
        self.assertEqual(replay(ledger.read_at("a", 2)),
                         {"status": "placed", "lines": [{"sku": "x"}],
                          "notes": [], "cancelled": False})
        ledger.close()

    def test_cursors_survive_restart_and_new_events_extend_them(self) -> None:
        ledger = Ledger(self.path)
        ledger.close()
        ledger = Ledger(self.path)
        events, _ = ledger.read_global(0, 100)
        self.assertEqual([e.cursor for e in events], [1, 2, 3, 4, 5])
        new = ledger.append("a", [{"type": "NoteRecorded", "payload": {"text": "new"}}], 2)
        self.assertEqual(new[0].cursor, 6)
        ledger.close()
        ledger = Ledger(self.path)
        events, _ = ledger.read_global(0, 100)
        self.assertEqual([e.cursor for e in events], [1, 2, 3, 4, 5, 6])
        self.assertEqual(events[-1].event_id, new[0].event_id)
        ledger.close()

    def test_concurrent_openers_build_one_consistent_order(self) -> None:
        ledgers: list[Ledger] = []
        lock = threading.Lock()

        def open_ledger(_: int) -> dict:
            ledger = Ledger(self.path)
            with lock:
                ledgers.append(ledger)
            events, _ = ledger.read_global(0, 100)
            return {(e.stream_id, e.version): e.cursor for e in events}

        with ThreadPoolExecutor(max_workers=8) as pool:
            views = list(pool.map(open_ledger, range(8)))
        expected = {(sid, version): index + 1
                    for index, (sid, version, *_rest) in enumerate(LEGACY_ROWS)}
        for view in views:
            self.assertEqual(view, expected)
        for ledger in ledgers:
            ledger.close()
        # Still exactly one dense order after the storm.
        ledger = Ledger(self.path)
        events, _ = ledger.read_global(0, 100)
        self.assertEqual([e.cursor for e in events], [1, 2, 3, 4, 5])
        ledger.close()

    def test_failed_migration_leaves_no_half_order_and_then_recovers(self) -> None:
        # A trigger that aborts any UPDATE on events sabotages the back-fill step.
        saboteur = sqlite3.connect(self.path)
        saboteur.execute(
            "CREATE TRIGGER abort_cursor_backfill AFTER UPDATE ON events "
            "BEGIN SELECT RAISE(ABORT, 'injected migration failure'); END")
        saboteur.commit()
        saboteur.close()

        with self.assertRaises(LedgerError) as caught:
            Ledger(self.path)
        self.assertEqual(caught.exception.code, "internal_error")

        # The DDL rolled back: no cursor column, no index, no seeded high-water mark.
        probe = sqlite3.connect(self.path)
        columns = {row[1] for row in probe.execute("PRAGMA table_info(events)")}
        self.assertNotIn("cursor", columns)
        self.assertEqual(probe.execute("SELECT COUNT(*) FROM events").fetchone()[0], 5)
        self.assertEqual(probe.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='events_cursor_idx'"
        ).fetchone()[0], 0)
        # event_sequence exists (base schema) but holds no row: the order was not built.
        self.assertEqual(probe.execute("SELECT COUNT(*) FROM event_sequence").fetchone()[0], 0)
        probe.close()

        # Remove the obstruction; a subsequent open completes the migration normally.
        repair = sqlite3.connect(self.path)
        repair.execute("DROP TRIGGER abort_cursor_backfill")
        repair.commit()
        repair.close()
        ledger = Ledger(self.path)
        events, _ = ledger.read_global(0, 100)
        self.assertEqual([e.cursor for e in events], [1, 2, 3, 4, 5])
        ledger.close()


class HttpAuditTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db_path = str(Path(cls.tmp.name) / "audit.sqlite")
        cls.server = serve(port=0, db=cls.db_path)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.ledger.close()  # type: ignore[attr-defined]
        cls.tmp.cleanup()

    def request(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                         method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def append(self, stream: str, events: list, expected: int, command_id: str | None = None) -> None:
        body = {"events": events, "expected_version": expected}
        if command_id is not None:
            body["command_id"] = command_id
        status, _ = self.request("POST", f"/streams/{stream}/events", body)
        self.assertEqual(status, 201)

    def test_ordered_cross_stream_page_shape(self) -> None:
        self.append("seed", [{"type": "OrderPlaced", "payload": {}},
                             {"type": "NoteRecorded", "payload": {"text": "n"}}], 0)
        self.append("other", [{"type": "LineItemAdded", "payload": {"sku": "s"}}], 0)
        status, body = self.request("GET", "/events?after=0&limit=1000")
        self.assertEqual(status, 200)
        seed = [e for e in body["events"] if e["stream_id"] == "seed"]
        other = [e for e in body["events"] if e["stream_id"] == "other"]
        # The two batch events are consecutive and the other stream follows them.
        self.assertEqual(seed[0]["version"], 1)
        self.assertEqual(seed[1]["cursor"], seed[0]["cursor"] + 1)
        self.assertEqual(other[0]["cursor"], seed[1]["cursor"] + 1)
        cursors = [e["cursor"] for e in body["events"]]
        self.assertEqual(cursors, sorted(cursors))
        for event in body["events"]:
            self.assertEqual(list(event.keys()),
                             ["cursor", "stream_id", "version", "event_id", "type", "payload"])

    def test_walk_next_cursor_with_concurrent_writer_never_skips_or_duplicates(self) -> None:
        base = "walk-a"
        for start in range(0, 50, 10):
            self.append(base, [{"type": "NoteRecorded", "payload": {"n": start + offset}}
                               for offset in range(10)], start)
        writer_done = threading.Event()

        def writer() -> None:
            try:
                stream = "walk-b"
                for index in range(60):
                    self.append(stream, [{"type": "NoteRecorded", "payload": {"n": index}}], index)
            finally:
                writer_done.set()

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            collected: list[int] = []
            after = 0
            for _ in range(1000):
                status, body = self.request("GET", f"/events?after={after}&limit=3")
                self.assertEqual(status, 200)
                events = body["events"]
                if events:
                    # Every page begins exactly one past the previous page's end.
                    self.assertEqual(events[0]["cursor"], after + 1)
                    collected.extend(e["cursor"] for e in events)
                    after = body["next_cursor"]
                elif not writer_done.is_set():
                    # Empty boundary while the writer is still in flight: the same
                    # after is retained and later commits surface on the next poll.
                    self.assertEqual(body["next_cursor"], after)
                    time.sleep(0.002)
                else:
                    self.assertFalse(body["has_more"])
                    break
        finally:
            writer_done.wait(5)
            thread.join()
        # Drain anything the writer committed after the reader's last poll.
        while True:
            status, body = self.request("GET", f"/events?after={after}&limit=1000")
            if not body["events"]:
                self.assertFalse(body["has_more"])
                break
            self.assertEqual(body["events"][0]["cursor"], after + 1)
            collected.extend(e["cursor"] for e in body["events"])
            after = body["next_cursor"]
            if not body["has_more"]:
                break
        self.assertEqual(collected, sorted(collected))
        self.assertEqual(len(collected), len(set(collected)))
        self.assertEqual(collected, list(range(1, len(collected) + 1)))

    def test_default_limit_is_100_and_max_is_1000(self) -> None:
        # Seed a self-contained 101-event prefix so this does not depend on
        # which sibling test ran first: global cursors are dense from 1.
        stream = "limit-stream"
        self.append(stream, [{"type": "NoteRecorded", "payload": {"k": i}} for i in range(100)], 0)
        self.append(stream, [{"type": "NoteRecorded", "payload": {"k": 100}}], 100)
        status, body = self.request("GET", "/events")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["events"]), 100)
        self.assertEqual(body["next_cursor"], 100)
        self.assertTrue(body["has_more"])
        status, body = self.request("GET", "/events?limit=1000")
        self.assertEqual(status, 200)
        head = body["next_cursor"]
        self.assertGreaterEqual(head, 101)
        self.assertFalse(body["has_more"])
        # after past the global head: empty page, after preserved, has_more false.
        status, body = self.request("GET", f"/events?after={head}")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"events": [], "next_cursor": head, "has_more": False})
        status, body = self.request("GET", "/events?after=99999999")
        self.assertEqual(body, {"events": [], "next_cursor": 99999999, "has_more": False})

    def test_invalid_queries_are_400(self) -> None:
        bad = ["limit=0", "limit=1001", "limit=true", "limit=1.0", "limit=1e2",
               "limit=abc", "limit=%202", "limit=%D9%A2", "limit=2&limit=3",
               "after=1.0", "after=-1", "after=true", "after=1e1", "after=%201",
               "after=%D9%A1", "after=1&after=2", "after=", "after",
               "foo=1", "after=1&foo=2", "limit=2&since=1", "AFTER=1"]
        for query in bad:
            status, body = self.request("GET", f"/events?{query}")
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), query)

    def test_unknown_routes_and_methods_still_404(self) -> None:
        self.assertEqual(self.request("GET", "/events/1")[0], 404)
        self.assertEqual(self.request("GET", "/event")[0], 404)
        self.assertEqual(self.request("POST", "/events", {"events": []})[0], 404)

    def test_snapshots_and_retries_do_not_appear_in_audit(self) -> None:
        self.append("audit-x", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="ax-1")
        self.request("POST", "/streams/audit-x/snapshots", {"at_version": 1})
        # Idempotent retry must not create a second audited event.
        self.append("audit-x", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="ax-1")
        status, body = self.request("GET", "/events?limit=1000")
        rows = [e for e in body["events"] if e["stream_id"] == "audit-x"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["version"], 1)

    def test_cursors_persist_across_ledger_reopen(self) -> None:
        self.append("persist", [{"type": "OrderPlaced", "payload": {}}], 0)
        status, before = self.request("GET", "/events?limit=1000")
        self.assertEqual(status, 200)
        # Open a second ledger over the same file (as a restarting process would),
        # without disturbing the running server: cursors must be identical.
        reopened = Ledger(self.db_path)
        try:
            events, more = reopened.read_global(0, 1000)
            self.assertFalse(more)
            self.assertEqual(
                [(e.cursor, e.stream_id, e.event_id) for e in events],
                [(e["cursor"], e["stream_id"], e["event_id"]) for e in before["events"]])
        finally:
            reopened.close()


if __name__ == "__main__":
    unittest.main()
