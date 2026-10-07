"""Tests for long-polling incremental subscription on GET /events.

Covers strict ``wait_ms`` parsing (0..30000, ASCII decimals only), immediate
pages when events already exist, wake-on-commit after a wait, timeout empty
pages, whole-batch visibility (single-stream and cross-stream transactions),
no-skip/no-duplicate paging while a writer appends continuously, the fact
that waiting consumes no cursor and writes nothing, non-interference with
concurrent reads and writes, commits made by another process on the same
file, and visibility after the timeout boundary.
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
from pathlib import Path

from eventledger import (  # noqa: F401
    InvalidRequest,
    Ledger,
    parse_events_query,
    serve,
)


class ParseEventsQueryTests(unittest.TestCase):
    def test_defaults_and_combinations(self) -> None:
        self.assertEqual(parse_events_query(""), (0, 100, 0))
        self.assertEqual(parse_events_query("wait_ms=0"), (0, 100, 0))
        self.assertEqual(parse_events_query("after=5"), (5, 100, 0))
        self.assertEqual(parse_events_query("limit=20"), (0, 20, 0))
        self.assertEqual(parse_events_query("after=5&limit=20&wait_ms=1000"), (5, 20, 1000))
        # Parameter order is irrelevant.
        self.assertEqual(parse_events_query("wait_ms=1&limit=2&after=3"), (3, 2, 1))

    def test_leading_zeros_and_ascii_encoding(self) -> None:
        self.assertEqual(parse_events_query("wait_ms=00"), (0, 100, 0))
        self.assertEqual(parse_events_query("wait_ms=00012"), (0, 100, 12))
        self.assertEqual(parse_events_query("wait_ms=030000"), (0, 100, 30000))
        self.assertEqual(parse_events_query("wait_ms=%33%30%30%30%30"), (0, 100, 30000))

    def test_boundaries_accepted(self) -> None:
        self.assertEqual(parse_events_query("wait_ms=0")[2], 0)
        self.assertEqual(parse_events_query("wait_ms=30000")[2], 30000)

    def test_invalid_wait_ms_shapes(self) -> None:
        bad = ["wait_ms=", "wait_ms", "wait_ms=30001", "wait_ms=030001",
               "wait_ms=+1", "wait_ms=-1", "wait_ms=-0", "wait_ms=1.0", "wait_ms=0.0",
               "wait_ms=1e2", "wait_ms=1E2", "wait_ms=0x1", "wait_ms=abc",
               "wait_ms=true", "wait_ms=false", "wait_ms=null", "wait_ms=%201",
               "wait_ms=1%20", "wait_ms=%2B1", "wait_ms=%2D1", "wait_ms=%D9%A1",
               "wait_ms=999999999999999999999999", "wait_ms=1&wait_ms=2",
               "wait_ms=2&wait_ms=2"]
        for query in bad:
            with self.subTest(query=query):
                with self.assertRaises(InvalidRequest):
                    parse_events_query(query)

    def test_after_and_limit_rules_are_unchanged(self) -> None:
        for query in ["after=1.0", "after=-1", "after=true", "after=1&after=2", "after=",
                      "limit=0", "limit=1001", "limit=true", "limit=1.0", "limit=2&limit=3"]:
            with self.subTest(query=query):
                with self.assertRaises(InvalidRequest):
                    parse_events_query(query)
        self.assertEqual(parse_events_query("after=01&limit=0010&wait_ms=00"), (1, 10, 0))

    def test_unknown_parameter_still_rejected(self) -> None:
        for query in ["foo=1", "wait_ms=1&foo=2", "after=1&wait_ms=1&since=3",
                      "waitMS=1", "WAIT_MS=1", "after=1&foo="]:
            with self.subTest(query=query):
                with self.assertRaises(InvalidRequest):
                    parse_events_query(query)

    def test_empty_chunks_remain_ignored(self) -> None:
        self.assertEqual(parse_events_query("&wait_ms=5&"), (0, 100, 5))


class LedgerWaitUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger(":memory:")

    def tearDown(self) -> None:
        self.ledger.close()

    def append(self, stream: str, count: int, expected: int, command_id: str | None = None) -> None:
        self.ledger.append(
            stream, [{"type": "NoteRecorded", "payload": {"n": i}} for i in range(count)],
            expected, command_id=command_id)

    def test_events_present_return_immediately_even_with_large_wait(self) -> None:
        self.append("a", 2, 0)
        started = time.monotonic()
        events, has_more = self.ledger.read_global_wait(0, 100, 30_000)
        self.assertLess(time.monotonic() - started, 0.5)
        self.assertEqual([e.cursor for e in events], [1, 2])
        self.assertFalse(has_more)

    def test_wait_zero_on_empty_is_an_empty_page(self) -> None:
        events, has_more = self.ledger.read_global_wait(0, 100, 0)
        self.assertEqual(events, [])
        self.assertFalse(has_more)

    def test_times_out_with_empty_page(self) -> None:
        started = time.monotonic()
        events, has_more = self.ledger.read_global_wait(0, 100, 200)
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, 0.18)
        self.assertLess(elapsed, 1.0)
        self.assertEqual(events, [])
        self.assertFalse(has_more)

    def test_wakes_when_a_batch_arrives(self) -> None:
        def writer() -> None:
            time.sleep(0.2)
            self.append("a", 1, 0)

        thread = threading.Thread(target=writer)
        thread.start()
        started = time.monotonic()
        events, has_more = self.ledger.read_global_wait(0, 100, 5_000)
        elapsed = time.monotonic() - started
        thread.join()
        self.assertGreaterEqual(elapsed, 0.18)
        self.assertLess(elapsed, 2.0)
        self.assertEqual([e.cursor for e in events], [1])
        self.assertFalse(has_more)

    def test_whole_batch_arrives_together_then_next_wait_times_out(self) -> None:
        def writer() -> None:
            time.sleep(0.2)
            self.append("a", 4, 0)

        thread = threading.Thread(target=writer)
        thread.start()
        events, has_more = self.ledger.read_global_wait(0, 100, 5_000)
        thread.join()
        # The batch is one commit: all four cursors cross the boundary together.
        self.assertEqual([(e.stream_id, e.version, e.cursor) for e in events],
                         [("a", 1, 1), ("a", 2, 2), ("a", 3, 3), ("a", 4, 4)])
        self.assertFalse(has_more)
        started = time.monotonic()
        more, has_more = self.ledger.read_global_wait(4, 100, 200)
        self.assertGreaterEqual(time.monotonic() - started, 0.18)
        self.assertEqual(more, [])
        self.assertFalse(has_more)

    def test_event_committed_after_timeout_is_seen_on_retry(self) -> None:
        started = time.monotonic()
        events, _ = self.ledger.read_global_wait(0, 100, 200)
        self.assertGreaterEqual(time.monotonic() - started, 0.18)
        self.assertEqual(events, [])
        self.append("a", 1, 0)
        # Same after as the timed-out request: the late commit is visible now.
        events, has_more = self.ledger.read_global_wait(0, 100, 0)
        self.assertEqual([e.cursor for e in events], [1])
        self.assertFalse(has_more)

    def test_waiting_consumes_no_cursor(self) -> None:
        self.ledger.read_global_wait(0, 100, 150)
        written = self.ledger.append("a", [{"type": "OrderPlaced", "payload": {}}], 0)
        self.assertEqual(written[0].cursor, 1)

    def test_idempotent_retry_and_snapshot_do_not_wake_waiters(self) -> None:
        self.append("a", 1, 0, command_id="cmd-1")

        def noise() -> None:
            time.sleep(0.15)
            # Same command_id: stored response, no new cursor, no notification.
            retry = self.ledger.append(
                "a", [{"type": "NoteRecorded", "payload": {"n": 0}}], 0,
                command_id="cmd-1")
            self.assertEqual([e.cursor for e in retry], [1])
            # Snapshots are derived data and must not look like new events either.
            self.ledger.snapshot("a", 1)

        thread = threading.Thread(target=noise)
        thread.start()
        started = time.monotonic()
        events, has_more = self.ledger.read_global_wait(1, 100, 400)
        elapsed = time.monotonic() - started
        thread.join()
        self.assertGreaterEqual(elapsed, 0.35)
        self.assertEqual(events, [])
        self.assertFalse(has_more)

    def test_wait_does_not_block_appends(self) -> None:
        def waiter() -> None:
            self.ledger.read_global_wait(0, 100, 3_000)

        thread = threading.Thread(target=waiter)
        thread.start()
        time.sleep(0.15)
        started = time.monotonic()
        self.ledger.append("b", [{"type": "OrderPlaced", "payload": {}}], 0)
        # The parked waiter must not have been holding the append lock.
        self.assertLess(time.monotonic() - started, 0.5)
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())


class HttpSubscriptionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db_path = str(Path(cls.tmp.name) / "subscription.sqlite")
        cls.server = serve(port=0, db=cls.db_path)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.ledger.close()  # type: ignore[attr-defined]
        cls.tmp.cleanup()

    def request(self, method: str, path: str, body: dict | None = None,
                timeout: float = 10.0) -> tuple[int, dict, float]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                         method=method,
                                         headers={"Content-Type": "application/json"})
        started = time.monotonic()
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read() or b"{}")
                return response.status, payload, time.monotonic() - started
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}"), time.monotonic() - started

    def append(self, stream: str, events: list, expected: int, command_id: str | None = None) -> None:
        body = {"events": events, "expected_version": expected}
        if command_id is not None:
            body["command_id"] = command_id
        status, _, _ = self.request("POST", f"/streams/{stream}/events", body)
        self.assertEqual(status, 201)

    def head_cursor(self) -> int:
        status, body, _ = self.request("GET", "/events?limit=1000")
        self.assertEqual(status, 200)
        return body["next_cursor"] if body["events"] else 0

    # ----- immediate / default behaviour -------------------------------------

    def test_existing_events_return_immediately(self) -> None:
        self.append("imm", [{"type": "OrderPlaced", "payload": {}},
                            {"type": "NoteRecorded", "payload": {"text": "n"}}], 0)
        head = self.head_cursor()
        status, body, elapsed = self.request(
            "GET", f"/events?after={head - 2}&limit=100&wait_ms=30000")
        self.assertEqual(status, 200)
        self.assertLess(elapsed, 1.0)
        self.assertEqual(len(body["events"]), 2)
        self.assertEqual(body["next_cursor"], head)
        self.assertFalse(body["has_more"])
        for event in body["events"]:
            self.assertEqual(list(event.keys()),
                             ["cursor", "stream_id", "version", "event_id", "type", "payload"])

    def test_default_and_wait_zero_still_return_empty_page_at_once(self) -> None:
        # A fresh after beyond the head keeps the classic empty-page shape.
        head = self.head_cursor()
        for path in [f"/events?after={head}", f"/events?after={head}&wait_ms=0"]:
            with self.subTest(path=path):
                status, body, elapsed = self.request("GET", path)
                self.assertEqual(status, 200)
                self.assertLess(elapsed, 0.5)
                self.assertEqual(body, {"events": [], "next_cursor": head, "has_more": False})

    # ----- waiting, timeout and commit boundaries -----------------------------

    def test_request_waits_then_returns_the_arriving_event(self) -> None:
        head = self.head_cursor()

        def writer() -> None:
            time.sleep(0.3)
            self.append("arrive", [{"type": "OrderPlaced", "payload": {}}], 0)

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            status, body, elapsed = self.request(
                "GET", f"/events?after={head}&wait_ms=4000", timeout=8)
        finally:
            thread.join()
        self.assertEqual(status, 200)
        self.assertGreaterEqual(elapsed, 0.25)
        self.assertLess(elapsed, 2.0)
        self.assertEqual(len(body["events"]), 1)
        self.assertEqual(body["events"][0]["cursor"], head + 1)
        self.assertEqual(body["next_cursor"], head + 1)
        self.assertFalse(body["has_more"])

    def test_timeout_returns_empty_page_preserving_after(self) -> None:
        head = self.head_cursor()
        status, body, elapsed = self.request(
            "GET", f"/events?after={head}&wait_ms=300", timeout=5)
        self.assertEqual(status, 200)
        self.assertGreaterEqual(elapsed, 0.25)
        self.assertLess(elapsed, 1.5)
        self.assertEqual(body, {"events": [], "next_cursor": head, "has_more": False})

    def test_commit_after_timeout_boundary_is_visible_on_retry(self) -> None:
        head = self.head_cursor()
        status, body, _ = self.request(
            "GET", f"/events?after={head}&wait_ms=250", timeout=5)
        self.assertEqual((status, body["events"], body["next_cursor"], body["has_more"]),
                         (200, [], head, False))
        # Committed strictly after the timed-out response chose its boundary:
        # it must not have leaked into that response, but a same-after retry
        # must observe it immediately.
        self.append("late", [{"type": "NoteRecorded", "payload": {"text": "late"}}], 0)
        status, body, elapsed = self.request(
            "GET", f"/events?after={head}&wait_ms=0", timeout=5)
        self.assertEqual(status, 200)
        self.assertLess(elapsed, 0.5)
        self.assertEqual([e["cursor"] for e in body["events"]], [head + 1])
        self.assertEqual(body["next_cursor"], head + 1)
        self.assertFalse(body["has_more"])

    def test_whole_batch_committed_during_wait_enters_one_page(self) -> None:
        head = self.head_cursor()

        def writer() -> None:
            time.sleep(0.3)
            # One append batch = one commit; its four cursors are contiguous.
            self.append("batch", [{"type": "NoteRecorded", "payload": {"k": i}}
                                  for i in range(4)], 0)

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            status, body, elapsed = self.request(
                "GET", f"/events?after={head}&limit=100&wait_ms=4000", timeout=8)
        finally:
            thread.join()
        self.assertEqual(status, 200)
        self.assertGreaterEqual(elapsed, 0.25)
        self.assertEqual([e["cursor"] for e in body["events"]],
                         [head + 1, head + 2, head + 3, head + 4])
        self.assertTrue(all(e["stream_id"] == "batch" for e in body["events"]))
        self.assertEqual(body["next_cursor"], head + 4)
        self.assertFalse(body["has_more"])
        # Nothing else follows: the next long poll times out with an empty page.
        status, body, elapsed = self.request(
            "GET", f"/events?after={head + 4}&wait_ms=250", timeout=5)
        self.assertEqual(status, 200)
        self.assertGreaterEqual(elapsed, 0.2)
        self.assertEqual(body, {"events": [], "next_cursor": head + 4, "has_more": False})

    def test_cross_stream_transaction_arrives_as_one_atomic_batch(self) -> None:
        head = self.head_cursor()
        command_id = f"tx-wait-{head}"

        def writer() -> None:
            time.sleep(0.3)
            streams = [
                {"stream_id": "tx-a", "events": [{"type": "OrderPlaced", "payload": {}},
                                                 {"type": "NoteRecorded", "payload": {"n": 1}}],
                 "expected_version": 0},
                {"stream_id": "tx-b", "events": [{"type": "LineItemAdded", "payload": {"sku": "x"}},
                                                 {"type": "OrderCancelled", "payload": {}}],
                 "expected_version": 0},
            ]
            status, _, _ = self.request("POST", f"/transactions/{command_id}", {"streams": streams})
            self.assertEqual(status, 201)

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            status, body, elapsed = self.request(
                "GET", f"/events?after={head}&limit=100&wait_ms=4000", timeout=8)
        finally:
            thread.join()
        self.assertEqual(status, 200)
        self.assertGreaterEqual(elapsed, 0.25)
        self.assertEqual([e["cursor"] for e in body["events"]],
                         [head + 1, head + 2, head + 3, head + 4])
        self.assertEqual([e["stream_id"] for e in body["events"]],
                         ["tx-a", "tx-a", "tx-b", "tx-b"])
        self.assertEqual(body["next_cursor"], head + 4)
        self.assertFalse(body["has_more"])

    # ----- non-interference ---------------------------------------------------

    def test_long_poll_does_not_block_concurrent_reads_and_writes(self) -> None:
        head = self.head_cursor()
        result: dict[str, tuple] = {}

        def waiter() -> None:
            result["wait"] = self.request(
                "GET", f"/events?after={head}&wait_ms=8000", timeout=12)

        thread = threading.Thread(target=waiter)
        thread.start()
        time.sleep(0.3)
        # A write while the waiter is parked completes promptly and releases it.
        status, _, write_elapsed = self.request(
            "POST", "/streams/unblock/events",
            {"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0})
        self.assertEqual(status, 201)
        self.assertLess(write_elapsed, 1.0)
        status, body, read_elapsed = self.request("GET", "/health")
        self.assertEqual((status, body), (200, {"status": "ok"}))
        self.assertLess(read_elapsed, 1.0)
        thread.join(timeout=3)
        self.assertFalse(thread.is_alive())
        wait_status, wait_body, _ = result["wait"]
        self.assertEqual(wait_status, 200)
        self.assertEqual([e["cursor"] for e in wait_body["events"]], [head + 1])

    def test_waiting_writes_nothing_and_consumes_no_cursor(self) -> None:
        head = self.head_cursor()
        probe = sqlite3.connect(self.db_path)
        try:
            commands_before = probe.execute("SELECT COUNT(*) FROM commands").fetchone()[0]
            snapshots_before = probe.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
        finally:
            probe.close()
        status, body, _ = self.request(
            "GET", f"/events?after={head}&wait_ms=250", timeout=5)
        self.assertEqual(status, 200)
        self.assertEqual(body["events"], [])
        self.append("noconsume", [{"type": "OrderPlaced", "payload": {}}], 0)
        # The genuine append takes the very next cursor: the wait reserved none.
        status, body, _ = self.request("GET", f"/events?after={head}&wait_ms=0")
        self.assertEqual(status, 200)
        self.assertEqual([e["cursor"] for e in body["events"]], [head + 1])
        probe = sqlite3.connect(self.db_path)
        try:
            # The wait itself created neither idempotency records nor snapshots.
            self.assertEqual(probe.execute("SELECT COUNT(*) FROM commands").fetchone()[0],
                             commands_before)
            self.assertEqual(probe.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0],
                             snapshots_before)
        finally:
            probe.close()

    def test_commit_from_another_process_is_noticed(self) -> None:
        head = self.head_cursor()
        other = Ledger(self.db_path)

        def writer() -> None:
            time.sleep(0.3)
            try:
                other.append("other-process", [{"type": "OrderPlaced", "payload": {}}], 0)
            finally:
                other.close()

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            # The in-process condition cannot be signalled cross-process; the
            # periodic safety re-check must still surface the commit well before
            # the deadline.
            status, body, elapsed = self.request(
                "GET", f"/events?after={head}&wait_ms=5000", timeout=9)
        finally:
            thread.join()
        self.assertEqual(status, 200)
        self.assertGreaterEqual(elapsed, 0.25)
        self.assertLess(elapsed, 2.5)
        self.assertEqual([e["cursor"] for e in body["events"]], [head + 1])
        self.assertEqual(body["next_cursor"], head + 1)

    # ----- continuous concurrent pagination -----------------------------------

    def test_walk_with_wait_under_continuous_appends_never_skips_or_duplicates(self) -> None:
        head = self.head_cursor()
        total = 40
        writer_done = threading.Event()

        def writer() -> None:
            try:
                stream = "wait-walk"
                for index in range(total):
                    for _attempt in range(100):
                        status, _, _ = self.request(
                            "POST", f"/streams/{stream}/events",
                            {"events": [{"type": "NoteRecorded", "payload": {"n": index}}],
                             "expected_version": index})
                        if status == 201:
                            break
                        # Only a version race is retriable; anything else fails the test.
                        self.assertEqual(status, 409)
                    # A little pacing so the reader actually parks between pages.
                    time.sleep(0.005)
            finally:
                writer_done.set()

        thread = threading.Thread(target=writer)
        thread.start()
        collected: list[int] = []
        after = head
        pages = 0
        try:
            while after < head + total:
                status, body, _ = self.request(
                    "GET", f"/events?after={after}&limit=3&wait_ms=2000", timeout=8)
                self.assertEqual(status, 200)
                pages += 1
                events = body["events"]
                if events:
                    # Every page begins exactly one past the previous page's end,
                    # even while batches keep committing around its boundary.
                    self.assertEqual(events[0]["cursor"], after + 1)
                    collected.extend(e["cursor"] for e in events)
                    after = body["next_cursor"]
                else:
                    # Empty page: the writer may still be between appends; keep
                    # the same after and long-poll again. This must never skip:
                    # assert the server preserved the position.
                    self.assertEqual(body, {"events": [], "next_cursor": after,
                                            "has_more": False})
                self.assertLess(pages, 500)
        finally:
            writer_done.wait(5)
            thread.join()
        self.assertTrue(writer_done.is_set())
        self.assertEqual(collected, list(range(head + 1, head + total + 1)))

    # ----- strict HTTP validation ---------------------------------------------

    def test_invalid_wait_ms_is_400(self) -> None:
        bad = ["wait_ms=", "wait_ms", "wait_ms=30001", "wait_ms=030001", "wait_ms=-1",
               "wait_ms=+1", "wait_ms=1.0", "wait_ms=1e2", "wait_ms=0x1", "wait_ms=abc",
               "wait_ms=true", "wait_ms=false", "wait_ms=null", "wait_ms=%201",
               "wait_ms=1%20", "wait_ms=%D9%A1", "wait_ms=1&wait_ms=2",
               "wait_ms=99999999999999999999"]
        for query in bad:
            with self.subTest(query=query):
                status, body, _ = self.request("GET", f"/events?{query}")
                self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))

    def test_boundary_wait_ms_values_accepted(self) -> None:
        # Seed inside the test so the method is order-independent: an event
        # strictly past ``after`` makes even wait_ms=30000 return immediately.
        head = self.head_cursor()
        self.append("wait-boundary", [{"type": "OrderPlaced", "payload": {}}], 0)
        for raw in ["0", "00", "000", "30000", "030000"]:
            with self.subTest(wait_ms=raw):
                status, body, elapsed = self.request(
                    "GET", f"/events?after={head}&wait_ms={raw}")
                self.assertEqual(status, 200)
                self.assertLess(elapsed, 0.5)
                self.assertEqual([e["cursor"] for e in body["events"]], [head + 1])

    def test_unknown_parameters_and_old_404_routes_are_preserved(self) -> None:
        for query in ["foo=1", "wait_ms=1&foo=2", "after=0&wait_ms=1&since=3",
                      "waitMS=1", "WAIT_MS=1"]:
            with self.subTest(query=query):
                status, body, _ = self.request("GET", f"/events?{query}")
                self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertEqual(self.request("GET", "/events/1")[0], 404)
        self.assertEqual(self.request("GET", "/event?wait_ms=10")[0], 404)
        self.assertEqual(self.request("POST", "/events", {"events": []})[0], 404)


if __name__ == "__main__":
    unittest.main()
