"""Tests for long-polling incremental subscription on GET /events.

Covers the strict ``wait_ms`` parsing rules, immediate return when events are
already committed, waking on a commit that lands during the wait, the empty
timeout page, whole-batch visibility, non-blocking behaviour towards other
requests, and no-skip/no-duplicate paging while appends continue.
"""
from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from eventledger import InvalidRequest, Ledger, parse_audit_query, serve


class ParseWaitMsTests(unittest.TestCase):
    def test_defaults_and_valid_shapes(self) -> None:
        self.assertEqual(parse_audit_query(""), (0, 100, 0))
        self.assertEqual(parse_audit_query("wait_ms=0"), (0, 100, 0))
        self.assertEqual(parse_audit_query("wait_ms=1"), (0, 100, 1))
        self.assertEqual(parse_audit_query("wait_ms=30000"), (0, 100, 30000))
        # Leading zeros and percent-encoded ASCII digits are fine.
        self.assertEqual(parse_audit_query("wait_ms=007"), (0, 100, 7))
        self.assertEqual(parse_audit_query("wait_ms=000"), (0, 100, 0))
        self.assertEqual(parse_audit_query("wait_ms=030000"), (0, 100, 30000))
        self.assertEqual(parse_audit_query("wait_ms=%32%35%30"), (0, 100, 250))
        # Combines freely with after/limit in any order.
        self.assertEqual(parse_audit_query("after=5&limit=10&wait_ms=250"), (5, 10, 250))
        self.assertEqual(parse_audit_query("wait_ms=250&after=5"), (5, 100, 250))
        self.assertEqual(parse_audit_query("limit=10&wait_ms=1"), (0, 10, 1))

    def test_invalid_wait_ms_shapes(self) -> None:
        bad = ["wait_ms=", "wait_ms", "wait_ms=+1", "wait_ms=-1", "wait_ms=-0",
               "wait_ms=1.0", "wait_ms=0.5", "wait_ms=1e3", "wait_ms=1E3",
               "wait_ms=0x10", "wait_ms=abc", "wait_ms=true", "wait_ms=false",
               "wait_ms=%201", "wait_ms=1%20", "wait_ms=%2B1", "wait_ms=%2D1",
               "wait_ms=%D9%A1", "wait_ms=1&wait_ms=2", "wait_ms=2&wait_ms=2",
               "wait_ms=30001", "wait_ms=030001", "wait_ms=999999999"]
        for query in bad:
            with self.subTest(query=query):
                with self.assertRaises(InvalidRequest):
                    parse_audit_query(query)

    def test_unknown_parameters_are_still_rejected(self) -> None:
        for query in ["wait=1", "waitms=1", "WAIT_MS=1", "Wait_Ms=1", "timeout=100",
                      "wait_ms=1&foo=2", "foo=1&wait_ms=1", "wait_ms=1&since=2"]:
            with self.subTest(query=query):
                with self.assertRaises(InvalidRequest):
                    parse_audit_query(query)

    def test_after_and_limit_rules_unchanged(self) -> None:
        self.assertEqual(parse_audit_query("after=5&wait_ms=10"), (5, 100, 10))
        for query in ["after=1.0&wait_ms=1", "limit=0&wait_ms=1", "limit=1001&wait_ms=1",
                      "after=&wait_ms=1", "limit=abc&wait_ms=1"]:
            with self.subTest(query=query):
                with self.assertRaises(InvalidRequest):
                    parse_audit_query(query)


class LongPollUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger(":memory:")

    def tearDown(self) -> None:
        self.ledger.close()

    def append_note(self, stream: str = "s", expected: int | None = None) -> None:
        current = self.ledger.version(stream) if expected is None else expected
        self.ledger.append(stream, [{"type": "NoteRecorded", "payload": {}}], current)

    def test_wait_ms_zero_matches_plain_read(self) -> None:
        self.append_note()
        events, more = self.ledger.read_global_wait(0, 10, 0)
        self.assertEqual([e.cursor for e in events], [1])
        self.assertFalse(more)
        events, more = self.ledger.read_global_wait(1, 10, 0)
        self.assertEqual(events, [])
        self.assertFalse(more)

    def test_returns_immediately_when_events_already_committed(self) -> None:
        self.append_note()
        started = time.monotonic()
        events, more = self.ledger.read_global_wait(0, 10, 30000)
        self.assertLess(time.monotonic() - started, 1.0)
        self.assertEqual([e.cursor for e in events], [1])
        self.assertFalse(more)

    def test_wakes_when_a_batch_commits_during_the_wait(self) -> None:
        def writer() -> None:
            time.sleep(0.15)
            self.ledger.append("s", [{"type": "OrderPlaced", "payload": {}},
                                     {"type": "NoteRecorded", "payload": {"text": "a"}},
                                     {"type": "OrderCancelled", "payload": {}}], 0)

        thread = threading.Thread(target=writer)
        thread.start()
        started = time.monotonic()
        events, more = self.ledger.read_global_wait(0, 10, 5000)
        elapsed = time.monotonic() - started
        thread.join()
        # The whole committed batch arrives at once, cursors dense from 1.
        self.assertGreaterEqual(elapsed, 0.1)
        self.assertEqual([e.cursor for e in events], [1, 2, 3])
        self.assertEqual([e.type for e in events],
                         ["OrderPlaced", "NoteRecorded", "OrderCancelled"])
        self.assertFalse(more)

    def test_timeout_returns_empty_page(self) -> None:
        started = time.monotonic()
        events, more = self.ledger.read_global_wait(0, 10, 200)
        elapsed = time.monotonic() - started
        self.assertEqual(events, [])
        self.assertFalse(more)
        self.assertGreaterEqual(elapsed, 0.18)
        self.assertLess(elapsed, 3.0)

    def test_event_committed_after_the_deadline_is_not_included(self) -> None:
        def writer() -> None:
            time.sleep(0.4)
            self.append_note()

        thread = threading.Thread(target=writer)
        thread.start()
        events, more = self.ledger.read_global_wait(0, 10, 150)
        # Committed after the deadline: this response stays empty...
        self.assertEqual(events, [])
        self.assertFalse(more)
        thread.join()
        # ...and the event is visible to the next poll with the same after.
        events, more = self.ledger.read_global_wait(0, 10, 0)
        self.assertEqual([e.cursor for e in events], [1])
        self.assertFalse(more)

    def test_waiting_consumes_no_cursors_and_writes_nothing(self) -> None:
        # Several long polls time out; the first real append still gets cursor 1.
        for _ in range(3):
            events, more = self.ledger.read_global_wait(0, 10, 20)
            self.assertEqual(events, [])
            self.assertFalse(more)
        written = self.ledger.append("s", [{"type": "OrderPlaced", "payload": {}}], 0,
                                     command_id="after-waits")
        self.assertEqual(written[0].cursor, 1)
        self.assertEqual(self.ledger.streams(), ["s"])

    def test_concurrent_appends_paged_without_skips_or_duplicates(self) -> None:
        total = 40
        done = threading.Event()

        def writer() -> None:
            try:
                for index in range(total):
                    self.ledger.append("w", [{"type": "NoteRecorded", "payload": {"n": index}}],
                                       index)
            finally:
                done.set()

        thread = threading.Thread(target=writer)
        thread.start()
        collected: list[int] = []
        after = 0
        while len(collected) < total:
            events, _ = self.ledger.read_global_wait(after, 4, 3000)
            for event in events:
                self.assertEqual(event.cursor, after + 1)
                collected.append(event.cursor)
                after = event.cursor
        done.wait(5)
        thread.join()
        self.assertEqual(collected, list(range(1, total + 1)))
        # The writer is done: the next poll is an immediate empty page.
        events, more = self.ledger.read_global_wait(after, 4, 0)
        self.assertEqual(events, [])
        self.assertFalse(more)


class HttpLongPollTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db_path = str(Path(cls.tmp.name) / "longpoll.sqlite")
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
                timeout: float = 10) -> tuple[int, dict]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                         method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def append(self, stream: str, events: list, expected: int) -> None:
        status, _ = self.request("POST", f"/streams/{stream}/events",
                                 {"events": events, "expected_version": expected})
        self.assertEqual(status, 201)

    def head(self) -> int:
        status, body = self.request("GET", "/events?limit=1000")
        self.assertEqual(status, 200)
        self.assertFalse(body["has_more"])
        return body["next_cursor"]

    def test_immediate_page_when_events_already_exist(self) -> None:
        self.append("lp-imm", [{"type": "OrderPlaced", "payload": {}}], 0)
        after = self.head() - 1
        started = time.monotonic()
        status, body = self.request("GET", f"/events?after={after}&wait_ms=5000")
        self.assertLess(time.monotonic() - started, 2.0)
        self.assertEqual(status, 200)
        self.assertEqual([e["cursor"] for e in body["events"]], [after + 1])
        self.assertEqual(body["next_cursor"], after + 1)
        self.assertFalse(body["has_more"])

    def test_waits_and_returns_the_committed_batch_whole(self) -> None:
        after = self.head()

        def writer() -> None:
            time.sleep(0.15)
            self.append("lp-batch", [{"type": "OrderPlaced", "payload": {"n": 1}},
                                     {"type": "LineItemAdded", "payload": {"sku": "s"}},
                                     {"type": "NoteRecorded", "payload": {"text": "t"}}], 0)

        thread = threading.Thread(target=writer)
        thread.start()
        started = time.monotonic()
        status, body = self.request("GET", f"/events?after={after}&wait_ms=5000&limit=10")
        elapsed = time.monotonic() - started
        thread.join()
        self.assertEqual(status, 200)
        self.assertGreaterEqual(elapsed, 0.1)
        # The batch committed during the wait enters the page as one piece.
        self.assertEqual([e["cursor"] for e in body["events"]],
                         [after + 1, after + 2, after + 3])
        self.assertEqual([e["stream_id"] for e in body["events"]], ["lp-batch"] * 3)
        self.assertEqual(body["next_cursor"], after + 3)
        self.assertFalse(body["has_more"])

    def test_timeout_returns_the_empty_page_shape(self) -> None:
        after = self.head()
        started = time.monotonic()
        status, body = self.request("GET", f"/events?after={after}&wait_ms=300")
        elapsed = time.monotonic() - started
        self.assertEqual(status, 200)
        self.assertEqual(body, {"events": [], "next_cursor": after, "has_more": False})
        self.assertGreaterEqual(elapsed, 0.25)
        self.assertLess(elapsed, 5.0)

    def test_event_committed_after_timeout_is_seen_by_the_next_poll(self) -> None:
        after = self.head()
        status, body = self.request("GET", f"/events?after={after}&wait_ms=200")
        self.assertEqual(body, {"events": [], "next_cursor": after, "has_more": False})
        self.append("lp-late", [{"type": "NoteRecorded", "payload": {}}], 0)
        status, body = self.request("GET", f"/events?after={after}&wait_ms=0")
        self.assertEqual(status, 200)
        self.assertEqual([e["cursor"] for e in body["events"]], [after + 1])
        self.assertEqual(body["next_cursor"], after + 1)

    def test_long_poll_does_not_block_other_requests(self) -> None:
        after = self.head()
        result: dict[str, object] = {}

        def subscriber() -> None:
            # Woken by the append below; without the append it would still
            # finish on its own after wait_ms.
            result["response"] = self.request(
                "GET", f"/events?after={after}&wait_ms=5000", timeout=10)

        thread = threading.Thread(target=subscriber)
        thread.start()
        time.sleep(0.1)  # let the subscriber park before we work around it
        started = time.monotonic()
        try:
            # Reads and writes proceed while the subscriber is parked.
            status, body = self.request("GET", "/streams")
            self.assertEqual(status, 200)
            self.append("lp-elsewhere", [{"type": "OrderPlaced", "payload": {}}], 0)
            status, body = self.request("GET", "/streams/lp-elsewhere")
            self.assertEqual(status, 200)
            self.assertEqual(body["state"]["status"], "placed")
            status, body = self.request("GET", "/events?after=0&limit=1")
            self.assertEqual(status, 200)
            self.assertEqual(len(body["events"]), 1)
        finally:
            thread.join(10)
        self.assertLess(time.monotonic() - started, 5.0)
        status, body = result["response"]  # type: ignore[misc]
        self.assertEqual(status, 200)
        self.assertEqual([e["cursor"] for e in body["events"]], [after + 1])
        self.assertEqual(body["events"][0]["stream_id"], "lp-elsewhere")

    def test_walk_pages_with_long_poll_under_continuous_appends(self) -> None:
        after = self.head()
        total = 30
        writer_done = threading.Event()

        def writer() -> None:
            try:
                for index in range(total):
                    self.append("lp-walk", [{"type": "NoteRecorded",
                                             "payload": {"n": index}}], index)
            finally:
                writer_done.set()

        thread = threading.Thread(target=writer)
        thread.start()
        collected: list[int] = []
        try:
            while len(collected) < total:
                status, body = self.request(
                    "GET", f"/events?after={after}&limit=4&wait_ms=5000")
                self.assertEqual(status, 200)
                for event in body["events"]:
                    self.assertEqual(event["cursor"], after + 1)
                    collected.append(event["cursor"])
                    after = event["cursor"]
        finally:
            writer_done.wait(10)
            thread.join()
        self.assertEqual(collected, list(range(self.head() - total + 1, self.head() + 1)))
        status, body = self.request("GET", f"/events?after={after}&limit=4&wait_ms=0")
        self.assertEqual(body, {"events": [], "next_cursor": after, "has_more": False})

    def test_invalid_wait_ms_is_400_over_http(self) -> None:
        bad = ["wait_ms=", "wait_ms", "wait_ms=-1", "wait_ms=+1", "wait_ms=1.0",
               "wait_ms=1e3", "wait_ms=true", "wait_ms=%201", "wait_ms=%D9%A1",
               "wait_ms=30001", "wait_ms=1&wait_ms=2", "wait_ms=abc"]
        for query in bad:
            status, body = self.request("GET", f"/events?{query}")
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), query)

    def test_unknown_parameters_are_400_and_unknown_routes_404(self) -> None:
        for query in ["wait=1", "wait_ms=1&foo=2", "timeout=100", "WAIT_MS=1"]:
            status, body = self.request("GET", f"/events?{query}")
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), query)
        self.assertEqual(self.request("GET", "/events/1?wait_ms=1")[0], 404)

    def test_wait_ms_zero_behaves_like_a_plain_poll(self) -> None:
        after = self.head()
        started = time.monotonic()
        status, body = self.request("GET", f"/events?after={after}&wait_ms=0")
        self.assertLess(time.monotonic() - started, 2.0)
        self.assertEqual(status, 200)
        self.assertEqual(body, {"events": [], "next_cursor": after, "has_more": False})


if __name__ == "__main__":
    unittest.main()
