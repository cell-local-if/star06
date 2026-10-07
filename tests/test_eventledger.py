"""Baseline tests for the event ledger: the shape a recording/acceptance run executes."""
from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from eventledger import (  # noqa: F401
    IdempotencyConflict,
    InvalidRequest,
    Ledger,
    StreamNotFound,
    VersionConflict,
    parse_at_version,
    parse_events_query,
    replay,
    serve,
    validate_append,
)


class LedgerUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger(":memory:")

    def tearDown(self) -> None:
        self.ledger.close()

    def test_append_then_read_is_ordered_and_versioned(self) -> None:
        written = self.ledger.append("order-1", [{"type": "OrderPlaced", "payload": {"total": 10}},
                                                 {"type": "LineItemAdded", "payload": {"sku": "a"}}], 0)
        self.assertEqual([e.version for e in written], [1, 2])
        self.assertEqual([e.type for e in self.ledger.read("order-1")], ["OrderPlaced", "LineItemAdded"])

    def test_optimistic_concurrency_rejects_stale_expected_version(self) -> None:
        self.ledger.append("order-2", [{"type": "OrderPlaced", "payload": {}}], 0)
        with self.assertRaises(VersionConflict):
            self.ledger.append("order-2", [{"type": "OrderCancelled", "payload": {}}], 0)

    def test_replay_is_deterministic(self) -> None:
        self.ledger.append("order-3", [{"type": "OrderPlaced", "payload": {}},
                                       {"type": "LineItemAdded", "payload": {"sku": "b"}},
                                       {"type": "NoteRecorded", "payload": {"text": "hi"}}], 0)
        first = replay(self.ledger.read("order-3"))
        second = replay(self.ledger.read("order-3"))
        self.assertEqual(first, second)
        self.assertEqual(first["status"], "placed")
        self.assertEqual(first["lines"], [{"sku": "b"}])

    def test_invalid_append_is_rejected_before_any_write(self) -> None:
        with self.assertRaises(Exception):
            validate_append("order-4", [{"type": "NotAnEvent"}], 0)
        self.assertEqual(self.ledger.version("order-4"), 0)


INITIAL_STATE = {"status": "unknown", "lines": [], "notes": [], "cancelled": False}


def sample_stream(ledger: Ledger, stream: str) -> None:
    ledger.append(stream, [
        {"type": "OrderPlaced", "payload": {"total": 10}},
        {"type": "LineItemAdded", "payload": {"sku": "a"}},
        {"type": "NoteRecorded", "payload": {"text": "hi"}},
        {"type": "OrderCancelled", "payload": {}},
    ], 0)


class HistoryUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger(":memory:")
        sample_stream(self.ledger, "h-1")

    def tearDown(self) -> None:
        self.ledger.close()

    def test_at_zero_on_existing_stream_is_initial_projection(self) -> None:
        self.assertEqual(self.ledger.read_at("h-1", 0), [])
        self.assertEqual(replay(self.ledger.read_at("h-1", 0)), INITIAL_STATE)

    def test_read_at_replays_exactly_versions_one_through_at(self) -> None:
        self.assertEqual([e.version for e in self.ledger.read_at("h-1", 2)], [1, 2])
        state = replay(self.ledger.read_at("h-1", 2))
        self.assertEqual(state["status"], "placed")
        self.assertEqual(state["lines"], [{"sku": "a"}])
        self.assertEqual(state["notes"], [])
        self.assertFalse(state["cancelled"])
        state = replay(self.ledger.read_at("h-1", 3))
        self.assertEqual(state["notes"], ["hi"])
        state = replay(self.ledger.read_at("h-1", 4))
        self.assertEqual(state["status"], "cancelled")
        self.assertTrue(state["cancelled"])

    def test_at_beyond_current_version_is_not_found(self) -> None:
        with self.assertRaises(StreamNotFound):
            self.ledger.read_at("h-1", 5)

    def test_never_written_stream_is_not_found_at_any_version(self) -> None:
        for at in (0, 1, 2):
            with self.assertRaises(StreamNotFound):
                self.ledger.read_at("ghost", at)

    def test_historical_reads_land_on_consistent_version_boundaries(self) -> None:
        stream = "h-concurrent"

        def append_events() -> None:
            expected = 0
            self.ledger.append(stream, [{"type": "OrderPlaced", "payload": {}}], expected)
            expected = 1
            for _ in range(60):
                self.ledger.append(stream, [{"type": "LineItemAdded", "payload": {"n": expected}}], expected)
                expected += 1

        def read_history(worker: int) -> None:
            for attempt in range(200):
                current = self.ledger.version(stream)
                if current < 2:
                    continue
                # Pick an interior version [1, current-1], so the boundary can only
                # be correct if read_at snapshot both the check and the fetch.
                at = 1 + ((worker * 7 + attempt) % (current - 1))
                events = self.ledger.read_at(stream, at)
                self.assertEqual(len(events), at)
                self.assertEqual([e.version for e in events], list(range(1, at + 1)))
                state = replay(events)
                self.assertEqual(state["status"], "placed")
                self.assertEqual(len(state["lines"]), at - 1)

        t = threading.Thread(target=append_events)
        t.start()
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(read_history, range(8)))
        t.join()


class HistoryPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "ledger.sqlite")
        self.ledger = Ledger(self.path)
        sample_stream(self.ledger, "p-1")

    def tearDown(self) -> None:
        self.ledger.close()
        self.tmp.cleanup()

    def reopen(self) -> Ledger:
        self.ledger.close()
        self.ledger = Ledger(self.path)
        return self.ledger

    def test_history_is_identical_after_reopen(self) -> None:
        before = {at: [e.as_json() for e in self.ledger.read_at("p-1", at)] for at in range(5)}
        self.reopen()
        after = {at: [e.as_json() for e in self.ledger.read_at("p-1", at)] for at in range(5)}
        self.assertEqual(before, after)
        self.assertEqual(self.ledger.read_at("p-1", 0), [])
        with self.assertRaises(StreamNotFound):
            self.ledger.read_at("p-1", 5)
        with self.assertRaises(StreamNotFound):
            self.ledger.read_at("ghost", 0)


class ParseAtVersionTests(unittest.TestCase):
    def test_absent_at_means_latest(self) -> None:
        self.assertIsNone(parse_at_version(""))
        self.assertIsNone(parse_at_version("since=2"))
        self.assertIsNone(parse_at_version("foo=1&bar=2"))

    def test_plain_decimal_non_negative_integers(self) -> None:
        self.assertEqual(parse_at_version("at=0"), 0)
        self.assertEqual(parse_at_version("at=12"), 12)
        self.assertEqual(parse_at_version("foo=1&at=2"), 2)
        self.assertEqual(parse_at_version("at=%31"), 1)

    def test_invalid_at_shapes(self) -> None:
        for bad in ("at=", "at", "at=+1", "at=-1", "at=-0", "at=1.0", "at=1e1",
                    "at=0x1", "at=foo", "at=%201", "at=1%20", "at=%2B1",
                    "at=%D9%A1", "at=1&at=2", "at=2&at=2"):
            with self.subTest(query=bad):
                with self.assertRaises(InvalidRequest):
                    parse_at_version(bad)


class IdempotencyUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "ledger.sqlite")
        self.ledger = Ledger(self.path)

    def tearDown(self) -> None:
        self.ledger.close()
        self.tmp.cleanup()

    def reopen(self) -> Ledger:
        self.ledger.close()
        self.ledger = Ledger(self.path)
        return self.ledger

    def events(self, stream: str = "c-1") -> list:
        return self.ledger.read(stream)

    def test_retry_returns_first_response_without_new_events(self) -> None:
        body = [{"type": "OrderPlaced", "payload": {"total": 10, "sku": "a"}},
                {"type": "NoteRecorded", "payload": {"text": "hi"}}]
        first = self.ledger.append("c-1", body, 0, command_id="cmd-1")
        # Same command; payload key order reversed -> semantically equal.
        retry_body = [{"type": "OrderPlaced", "payload": {"sku": "a", "total": 10}},
                      {"type": "NoteRecorded", "payload": {"text": "hi"}}]
        second = self.ledger.append("c-1", retry_body, 0, command_id="cmd-1")
        self.assertEqual([e.as_json() for e in first], [e.as_json() for e in second])
        self.assertEqual(len(self.events()), 2)

    def test_retry_survives_restart(self) -> None:
        written = self.ledger.append("c-2", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="cmd-2")
        self.reopen()
        again = self.ledger.append("c-2", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="cmd-2")
        self.assertEqual([e.as_json() for e in written], [e.as_json() for e in again])
        self.assertEqual(len(self.events("c-2")), 1)

    def test_different_request_same_command_is_idempotency_conflict(self) -> None:
        self.ledger.append("c-3", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="cmd-3")
        # Stream moved on; idempotency_conflict must win over version_conflict.
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append("c-3", [{"type": "OrderCancelled", "payload": {}}], 1, command_id="cmd-3")
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append("other", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="cmd-3")
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append("c-3", [{"type": "OrderPlaced", "payload": {}}], 1, command_id="cmd-3")
        self.assertEqual(len(self.events("c-3")), 1)

    def test_event_order_is_significant(self) -> None:
        self.ledger.append("c-4",
                           [{"type": "OrderPlaced", "payload": {}}, {"type": "NoteRecorded", "payload": {"text": "a"}}],
                           0, command_id="cmd-4")
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append("c-4",
                               [{"type": "NoteRecorded", "payload": {"text": "a"}},
                                {"type": "OrderPlaced", "payload": {}}],
                               0, command_id="cmd-4")

    def test_value_change_is_conflict(self) -> None:
        self.ledger.append("c-5", [{"type": "OrderPlaced", "payload": {"total": 10}}], 0, command_id="cmd-5")
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append("c-5", [{"type": "OrderPlaced", "payload": {"total": 11}}], 0, command_id="cmd-5")

    def test_invalid_command_id_is_rejected(self) -> None:
        for bad in ("", "x" * 201, 123, None):
            if bad is None:
                continue
            with self.assertRaises(InvalidRequest):
                self.ledger.append("c-6", [{"type": "OrderPlaced", "payload": {}}], 0, command_id=bad)
        self.assertEqual(self.ledger.version("c-6"), 0)

    def test_first_request_validation_and_version_conflict_unchanged(self) -> None:
        with self.assertRaises(InvalidRequest):
            self.ledger.append("c-7", [{"type": "Nope"}], 0, command_id="cmd-7")
        self.ledger.append("c-7", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="cmd-ok")
        with self.assertRaises(VersionConflict):
            self.ledger.append("c-7", [{"type": "OrderCancelled", "payload": {}}], 0, command_id="cmd-new")
        # Failed commands leave no hitable idempotency record; cmd-new can later succeed.
        written = self.ledger.append("c-7", [{"type": "OrderCancelled", "payload": {}}], 1, command_id="cmd-new")
        self.assertEqual([e.version for e in written], [2])

    def test_failed_batch_leaves_neither_events_nor_command_record(self) -> None:
        # valid first event shape but force a failure mid-batch is impossible via validation
        # (validation runs first), so assert the invalid whole-batch case directly.
        with self.assertRaises(InvalidRequest):
            self.ledger.append("c-8",
                               [{"type": "OrderPlaced", "payload": {}}, {"type": "Nope"}],
                               0, command_id="cmd-8")
        self.assertEqual(self.events("c-8"), [])
        # The command_id is still free for a successful first commit.
        written = self.ledger.append("c-8", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="cmd-8")
        self.assertEqual(written[0].version, 1)

    def test_concurrent_identical_retries_commit_one_batch(self) -> None:
        body = [{"type": "OrderPlaced", "payload": {"total": 1}}]

        def submit() -> list:
            return [e.as_json() for e in self.ledger.append("c-9", body, 0, command_id="cmd-9")]

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: submit(), range(8)))
        self.assertTrue(all(r == results[0] for r in results))
        self.assertEqual(len(self.events("c-9")), 1)

    def test_omitting_command_id_keeps_baseline_behavior(self) -> None:
        self.ledger.append("c-10", [{"type": "OrderPlaced", "payload": {}}], 0)
        with self.assertRaises(VersionConflict):
            self.ledger.append("c-10", [{"type": "OrderPlaced", "payload": {}}], 0)
        self.assertEqual(len(self.events("c-10")), 1)


class HttpSurfaceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = serve(port=0, db=":memory:")
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.ledger.close()  # type: ignore[attr-defined]

    def request(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def test_health(self) -> None:
        self.assertEqual(self.request("GET", "/health"), (200, {"status": "ok"}))

    def test_append_conflict_and_state(self) -> None:
        status, body = self.request("POST", "/streams/o-1/events",
                                    {"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0})
        self.assertEqual(status, 201)
        self.assertEqual(body["version"], 1)
        status, body = self.request("POST", "/streams/o-1/events",
                                    {"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0})
        self.assertEqual((status, body["error"]["code"]), (409, "version_conflict"))
        status, body = self.request("GET", "/streams/o-1")
        self.assertEqual(status, 200)
        self.assertEqual(body["state"]["status"], "placed")

    def test_unknown_stream_and_route(self) -> None:
        self.assertEqual(self.request("GET", "/streams/nope")[0], 404)
        self.assertEqual(self.request("GET", "/nope")[0], 404)


class HttpHistoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db_path = str(Path(cls.tmp.name) / "history.sqlite")
        cls.server = serve(port=0, db=cls.db_path)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        events = [
            {"type": "OrderPlaced", "payload": {"total": 10}},
            {"type": "LineItemAdded", "payload": {"sku": "a"}},
            {"type": "NoteRecorded", "payload": {"text": "hi"}},
            {"type": "OrderCancelled", "payload": {}},
        ]
        req = urllib.request.Request(
            f"http://127.0.0.1:{cls.port}/streams/hist/events",
            data=json.dumps({"events": events, "expected_version": 0}).encode(),
            method="POST", headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as response:
            assert response.status == 201

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.ledger.close()  # type: ignore[attr-defined]
        cls.tmp.cleanup()

    def request(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def test_latest_without_at_is_unchanged(self) -> None:
        status, body = self.request("GET", "/streams/hist")
        self.assertEqual(status, 200)
        self.assertEqual(body["version"], 4)
        self.assertEqual(body["state"],
                         {"status": "cancelled", "lines": [{"sku": "a"}],
                          "notes": ["hi"], "cancelled": True})

    def test_history_travels_by_version(self) -> None:
        status, body = self.request("GET", "/streams/hist?at=1")
        self.assertEqual(status, 200)
        self.assertEqual(body["version"], 1)
        self.assertEqual(body["state"],
                         {"status": "placed", "lines": [], "notes": [], "cancelled": False})
        status, body = self.request("GET", "/streams/hist?at=2")
        self.assertEqual(body["state"]["lines"], [{"sku": "a"}])
        self.assertEqual(body["version"], 2)
        status, body = self.request("GET", "/streams/hist?at=3")
        self.assertEqual(body["state"]["notes"], ["hi"])
        self.assertFalse(body["state"]["cancelled"])
        status, body = self.request("GET", "/streams/hist?at=4")
        self.assertEqual(body["state"]["status"], "cancelled")
        self.assertTrue(body["state"]["cancelled"])

    def test_digit_strings_with_leading_zeros_are_decimal(self) -> None:
        # Any pure-ASCII-digit string is a decimal non-negative integer.
        status, body = self.request("GET", "/streams/hist?at=00")
        self.assertEqual(status, 200)
        self.assertEqual(body["version"], 0)
        status, body = self.request("GET", "/streams/hist?at=02")
        self.assertEqual((status, body["version"]), (200, 2))

    def test_at_zero_on_existing_stream(self) -> None:
        status, body = self.request("GET", "/streams/hist?at=0")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"state": {"status": "unknown", "lines": [], "notes": [],
                                          "cancelled": False}, "version": 0})

    def test_future_version_is_not_found(self) -> None:
        status, body = self.request("GET", "/streams/hist?at=5")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        status, body = self.request("GET", "/streams/hist?at=1000")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))

    def test_never_written_stream(self) -> None:
        for path in ("/streams/ghost", "/streams/ghost?at=0", "/streams/ghost?at=1"):
            status, body = self.request("GET", path)
            self.assertEqual((status, body["error"]["code"]), (404, "not_found"), path)

    def test_invalid_at_is_bad_request_before_existence_check(self) -> None:
        # Whitespace, sign, decimal point, exponent, non-ASCII digits — all 400
        # even against a stream that does not exist: validation precedes lookup.
        bad_queries = [
            "at=", "at", "at=+1", "at=-1", "at=-0", "at=1.0", "at=1e1", "at=1E1",
            "at=0x1", "at=foo", "at=%201", "at=1%20", "at=%2B1", "at=%2D1",
            "at=%D9%A1", "at=1&at=2", "at=2&at=2",
        ]
        for query in bad_queries:
            status, body = self.request("GET", f"/streams/ghost?{query}")
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), query)

    def test_events_since_semantics_unchanged(self) -> None:
        status, body = self.request("GET", "/streams/hist/events?since=2")
        self.assertEqual(status, 200)
        self.assertEqual([e["version"] for e in body["events"]], [3, 4])
        self.assertEqual(self.request("GET", "/streams/ghost/events")[0], 404)
        # since beyond the end on an existing stream is an empty 200, not a 404.
        status, body = self.request("GET", "/streams/hist/events?since=4")
        self.assertEqual((status, body), (200, {"events": []}))

    def test_history_reads_add_no_events_and_keep_write_priorities(self) -> None:
        status, _ = self.request("POST", "/streams/hist/events",
                                 {"events": [{"type": "OrderPlaced", "payload": {}}],
                                  "expected_version": 0})
        self.assertEqual(status, 409)
        self.assertEqual(self.request("GET", "/streams/hist")[1]["version"], 4)



class HttpIdempotencyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = serve(port=0, db=":memory:")
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.ledger.close()  # type: ignore[attr-defined]

    def request(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def base(self, cid: str, **overrides) -> dict:
        body = {"events": [{"type": "OrderPlaced", "payload": {"total": 1, "sku": "s"}}],
                "expected_version": 0, "command_id": cid}
        body.update(overrides)
        return body

    def test_http_retry_is_idempotent_201(self) -> None:
        status, first = self.request("POST", "/streams/h-1/events", self.base("hc-1"))
        self.assertEqual(status, 201)
        status, second = self.request(
            "POST", "/streams/h-1/events",
            self.base("hc-1", events=[{"type": "OrderPlaced", "payload": {"sku": "s", "total": 1}}]))
        self.assertEqual(status, 201)
        self.assertEqual(first, second)
        status, read = self.request("GET", "/streams/h-1/events")
        self.assertEqual(status, 200)
        self.assertEqual(len(read["events"]), 1)

    def test_http_conflicts(self) -> None:
        self.request("POST", "/streams/h-2/events", self.base("hc-2"))
        # Different events -> idempotency_conflict, even though expected_version is now stale.
        status, body = self.request("POST", "/streams/h-2/events",
                                    self.base("hc-2", events=[{"type": "OrderCancelled", "payload": {}}]))
        self.assertEqual((status, body["error"]["code"]), (409, "idempotency_conflict"))
        status, body = self.request("POST", "/streams/h-2/events", self.base("hc-2", expected_version=1))
        self.assertEqual((status, body["error"]["code"]), (409, "idempotency_conflict"))
        # A fresh command_id with stale version still yields version_conflict.
        status, body = self.request("POST", "/streams/h-2/events",
                                    self.base("hc-other", expected_version=0))
        self.assertEqual((status, body["error"]["code"]), (409, "version_conflict"))

    def test_http_invalid_command_id(self) -> None:
        for bad in ("", "y" * 201, 5):
            status, body = self.request("POST", "/streams/h-3/events", self.base("hc-3", command_id=bad))
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertEqual(self.request("GET", "/streams/h-3")[0], 404)

    def test_http_invalid_body_still_beats_command_lookup(self) -> None:
        self.request("POST", "/streams/h-4/events", self.base("hc-4"))
        status, body = self.request("POST", "/streams/h-4/events",
                                    self.base("hc-4", events=[{"type": "Nope"}]))
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))

    def test_http_concurrent_retries(self) -> None:
        def submit(_: int) -> tuple[int, dict]:
            return self.request("POST", "/streams/h-5/events", self.base("hc-5"))

        with ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(submit, range(8)))
        self.assertTrue(all(status == 201 for status, _ in responses))
        bodies = [body for _, body in responses]
        self.assertTrue(all(body == bodies[0] for body in bodies))
        _, read = self.request("GET", "/streams/h-5/events")
        self.assertEqual(len(read["events"]), 1)

    def test_http_without_command_id(self) -> None:
        body = {"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0}
        self.assertEqual(self.request("POST", "/streams/h-6/events", body)[0], 201)
        status, resp = self.request("POST", "/streams/h-6/events", body)
        self.assertEqual((status, resp["error"]["code"]), (409, "version_conflict"))


class SnapshotUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "ledger.sqlite")
        self.ledger = Ledger(self.path)
        sample_stream(self.ledger, "s-1")

    def tearDown(self) -> None:
        self.ledger.close()
        self.tmp.cleanup()

    def reopen(self) -> Ledger:
        self.ledger.close()
        self.ledger = Ledger(self.path)
        return self.ledger

    def test_snapshot_matches_deterministic_replay(self) -> None:
        for at in range(5):
            state, created = self.ledger.snapshot("s-1", at)
            self.assertTrue(created)
            self.assertEqual(state, replay(self.ledger.read_at("s-1", at)))
        state, _ = self.ledger.snapshot("s-1", 0)
        self.assertEqual(state, INITIAL_STATE)

    def test_repeat_snapshot_does_not_recreate(self) -> None:
        first, created = self.ledger.snapshot("s-1", 2)
        self.assertTrue(created)
        second, created = self.ledger.snapshot("s-1", 2)
        self.assertFalse(created)
        self.assertEqual(first, second)

    def test_snapshot_survives_reopen(self) -> None:
        first, _ = self.ledger.snapshot("s-1", 3)
        self.reopen()
        again, created = self.ledger.snapshot("s-1", 3)
        self.assertFalse(created)
        self.assertEqual(first, again)

    def test_distinct_versions_kept_and_immune_to_later_appends(self) -> None:
        early, _ = self.ledger.snapshot("s-1", 2)
        late, _ = self.ledger.snapshot("s-1", 4)
        self.ledger.append("s-1", [{"type": "NoteRecorded", "payload": {"text": "later"}}], 4)
        again_early, created = self.ledger.snapshot("s-1", 2)
        self.assertFalse(created)
        self.assertEqual(again_early, early)
        again_late, _ = self.ledger.snapshot("s-1", 4)
        self.assertEqual(again_late, late)
        # 新版本是独立快照，不影响旧版本。
        newest, created = self.ledger.snapshot("s-1", 5)
        self.assertTrue(created)
        self.assertEqual(newest["notes"], ["hi", "later"])

    def test_invalid_at_version_is_rejected(self) -> None:
        for bad in (-1, -100, True, False, 1.0, 2.5, "1", "0", None, [1], {"v": 1}):
            with self.subTest(at_version=bad):
                with self.assertRaises(InvalidRequest):
                    self.ledger.snapshot("s-1", bad)

    def test_unknown_stream_and_future_version_are_not_found(self) -> None:
        for at in (0, 1):
            with self.assertRaises(StreamNotFound):
                self.ledger.snapshot("ghost", at)
        with self.assertRaises(StreamNotFound):
            self.ledger.snapshot("s-1", 5)
        # 失败不留快照：追加到版本 5 后首次固化仍应是 created=True。
        self.ledger.append("s-1", [{"type": "NoteRecorded", "payload": {"text": "x"}}], 4)
        _, created = self.ledger.snapshot("s-1", 5)
        self.assertTrue(created)

    def test_concurrent_identical_snapshots_create_exactly_once(self) -> None:
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.ledger.snapshot("s-1", 3), range(16)))
        self.assertEqual(sum(1 for _, created in results if created), 1)
        states = {json.dumps(state, sort_keys=True) for state, _ in results}
        self.assertEqual(len(states), 1)

    def test_snapshot_does_not_touch_events_or_commands(self) -> None:
        self.ledger.append("s-2", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="snap-cmd")
        self.ledger.snapshot("s-2", 1)
        self.assertEqual(len(self.ledger.read("s-2")), 1)
        retry = self.ledger.append("s-2", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="snap-cmd")
        self.assertEqual(len(retry), 1)
        self.assertEqual(len(self.ledger.read("s-2")), 1)


class HttpSnapshotTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db_path = str(Path(cls.tmp.name) / "snapshots.sqlite")
        cls.server = serve(port=0, db=cls.db_path)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        events = [
            {"type": "OrderPlaced", "payload": {"total": 10}},
            {"type": "LineItemAdded", "payload": {"sku": "a"}},
            {"type": "NoteRecorded", "payload": {"text": "hi"}},
            {"type": "OrderCancelled", "payload": {}},
        ]
        status, _ = cls().request("POST", "/streams/snap/events",
                                  {"events": events, "expected_version": 0})
        assert status == 201

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.ledger.close()  # type: ignore[attr-defined]
        cls.tmp.cleanup()

    def request(self, method: str, path: str, body: dict | None = None,
                raw: bytes | None = None) -> tuple[int, dict]:
        data = raw if raw is not None else (None if body is None else json.dumps(body).encode())
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def test_create_then_repeat_is_201_then_200_with_same_body(self) -> None:
        status, first = self.request("POST", "/streams/snap/snapshots", {"at_version": 2})
        self.assertEqual(status, 201)
        self.assertEqual(first, {"stream_id": "snap", "version": 2,
                                 "state": {"status": "placed", "lines": [{"sku": "a"}],
                                           "notes": [], "cancelled": False}})
        # state 逐字段等于 GET ?at=2 的重放结果。
        _, projection = self.request("GET", "/streams/snap?at=2")
        self.assertEqual(first["state"], projection["state"])
        status, second = self.request("POST", "/streams/snap/snapshots", {"at_version": 2})
        self.assertEqual(status, 200)
        self.assertEqual(first, second)

    def test_snapshot_at_zero_and_latest(self) -> None:
        status, body = self.request("POST", "/streams/snap/snapshots", {"at_version": 0})
        self.assertEqual(status, 201)
        self.assertEqual(body["state"], INITIAL_STATE)
        self.assertEqual(body["version"], 0)
        status, body = self.request("POST", "/streams/snap/snapshots", {"at_version": 4})
        self.assertEqual(status, 201)
        self.assertEqual(body["state"]["status"], "cancelled")

    def test_old_snapshots_unchanged_by_later_appends(self) -> None:
        self.request("POST", "/streams/snap-evolve/events",
                     {"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0})
        _, before = self.request("POST", "/streams/snap-evolve/snapshots", {"at_version": 1})
        self.request("POST", "/streams/snap-evolve/events",
                     {"events": [{"type": "OrderCancelled", "payload": {}}], "expected_version": 1})
        status, after = self.request("POST", "/streams/snap-evolve/snapshots", {"at_version": 1})
        self.assertEqual(status, 200)
        self.assertEqual(before, after)
        self.assertEqual(after["state"]["status"], "placed")

    def test_concurrent_submissions_create_once(self) -> None:
        self.request("POST", "/streams/snap-race/events",
                     {"events": [{"type": "OrderPlaced", "payload": {}},
                                 {"type": "NoteRecorded", "payload": {"text": "n"}}],
                      "expected_version": 0})

        def submit(_: int) -> tuple[int, dict]:
            return self.request("POST", "/streams/snap-race/snapshots", {"at_version": 2})

        with ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(submit, range(16)))
        self.assertEqual(sum(1 for status, _ in responses if status == 201), 1)
        self.assertEqual(sum(1 for status, _ in responses if status == 200), 15)
        bodies = {json.dumps(body, sort_keys=True) for _, body in responses}
        self.assertEqual(len(bodies), 1)

    def test_invalid_bodies_are_400_and_create_nothing(self) -> None:
        bad_bodies = [
            {},                                   # 缺 at_version
            {"at_version": 1, "extra": True},     # 未知字段
            {"at_version": -1},                   # 负数
            {"at_version": True},                 # 布尔
            {"at_version": 1.0},                  # 浮点
            {"at_version": "1"},                  # 字符串
            {"at_version": None},
        ]
        for body in bad_bodies:
            with self.subTest(body=body):
                status, resp = self.request("POST", "/streams/snap/snapshots", body)
                self.assertEqual((status, resp["error"]["code"]), (400, "invalid_request"))
        # 非对象与非法 JSON。
        status, resp = self.request("POST", "/streams/snap/snapshots", raw=b"[1, 2]")
        self.assertEqual((status, resp["error"]["code"]), (400, "invalid_request"))
        status, resp = self.request("POST", "/streams/snap/snapshots", raw=b"{not json")
        self.assertEqual((status, resp["error"]["code"]), (400, "invalid_request"))
        # 全部失败都不留快照：at_version=1 的首次固化仍应是 201。
        status, _ = self.request("POST", "/streams/snap/snapshots", {"at_version": 1})
        self.assertEqual(status, 201)

    def test_missing_content_length_is_400(self) -> None:
        import http.client
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.putrequest("POST", "/streams/snap/snapshots", skip_host=False, skip_accept_encoding=True)
        conn.endheaders(b'{"at_version": 1}')
        response = conn.getresponse()
        self.assertEqual(response.status, 400)
        self.assertEqual(json.loads(response.read())["error"]["code"], "invalid_request")
        conn.close()

    def test_not_found_cases_leave_no_snapshot(self) -> None:
        for body in ({"at_version": 0}, {"at_version": 1}):
            status, resp = self.request("POST", "/streams/ghost/snapshots", body)
            self.assertEqual((status, resp["error"]["code"]), (404, "not_found"))
        status, resp = self.request("POST", "/streams/snap/snapshots", {"at_version": 5})
        self.assertEqual((status, resp["error"]["code"]), (404, "not_found"))
        status, resp = self.request("POST", "/streams/snap/snapshots", {"at_version": 1000})
        self.assertEqual((status, resp["error"]["code"]), (404, "not_found"))

    def test_unknown_routes_still_404(self) -> None:
        status, resp = self.request("POST", "/streams/snap/unknown", {"at_version": 1})
        self.assertEqual((status, resp["error"]["code"]), (404, "not_found"))
        status, resp = self.request("POST", "/snapshots", {"at_version": 1})
        self.assertEqual((status, resp["error"]["code"]), (404, "not_found"))
        self.assertEqual(self.request("GET", "/streams/snap/snapshots")[0], 404)

    def test_snapshot_persists_across_ledger_reopen(self) -> None:
        self.request("POST", "/streams/snap-persist/events",
                     {"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0})
        status, first = self.request("POST", "/streams/snap-persist/snapshots", {"at_version": 1})
        self.assertEqual(status, 201)
        # 直接在同一文件上重开一个 Ledger，模拟重启后的重复提交。
        reopened = Ledger(self.db_path)
        try:
            state, created = reopened.snapshot("snap-persist", 1)
        finally:
            reopened.close()
        self.assertFalse(created)
        self.assertEqual(state, first["state"])


class ParseEventsQueryTests(unittest.TestCase):
    def test_defaults(self) -> None:
        self.assertEqual(parse_events_query(""), (0, 100))
        self.assertEqual(parse_events_query("after=5"), (5, 100))
        self.assertEqual(parse_events_query("limit=7"), (0, 7))
        self.assertEqual(parse_events_query("after=3&limit=2"), (3, 2))
        self.assertEqual(parse_events_query("limit=2&after=3"), (3, 2))
        self.assertEqual(parse_events_query("after=0&limit=1000"), (0, 1000))
        self.assertEqual(parse_events_query("after=%31%32"), (12, 100))

    def test_invalid_shapes(self) -> None:
        bad = [
            "after=", "after", "after=+1", "after=-1", "after=-0", "after=1.0",
            "after=1e1", "after=0x1", "after=foo", "after=%201", "after=1%20",
            "after=%D9%A1", "after=1&after=2", "after=2&after=2",
            "limit=", "limit", "limit=0", "limit=1001", "limit=-1", "limit=1.0",
            "limit=1e2", "limit=foo", "limit=2&limit=3",
            "foo=1", "after=1&foo=2", "at=1", "since=2",
        ]
        for query in bad:
            with self.subTest(query=query):
                with self.assertRaises(InvalidRequest):
                    parse_events_query(query)


class GlobalFeedUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger(":memory:")

    def tearDown(self) -> None:
        self.ledger.close()

    def test_cursor_is_global_strictly_increasing_across_streams(self) -> None:
        self.ledger.append("g-1", [{"type": "OrderPlaced", "payload": {}},
                                     {"type": "NoteRecorded", "payload": {"text": "a"}}], 0)
        self.ledger.append("g-2", [{"type": "OrderPlaced", "payload": {}}], 0)
        self.ledger.append("g-1", [{"type": "OrderCancelled", "payload": {}}], 2)
        events, next_cursor, has_more = self.ledger.read_global(0, 100)
        self.assertEqual([e.cursor for e in events], [1, 2, 3, 4])
        self.assertEqual([(e.stream_id, e.version) for e in events],
                         [("g-1", 1), ("g-1", 2), ("g-2", 1), ("g-1", 3)])
        self.assertEqual(next_cursor, 4)
        self.assertFalse(has_more)

    def test_pagination_covers_every_event_exactly_once(self) -> None:
        for index in range(7):
            self.ledger.append("g-p", [{"type": "NoteRecorded", "payload": {"text": str(index)}}],
                               index)
        seen: list[int] = []
        after = 0
        for _ in range(10):
            events, after, has_more = self.ledger.read_global(after, 3)
            seen.extend(e.cursor for e in events)
            if not has_more:
                break
        self.assertEqual(seen, list(range(1, 8)))
        self.assertEqual(after, 7)

    def test_empty_page_keeps_after_and_reports_no_more(self) -> None:
        self.ledger.append("g-e", [{"type": "OrderPlaced", "payload": {}}], 0)
        events, next_cursor, has_more = self.ledger.read_global(1, 100)
        self.assertEqual(events, [])
        self.assertEqual(next_cursor, 1)
        self.assertFalse(has_more)
        # after 大于当前最大位置同样是空页。
        events, next_cursor, has_more = self.ledger.read_global(999, 100)
        self.assertEqual((events, next_cursor, has_more), ([], 999, False))

    def test_empty_ledger_is_empty_page(self) -> None:
        self.assertEqual(self.ledger.read_global(0, 100), ([], 0, False))

    def test_snapshots_and_command_records_do_not_appear(self) -> None:
        self.ledger.append("g-s", [{"type": "OrderPlaced", "payload": {}}], 0, command_id="g-cmd")
        self.ledger.snapshot("g-s", 1)
        events, _, has_more = self.ledger.read_global(0, 100)
        self.assertEqual([e.type for e in events], ["OrderPlaced"])
        self.assertFalse(has_more)

    def test_page_boundary_excludes_later_commits(self) -> None:
        self.ledger.append("g-b", [{"type": "OrderPlaced", "payload": {}},
                                   {"type": "NoteRecorded", "payload": {"text": "a"}}], 0)
        events, next_cursor, has_more = self.ledger.read_global(0, 1)
        self.assertEqual([e.cursor for e in events], [1])
        self.assertTrue(has_more)
        # 边界后的新事件留给下一次查询。
        self.ledger.append("g-b", [{"type": "NoteRecorded", "payload": {"text": "x"}}], 2)
        events, next_cursor, has_more = self.ledger.read_global(next_cursor, 100)
        self.assertEqual([e.cursor for e in events], [2, 3])
        self.assertFalse(has_more)


class GlobalFeedPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "ledger.sqlite")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def make_legacy_db(self) -> list[tuple[str, int, str, str, str]]:
        """Build a pre-cursor database by hand: the old schema, no cursor column."""
        import sqlite3
        rows = [("s-1", 1, "e-1", "OrderPlaced", "{\"total\": 1}"),
                ("s-1", 2, "e-2", "NoteRecorded", "{\"text\": \"hi\"}"),
                ("s-2", 1, "e-3", "OrderPlaced", "{}"),
                ("s-1", 3, "e-4", "OrderCancelled", "{}")]
        db = sqlite3.connect(self.path)
        db.execute("""CREATE TABLE events (
            stream_id TEXT NOT NULL, version INTEGER NOT NULL, event_id TEXT NOT NULL,
            type TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (stream_id, version))""")
        db.executemany("INSERT INTO events VALUES (?, ?, ?, ?, ?)", rows)
        db.commit()
        db.close()
        return rows

    def test_migration_backfills_cursors_in_write_order_without_touching_facts(self) -> None:
        rows = self.make_legacy_db()
        ledger = Ledger(self.path)
        try:
            events, next_cursor, has_more = ledger.read_global(0, 100)
            self.assertEqual([e.cursor for e in events], [1, 2, 3, 4])
            self.assertEqual(next_cursor, 4)
            self.assertFalse(has_more)
            # 事件事实逐字段不变。
            self.assertEqual([(e.stream_id, e.version, e.event_id, e.type,
                               json.dumps(e.payload)) for e in events],
                             [(r[0], r[1], r[2], r[3], r[4]) for r in rows])
            # 按流读取与确定性重放不变。
            self.assertEqual([e.version for e in ledger.read("s-1")], [1, 2, 3])
            self.assertEqual(replay(ledger.read("s-1"))["status"], "cancelled")
        finally:
            ledger.close()

    def test_cursors_survive_restart_and_new_events_get_larger_cursors(self) -> None:
        self.make_legacy_db()
        ledger = Ledger(self.path)
        ledger.append("s-2", [{"type": "NoteRecorded", "payload": {"text": "n"}}], 1)
        ledger.close()
        reopened = Ledger(self.path)
        try:
            events, _, _ = reopened.read_global(0, 100)
            self.assertEqual([e.cursor for e in events], [1, 2, 3, 4, 5])
            self.assertEqual(events[-1].stream_id, "s-2")
            self.assertEqual(events[-1].version, 2)
        finally:
            reopened.close()

    def test_concurrent_initialization_forms_one_consistent_order(self) -> None:
        self.make_legacy_db()
        ledgers: list[Ledger] = []

        def open_ledger() -> None:
            ledgers.append(Ledger(self.path))

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda _: open_ledger(), range(4)))
        try:
            for ledger in ledgers:
                events, _, _ = ledger.read_global(0, 100)
                self.assertEqual([e.cursor for e in events], [1, 2, 3, 4])
        finally:
            for ledger in ledgers:
                ledger.close()
        # 迁移后重开，顺序不变、不重号。
        again = Ledger(self.path)
        try:
            events, _, _ = again.read_global(0, 100)
            self.assertEqual([e.cursor for e in events], [1, 2, 3, 4])
            self.assertEqual([e.event_id for e in events], ["e-1", "e-2", "e-3", "e-4"])
        finally:
            again.close()


class HttpGlobalFeedTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db_path = str(Path(cls.tmp.name) / "global.sqlite")
        cls.server = serve(port=0, db=cls.db_path)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls().request("POST", "/streams/ga-1/events",
                      {"events": [{"type": "OrderPlaced", "payload": {"total": 10}},
                                  {"type": "LineItemAdded", "payload": {"sku": "a"}}],
                       "expected_version": 0})
        cls().request("POST", "/streams/ga-2/events",
                      {"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0})
        cls().request("POST", "/streams/ga-1/events",
                      {"events": [{"type": "OrderCancelled", "payload": {}}], "expected_version": 2})
        cls().request("POST", "/streams/ga-1/snapshots", {"at_version": 3})

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.ledger.close()  # type: ignore[attr-defined]
        cls.tmp.cleanup()

    def request(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def test_default_page_lists_all_events_in_cursor_order(self) -> None:
        status, body = self.request("GET", "/events")
        self.assertEqual(status, 200)
        self.assertEqual([e["cursor"] for e in body["events"]], [1, 2, 3, 4])
        self.assertEqual([(e["stream_id"], e["version"]) for e in body["events"]],
                         [("ga-1", 1), ("ga-1", 2), ("ga-2", 1), ("ga-1", 3)])
        self.assertEqual(body["events"][0]["type"], "OrderPlaced")
        self.assertEqual(body["events"][0]["payload"], {"total": 10})
        self.assertIn("event_id", body["events"][0])
        self.assertEqual(body["next_cursor"], 4)
        self.assertFalse(body["has_more"])

    def test_pagination_with_after_and_limit(self) -> None:
        status, page1 = self.request("GET", "/events?limit=2")
        self.assertEqual(status, 200)
        self.assertEqual([e["cursor"] for e in page1["events"]], [1, 2])
        self.assertEqual(page1["next_cursor"], 2)
        self.assertTrue(page1["has_more"])
        status, page2 = self.request("GET", f"/events?after={page1['next_cursor']}&limit=2")
        self.assertEqual([e["cursor"] for e in page2["events"]], [3, 4])
        self.assertEqual(page2["next_cursor"], 4)
        self.assertFalse(page2["has_more"])
        # 末页之后继续翻：空页保持 after。
        status, page3 = self.request("GET", f"/events?after={page2['next_cursor']}")
        self.assertEqual((status, page3), (200, {"events": [], "next_cursor": 4, "has_more": False}))

    def test_after_beyond_max_is_empty_page(self) -> None:
        status, body = self.request("GET", "/events?after=1000000")
        self.assertEqual((status, body), (200, {"events": [], "next_cursor": 1000000,
                                                "has_more": False}))

    def test_snapshots_and_idempotency_records_are_not_events(self) -> None:
        _, body = self.request("GET", "/events")
        self.assertEqual(len(body["events"]), 4)
        self.assertTrue(all(set(e) == {"cursor", "stream_id", "version", "event_id",
                                       "type", "payload"} for e in body["events"]))

    def test_invalid_params_are_400(self) -> None:
        bad = [
            "after=", "after", "after=+1", "after=-1", "after=1.0", "after=1e1",
            "after=%201", "after=%D9%A1", "after=1&after=2",
            "limit=0", "limit=1001", "limit=-1", "limit=1.0", "limit=1e2",
            "limit=foo", "limit=2&limit=2", "limit=",
            "foo=1", "at=1", "since=2", "after=1&bar=2",
        ]
        for query in bad:
            with self.subTest(query=query):
                status, body = self.request("GET", f"/events?{query}")
                self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))

    def test_unknown_routes_still_404(self) -> None:
        self.assertEqual(self.request("GET", "/events/foo")[0], 404)
        self.assertEqual(self.request("POST", "/events", {})[0], 404)


class HttpGlobalFeedConcurrencyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.server = serve(port=0, db=":memory:")
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.ledger.close()  # type: ignore[attr-defined]

    def request(self, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def test_concurrent_pagination_never_repeats_or_skips(self) -> None:
        # 持续追加的同时按 next_cursor 翻页：不重不漏。
        total = 30

        def append_events() -> None:
            for index in range(total):
                self.request("POST", "/streams/ga-busy/events",
                             {"events": [{"type": "NoteRecorded", "payload": {"text": str(index)}}],
                              "expected_version": index})

        writer = threading.Thread(target=append_events)
        writer.start()
        seen: list[int] = []
        after = 0
        while True:
            status, page = self.request("GET", f"/events?after={after}&limit=4")
            self.assertEqual(status, 200)
            cursors = [e["cursor"] for e in page["events"]]
            self.assertEqual(cursors, sorted(cursors))
            seen.extend(cursors)
            after = page["next_cursor"]
            if not page["has_more"]:
                if writer.is_alive():
                    continue  # 边界后的新事件留给下一次查询
                break
        writer.join()
        # 写线程结束后再扫一次尾。
        while True:
            _, page = self.request("GET", f"/events?after={after}&limit=4")
            seen.extend(e["cursor"] for e in page["events"])
            after = page["next_cursor"]
            if not page["has_more"]:
                break
        self.assertEqual(seen, list(range(1, total + 1)))
        self.assertEqual(len(seen), len(set(seen)))


if __name__ == "__main__":
    unittest.main()
