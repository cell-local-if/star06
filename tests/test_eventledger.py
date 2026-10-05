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


if __name__ == "__main__":
    unittest.main()
