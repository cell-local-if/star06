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
    VersionConflict,
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


class HttpTimeTravelTests(unittest.TestCase):
    """GET /streams/{stream_id}?at=<version>: historical state queries."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db = str(Path(cls.tmp.name) / "ledger.sqlite")
        cls.server = serve(port=0, db=cls.db)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
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

    @classmethod
    def seed(cls, client: "HttpTimeTravelTests", stream: str) -> None:
        status, _ = client.request("POST", f"/streams/{stream}/events",
                                   {"events": [{"type": "OrderPlaced", "payload": {"total": 10}},
                                               {"type": "LineItemAdded", "payload": {"sku": "a"}}],
                                    "expected_version": 0})
        assert status == 201
        status, _ = client.request("POST", f"/streams/{stream}/events",
                                   {"events": [{"type": "NoteRecorded", "payload": {"text": "hi"}},
                                               {"type": "OrderCancelled", "payload": {}}],
                                    "expected_version": 2})
        assert status == 201

    def test_at_replays_only_events_up_to_that_version(self) -> None:
        self.seed(self, "t-1")
        status, body = self.request("GET", "/streams/t-1?at=2")
        self.assertEqual(status, 200)
        self.assertEqual(body["version"], 2)
        self.assertEqual(body["state"], {"status": "placed", "lines": [{"sku": "a"}],
                                         "notes": [], "cancelled": False})
        status, body = self.request("GET", "/streams/t-1?at=4")
        self.assertEqual(status, 200)
        self.assertEqual(body["version"], 4)
        self.assertEqual(body["state"]["status"], "cancelled")
        self.assertEqual(body["state"]["notes"], ["hi"])
        # Latest projection without at is unchanged.
        status, latest = self.request("GET", "/streams/t-1")
        self.assertEqual((status, latest["version"]), (200, 4))
        self.assertEqual(latest["state"], body["state"])

    def test_at_zero_on_existing_stream_returns_initial_projection(self) -> None:
        self.seed(self, "t-2")
        status, body = self.request("GET", "/streams/t-2?at=0")
        self.assertEqual(status, 200)
        self.assertEqual(body, {"state": {"status": "unknown", "lines": [], "notes": [],
                                          "cancelled": False},
                                "version": 0})

    def test_at_zero_on_missing_stream_is_not_found(self) -> None:
        self.assertEqual(self.request("GET", "/streams/t-missing?at=0")[0], 404)

    def test_at_beyond_current_version_is_not_found(self) -> None:
        self.seed(self, "t-3")
        status, body = self.request("GET", "/streams/t-3?at=5")
        self.assertEqual((status, body["error"]["code"]), (404, "not_found"))
        self.assertEqual(self.request("GET", "/streams/t-3?at=99999999999999999999999")[0], 404)

    def test_invalid_at_is_rejected_before_existence_check(self) -> None:
        for bad in ("", "+1", " 1", "1 ", "1.0", "1e2", "-1", "abc", "0x10"):
            for stream in ("t-4", "t-never-written"):
                status, body = self.request("GET", f"/streams/{stream}?at={bad.replace(' ', '%20')}")
                self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"),
                                 f"at={bad!r} on {stream}")
        self.seed(self, "t-4")
        status, body = self.request("GET", "/streams/t-4?at=1&at=2")
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        status, body = self.request("GET", "/streams/t-4?at")
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))

    def test_historical_query_survives_reopen(self) -> None:
        self.seed(self, "t-5")
        status, before = self.request("GET", "/streams/t-5?at=3")
        self.assertEqual(status, 200)
        # Restart the whole service on the same sqlite file (the handler closes
        # over its ledger, so swapping server.ledger alone would not reopen it).
        cls = self.__class__
        cls.server.shutdown()
        cls.server.server_close()
        cls.server.ledger.close()
        cls.server = serve(port=0, db=cls.db)
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        status, after = self.request("GET", "/streams/t-5?at=3")
        self.assertEqual((status, after), (200, before))

    def test_concurrent_appends_never_straddle_the_boundary(self) -> None:
        status, _ = self.request("POST", "/streams/t-6/events",
                                 {"events": [{"type": "OrderPlaced", "payload": {}}],
                                  "expected_version": 0})
        self.assertEqual(status, 201)
        stop = threading.Event()
        failures: list[str] = []

        def append_more() -> None:
            while not stop.is_set():
                current = self.server.ledger.version("t-6")  # type: ignore[attr-defined]
                self.request("POST", "/streams/t-6/events",
                             {"events": [{"type": "NoteRecorded", "payload": {"text": "x"}}],
                              "expected_version": current})

        def check() -> None:
            while not stop.is_set():
                status, body = self.request("GET", "/streams/t-6?at=1")
                if status != 200:
                    failures.append(f"status {status}")
                    return
                if body["version"] != 1 or body["state"] != replay(
                        [e for e in self.server.ledger.read("t-6") if e.version <= 1]):  # type: ignore[attr-defined]
                    failures.append(f"inconsistent boundary: {body}")
                    return

        writers = [threading.Thread(target=append_more) for _ in range(2)]
        readers = [threading.Thread(target=check) for _ in range(4)]
        for thread in writers + readers:
            thread.start()
        readers[0].join(timeout=2)
        stop.set()
        for thread in writers + readers:
            thread.join()
        self.assertEqual(failures, [])

    def test_events_since_semantics_unchanged(self) -> None:
        self.seed(self, "t-7")
        status, body = self.request("GET", "/streams/t-7/events?since=2")
        self.assertEqual(status, 200)
        self.assertEqual([e["version"] for e in body["events"]], [3, 4])
        self.assertEqual(self.request("GET", "/streams/t-missing-2/events")[0], 404)


class ParseAtTests(unittest.TestCase):
    def test_valid_values(self) -> None:
        from eventledger import parse_at
        self.assertIsNone(parse_at(""))
        self.assertIsNone(parse_at("since=3"))
        self.assertEqual(parse_at("at=0"), 0)
        self.assertEqual(parse_at("at=007"), 7)
        self.assertEqual(parse_at("since=1&at=42"), 42)

    def test_invalid_values(self) -> None:
        from eventledger import parse_at
        for query in ("at=", "at", "at=+1", "at=-1", "at= 1", "at=1 ", "at=1.0",
                      "at=1e3", "at=abc", "at=1&at=2", "at=1&at=1"):
            with self.assertRaises(InvalidRequest, msg=query):
                parse_at(query)


if __name__ == "__main__":
    unittest.main()
