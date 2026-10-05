"""Baseline tests for the event ledger: the shape a recording/acceptance run executes."""
from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from eventledger import (IdempotencyConflict, InvalidRequest, Ledger, VersionConflict,
                         replay, serve, validate_append)


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


class IdempotencyUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger(":memory:")

    def tearDown(self) -> None:
        self.ledger.close()

    def test_retry_returns_first_response_without_new_events(self) -> None:
        events = [{"type": "OrderPlaced", "payload": {"total": 10}}]
        first = self.ledger.append("s-1", events, 0, "cmd-1")
        second = self.ledger.append("s-1", events, 0, "cmd-1")
        self.assertEqual([e.event_id for e in second], [e.event_id for e in first])
        self.assertEqual([e.as_json() for e in second], [e.as_json() for e in first])
        self.assertEqual(self.ledger.version("s-1"), 1)

    def test_payload_key_order_is_semantically_equal(self) -> None:
        first = self.ledger.append("s-2", [{"type": "OrderPlaced", "payload": {"a": 1, "b": 2}}], 0, "cmd-2")
        second = self.ledger.append("s-2", [{"type": "OrderPlaced", "payload": {"b": 2, "a": 1}}], 0, "cmd-2")
        self.assertEqual([e.event_id for e in second], [e.event_id for e in first])
        self.assertEqual(self.ledger.version("s-2"), 1)

    def test_array_order_is_significant(self) -> None:
        events = [{"type": "OrderPlaced", "payload": {}}, {"type": "NoteRecorded", "payload": {"text": "x"}}]
        self.ledger.append("s-3", events, 0, "cmd-3")
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append("s-3", list(reversed(events)), 0, "cmd-3")

    def test_different_request_with_same_command_id_conflicts(self) -> None:
        self.ledger.append("s-4", [{"type": "OrderPlaced", "payload": {}}], 0, "cmd-4")
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append("s-4", [{"type": "OrderCancelled", "payload": {}}], 1, "cmd-4")
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append("s-4", [{"type": "OrderPlaced", "payload": {}}], 1, "cmd-4")
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append("s-5", [{"type": "OrderPlaced", "payload": {}}], 0, "cmd-4")
        self.assertEqual(self.ledger.version("s-4"), 1)
        self.assertEqual(self.ledger.version("s-5"), 0)

    def test_idempotency_conflict_precedes_stale_version_conflict(self) -> None:
        self.ledger.append("s-6", [{"type": "OrderPlaced", "payload": {}}], 0, "cmd-6")
        self.ledger.append("s-6", [{"type": "NoteRecorded", "payload": {"text": "n"}}], 1)
        # expected_version 0 is stale now, but the recorded command differs only in events:
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append("s-6", [{"type": "OrderCancelled", "payload": {}}], 0, "cmd-6")

    def test_version_conflict_leaves_no_idempotency_record(self) -> None:
        self.ledger.append("s-7", [{"type": "OrderPlaced", "payload": {}}], 0)
        with self.assertRaises(VersionConflict):
            self.ledger.append("s-7", [{"type": "OrderCancelled", "payload": {}}], 0, "cmd-7")
        # The failed attempt must not be replayable: the same command_id is still free.
        written = self.ledger.append("s-7", [{"type": "OrderCancelled", "payload": {}}], 1, "cmd-7")
        self.assertEqual(written[-1].version, 2)

    def test_invalid_events_leave_no_idempotency_record(self) -> None:
        with self.assertRaises(InvalidRequest):
            self.ledger.append("s-8", [{"type": "NotAnEvent"}], 0, "cmd-8")
        written = self.ledger.append("s-8", [{"type": "OrderPlaced", "payload": {}}], 0, "cmd-8")
        self.assertEqual(written[-1].version, 1)

    def test_invalid_command_id_rejected(self) -> None:
        for bad in ("", "x" * 201, 42, {"id": 1}):
            with self.assertRaises(InvalidRequest):
                self.ledger.append("s-9", [{"type": "OrderPlaced", "payload": {}}], 0, bad)
        self.assertEqual(self.ledger.version("s-9"), 0)

    def test_concurrent_identical_retries_land_one_batch(self) -> None:
        results: list[list] = [None] * 8  # type: ignore[list-item]

        def attempt(index: int) -> None:
            results[index] = self.ledger.append(
                "s-10", [{"type": "OrderPlaced", "payload": {"n": 1}}], 0, "cmd-10")

        threads = [threading.Thread(target=attempt, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(self.ledger.version("s-10"), 1)
        first_ids = [e.event_id for e in results[0]]
        for result in results[1:]:
            self.assertEqual([e.event_id for e in result], first_ids)

    def test_idempotency_survives_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = str(Path(tmp) / "ledger.sqlite")
            ledger = Ledger(db)
            first = ledger.append("s-11", [{"type": "OrderPlaced", "payload": {"total": 5}}], 0, "cmd-11")
            ledger.close()
            reopened = Ledger(db)
            try:
                second = reopened.append("s-11", [{"type": "OrderPlaced", "payload": {"total": 5}}], 0, "cmd-11")
                self.assertEqual([e.as_json() for e in second], [e.as_json() for e in first])
                self.assertEqual(reopened.version("s-11"), 1)
                with self.assertRaises(IdempotencyConflict):
                    reopened.append("s-11", [{"type": "OrderPlaced", "payload": {"total": 6}}], 0, "cmd-11")
            finally:
                reopened.close()


class IdempotencyHttpTests(unittest.TestCase):
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

    def test_retry_over_http_returns_identical_201(self) -> None:
        body = {"events": [{"type": "OrderPlaced", "payload": {"a": 1, "b": 2}}],
                "expected_version": 0, "command_id": "http-cmd-1"}
        first_status, first = self.request("POST", "/streams/h-1/events", body)
        self.assertEqual(first_status, 201)
        retry = dict(body, events=[{"type": "OrderPlaced", "payload": {"b": 2, "a": 1}}])
        second_status, second = self.request("POST", "/streams/h-1/events", retry)
        self.assertEqual(second_status, 201)
        self.assertEqual(second, first)
        status, events_body = self.request("GET", "/streams/h-1/events")
        self.assertEqual(status, 200)
        self.assertEqual(len(events_body["events"]), 1)

    def test_conflicting_command_id_over_http(self) -> None:
        body = {"events": [{"type": "OrderPlaced", "payload": {}}],
                "expected_version": 0, "command_id": "http-cmd-2"}
        self.assertEqual(self.request("POST", "/streams/h-2/events", body)[0], 201)
        conflict = dict(body, expected_version=0,
                        events=[{"type": "OrderCancelled", "payload": {}}])
        status, reply = self.request("POST", "/streams/h-2/events", conflict)
        self.assertEqual((status, reply["error"]["code"]), (409, "idempotency_conflict"))

    def test_invalid_command_id_over_http(self) -> None:
        body = {"events": [{"type": "OrderPlaced", "payload": {}}],
                "expected_version": 0, "command_id": ""}
        status, reply = self.request("POST", "/streams/h-3/events", body)
        self.assertEqual((status, reply["error"]["code"]), (400, "invalid_request"))

    def test_omitted_command_id_keeps_baseline_behaviour(self) -> None:
        body = {"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0}
        self.assertEqual(self.request("POST", "/streams/h-4/events", body)[0], 201)
        status, reply = self.request("POST", "/streams/h-4/events", body)
        self.assertEqual((status, reply["error"]["code"]), (409, "version_conflict"))


if __name__ == "__main__":
    unittest.main()
