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


class CommandIdempotencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger(":memory:")

    def tearDown(self) -> None:
        self.ledger.close()

    def command(self, **overrides: object) -> dict:
        body: dict = {"stream_id": "order-1", "expected_version": 0, "command_id": "cmd-1",
                      "events": [{"type": "OrderPlaced", "payload": {"total": 10}}]}
        body.update(overrides)
        return body

    def test_identical_retry_replays_without_writing(self) -> None:
        body = self.command()
        first, replayed = self.ledger.append_command(body["stream_id"], body["events"],
                                                     body["expected_version"], body["command_id"])
        self.assertFalse(replayed)
        second, replayed = self.ledger.append_command(body["stream_id"], body["events"],
                                                      body["expected_version"], body["command_id"])
        self.assertTrue(replayed)
        self.assertEqual([e.as_json() for e in second], [e.as_json() for e in first])
        self.assertEqual(self.ledger.version("order-1"), 1)

    def test_same_command_id_with_different_content_conflicts(self) -> None:
        body = self.command()
        self.ledger.append_command(body["stream_id"], body["events"], 0, body["command_id"])
        for changed in (self.command(events=[{"type": "OrderPlaced", "payload": {"total": 11}}]),
                        self.command(expected_version=1),
                        self.command(stream_id="order-2")):
            with self.assertRaises(IdempotencyConflict):
                self.ledger.append_command(changed["stream_id"], changed["events"],
                                           changed["expected_version"], changed["command_id"])
        self.assertEqual(self.ledger.version("order-1"), 1)
        self.assertEqual(self.ledger.version("order-2"), 0)

    def test_invalid_command_id_is_rejected(self) -> None:
        for bad in ("", "x" * 129, None, 42, True):
            with self.assertRaises(InvalidRequest):
                self.ledger.append_command("order-1", [{"type": "OrderPlaced", "payload": {}}], 0, bad)
        self.assertEqual(self.ledger.version("order-1"), 0)

    def test_failed_command_leaves_no_record(self) -> None:
        with self.assertRaises(InvalidRequest):
            self.ledger.append_command("order-1", [{"type": "NotAnEvent"}], 0, "cmd-1")
        with self.assertRaises(VersionConflict):
            self.ledger.append_command("order-1", [{"type": "OrderPlaced", "payload": {}}], 5, "cmd-1")
        written, replayed = self.ledger.append_command("order-1", [{"type": "OrderPlaced", "payload": {}}], 0, "cmd-1")
        self.assertFalse(replayed)
        self.assertEqual([e.version for e in written], [1])

    def test_command_record_survives_restart(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ledger.sqlite"
            ledger = Ledger(path)
            first, _ = ledger.append_command("order-1", [{"type": "OrderPlaced", "payload": {}}], 0, "cmd-1")
            ledger.close()
            ledger = Ledger(path)
            try:
                second, replayed = ledger.append_command("order-1", [{"type": "OrderPlaced", "payload": {}}], 0, "cmd-1")
                self.assertTrue(replayed)
                self.assertEqual([e.as_json() for e in second], [e.as_json() for e in first])
                with self.assertRaises(IdempotencyConflict):
                    ledger.append_command("order-1", [{"type": "OrderCancelled", "payload": {}}], 1, "cmd-1")
            finally:
                ledger.close()


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

    def test_command_id_replay_and_conflict(self) -> None:
        body = {"events": [{"type": "OrderPlaced", "payload": {"total": 5}}],
                "expected_version": 0, "command_id": "cmd-http-1"}
        status, first = self.request("POST", "/streams/o-2/events", body)
        self.assertEqual(status, 201)
        self.assertEqual(first["version"], 1)
        status, second = self.request("POST", "/streams/o-2/events", body)
        self.assertEqual((status, second), (200, first))
        status, third = self.request("POST", "/streams/o-2/events",
                                     {"events": [{"type": "NoteRecorded", "payload": {"text": "x"}}],
                                      "expected_version": 1, "command_id": "cmd-http-1"})
        self.assertEqual((status, third["error"]["code"]), (409, "idempotency_conflict"))
        status, state = self.request("GET", "/streams/o-2")
        self.assertEqual((status, state["version"]), (200, 1))

    def test_command_id_validation(self) -> None:
        for bad in ("", "x" * 129, None, 7):
            status, body = self.request("POST", "/streams/o-3/events",
                                        {"events": [{"type": "OrderPlaced", "payload": {}}],
                                         "expected_version": 0, "command_id": bad})
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertEqual(self.request("GET", "/streams/o-3")[0], 404)


if __name__ == "__main__":
    unittest.main()
