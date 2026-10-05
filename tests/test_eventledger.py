"""Baseline tests for the event ledger: the shape a recording/acceptance run executes."""
from __future__ import annotations

import json
import threading
import unittest
import urllib.error
import urllib.request

from eventledger import (  # noqa: E402
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

    def test_command_retry_replays_first_result_without_new_version(self) -> None:
        events = [{"type": "OrderPlaced", "payload": {"total": 10}}]
        created, first = self.ledger.append_command("cmd-1", "order-5", events, 0)
        self.assertTrue(created)
        self.assertEqual(first["version"], 1)
        first_ids = [e["event_id"] for e in first["events"]]
        created, second = self.ledger.append_command("cmd-1", "order-5", events, 0)
        self.assertFalse(created)
        self.assertEqual(second, first)
        self.assertEqual([e["event_id"] for e in second["events"]], first_ids)
        self.assertEqual(self.ledger.version("order-5"), 1)
        self.assertEqual(len(self.ledger.read("order-5")), 1)

    def test_command_conflicts_on_different_inputs_without_writing(self) -> None:
        events = [{"type": "OrderPlaced", "payload": {}}]
        self.ledger.append_command("cmd-2", "order-6", events, 0)
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append_command("cmd-2", "order-6", [{"type": "NoteRecorded", "payload": {}}], 0)
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append_command("cmd-2", "order-7", events, 0)
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append_command("cmd-2", "order-6", events, 1)
        self.assertEqual(self.ledger.version("order-6"), 1)
        self.assertEqual(self.ledger.version("order-7"), 0)
        self.assertEqual(len(self.ledger.read("order-6")), 1)

    def test_command_invalid_request_leaves_no_record(self) -> None:
        with self.assertRaises(InvalidRequest):
            self.ledger.append_command("cmd-3", "order-8", [{"type": "NotAnEvent"}], 0)
        with self.assertRaises(InvalidRequest):
            self.ledger.append_command("", "order-8", [{"type": "OrderPlaced", "payload": {}}], 0)
        with self.assertRaises(InvalidRequest):
            self.ledger.append_command("x" * 129, "order-8", [{"type": "OrderPlaced", "payload": {}}], 0)
        # The command_id is now reusable: nothing was persisted.
        created, result = self.ledger.append_command("cmd-3", "order-8",
                                                     [{"type": "OrderPlaced", "payload": {}}], 0)
        self.assertTrue(created)
        self.assertEqual(result["version"], 1)

    def test_command_version_conflict_is_not_an_idempotency_conflict(self) -> None:
        events = [{"type": "OrderPlaced", "payload": {}}]
        self.ledger.append_command("cmd-4", "order-9", events, 0)
        with self.assertRaises(VersionConflict):
            self.ledger.append_command("cmd-5", "order-9", events, 0)
        self.assertIsNone(self.ledger.command("cmd-5"))

    def test_command_record_survives_reopen(self) -> None:
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "ledger.sqlite"
            first_ledger = Ledger(db)
            events = [{"type": "OrderPlaced", "payload": {"total": 42}}]
            _, first = first_ledger.append_command("cmd-persist", "order-10", events, 0)
            first_ledger.close()

            reopened = Ledger(db)
            try:
                created, second = reopened.append_command("cmd-persist", "order-10", events, 0)
                self.assertFalse(created)
                self.assertEqual(second, first)
                self.assertEqual(reopened.version("order-10"), 1)
                with self.assertRaises(IdempotencyConflict):
                    reopened.append_command("cmd-persist", "order-10",
                                            [{"type": "NoteRecorded", "payload": {}}], 0)
            finally:
                reopened.close()


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

    def test_command_id_retry_returns_200_with_first_result(self) -> None:
        payload = {"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0,
                   "command_id": "http-cmd-1"}
        status, first = self.request("POST", "/streams/c-1/events", payload)
        self.assertEqual(status, 201)
        self.assertEqual(first["version"], 1)
        status, second = self.request("POST", "/streams/c-1/events", payload)
        self.assertEqual(status, 200)
        self.assertEqual(second, first)
        status, body = self.request("GET", "/streams/c-1/events")
        self.assertEqual(status, 200)
        self.assertEqual(len(body["events"]), 1)
        self.assertEqual(body["events"][0]["event_id"], first["events"][0]["event_id"])

    def test_command_id_conflict_returns_409_and_writes_nothing(self) -> None:
        base = {"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0,
                "command_id": "http-cmd-2"}
        status, _ = self.request("POST", "/streams/c-2/events", base)
        self.assertEqual(status, 201)
        status, body = self.request("POST", "/streams/c-2/events",
                                    {**base, "events": [{"type": "NoteRecorded", "payload": {"text": "x"}}]})
        self.assertEqual((status, body["error"]["code"]), (409, "idempotency_conflict"))
        status, body = self.request("POST", "/streams/c-other/events", base)
        self.assertEqual((status, body["error"]["code"]), (409, "idempotency_conflict"))
        self.assertEqual(self.request("GET", "/streams/c-other")[0], 404)

    def test_command_id_invalid_request_returns_400(self) -> None:
        for bad_command_id in ("", 7, None):
            status, body = self.request(
                "POST", "/streams/c-3/events",
                {"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0,
                 "command_id": bad_command_id})
            self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"), bad_command_id)
        status, body = self.request(
            "POST", "/streams/c-3/events",
            {"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0,
             "command_id": "x" * 129})
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        self.assertEqual(self.request("GET", "/streams/c-3")[0], 404)

    def test_command_id_invalid_batch_leaves_no_command_record(self) -> None:
        bad = {"events": [{"type": "NotAnEvent"}], "expected_version": 0, "command_id": "http-cmd-4"}
        status, body = self.request("POST", "/streams/c-4/events", bad)
        self.assertEqual((status, body["error"]["code"]), (400, "invalid_request"))
        good = {**bad, "events": [{"type": "OrderPlaced", "payload": {}}]}
        status, body = self.request("POST", "/streams/c-4/events", good)
        self.assertEqual(status, 201)
        self.assertEqual(body["version"], 1)

    def test_command_id_stale_expected_version_is_version_conflict(self) -> None:
        first = {"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0,
                 "command_id": "http-cmd-5"}
        self.assertEqual(self.request("POST", "/streams/c-5/events", first)[0], 201)
        second = {"events": [{"type": "LineItemAdded", "payload": {}}], "expected_version": 0,
                  "command_id": "http-cmd-6"}
        status, body = self.request("POST", "/streams/c-5/events", second)
        self.assertEqual((status, body["error"]["code"]), (409, "version_conflict"))
        # The failed command was not recorded: the same command_id works once the version matches.
        second["expected_version"] = 1
        status, body = self.request("POST", "/streams/c-5/events", second)
        self.assertEqual(status, 201)
        self.assertEqual(body["version"], 2)

    def test_request_without_command_id_is_unchanged(self) -> None:
        payload = {"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0}
        status, first = self.request("POST", "/streams/c-6/events", payload)
        self.assertEqual(status, 201)
        status, body = self.request("POST", "/streams/c-6/events", payload)
        self.assertEqual((status, body["error"]["code"]), (409, "version_conflict"))

    def test_command_record_survives_server_restart(self) -> None:
        import tempfile

        db_path = tempfile.mktemp(suffix=".sqlite")
        server = serve(port=0, db=db_path)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            payload = {"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0,
                       "command_id": "http-cmd-persist"}

            def post() -> tuple[int, dict]:
                data = json.dumps(payload).encode()
                request = urllib.request.Request(f"http://127.0.0.1:{port}/streams/c-7/events",
                                                 data=data, method="POST",
                                                 headers={"Content-Type": "application/json"})
                try:
                    with urllib.request.urlopen(request, timeout=5) as response:
                        return response.status, json.loads(response.read() or b"{}")
                except urllib.error.HTTPError as error:
                    return error.code, json.loads(error.read() or b"{}")

            status, first = post()
            self.assertEqual(status, 201)
        finally:
            server.shutdown()
            server.server_close()
            server.ledger.close()  # type: ignore[attr-defined]

        server = serve(port=0, db=db_path)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            data = json.dumps(payload).encode()
            request = urllib.request.Request(f"http://127.0.0.1:{port}/streams/c-7/events",
                                             data=data, method="POST",
                                             headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=5) as response:
                status, second = response.status, json.loads(response.read() or b"{}")
            self.assertEqual(status, 200)
            self.assertEqual(second, first)
        finally:
            server.shutdown()
            server.server_close()
            server.ledger.close()  # type: ignore[attr-defined]


if __name__ == "__main__":
    unittest.main()
