"""Tests for the cross-aggregate transaction entry point (POST /transactions/{command_id}).

Covers whole-request validation before any write, atomic multi-stream commit with
consecutive global cursors, transaction-level idempotency (retry, restart,
concurrency, semantic conflict), first-mismatch version conflicts that write
nothing, and persistence of both outcomes across ledger reopen.
"""
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
    validate_transaction,
)


def two_stream_body(a_expected: int = 0, b_expected: int = 0) -> list[dict]:
    return [
        {"stream_id": "tx-a", "expected_version": a_expected,
         "events": [{"type": "OrderPlaced", "payload": {"total": 10}},
                    {"type": "NoteRecorded", "payload": {"text": "hi"}}]},
        {"stream_id": "tx-b", "expected_version": b_expected,
         "events": [{"type": "LineItemAdded", "payload": {"sku": "s"}}]},
    ]


class TransactionUnitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger(":memory:")

    def tearDown(self) -> None:
        self.ledger.close()

    def test_commit_gives_each_stream_consecutive_versions(self) -> None:
        response = self.ledger.append_transaction("t-1", two_stream_body())
        self.assertEqual([s["stream_id"] for s in response["streams"]], ["tx-a", "tx-b"])
        self.assertEqual([s["version"] for s in response["streams"]], [2, 1])
        self.assertEqual([e["version"] for e in response["streams"][0]["events"]], [1, 2])
        self.assertEqual([e["version"] for e in response["streams"][1]["events"]], [1])
        # Response events keep the cursor-less per-stream shape.
        for stream in response["streams"]:
            for event in stream["events"]:
                self.assertEqual(list(event.keys()),
                                 ["stream_id", "version", "event_id", "type", "payload"])
        self.assertEqual(self.ledger.version("tx-a"), 2)
        self.assertEqual(self.ledger.version("tx-b"), 1)

    def test_cursors_are_consecutive_in_streams_order(self) -> None:
        self.ledger.append("other", [{"type": "OrderPlaced", "payload": {}}], 0)
        self.ledger.append_transaction("t-2", two_stream_body())
        events, more = self.ledger.read_global(0, 100)
        self.assertFalse(more)
        self.assertEqual(
            [(e.stream_id, e.version, e.cursor) for e in events],
            [("other", 1, 1), ("tx-a", 1, 2), ("tx-a", 2, 3), ("tx-b", 1, 4)])

    def test_reads_and_replay_match_sequential_appends(self) -> None:
        self.ledger.append_transaction("t-3", two_stream_body())
        self.assertEqual([e.type for e in self.ledger.read("tx-a")],
                         ["OrderPlaced", "NoteRecorded"])
        self.assertEqual(replay(self.ledger.read("tx-a"))["notes"], ["hi"])
        self.assertEqual(replay(self.ledger.read_at("tx-a", 1))["status"], "placed")
        self.assertEqual(replay(self.ledger.read("tx-b"))["lines"], [{"sku": "s"}])

    def test_validation_rejects_before_any_write(self) -> None:
        bad_streams = [
            [],                                                              # empty
            [{"stream_id": "tx-a", "events": [{"type": "OrderPlaced", "payload": {}}],
              "expected_version": 0},
             {"stream_id": "tx-a", "events": [{"type": "NoteRecorded", "payload": {}}],
              "expected_version": 1}],                                       # duplicate stream_id
            [{"stream_id": "tx-a", "events": [], "expected_version": 0}],    # empty events
            [{"stream_id": "tx-a", "events": [{"type": "Nope"}], "expected_version": 0}],
            [{"stream_id": "tx-a", "events": [{"type": "OrderPlaced", "payload": {}, "x": 1}],
              "expected_version": 0}],                                       # unknown event field
            [{"stream_id": "tx-a", "events": [{"type": "OrderPlaced", "payload": {}}],
              "expected_version": -1}],
            [{"stream_id": "", "events": [{"type": "OrderPlaced", "payload": {}}],
              "expected_version": 0}],
            [{"stream_id": "tx-a", "events": [{"type": "OrderPlaced", "payload": {}}],
              "expected_version": 0, "command_id": "nested"}],               # unknown item field
            [{"events": [{"type": "OrderPlaced", "payload": {}}], "expected_version": 0}],
            ["not-an-object"],
        ]
        for streams in bad_streams:
            with self.subTest(streams=streams):
                with self.assertRaises(InvalidRequest):
                    self.ledger.append_transaction("t-bad", streams)
        for bad in ("", "x" * 201, 123):
            with self.subTest(command_id=bad):
                with self.assertRaises(InvalidRequest):
                    self.ledger.append_transaction(bad, two_stream_body())
        # Nothing was written by any of the failures.
        self.assertEqual(self.ledger.streams(), [])
        events, _ = self.ledger.read_global(0, 100)
        self.assertEqual(events, [])
        # The command_id used by failed requests is still free.
        response = self.ledger.append_transaction("t-bad", two_stream_body())
        self.assertEqual(response["streams"][0]["version"], 2)

    def test_validate_transaction_shapes(self) -> None:
        with self.assertRaises(InvalidRequest):
            validate_transaction(None)
        with self.assertRaises(InvalidRequest):
            validate_transaction({})
        cleaned = validate_transaction(two_stream_body())
        self.assertEqual([item[0] for item in cleaned], ["tx-a", "tx-b"])

    def test_retry_returns_first_response_without_new_events_or_cursors(self) -> None:
        first = self.ledger.append_transaction("t-4", two_stream_body())
        # Payload key order reversed: semantically equal.
        reordered = [
            {"stream_id": "tx-a", "expected_version": 0,
             "events": [{"type": "OrderPlaced", "payload": {"total": 10}},
                        {"type": "NoteRecorded", "payload": {"text": "hi"}}]},
            {"stream_id": "tx-b", "expected_version": 0,
             "events": [{"type": "LineItemAdded", "payload": {"sku": "s"}}]},
        ]
        second = self.ledger.append_transaction("t-4", reordered)
        self.assertEqual(first, second)
        self.assertEqual(len(self.ledger.read("tx-a")), 2)
        self.assertEqual(len(self.ledger.read("tx-b")), 1)
        events, _ = self.ledger.read_global(0, 100)
        self.assertEqual([e.cursor for e in events], [1, 2, 3])

    def test_semantic_difference_is_idempotency_conflict(self) -> None:
        self.ledger.append_transaction("t-5", two_stream_body())
        changed_events = two_stream_body()
        changed_events[0]["events"][0]["payload"] = {"total": 11}
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append_transaction("t-5", changed_events)
        reordered_streams = [two_stream_body()[1], two_stream_body()[0]]
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append_transaction("t-5", reordered_streams)
        dropped = two_stream_body()[:1]
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append_transaction("t-5", dropped)
        stale = two_stream_body(a_expected=2, b_expected=1)
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append_transaction("t-5", stale)
        # Nothing extra landed.
        self.assertEqual(len(self.ledger.read("tx-a")), 2)
        self.assertEqual(len(self.ledger.read("tx-b")), 1)

    def test_idempotency_conflict_beats_version_conflict(self) -> None:
        self.ledger.append_transaction("t-6", two_stream_body())
        # Same command, different events AND now-stale expected_version.
        changed = two_stream_body()
        changed[1]["events"] = [{"type": "OrderCancelled", "payload": {}}]
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append_transaction("t-6", changed)

    def test_first_version_conflict_in_array_order_wins_and_writes_nothing(self) -> None:
        self.ledger.append("tx-a", [{"type": "OrderPlaced", "payload": {}}], 0)
        self.ledger.append("tx-b", [{"type": "OrderPlaced", "payload": {}}], 0)
        # Both streams mismatch; the first array entry must be the reported one.
        with self.assertRaises(VersionConflict) as caught:
            self.ledger.append_transaction("t-7", two_stream_body(a_expected=0, b_expected=0))
        self.assertIn("tx-a", str(caught.exception))
        self.assertEqual(self.ledger.version("tx-a"), 1)
        self.assertEqual(self.ledger.version("tx-b"), 1)
        events, _ = self.ledger.read_global(0, 100)
        self.assertEqual([e.cursor for e in events], [1, 2])
        # A failed transaction leaves no hitable idempotency record.
        response = self.ledger.append_transaction("t-7", two_stream_body(a_expected=1, b_expected=1))
        self.assertEqual([s["version"] for s in response["streams"]], [3, 2])

    def test_concurrent_identical_retries_commit_one_batch(self) -> None:
        def submit(_: int) -> dict:
            return self.ledger.append_transaction("t-8", two_stream_body())

        with ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(submit, range(16)))
        self.assertTrue(all(r == responses[0] for r in responses))
        self.assertEqual(len(self.ledger.read("tx-a")), 2)
        self.assertEqual(len(self.ledger.read("tx-b")), 1)
        events, _ = self.ledger.read_global(0, 100)
        self.assertEqual([e.cursor for e in events], [1, 2, 3])


class TransactionPersistenceTests(unittest.TestCase):
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

    def test_result_and_idempotent_hit_survive_reopen(self) -> None:
        first = self.ledger.append_transaction("t-p1", two_stream_body())
        self.reopen()
        again = self.ledger.append_transaction("t-p1", two_stream_body())
        self.assertEqual(first, again)
        self.assertEqual(len(self.ledger.read("tx-a")), 2)
        self.assertEqual(len(self.ledger.read("tx-b")), 1)
        events, _ = self.ledger.read_global(0, 100)
        self.assertEqual([e.cursor for e in events], [1, 2, 3])
        # A new transaction after reopen continues the dense cursor sequence.
        followup = self.ledger.append_transaction("t-p2", [
            {"stream_id": "tx-c", "expected_version": 0,
             "events": [{"type": "NoteRecorded", "payload": {"text": "n"}}]}])
        self.assertEqual(followup["streams"][0]["version"], 1)
        events, _ = self.ledger.read_global(0, 100)
        self.assertEqual([e.cursor for e in events], [1, 2, 3, 4])
        # Conflict outcomes are also stable across reopen.
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append_transaction("t-p1", two_stream_body(a_expected=0, b_expected=1))
        with self.assertRaises(VersionConflict):
            self.ledger.append_transaction("t-p3", two_stream_body(a_expected=0, b_expected=0))


class HttpTransactionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db_path = str(Path(cls.tmp.name) / "transactions.sqlite")
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
                raw: bytes | None = None) -> tuple[int, dict]:
        data = raw if raw is not None else (None if body is None else json.dumps(body).encode())
        request = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data,
                                         method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def transaction(self, command_id: str, a: str = "h-a", b: str = "h-b",
                    a_expected: int = 0, b_expected: int = 0) -> tuple[int, dict]:
        return self.request("POST", f"/transactions/{command_id}", {"streams": [
            {"stream_id": a, "expected_version": a_expected,
             "events": [{"type": "OrderPlaced", "payload": {"total": 1}}]},
            {"stream_id": b, "expected_version": b_expected,
             "events": [{"type": "NoteRecorded", "payload": {"text": "n"}}]},
        ]})

    def test_commit_response_shape(self) -> None:
        status, body = self.transaction("http-1")
        self.assertEqual(status, 201)
        self.assertEqual(set(body), {"transaction_id", "streams"})
        self.assertTrue(body["transaction_id"])
        self.assertEqual([s["stream_id"] for s in body["streams"]], ["h-a", "h-b"])
        self.assertEqual([s["version"] for s in body["streams"]], [1, 1])
        for stream in body["streams"]:
            self.assertEqual(set(stream), {"stream_id", "version", "events"})
            for event in stream["events"]:
                self.assertEqual(list(event.keys()),
                                 ["stream_id", "version", "event_id", "type", "payload"])
        # The batch is one contiguous run in the global audit trail.
        status, audit = self.request("GET", "/events?limit=1000")
        mine = [e for e in audit["events"] if e["stream_id"] in ("h-a", "h-b")]
        self.assertEqual([(e["stream_id"], e["version"]) for e in mine],
                         [("h-a", 1), ("h-b", 1)])
        self.assertEqual(mine[1]["cursor"], mine[0]["cursor"] + 1)

    def test_retry_returns_identical_response(self) -> None:
        status, first = self.transaction("http-2", a="r-a", b="r-b")
        self.assertEqual(status, 201)
        status, second = self.transaction("http-2", a="r-a", b="r-b")
        self.assertEqual(status, 201)
        self.assertEqual(first, second)
        _, read = self.request("GET", "/streams/r-a/events")
        self.assertEqual(len(read["events"]), 1)

    def test_idempotency_conflict_over_http(self) -> None:
        status, _ = self.transaction("http-3", a="i-a", b="i-b")
        self.assertEqual(status, 201)
        status, body = self.request("POST", "/transactions/http-3", {"streams": [
            {"stream_id": "i-a", "expected_version": 0,
             "events": [{"type": "OrderCancelled", "payload": {}}]},
        ]})
        self.assertEqual((status, body["error"]["code"]), (409, "idempotency_conflict"))

    def test_version_conflict_writes_nothing(self) -> None:
        status, _ = self.transaction("http-4", a="vc-a", b="vc-b")
        self.assertEqual(status, 201)
        status, body = self.request("POST", "/transactions/http-5", {"streams": [
            {"stream_id": "vc-a", "expected_version": 0,
             "events": [{"type": "NoteRecorded", "payload": {"text": "x"}}]},
            {"stream_id": "vc-c", "expected_version": 0,
             "events": [{"type": "OrderPlaced", "payload": {}}]},
        ]})
        self.assertEqual((status, body["error"]["code"]), (409, "version_conflict"))
        # Neither the conflicting stream nor the valid sibling moved.
        self.assertEqual(self.request("GET", "/streams/vc-a")[1]["version"], 1)
        self.assertEqual(self.request("GET", "/streams/vc-c")[0], 404)
        # The failed command_id is still usable for a later valid commit.
        status, _ = self.request("POST", "/transactions/http-5", {"streams": [
            {"stream_id": "vc-c", "expected_version": 0,
             "events": [{"type": "OrderPlaced", "payload": {}}]},
        ]})
        self.assertEqual(status, 201)

    def test_invalid_requests_are_400_and_write_nothing(self) -> None:
        bad_bodies = [
            {},                                            # missing streams
            {"streams": [], "extra": 1},                   # unknown top-level field
            {"streams": []},                               # empty streams
            {"streams": [{"stream_id": "v-1", "expected_version": 0,
                          "events": [{"type": "OrderPlaced", "payload": {}}]},
                         {"stream_id": "v-1", "expected_version": 1,
                          "events": [{"type": "NoteRecorded", "payload": {}}]}]},
            {"streams": [{"stream_id": "v-2", "expected_version": 0}], "x": 1},
            {"streams": [{"stream_id": "v-2", "expected_version": 0,
                          "events": [{"type": "Nope"}]}]},
            {"streams": [{"stream_id": "v-2", "expected_version": True,
                          "events": [{"type": "OrderPlaced", "payload": {}}]}]},
            {"streams": [{"stream_id": "v-2", "expected_version": 0,
                          "events": [{"type": "OrderPlaced", "payload": {}}],
                          "command_id": "nested"}]},
            {"streams": "not-a-list"},
        ]
        for body in bad_bodies:
            with self.subTest(body=body):
                status, resp = self.request("POST", "/transactions/http-bad", body)
                self.assertEqual((status, resp["error"]["code"]), (400, "invalid_request"))
        status, resp = self.request("POST", "/transactions/http-bad", raw=b"[1, 2]")
        self.assertEqual((status, resp["error"]["code"]), (400, "invalid_request"))
        status, resp = self.request("POST", "/transactions/http-bad", raw=b"{not json")
        self.assertEqual((status, resp["error"]["code"]), (400, "invalid_request"))
        # command_id over 200 characters is invalid; the route itself exists.
        status, resp = self.request("POST", f"/transactions/{'x' * 201}",
                                    {"streams": [{"stream_id": "v-3", "expected_version": 0,
                                                  "events": [{"type": "OrderPlaced", "payload": {}}]}]})
        self.assertEqual((status, resp["error"]["code"]), (400, "invalid_request"))
        # None of the failures wrote anything.
        self.assertEqual(self.request("GET", "/streams/v-1")[0], 404)
        self.assertEqual(self.request("GET", "/streams/v-2")[0], 404)
        self.assertEqual(self.request("GET", "/streams/v-3")[0], 404)

    def test_concurrent_retries_commit_exactly_one_batch(self) -> None:
        def submit(_: int) -> tuple[int, dict]:
            return self.transaction("http-race", a="race-a", b="race-b")

        with ThreadPoolExecutor(max_workers=8) as pool:
            responses = list(pool.map(submit, range(16)))
        self.assertTrue(all(status == 201 for status, _ in responses))
        bodies = {json.dumps(body, sort_keys=True) for _, body in responses}
        self.assertEqual(len(bodies), 1)
        _, read_a = self.request("GET", "/streams/race-a/events")
        _, read_b = self.request("GET", "/streams/race-b/events")
        self.assertEqual(len(read_a["events"]), 1)
        self.assertEqual(len(read_b["events"]), 1)

    def test_unknown_routes_still_404(self) -> None:
        self.assertEqual(self.request("GET", "/transactions/http-1")[0], 404)
        self.assertEqual(self.request("POST", "/transactions", {"streams": []})[0], 404)
        self.assertEqual(self.request("POST", "/transactions/x/y", {"streams": []})[0], 404)

    def test_single_stream_surface_is_unaffected(self) -> None:
        status, _ = self.request("POST", "/streams/solo/events",
                                 {"events": [{"type": "OrderPlaced", "payload": {}}],
                                  "expected_version": 0, "command_id": "solo-1"})
        self.assertEqual(status, 201)
        # command_id namespaces are independent: the same id can name a transaction.
        status, _ = self.request("POST", "/transactions/solo-1", {"streams": [
            {"stream_id": "solo-tx", "expected_version": 0,
             "events": [{"type": "OrderPlaced", "payload": {}}]}]})
        self.assertEqual(status, 201)


if __name__ == "__main__":
    unittest.main()
