"""Cross-stream transactions: POST /transactions/{command_id}."""
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
    serve,
    transaction_fingerprint,
    validate_transaction,
)

EV = lambda kind, payload=None: {"type": kind, "payload": payload if payload is not None else {}}


class TransactionValidationTests(unittest.TestCase):
    def test_streams_must_be_nonempty_array_of_complete_objects(self) -> None:
        for bad in (None, [], "x", [{}], [{"stream_id": "a"}],
                    [{"stream_id": "a", "events": []}],
                    [{"events": [EV("OrderPlaced")], "expected_version": 0}]):
            with self.assertRaises(InvalidRequest):
                validate_transaction(bad)

    def test_duplicate_stream_id_is_invalid(self) -> None:
        item = {"stream_id": "a", "events": [EV("OrderPlaced")], "expected_version": 0}
        with self.assertRaises(InvalidRequest):
            validate_transaction([item, dict(item)])

    def test_single_stream_rules_apply_per_item(self) -> None:
        good = {"stream_id": "a", "events": [EV("OrderPlaced")], "expected_version": 0}
        for bad_item in (
            {"stream_id": "", "events": [EV("OrderPlaced")], "expected_version": 0},
            {"stream_id": "a", "events": [{"type": "Nope"}], "expected_version": 0},
            {"stream_id": "a", "events": [EV("OrderPlaced")], "expected_version": -1},
            {"stream_id": "a", "events": [EV("OrderPlaced")], "expected_version": True},
            {"stream_id": "a", "events": [EV("OrderPlaced")], "expected_version": 0, "x": 1},
        ):
            with self.assertRaises(InvalidRequest):
                validate_transaction([good, bad_item])

    def test_fingerprint_orders_streams_but_not_payload_keys(self) -> None:
        left = [{"stream_id": "a", "expected_version": 0,
                 "events": [EV("OrderPlaced", {"total": 1, "sku": "x"})]}]
        right = [{"events": [{"type": "OrderPlaced", "payload": {"sku": "x", "total": 1}}],
                  "stream_id": "a", "expected_version": 0}]
        self.assertEqual(transaction_fingerprint(validate_transaction(left)),
                         transaction_fingerprint(validate_transaction(right)))


class TransactionLedgerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.ledger = Ledger(":memory:")

    def tearDown(self) -> None:
        self.ledger.close()

    def test_atomic_commit_allocates_versions_and_contiguous_cursors(self) -> None:
        response = self.ledger.transaction("tx-1", [
            {"stream_id": "a", "events": [EV("OrderPlaced"), EV("LineItemAdded", {"sku": "x"})],
             "expected_version": 0},
            {"stream_id": "b", "events": [EV("NoteRecorded", {"text": "hi"})], "expected_version": 0},
        ])
        self.assertEqual(response["transaction_id"], "tx-1")
        self.assertEqual([s["stream_id"] for s in response["streams"]], ["a", "b"])
        self.assertEqual([s["version"] for s in response["streams"]], [2, 1])
        self.assertEqual([e["version"] for e in response["streams"][0]["events"]], [1, 2])
        self.assertEqual([e.version for e in self.ledger.read("a")], [1, 2])
        events, _ = self.ledger.read_global(0, 100)
        self.assertEqual([(e.stream_id, e.version, e.cursor) for e in events],
                         [("a", 1, 1), ("a", 2, 2), ("b", 1, 3)])

    def test_first_version_mismatch_wins_and_nothing_is_written(self) -> None:
        self.ledger.append("a", [EV("OrderPlaced")], 0)
        with self.assertRaises(VersionConflict) as caught:
            self.ledger.transaction("tx-2", [
                {"stream_id": "new", "events": [EV("OrderPlaced")], "expected_version": 0},
                {"stream_id": "a", "events": [EV("OrderCancelled")], "expected_version": 0},
            ])
        self.assertIn("'a'", str(caught.exception))
        self.assertEqual(self.ledger.version("new"), 0)
        # The command_id is still free for a later successful commit.
        response = self.ledger.transaction("tx-2", [
            {"stream_id": "new", "events": [EV("OrderPlaced")], "expected_version": 0}])
        self.assertEqual(response["streams"][0]["version"], 1)

    def test_retry_returns_first_response_without_new_events(self) -> None:
        request = [
            {"stream_id": "c", "events": [EV("OrderPlaced", {"total": 1, "sku": "a"})],
             "expected_version": 0},
            {"stream_id": "d", "events": [EV("NoteRecorded", {"text": "hi"})], "expected_version": 0},
        ]
        first = self.ledger.transaction("tx-3", request)
        retry = [
            {"stream_id": "c", "events": [{"type": "OrderPlaced", "payload": {"sku": "a", "total": 1}}],
             "expected_version": 0},
            {"stream_id": "d", "events": [EV("NoteRecorded", {"text": "hi"})], "expected_version": 0},
        ]
        second = self.ledger.transaction("tx-3", retry)
        self.assertEqual(first, second)
        events, _ = self.ledger.read_global(0, 100)
        self.assertEqual(len(events), 2)

    def test_semantic_difference_is_idempotency_conflict_even_when_versions_stale(self) -> None:
        self.ledger.transaction("tx-4", [
            {"stream_id": "e", "events": [EV("OrderPlaced")], "expected_version": 0}])
        for other in (
            [{"stream_id": "e", "events": [EV("OrderCancelled")], "expected_version": 1}],
            [{"stream_id": "f", "events": [EV("OrderPlaced")], "expected_version": 0}],
            [{"stream_id": "e", "events": [EV("OrderPlaced")], "expected_version": 1}],
        ):
            with self.assertRaises(IdempotencyConflict):
                self.ledger.transaction("tx-4", other)
        self.assertEqual(self.ledger.version("e"), 1)
        self.assertEqual(self.ledger.version("f"), 0)

    def test_stream_order_is_significant(self) -> None:
        body = [
            {"stream_id": "g", "events": [EV("OrderPlaced")], "expected_version": 0},
            {"stream_id": "h", "events": [EV("OrderPlaced")], "expected_version": 0},
        ]
        self.ledger.transaction("tx-5", body)
        with self.assertRaises(IdempotencyConflict):
            self.ledger.transaction("tx-5", list(reversed(body)))

    def test_command_id_namespace_is_shared_with_single_stream_appends(self) -> None:
        self.ledger.append("i", [EV("OrderPlaced")], 0, command_id="shared")
        with self.assertRaises(IdempotencyConflict):
            self.ledger.transaction("shared", [
                {"stream_id": "j", "events": [EV("OrderPlaced")], "expected_version": 0}])
        self.ledger.transaction("shared-tx", [
            {"stream_id": "k", "events": [EV("OrderPlaced")], "expected_version": 0}])
        with self.assertRaises(IdempotencyConflict):
            self.ledger.append("k", [EV("OrderCancelled")], 1, command_id="shared-tx")

    def test_concurrent_identical_retries_commit_one_batch(self) -> None:
        body = [
            {"stream_id": "m", "events": [EV("OrderPlaced")], "expected_version": 0},
            {"stream_id": "n", "events": [EV("OrderPlaced"), EV("OrderCancelled")], "expected_version": 0},
        ]

        def submit() -> dict:
            return self.ledger.transaction("tx-6", body)

        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(lambda _: submit(), range(12)))
        self.assertEqual(len({json.dumps(r, sort_keys=True) for r in results}), 1)
        self.assertEqual(self.ledger.version("m"), 1)
        self.assertEqual(self.ledger.version("n"), 2)

    def test_invalid_request_writes_nothing(self) -> None:
        with self.assertRaises(InvalidRequest):
            self.ledger.transaction("tx-7", [
                {"stream_id": "o", "events": [EV("OrderPlaced")], "expected_version": 0},
                {"stream_id": "p", "events": [{"type": "Nope"}], "expected_version": 0},
            ])
        self.assertEqual(self.ledger.version("o"), 0)
        self.assertEqual(self.ledger.version("p"), 0)
        events, _ = self.ledger.read_global(0, 100)
        self.assertEqual(events, [])


class TransactionPersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "tx.sqlite")
        self.ledger = Ledger(self.path)

    def tearDown(self) -> None:
        self.ledger.close()
        self.tmp.cleanup()

    def test_result_and_idempotent_hit_survive_restart(self) -> None:
        body = [
            {"stream_id": "r", "events": [EV("OrderPlaced")], "expected_version": 0},
            {"stream_id": "s", "events": [EV("OrderPlaced"), EV("OrderCancelled")], "expected_version": 0},
        ]
        first = self.ledger.transaction("tx-p", body)
        self.ledger.close()
        reopened = Ledger(self.path)
        self.assertEqual(reopened.transaction("tx-p", body), first)
        self.assertEqual(reopened.version("r"), 1)
        self.assertEqual(reopened.version("s"), 2)
        events, _ = reopened.read_global(0, 100)
        self.assertEqual([e.cursor for e in events], [1, 2, 3])
        following = reopened.transaction("tx-p2", [
            {"stream_id": "r", "events": [EV("NoteRecorded", {"text": "n"})], "expected_version": 1}])
        self.assertEqual(following["streams"][0]["events"][0]["version"], 2)
        events, _ = reopened.read_global(3, 10)
        self.assertEqual([(e.stream_id, e.version, e.cursor) for e in events], [("r", 2, 4)])
        reopened.close()


class TransactionHttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        cls.server = serve(port=0, db=str(Path(cls.tmp.name) / "http.sqlite"))
        cls.port = cls.server.server_address[1]
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.ledger.close()  # type: ignore[attr-defined]
        cls.tmp.cleanup()

    def request(self, path: str, body=None) -> tuple[int, dict]:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.port}{path}", data=data, method="POST",
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read() or b"{}")
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read() or b"{}")

    def test_success_retry_conflict_and_validation(self) -> None:
        body = {"streams": [
            {"stream_id": "t-a", "events": [EV("OrderPlaced"), EV("LineItemAdded", {"sku": "x"})],
             "expected_version": 0},
            {"stream_id": "t-b", "events": [EV("NoteRecorded", {"text": "hi"})], "expected_version": 0},
        ]}
        status, first = self.request("/transactions/http-1", body)
        self.assertEqual(status, 201)
        self.assertEqual(first["transaction_id"], "http-1")
        self.assertEqual([s["version"] for s in first["streams"]], [2, 1])
        self.assertTrue(all("cursor" not in event for stream in first["streams"]
                            for event in stream["events"]))
        status, second = self.request("/transactions/http-1", body)
        self.assertEqual((status, second), (201, first))

        status, payload = self.request("/transactions/http-1", {"streams": [
            {"stream_id": "t-a", "events": [EV("OrderCancelled")], "expected_version": 2},
            {"stream_id": "t-b", "events": [EV("NoteRecorded", {"text": "hi"})], "expected_version": 1}]})
        self.assertEqual((status, payload["error"]["code"]), (409, "idempotency_conflict"))

        status, payload = self.request("/transactions/http-bad", {"streams": []})
        self.assertEqual((status, payload["error"]["code"]), (400, "invalid_request"))
        status, payload = self.request("/transactions/http-bad", {"streams": [
            {"stream_id": "t-a", "events": [EV("OrderPlaced")], "expected_version": 0},
            {"stream_id": "t-a", "events": [EV("OrderCancelled")], "expected_version": 1}]})
        self.assertEqual((status, payload["error"]["code"]), (400, "invalid_request"))
        status, payload = self.request("/transactions/http-bad", {"streams": [
            {"stream_id": "x", "events": [EV("OrderPlaced")], "expected_version": 0}], "other": 1})
        self.assertEqual((status, payload["error"]["code"]), (400, "invalid_request"))
        status, payload = self.request("/transactions/" + "z" * 201, {"streams": [
            {"stream_id": "x", "events": [EV("OrderPlaced")], "expected_version": 0}]})
        self.assertEqual((status, payload["error"]["code"]), (400, "invalid_request"))

    def test_version_conflict_leaves_no_partial_state(self) -> None:
        status, payload = self.request("/transactions/http-2", {"streams": [
            {"stream_id": "t-new", "events": [EV("OrderPlaced")], "expected_version": 0},
            {"stream_id": "t-a", "events": [EV("OrderCancelled")], "expected_version": 0}]})
        self.assertEqual((status, payload["error"]["code"]), (409, "version_conflict"))
        self.assertEqual(self.server.ledger.version("t-new"), 0)  # type: ignore[attr-defined]
        status, _ = self.request("/transactions/http-2", {"streams": [
            {"stream_id": "t-new", "events": [EV("OrderPlaced")], "expected_version": 0}]})
        self.assertEqual(status, 201)

    def test_unknown_route_is_404(self) -> None:
        self.assertEqual(self.request("/transactions", {"streams": []})[0], 404)


if __name__ == "__main__":
    unittest.main()
