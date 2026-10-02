"""Tests for PageStore.write_batch_if conditional atomic batches.

The condition check and the commit are one exclusive-locked serial
operation: the batch lands exactly when every precondition holds on the
current live state, otherwise the call returns ``None`` having consumed no
sequence number and changed nothing -- not even a pre-existing
half-written tail.  These tests cover condition semantics, validation
precedence, sequence numbering, the lost-update race across threads and
processes, tail/corruption handling, persistence and the unchanged
counting/snapshot/CLI conventions.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from page_store.core import PAGE_SIZE, PageStore  # noqa: E402

ENVPATH = str(REPO_ROOT) + os.pathsep + os.environ.get("PYTHONPATH", "")

# One compare-and-set attempt per process: every worker waits for the "go"
# file, then races to change ``shared`` from "0" to its own value.  Only one
# attempt may succeed; the coordination lock decides which.
RACE_WORKER = r"""
import json, os, sys
sys.path.insert(0, os.environ["PYTHONPATH"])
from page_store.core import PageStore
root, wid = sys.argv[1], sys.argv[2]
store = PageStore(root)
go = os.path.join(root, "go")
while not os.path.exists(go):
    pass
result = store.write_batch_if(
    {"shared": "0"},
    [{"op": "put", "key": "shared", "value": f"won-by-{wid}"},
     {"op": "put", "key": f"seat-{wid}", "value": "ok"}])
print(json.dumps(result))
"""


def encode(record: dict) -> bytes:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


def put_record(key: str, value: str) -> bytes:
    return encode({"op": "put", "key": key, "value": value})


def batch_frame(ops: list[dict]) -> bytes:
    return encode({"op": "batch", "ops": ops})


def outer_frames(data: bytes) -> list[dict]:
    out, offset = [], 0
    while offset + 4 <= len(data):
        size = int.from_bytes(data[offset:offset + 4], "big")
        out.append(json.loads(data[offset + 4:offset + 4 + size].decode("utf-8")))
        offset += 4 + size
    assert offset == len(data), "tail bytes left over"
    return out


class ConditionalTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.store = PageStore(self.root)
        self.store.init()

    @property
    def data(self) -> bytes:
        return self.store.path.read_bytes()

    def write(self, blob: bytes) -> None:
        self.store.path.write_bytes(blob)


class ConditionSemanticsTests(ConditionalTestBase):
    def test_empty_expected_commits_unconditionally(self) -> None:
        self.assertEqual(
            self.store.write_batch_if(
                {}, [{"op": "put", "key": "a", "value": "1"}]),
            [1])

    def test_string_requires_verbatim_live_value(self) -> None:
        self.store.put("a", "1")
        self.assertIsNone(self.store.write_batch_if(
            {"a": "2"}, [{"op": "put", "key": "b", "value": "x"}]))
        self.assertEqual(self.store.write_batch_if(
            {"a": "1"}, [{"op": "put", "key": "b", "value": "2"}]), [2])
        # verbatim: surrounding whitespace and type matter
        self.assertIsNone(self.store.write_batch_if(
            {"b": " 2"}, [{"op": "put", "key": "c", "value": "3"}]))

    def test_none_requires_absence_and_deleted_counts_as_absent(self) -> None:
        self.assertIsNone(self.store.write_batch_if(
            {"a": "1"}, [{"op": "put", "key": "x", "value": "y"}]))
        self.assertEqual(self.store.write_batch_if(
            {"a": None}, [{"op": "put", "key": "a", "value": "1"}]), [1])
        self.assertEqual(self.store.delete("a"), 2)
        # deleted is absent again
        self.assertEqual(self.store.write_batch_if(
            {"a": None}, [{"op": "put", "key": "a", "value": "3"}]), [3])

    def test_empty_string_is_a_value_not_absence(self) -> None:
        self.store.put("e", "")
        self.assertIsNone(self.store.write_batch_if(
            {"e": None}, [{"op": "put", "key": "x", "value": "y"}]))
        self.assertEqual(self.store.write_batch_if(
            {"e": ""}, [{"op": "put", "key": "x", "value": "y"}]), [2])

    def test_all_conditions_must_hold(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        self.assertIsNone(self.store.write_batch_if(
            {"a": "1", "b": "3"},
            [{"op": "put", "key": "c", "value": "x"}]))
        self.assertEqual(self.store.write_batch_if(
            {"a": "1", "b": "2", "c": None},
            [{"op": "put", "key": "c", "value": "x"}]), [3])

    def test_condition_keys_need_not_be_written(self) -> None:
        self.store.put("guard", "ok")
        self.assertEqual(self.store.write_batch_if(
            {"guard": "ok"},
            [{"op": "put", "key": "data", "value": "v"}]), [2])
        self.assertEqual(self.store.get("guard"), "ok")

    def test_condition_is_checked_against_pre_batch_state(self) -> None:
        # the first operation removes the condition key, yet the condition is
        # evaluated before the batch applies, so this must still commit
        self.store.put("a", "1")
        self.assertEqual(self.store.write_batch_if(
            {"a": "1"},
            [{"op": "delete", "key": "a"},
             {"op": "put", "key": "a", "value": "2"}]), [2, 3])
        self.assertEqual(self.store.get("a"), "2")

    def test_failure_consumes_no_number_and_changes_nothing(self) -> None:
        self.store.put("a", "1")
        self.assertIsNone(self.store.write_batch_if(
            {"a": "other"}, [{"op": "put", "key": "b", "value": "2"}]))
        self.assertEqual(self.store.stats(), {"pages": 1, "records": 1, "keys": 1})
        # the next success takes the number the failed call would have taken
        self.assertEqual(self.store.write_batch_if(
            {"a": "1"}, [{"op": "put", "key": "b", "value": "2"}]), [2])

    def test_batch_ordering_and_numbering_match_write_batch(self) -> None:
        self.store.put("a", "1")
        seqs = self.store.write_batch_if(
            {"a": "1"},
            [{"op": "put", "key": "k", "value": "v1"},
             {"op": "put", "key": "k", "value": "v2"},
             {"op": "delete", "key": "missing"},
             {"op": "delete", "key": "a"}])
        self.assertEqual(seqs, [2, 3, 4, 5])  # duplicates/deletes take numbers
        self.assertEqual(self.store.scan(), [("k", "v2")])

    def test_conditions_do_not_count_as_records(self) -> None:
        self.store.put("a", "1")
        conditions = {f"c{i}": None for i in range(10)}
        conditions["a"] = "1"
        before = self.store.stats()
        self.assertEqual(self.store.write_batch_if(
            conditions, [{"op": "put", "key": "b", "value": "2"}]), [2])
        after = self.store.stats()
        self.assertEqual(after["records"] - before["records"], 1)
        self.assertEqual(after["keys"] - before["keys"], 1)

    def test_successful_batch_persists_on_reopen(self) -> None:
        self.store.put("a", "1")
        self.assertEqual(self.store.write_batch_if(
            {"a": "1"},
            [{"op": "put", "key": "b", "value": "2"},
             {"op": "delete", "key": "a"}]), [2, 3])
        fresh = PageStore(self.root)
        self.assertEqual(fresh.scan(), [("b", "2")])
        self.assertEqual(fresh.stats(), {"pages": 1, "records": 3, "keys": 1})
        self.assertEqual(fresh.verify()["status"], "ok")
        frames = outer_frames(self.data)
        self.assertEqual(frames[-1]["op"], "batch")
        self.assertNotIn("expected", frames[-1])  # conditions are not stored


class ExpectedValidationTests(ConditionalTestBase):
    def assert_rejected(self, expected: object, operations: object) -> None:
        before = self.data
        with self.assertRaises(ValueError):
            self.store.write_batch_if(expected, operations)  # type: ignore[arg-type]
        self.assertEqual(self.data, before)

    def test_expected_must_be_a_dict(self) -> None:
        ops = [{"op": "put", "key": "k", "value": "v"}]
        for bad in (None, [], [("k", "v")], "", "x", (("k", "v"),)):
            self.assert_rejected(bad, ops)

    def test_expected_keys_must_be_non_empty_strings(self) -> None:
        ops = [{"op": "put", "key": "k", "value": "v"}]
        for bad_key in (None, 1, b"k", "", object(), True):
            self.assert_rejected({bad_key: "v"}, ops)  # type: ignore[dict-item]

    def test_expected_values_must_be_string_or_none(self) -> None:
        ops = [{"op": "put", "key": "k", "value": "v"}]
        for bad_value in (1, b"v", object(), [], {}, True, False):
            self.assert_rejected({"k": bad_value}, ops)

    def test_operations_follow_write_batch_rules(self) -> None:
        good_expected = {"a": "1"}
        self.assert_rejected(good_expected, None)
        self.assert_rejected(good_expected, [])
        self.assert_rejected(good_expected, [{"op": "fetch", "key": "k"}])
        self.assert_rejected(good_expected, [{"op": "put", "key": "k"}])
        self.assert_rejected(good_expected,
                             [{"op": "put", "key": "k", "value": "v", "x": 1}])
        self.assert_rejected(good_expected,
                             [{"op": "delete", "key": ""}])
        self.assert_rejected(good_expected,
                             [{"op": "put", "key": "k", "value": 1}])
        self.assert_rejected(good_expected,
                             [{"op": "put", "key": "k",
                               "value": "x" * (PAGE_SIZE + 1)}])

    def test_both_arguments_validated_before_anything_else(self) -> None:
        # validation beats storage errors: missing root never gets created
        missing = self.root / "nope"
        with self.assertRaises(ValueError):
            PageStore(missing).write_batch_if(
                {"k": 1}, [{"op": "put", "key": "k", "value": "v"}])
        with self.assertRaises(ValueError):
            PageStore(missing).write_batch_if({}, [])
        self.assertFalse(missing.exists())
        # a root that is a file still reports ValueError for bad arguments
        a_file = self.root / "a-file"
        a_file.write_bytes(b"")
        with self.assertRaises(ValueError):
            PageStore(a_file).write_batch_if(
                None, [{"op": "put", "key": "k", "value": "v"}])  # type: ignore[arg-type]

    def test_valid_input_on_missing_store_raises_filenotfound(self) -> None:
        missing = self.root / "nope"
        with self.assertRaises(FileNotFoundError):
            PageStore(missing).write_batch_if(
                {}, [{"op": "put", "key": "k", "value": "v"}])
        self.assertFalse(missing.exists())


class TailAndCorruptionTests(ConditionalTestBase):
    def test_failed_condition_leaves_half_written_tail_intact(self) -> None:
        self.store.put("a", "1")
        frame = batch_frame([{"op": "put", "key": f"k{i:03d}", "value": "x" * 60}
                             for i in range(60)])
        torn = self.data + frame[:len(frame) // 2]
        self.write(torn)
        fresh = PageStore(self.root)
        # condition cannot hold: no append AND no truncation of the tail
        self.assertIsNone(fresh.write_batch_if(
            {"a": "2"}, [{"op": "put", "key": "b", "value": "2"}]))
        self.assertEqual(self.data, torn)
        outcome = fresh.verify()
        self.assertEqual(outcome["status"], "incomplete_tail")
        self.assertEqual(outcome["complete_records"], 1)
        self.assertEqual(fresh.stats()["records"], 1)
        # a successful condition then overwrites the tail as usual
        self.assertEqual(fresh.write_batch_if(
            {"a": "1"}, [{"op": "put", "key": "b", "value": "2"}]), [2])
        self.assertEqual(outer_frames(self.data),
                         [{"op": "put", "key": "a", "value": "1"},
                          {"op": "batch",
                           "ops": [{"op": "put", "key": "b", "value": "2"}]}])
        self.assertEqual(fresh.verify()["status"], "ok")

    def test_corrupt_middle_preceded_failed_condition(self) -> None:
        blob = (put_record("a", "1") + b"\x00\x00\x00\x09broken"
                + put_record("b", "2"))
        self.write(blob)
        fresh = PageStore(self.root)
        # a condition that cannot hold still surfaces corruption
        with self.assertRaises(RuntimeError) as caught:
            fresh.write_batch_if(
                {"a": "other"}, [{"op": "put", "key": "c", "value": "3"}])
        self.assertEqual(str(caught.exception), "corrupt_middle")
        self.assertEqual(self.data, blob)
        # and so does a condition that would hold on the readable prefix
        with self.assertRaises(RuntimeError) as caught:
            fresh.write_batch_if(
                {"a": "1"}, [{"op": "put", "key": "c", "value": "3"}])
        self.assertEqual(str(caught.exception), "corrupt_middle")
        self.assertEqual(self.data, blob)

    @unittest.skipIf(hasattr(os, "geteuid") and os.geteuid() == 0,
                     "root bypasses permission bits")
    def test_io_failure_raises_oserror_and_rolls_back(self) -> None:
        self.store.put("a", "1")
        old = self.data
        os.chmod(self.store.path, 0)
        try:
            with self.assertRaises(OSError):
                self.store.write_batch_if(
                    {"a": "1"}, [{"op": "put", "key": "b", "value": "2"}])
        finally:
            os.chmod(self.store.path, 0o600)
        self.assertEqual(self.data, old)
        self.assertEqual(self.store.stats(), {"pages": 1, "records": 1, "keys": 1})


class ThreadRaceTests(ConditionalTestBase):
    def test_two_threads_racing_same_old_value_exactly_one_wins(self) -> None:
        n = 8
        barrier = threading.Barrier(n)
        results: list[list[int] | None] = []
        lock = threading.Lock()

        def attempt(tid: int) -> None:
            local = PageStore(self.root)
            barrier.wait()
            outcome = local.write_batch_if(
                {"shared": "0"},
                [{"op": "put", "key": "shared", "value": f"won-by-{tid}"},
                 {"op": "put", "key": f"seat-{tid}", "value": "ok"}])
            with lock:
                results.append(outcome)

        self.store.put("shared", "0")
        threads = [threading.Thread(target=attempt, args=(t,)) for t in range(n)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        winners = [r for r in results if r is not None]
        self.assertEqual(len(winners), 1)
        self.assertEqual(winners[0], [2, 3])
        self.assertEqual(len([r for r in results if r is None]), n - 1)
        # losers' side-effect operations are entirely absent
        live = dict(self.store.scan())
        self.assertEqual(len(live), 2)
        self.assertIn(live["shared"], {f"won-by-{t}" for t in range(n)})
        self.assertEqual(self.store.stats()["records"], 3)
        self.assertEqual(self.store.verify()["status"], "ok")

    def test_retrying_cas_increments_serialise_without_lost_updates(self) -> None:
        n_threads, per_thread = 4, 50
        barrier = threading.Barrier(n_threads)
        errors: list[BaseException] = []

        def increment() -> None:
            local = PageStore(self.root)
            barrier.wait()
            try:
                for _ in range(per_thread):
                    while True:
                        old = local.get("counter")
                        new = str(int(old) + 1)
                        if local.write_batch_if(
                                {"counter": old},
                                [{"op": "put", "key": "counter",
                                  "value": new}]) is not None:
                            break
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        self.store.put("counter", "0")
        threads = [threading.Thread(target=increment) for _ in range(n_threads)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=60)
        self.assertEqual(errors, [])
        self.assertEqual(self.store.get("counter"),
                         str(n_threads * per_thread))
        # every successful attempt added exactly one dense record
        self.assertEqual(self.store.stats()["records"],
                         1 + n_threads * per_thread)
        self.assertEqual(self.store.verify()["status"], "ok")

    def test_failed_callers_retry_can_succeed_after_other_writer(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        a.put("k", "v0")
        self.assertIsNone(b.write_batch_if(
            {"k": "v1"}, [{"op": "put", "key": "k", "value": "b0"}]))
        self.assertEqual(a.write_batch_if(
            {"k": "v0"}, [{"op": "put", "key": "k", "value": "v1"}]), [2])
        # b now retries against the committed value and wins
        self.assertEqual(b.write_batch_if(
            {"k": "v1"}, [{"op": "put", "key": "k", "value": "v2"}]), [3])
        self.assertEqual(a.get("k"), "v2")


class ProcessRaceTests(ConditionalTestBase):
    def test_two_processes_racing_same_old_value_exactly_one_wins(self) -> None:
        n = 8
        self.store.put("shared", "0")
        procs = [subprocess.Popen(
            [sys.executable, "-c", RACE_WORKER, str(self.root), str(w)],
            cwd=REPO_ROOT, env={**os.environ, "PYTHONPATH": ENVPATH},
            stdout=subprocess.PIPE, text=True) for w in range(n)]
        # release every worker at once once they are all spinning
        (self.root / "go").write_bytes(b"")
        outcomes = []
        for proc in procs:
            out, err = proc.communicate(timeout=60)
            self.assertEqual(proc.returncode, 0, err)
            outcomes.append(json.loads(out))
        winners = [o for o in outcomes if o is not None]
        self.assertEqual(len(winners), 1, outcomes)
        self.assertEqual(winners[0], [2, 3])
        fresh = PageStore(self.root)
        self.assertEqual(fresh.stats()["records"], 3)
        self.assertEqual(fresh.verify()["status"], "ok")
        live = dict(fresh.scan())
        self.assertEqual(len(live), 2)
        winner_value = fresh.get("shared")
        self.assertIsNotNone(winner_value)
        self.assertTrue(winner_value.startswith("won-by-"))
        self.assertEqual(fresh.get(f"seat-{winner_value[len('won-by-'):]}"),
                         "ok")


class IntegrationTests(ConditionalTestBase):
    def test_counts_flow_through_stats_recover_verify_compact(self) -> None:
        self.store.put("x", "0")
        self.store.write_batch_if(
            {"x": "0"},
            [{"op": "put", "key": "a", "value": "1"},
             {"op": "put", "key": "b", "value": "2"},
             {"op": "delete", "key": "x"},
             {"op": "put", "key": "a", "value": "one"}])
        self.assertEqual(self.store.stats(),
                         {"pages": 1, "records": 5, "keys": 2})
        recovered = self.store.recover()
        self.assertFalse(recovered["truncated"])
        self.assertEqual(recovered["records"], 5)
        verified = self.store.verify()
        self.assertEqual(verified["status"], "ok")
        self.assertEqual(verified["complete_records"], 5)
        result = self.store.compact()
        self.assertEqual(result["records_before"], 5)
        self.assertEqual(result["records_after"], 2)
        # numbering continues from the post-compaction count
        self.assertEqual(self.store.write_batch_if(
            {}, [{"op": "delete", "key": "a"}]), [3])

    def test_old_format_store_needs_no_migration(self) -> None:
        blob = put_record("old", "1") + put_record("old", "2")
        self.write(blob)
        fresh = PageStore(self.root)
        self.assertEqual(fresh.write_batch_if(
            {"old": "2"},
            [{"op": "put", "key": "new", "value": "3"},
             {"op": "delete", "key": "old"}]), [3, 4])
        self.assertEqual(fresh.scan(), [("new", "3")])
        self.assertEqual(fresh.verify()["status"], "ok")

    def test_reset_clears_and_condition_sees_empty_state(self) -> None:
        self.store.put("a", "1")
        self.store.init()
        self.assertEqual(self.store.stats(),
                         {"pages": 0, "records": 0, "keys": 0})
        self.assertIsNone(self.store.write_batch_if(
            {"a": "1"}, [{"op": "put", "key": "a", "value": "2"}]))
        self.assertEqual(self.store.write_batch_if(
            {"a": None}, [{"op": "put", "key": "a", "value": "2"}]), [1])

    def test_snapframes_show_only_before_or_after_state(self) -> None:
        self.store.put("a", "1")
        before = self.store.snapshot()
        self.assertIsNone(self.store.write_batch_if(
            {"a": "nope"}, [{"op": "put", "key": "b", "value": "2"}]))
        self.store.write_batch_if(
            {"a": "1"}, [{"op": "put", "key": "b", "value": "2"}])
        self.assertEqual(before.scan(), [("a", "1")])
        self.assertEqual(before.stats(), {"pages": 1, "records": 1, "keys": 1})
        after = self.store.snapshot()
        self.assertEqual(after.scan(), [("a", "1"), ("b", "2")])
        self.assertEqual(after.stats(), {"pages": 1, "records": 2, "keys": 2})

    def test_no_new_cli_surface_and_report_unchanged(self) -> None:
        result = subprocess.run(
            [sys.executable, "-m", "page_store", "--root", str(self.root),
             "write-batch-if"],
            cwd=REPO_ROOT, capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        report = subprocess.run(
            [sys.executable, "-m", "page_store", "--root", str(self.root),
             "report"],
            cwd=REPO_ROOT, capture_output=True, text=True)
        self.assertEqual(report.returncode, 0)
        payload = json.loads(report.stdout)
        self.assertEqual(payload["readiness"],
                         {"append": True, "recover": True,
                          "compaction": True, "snapshotRead": False})


if __name__ == "__main__":
    unittest.main()
