"""Tests for PageStore.write_batch_if conditional batch commits.

The condition check and the commit are one serial operation shared with all
other writes, resets, recoveries and compactions.  Both arguments are
validated wholesale before the store is touched; a failed condition returns
None without consuming a sequence number or changing a single byte of the
page file (a half-written tail included), while a successful batch is
indistinguishable on disk from a write_batch frame.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from page_store.core import PAGE_SIZE, PageStore  # noqa: E402

ENVPATH = str(REPO_ROOT) + os.pathsep + os.environ.get("PYTHONPATH", "")

# Two workers both require "shared" == "old" and try to replace it with
# their own new value; exactly one of them may succeed.
RACE_WORKER = r"""
import json, os, sys
sys.path.insert(0, os.environ["PYTHONPATH"])
from page_store.core import PageStore
root, wid = sys.argv[1], int(sys.argv[2])
store = PageStore(root)
result = store.write_batch_if(
    {"shared": "old"},
    [{"op": "put", "key": "shared", "value": f"new-{wid}"}])
print(json.dumps(result))
"""


def encode(record: dict) -> bytes:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


def put_record(key: str, value: str) -> bytes:
    return encode({"op": "put", "key": key, "value": value})


class BatchIfTestBase(unittest.TestCase):
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


class BatchIfValidationTests(BatchIfTestBase):
    OPS = [{"op": "put", "key": "k", "value": "v"}]

    def assert_rejected(self, expected: object, operations: object) -> None:
        before = self.data
        with self.assertRaises(ValueError):
            self.store.write_batch_if(expected, operations)  # type: ignore[arg-type]
        self.assertEqual(self.data, before)  # page file untouched

    def test_expected_must_be_a_dict(self) -> None:
        for bad in (None, 1, "k", [("k", "v")], {("k", "v")}, object()):
            self.assert_rejected(bad, self.OPS)

    def test_expected_keys_must_be_non_empty_strings(self) -> None:
        for bad_key in ("", None, 1, b"k", object()):
            self.assert_rejected({bad_key: "v"}, self.OPS)
            self.assert_rejected({"good": "v", bad_key: None}, self.OPS)

    def test_expected_values_must_be_strings_or_none(self) -> None:
        for bad_value in (1, True, b"v", ["v"], {"v": 1}, object()):
            self.assert_rejected({"k": bad_value}, self.OPS)

    def test_operations_follow_write_batch_rules(self) -> None:
        good = {"k": "v"}
        self.assert_rejected(good, None)
        self.assert_rejected(good, [])
        self.assert_rejected(good, [{"op": "put", "key": "k"}])
        self.assert_rejected(good, [{"op": "delete", "key": "k", "x": 1}])
        self.assert_rejected(good, [{"op": "put", "key": "", "value": "v"}])
        self.assert_rejected(good, [{"op": "put", "key": "k", "value": 1}])
        self.assert_rejected(
            good, [{"op": "put", "key": "big", "value": "x" * (PAGE_SIZE + 1)}])

    def test_both_arguments_validated_before_any_storage_error(self) -> None:
        missing = self.root / "nope"
        store = PageStore(missing)
        # invalid input raises ValueError even though there is no store at all
        with self.assertRaises(ValueError):
            store.write_batch_if(None, self.OPS)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            store.write_batch_if({}, [])
        self.assertFalse(missing.exists())

    def test_failed_validation_consumes_no_number(self) -> None:
        self.store.put("a", "1")
        with self.assertRaises(ValueError):
            self.store.write_batch_if({"a": 1}, self.OPS)  # type: ignore[dict-item]
        self.assertEqual(self.store.stats(), {"pages": 1, "records": 1, "keys": 1})
        self.assertEqual(
            self.store.write_batch_if({}, self.OPS), [2])


class BatchIfConditionTests(BatchIfTestBase):
    def test_string_condition_requires_exact_live_value(self) -> None:
        self.store.put("k", "v1")
        self.assertIsNone(self.store.write_batch_if(
            {"k": "v2"}, [{"op": "put", "key": "k", "value": "v3"}]))
        self.assertEqual(self.store.get("k"), "v1")
        self.assertEqual(self.store.write_batch_if(
            {"k": "v1"}, [{"op": "put", "key": "k", "value": "v3"}]), [2])
        self.assertEqual(self.store.get("k"), "v3")

    def test_none_condition_requires_absence(self) -> None:
        self.store.put("k", "v")
        self.assertIsNone(self.store.write_batch_if(
            {"k": None}, [{"op": "put", "key": "x", "value": "1"}]))
        self.store.delete("k")
        # a deleted key counts as absent
        self.assertEqual(self.store.write_batch_if(
            {"k": None}, [{"op": "put", "key": "x", "value": "1"}]), [3])
        self.assertEqual(self.store.get("x"), "1")

    def test_empty_string_value_is_not_absence(self) -> None:
        self.store.put("k", "")
        self.assertIsNone(self.store.write_batch_if(
            {"k": None}, [{"op": "put", "key": "x", "value": "1"}]))
        # ...but an empty-string condition matches it exactly
        self.assertEqual(self.store.write_batch_if(
            {"k": ""}, [{"op": "put", "key": "x", "value": "1"}]), [2])

    def test_string_condition_on_missing_key_fails(self) -> None:
        self.assertIsNone(self.store.write_batch_if(
            {"missing": "v"}, [{"op": "put", "key": "x", "value": "1"}]))
        self.assertEqual(self.store.stats()["records"], 0)

    def test_condition_keys_need_not_participate_in_writes(self) -> None:
        self.store.put("guard", "on")
        seqs = self.store.write_batch_if(
            {"guard": "on", "absent": None},
            [{"op": "put", "key": "a", "value": "1"},
             {"op": "delete", "key": "guard"}])
        self.assertEqual(seqs, [2, 3])
        self.assertEqual(self.store.scan(), [("a", "1")])

    def test_empty_expected_commits_unconditionally(self) -> None:
        self.assertEqual(self.store.write_batch_if(
            {}, [{"op": "put", "key": "a", "value": "1"},
                 {"op": "delete", "key": "missing"}]), [1, 2])
        self.assertEqual(self.store.scan(), [("a", "1")])

    def test_all_conditions_must_hold(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        self.assertIsNone(self.store.write_batch_if(
            {"a": "1", "b": "two"}, [{"op": "put", "key": "c", "value": "3"}]))
        self.assertEqual(self.store.scan(), [("a", "1"), ("b", "2")])
        self.assertEqual(self.store.write_batch_if(
            {"a": "1", "b": "2"}, [{"op": "put", "key": "c", "value": "3"}]),
            [3])
        self.assertEqual(self.store.get("c"), "3")


class BatchIfAtomicityTests(BatchIfTestBase):
    def test_failed_condition_leaves_file_and_sequence_untouched(self) -> None:
        self.store.put("a", "1")
        before = self.data
        self.assertIsNone(self.store.write_batch_if(
            {"a": "other"}, [{"op": "put", "key": "b", "value": "2"}]))
        self.assertEqual(self.data, before)  # byte-for-byte unchanged
        self.assertEqual(self.store.stats(), {"pages": 1, "records": 1, "keys": 1})
        # the next successful write numbers as if the failed call never ran
        self.assertEqual(self.store.put("b", "2"), 2)

    def test_failed_condition_does_not_truncate_half_written_tail(self) -> None:
        self.store.put("a", "1")
        frame = encode({"op": "put", "key": "torn", "value": "x" * 100})
        self.write(self.data + frame[:len(frame) - 5])
        torn = self.data
        fresh = PageStore(self.root)
        self.assertIsNone(fresh.write_batch_if(
            {"a": "other"}, [{"op": "put", "key": "b", "value": "2"}]))
        self.assertEqual(self.data, torn)  # the tail survives a failed check
        # ...and is still discarded by the next successful write, as before
        self.assertEqual(fresh.write_batch_if(
            {"a": "1"}, [{"op": "put", "key": "b", "value": "2"}]), [2])
        self.assertNotIn(b"torn", self.data)
        self.assertEqual(fresh.scan(), [("a", "1"), ("b", "2")])

    def test_successful_batch_persists_across_reopen(self) -> None:
        self.store.put("a", "1")
        seqs = self.store.write_batch_if(
            {"a": "1"},
            [{"op": "put", "key": f"k{i}", "value": f"v{i}"} for i in range(5)]
            + [{"op": "delete", "key": "a"}])
        self.assertEqual(seqs, [2, 3, 4, 5, 6, 7])
        fresh = PageStore(self.root)
        self.assertEqual(fresh.stats()["records"], 7)
        self.assertIsNone(fresh.get("a"))
        self.assertEqual(fresh.get("k4"), "v4")
        self.assertEqual(fresh.verify()["status"], "ok")

    def test_conditions_are_not_recorded(self) -> None:
        self.store.put("guard", "on")
        self.store.write_batch_if(
            {"guard": "on", "missing": None},
            [{"op": "put", "key": "a", "value": "1"}])
        # only the pre-existing put and the one batch operation count
        self.assertEqual(self.store.stats(),
                         {"pages": 1, "records": 2, "keys": 2})
        outcome = self.store.recover()
        self.assertEqual(outcome["records"], 2)
        self.assertEqual(self.store.verify()["complete_records"], 2)
        result = self.store.compact()
        self.assertEqual(result["records_before"], 2)
        self.assertEqual(result["records_after"], 2)
        self.assertEqual(self.store.scan(),
                         [("a", "1"), ("guard", "on")])

    def test_corrupt_middle_raises_even_when_condition_would_fail(self) -> None:
        blob = (put_record("a", "1") + b"\x00\x00\x00\x09broken"
                + put_record("b", "2"))
        self.write(blob)
        fresh = PageStore(self.root)
        with self.assertRaises(RuntimeError) as caught:
            fresh.write_batch_if(
                {"a": "no-such-value"},
                [{"op": "put", "key": "c", "value": "3"}])
        self.assertEqual(str(caught.exception), "corrupt_middle")
        self.assertEqual(self.data, blob)

    def test_missing_store_raises_filenotfound_after_validation(self) -> None:
        missing = self.root / "nope"
        with self.assertRaises(FileNotFoundError):
            PageStore(missing).write_batch_if(
                {"k": "v"}, [{"op": "put", "key": "k", "value": "v"}])
        self.assertFalse(missing.exists())
        # a root that is a file behaves the same way
        file_root = self.root / "afile"
        file_root.write_text("x")
        with self.assertRaises(FileNotFoundError):
            PageStore(file_root).write_batch_if(
                {}, [{"op": "put", "key": "k", "value": "v"}])

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


class BatchIfSerialOrderingTests(BatchIfTestBase):
    def test_concurrent_check_and_commit_only_one_wins(self) -> None:
        self.store.put("shared", "old")
        barrier = threading.Barrier(2)
        results: list[list[int] | None] = []

        def contender(value: str) -> None:
            local = PageStore(self.root)
            barrier.wait(timeout=10)
            results.append(local.write_batch_if(
                {"shared": "old"},
                [{"op": "put", "key": "shared", "value": value}]))

        threads = [threading.Thread(target=contender, args=(f"new-{i}",))
                   for i in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        winners = [r for r in results if r is not None]
        self.assertEqual(len(winners), 1)
        self.assertEqual(winners[0], [2])
        self.assertEqual(sorted(r for r in results if r is None), [None])
        self.assertIn(self.store.get("shared"), ("new-0", "new-1"))
        self.assertEqual(self.store.stats()["records"], 2)

    def test_concurrent_check_and_commit_across_processes(self) -> None:
        self.store.put("shared", "old")
        procs = [subprocess.Popen(
            [sys.executable, "-c", RACE_WORKER, str(self.root), str(w)],
            cwd=REPO_ROOT, env={**os.environ, "PYTHONPATH": ENVPATH},
            stdout=subprocess.PIPE, text=True) for w in range(2)]
        outcomes = []
        for proc in procs:
            out, err = proc.communicate(timeout=60)
            self.assertEqual(proc.returncode, 0, err)
            outcomes.append(json.loads(out))
        winners = [r for r in outcomes if r is not None]
        self.assertEqual(len(winners), 1)
        self.assertEqual(winners[0], [2])
        self.assertEqual(self.store.stats()["records"], 2)
        self.assertEqual(self.store.verify()["status"], "ok")

    def test_serialises_with_writes_recover_and_compact(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        self.assertEqual(a.put("k", "1"), 1)
        self.assertEqual(b.write_batch_if(
            {"k": "1"}, [{"op": "put", "key": "k", "value": "2"}]), [2])
        result = a.compact()
        self.assertEqual(result["records_after"], 1)
        # the condition observes the post-compaction serial point
        self.assertEqual(b.write_batch_if(
            {"k": "2"}, [{"op": "delete", "key": "k"}]), [2])
        self.assertIsNone(a.get("k"))
        a.recover()
        self.assertEqual(a.stats(), {"pages": 1, "records": 2, "keys": 0})
        # after a reset the old value is gone, so the condition fails
        a.init()
        self.assertIsNone(b.write_batch_if(
            {"k": "2"}, [{"op": "put", "key": "x", "value": "9"}]))
        self.assertEqual(b.stats(), {"pages": 0, "records": 0, "keys": 0})

    def test_readers_see_only_before_or_after_states(self) -> None:
        self.store.put("shared", "old")
        stop = threading.Event()
        errors: list[BaseException] = []

        def writer() -> None:
            local = PageStore(self.root)
            try:
                i = 0
                while not stop.is_set():
                    local.write_batch_if(
                        {"shared": f"v{i}"},
                        [{"op": "put", "key": "tmp", "value": "z"},
                         {"op": "delete", "key": "tmp"},
                         {"op": "put", "key": "shared", "value": f"v{i + 1}"}])
                    i += 1
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        def reader() -> None:
            local = PageStore(self.root)
            try:
                while not stop.is_set():
                    live = dict(local.scan())
                    self.assertNotIn("tmp", live)
                    self.assertRegex(live["shared"], r"^v\d+$")
                    snap = local.snapshot()
                    self.assertNotIn("tmp", dict(snap.scan()))
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        # seed the first expected value
        self.store.put("shared", "v0")
        threads = [threading.Thread(target=writer),
                   threading.Thread(target=reader),
                   threading.Thread(target=reader)]
        for thread in threads:
            thread.start()
        time.sleep(0.5)
        stop.set()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(errors, [])
        self.assertEqual(self.store.verify()["status"], "ok")

    def test_snapshot_taken_before_is_frozen(self) -> None:
        self.store.put("a", "1")
        before = self.store.snapshot()
        self.store.write_batch_if(
            {"a": "1"}, [{"op": "put", "key": "a", "value": "2"}])
        self.assertEqual(before.scan(), [("a", "1")])
        self.assertEqual(before.stats(), {"pages": 1, "records": 1, "keys": 1})
        after = self.store.snapshot()
        self.assertEqual(after.scan(), [("a", "2")])
        self.assertEqual(after.stats()["records"], 2)


if __name__ == "__main__":
    unittest.main()
