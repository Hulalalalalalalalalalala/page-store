"""Tests for PageStore.write_batch_if_range range-conditional commits.

The range comparison and the commit are one serial operation shared with
all other writes, resets, recoveries and compactions.  All arguments are
validated wholesale before the store is touched; a mismatch returns None
without consuming a sequence number or changing a single byte of the page
file (a half-written tail included, and never participates in the
comparison), while a successful batch is indistinguishable on disk from a
write_batch frame.
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

# Two workers both require the whole ("a", "c") range to be {"a": "1",
# "b": "1"} and each insert their own key inside that range; exactly one
# may succeed.
RACE_WORKER = r"""
import json, os, sys
sys.path.insert(0, os.environ["PYTHONPATH"])
from page_store.core import PageStore
root, wid = sys.argv[1], int(sys.argv[2])
store = PageStore(root)
result = store.write_batch_if_range(
    {"a": "1", "b": "1"},
    [{"op": "put", "key": f"b{wid}", "value": "x"}],
    start="a", end="c")
print(json.dumps(result))
"""


def encode(record: dict) -> bytes:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


def put_record(key: str, value: str) -> bytes:
    return encode({"op": "put", "key": key, "value": value})


class BatchIfRangeTestBase(unittest.TestCase):
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


OPS = [{"op": "put", "key": "z", "value": "v"}]


class BatchIfRangeValidationTests(BatchIfRangeTestBase):
    def assert_rejected(self, expected: object, operations: object = OPS,
                        start: object = None, end: object = None) -> None:
        before = self.data
        with self.assertRaises(ValueError):
            self.store.write_batch_if_range(  # type: ignore[arg-type]
                expected, operations, start=start, end=end)
        self.assertEqual(self.data, before)  # page file untouched

    def test_expected_must_be_a_dict(self) -> None:
        for bad in (None, 1, "k", [("k", "v")], {("k", "v")}, object()):
            self.assert_rejected(bad)

    def test_expected_keys_must_be_non_empty_strings(self) -> None:
        for bad_key in ("", None, 1, b"k", object()):
            self.assert_rejected({bad_key: "v"})
            self.assert_rejected({"good": "v", bad_key: "w"})

    def test_expected_values_must_be_strings(self) -> None:
        for bad_value in (None, 1, True, b"v", ["v"], {"v": 1}, object()):
            self.assert_rejected({"k": bad_value})

    def test_boundaries_must_be_strings_or_none(self) -> None:
        for bad in (1, b"a", True, [], object()):
            self.assert_rejected({}, start=bad)
            self.assert_rejected({}, end=bad)

    def test_empty_string_boundary_is_legal(self) -> None:
        # "" is the smallest possible key: it is a valid open-like start and
        # an end that excludes every key.
        self.assertEqual(self.store.write_batch_if_range(
            {}, OPS, start="", end=""), [1])  # empty range, no keys -> match
        self.store.put("a", "1")
        self.assertEqual(self.store.write_batch_if_range(
            {}, OPS, start="", end=""), [3])  # range still empty
        self.assertEqual(self.store.write_batch_if_range(
            {"a": "1"}, OPS, start="", end="b"), [4])

    def test_condition_keys_must_lie_in_range(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        self.assert_rejected({"a": "1"}, start="b", end="c")
        self.assert_rejected({"b": "2"}, start="a", end="b")  # end excluded
        self.assert_rejected({"c": "x"}, start="a", end="c")
        self.assert_rejected({"": "v"}, start="a", end="c")

    def test_empty_range_requires_empty_expected(self) -> None:
        # start >= end -> empty range; any condition key is out of range
        self.assert_rejected({"a": "1"}, start="c", end="a")
        self.assert_rejected({"a": "1"}, start="a", end="a")
        # but an empty expectation on an empty range matches even when the
        # store has keys elsewhere
        self.store.put("a", "1")
        self.assertEqual(self.store.write_batch_if_range(
            {}, OPS, start="z", end="a"), [2])

    def test_operations_follow_write_batch_rules(self) -> None:
        self.assert_rejected({}, None)
        self.assert_rejected({}, [])
        self.assert_rejected({}, [{"op": "put", "key": "k"}])
        self.assert_rejected({}, [{"op": "delete", "key": "k", "x": 1}])
        self.assert_rejected({}, [{"op": "put", "key": "", "value": "v"}])
        self.assert_rejected({}, [{"op": "put", "key": "k", "value": 1}])
        self.assert_rejected(
            {}, [{"op": "put", "key": "big", "value": "x" * (PAGE_SIZE + 1)}])

    def test_all_arguments_validated_before_any_storage_error(self) -> None:
        missing = self.root / "nope"
        store = PageStore(missing)
        # invalid input raises ValueError even though there is no store at all
        with self.assertRaises(ValueError):
            store.write_batch_if_range(None, OPS)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            store.write_batch_if_range({}, [], start=1)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            store.write_batch_if_range({"z": "v"}, OPS, start="a", end="z")
        self.assertFalse(missing.exists())

    def test_failed_validation_consumes_no_number(self) -> None:
        self.store.put("a", "1")
        with self.assertRaises(ValueError):
            self.store.write_batch_if_range(
                {"a": 1}, OPS)  # type: ignore[dict-item]
        self.assertEqual(self.store.stats(), {"pages": 1, "records": 1, "keys": 1})
        self.assertEqual(self.store.write_batch_if_range(
            {"a": "1"}, OPS), [2])


class BatchIfRangeComparisonTests(BatchIfRangeTestBase):
    def seed(self) -> None:
        for key, value in (("a", "1"), ("b", "2"), ("c", "3"),
                           ("d", "4"), ("e", "5")):
            self.store.put(key, value)

    def test_exact_match_commits(self) -> None:
        self.seed()
        seqs = self.store.write_batch_if_range(
            {"b": "2", "c": "3"},
            [{"op": "put", "key": "f", "value": "6"}], start="b", end="d")
        self.assertEqual(seqs, [6])
        self.assertEqual(self.store.get("f"), "6")

    def test_expected_order_is_irrelevant(self) -> None:
        self.seed()
        seqs = self.store.write_batch_if_range(
            {"c": "3", "a": "1", "b": "2"}, OPS, start="a", end="d")
        self.assertEqual(seqs, [6])

    def test_changed_value_mismatches(self) -> None:
        self.seed()
        self.assertIsNone(self.store.write_batch_if_range(
            {"a": "1", "b": "two", "c": "3"}, OPS, start="a", end="d"))
        self.assertEqual(self.store.scan(start="a", end="f"),
                         [("a", "1"), ("b", "2"), ("c", "3"),
                          ("d", "4"), ("e", "5")])

    def test_added_key_mismatches_even_when_expected_keys_match(self) -> None:
        self.seed()
        # the scan-then-insert case: range [a, d) holds a, b AND c, but the
        # expectation only lists a and b
        self.assertIsNone(self.store.write_batch_if_range(
            {"a": "1", "b": "2"}, OPS, start="a", end="d"))
        self.assertEqual(self.store.stats()["records"], 5)

    def test_deleted_key_mismatches(self) -> None:
        self.seed()
        self.store.delete("b")
        self.assertIsNone(self.store.write_batch_if_range(
            {"a": "1", "b": "2", "c": "3"}, OPS, start="a", end="d"))
        # ...and matching the actual post-delete state commits
        self.assertEqual(self.store.write_batch_if_range(
            {"a": "1", "c": "3"}, OPS, start="a", end="d"), [7])

    def test_empty_expected_requires_no_keys_in_range(self) -> None:
        self.seed()
        self.assertIsNone(self.store.write_batch_if_range(
            {}, OPS, start="b", end="d"))
        # an untouched gap is empty
        self.assertEqual(self.store.write_batch_if_range(
            {}, OPS, start="b0", end="b9"), [6])
        # deleting the range's keys makes it empty
        self.store.write_batch([
            {"op": "delete", "key": "b"}, {"op": "delete", "key": "c"}])
        self.assertEqual(self.store.write_batch_if_range(
            {}, OPS, start="b", end="d"), [9])

    def test_changes_outside_range_are_ignored(self) -> None:
        self.seed()
        # keys below start and at/above end may be added, deleted or changed
        self.store.put("z", "9")
        self.store.put("a", "changed")
        self.assertEqual(self.store.write_batch_if_range(
            {"b": "2", "c": "3"},
            [{"op": "put", "key": "a", "value": "outside-write"},
             {"op": "put", "key": "zz", "value": "also-outside"}],
            start="b", end="d"), [8, 9])
        self.assertEqual(self.store.get("a"), "outside-write")

    def test_operations_may_touch_out_of_range_keys(self) -> None:
        self.seed()
        seqs = self.store.write_batch_if_range(
            {"b": "2", "c": "3"},
            [{"op": "delete", "key": "a"},
             {"op": "put", "key": "e", "value": "changed"},
             {"op": "delete", "key": "missing"}],
            start="b", end="d")
        self.assertEqual(seqs, [6, 7, 8])
        self.assertIsNone(self.store.get("a"))
        self.assertEqual(self.store.get("e"), "changed")

    def test_restored_value_matches_again(self) -> None:
        self.seed()
        self.store.put("b", "other")
        self.assertIsNone(self.store.write_batch_if_range(
            {"b": "2"}, OPS, start="b", end="c"))
        self.store.put("b", "2")  # history changed, then restored
        self.assertEqual(self.store.write_batch_if_range(
            {"b": "2"}, OPS, start="b", end="c"), [8])

    def test_open_and_default_boundaries(self) -> None:
        self.seed()
        # end open: everything from b upward
        self.assertEqual(self.store.write_batch_if_range(
            {"b": "2", "c": "3", "d": "4", "e": "5"}, OPS,
            start="b"), [6])
        # both open: the whole live directory
        self.assertEqual(self.store.write_batch_if_range(
            {"a": "1", "b": "2", "c": "3", "d": "4", "e": "5", "z": "v"},
            [{"op": "delete", "key": "z"}]), [7])
        # empty store and empty expectation
        root2 = self.root / "s2"
        s2 = PageStore(root2)
        s2.init()
        self.assertEqual(s2.write_batch_if_range({}, OPS), [1])

    def test_empty_string_value_matches_exactly(self) -> None:
        self.store.put("a", "")
        self.assertIsNone(self.store.write_batch_if_range(
            {}, OPS, start="a", end="b"))
        self.assertEqual(self.store.write_batch_if_range(
            {"a": ""}, OPS, start="a", end="b"), [2])


class BatchIfRangeAtomicityTests(BatchIfRangeTestBase):
    def test_mismatch_leaves_file_and_sequence_untouched(self) -> None:
        self.store.put("a", "1")
        before = self.data
        self.assertIsNone(self.store.write_batch_if_range(
            {"a": "other"}, [{"op": "put", "key": "b", "value": "2"}]))
        self.assertEqual(self.data, before)  # byte-for-byte unchanged
        self.assertEqual(self.store.stats(), {"pages": 1, "records": 1, "keys": 1})
        self.assertEqual(self.store.put("b", "2"), 2)

    def test_mismatch_does_not_truncate_or_compare_half_written_tail(self) -> None:
        self.store.put("a", "1")
        frame = encode({"op": "put", "key": "torn", "value": "x" * 100})
        self.write(self.data + frame[:len(frame) - 5])
        torn = self.data
        fresh = PageStore(self.root)
        # the torn key is not live, so a range claiming it exists mismatches;
        # the tail bytes themselves survive the failed commit untouched
        self.assertIsNone(fresh.write_batch_if_range(
            {"a": "1", "torn": "?"},
            [{"op": "put", "key": "b", "value": "2"}], start="a", end="z"))
        self.assertEqual(self.data, torn)
        # a non-empty expectation over an empty live gap mismatches, and the
        # tail still survives untouched
        self.assertIsNone(fresh.write_batch_if_range(
            {"m": "x"}, [{"op": "put", "key": "b", "value": "2"}],
            start="m", end="n"))
        self.assertEqual(self.data, torn)
        # ...and a successful commit discards it, as in write_batch
        self.assertEqual(fresh.write_batch_if_range(
            {"a": "1"}, [{"op": "put", "key": "b", "value": "2"}]), [2])
        self.assertNotIn(b"torn", self.data)
        self.assertEqual(fresh.scan(), [("a", "1"), ("b", "2")])

    def test_successful_multipage_batch_persists_across_reopen(self) -> None:
        self.store.put("a", "1")
        big = "x" * 3000
        seqs = self.store.write_batch_if_range(
            {"a": "1"},
            [{"op": "put", "key": f"k{i}", "value": f"{big}{i}"}
             for i in range(5)]
            + [{"op": "delete", "key": "a"}])
        self.assertEqual(seqs, [2, 3, 4, 5, 6, 7])
        fresh = PageStore(self.root)
        self.assertEqual(fresh.stats()["records"], 7)
        self.assertIsNone(fresh.get("a"))
        self.assertTrue(fresh.get("k4").endswith("4"))
        self.assertEqual(fresh.verify()["status"], "ok")

    def test_duplicate_keys_and_missing_deletes_count(self) -> None:
        self.assertEqual(self.store.write_batch_if_range(
            {},
            [{"op": "put", "key": "k", "value": "1"},
             {"op": "put", "key": "k", "value": "2"},
             {"op": "delete", "key": "missing"},
             {"op": "delete", "key": "k"}]), [1, 2, 3, 4])
        self.assertEqual(self.store.stats(),
                         {"pages": 1, "records": 4, "keys": 0})

    def test_conditions_are_not_recorded(self) -> None:
        self.store.put("guard", "on")
        self.store.write_batch_if_range(
            {"guard": "on"}, [{"op": "put", "key": "a", "value": "1"}])
        self.assertEqual(self.store.stats(),
                         {"pages": 1, "records": 2, "keys": 2})
        self.assertEqual(self.store.recover()["records"], 2)
        self.assertEqual(self.store.verify()["complete_records"], 2)
        result = self.store.compact()
        self.assertEqual(result["records_before"], 2)
        self.assertEqual(result["records_after"], 2)
        self.assertEqual(self.store.scan(),
                         [("a", "1"), ("guard", "on")])

    def test_corrupt_middle_raises_even_when_comparison_would_fail(self) -> None:
        blob = (put_record("a", "1") + b"\x00\x00\x00\x09broken"
                + put_record("b", "2"))
        self.write(blob)
        fresh = PageStore(self.root)
        with self.assertRaises(RuntimeError) as caught:
            fresh.write_batch_if_range(
                {"a": "no-such-value"},
                [{"op": "put", "key": "c", "value": "3"}], end="z")
        self.assertEqual(str(caught.exception), "corrupt_middle")
        self.assertEqual(self.data, blob)

    def test_missing_store_raises_filenotfound_after_validation(self) -> None:
        missing = self.root / "nope"
        with self.assertRaises(FileNotFoundError):
            PageStore(missing).write_batch_if_range(
                {}, [{"op": "put", "key": "k", "value": "v"}])
        self.assertFalse(missing.exists())
        file_root = self.root / "afile"
        file_root.write_text("x")
        with self.assertRaises(FileNotFoundError):
            PageStore(file_root).write_batch_if_range(
                {}, [{"op": "put", "key": "k", "value": "v"}])

    def test_old_format_store_needs_no_migration(self) -> None:
        blob = put_record("old", "1") + put_record("old", "2")
        self.write(blob)
        fresh = PageStore(self.root)
        self.assertEqual(fresh.write_batch_if_range(
            {"old": "2"},
            [{"op": "put", "key": "new", "value": "3"},
             {"op": "delete", "key": "old"}]), [3, 4])
        self.assertEqual(fresh.scan(), [("new", "3")])
        self.assertEqual(fresh.verify()["status"], "ok")


class BatchIfRangeSerialOrderingTests(BatchIfRangeTestBase):
    def test_concurrent_scan_insert_only_one_wins(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "1")
        barrier = threading.Barrier(2)
        results: list[list[int] | None] = []

        def contender(i: int) -> None:
            local = PageStore(self.root)
            barrier.wait(timeout=10)
            results.append(local.write_batch_if_range(
                {"a": "1", "b": "1"},
                [{"op": "put", "key": f"b{i}", "value": "x"}],
                start="a", end="c"))

        threads = [threading.Thread(target=contender, args=(i,))
                   for i in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        winners = [r for r in results if r is not None]
        self.assertEqual(len(winners), 1)
        self.assertEqual(winners[0], [3])
        self.assertEqual(self.store.stats()["records"], 3)

    def test_concurrent_scan_insert_across_processes(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "1")
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
        self.assertEqual(winners[0], [3])
        self.assertEqual(self.store.verify()["status"], "ok")

    def test_serialises_with_writes_recover_compact_and_reset(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        self.assertEqual(a.put("k", "1"), 1)
        self.assertEqual(b.write_batch_if_range(
            {"k": "1"}, [{"op": "put", "key": "k", "value": "2"}]), [2])
        result = a.compact()
        self.assertEqual(result["records_after"], 1)
        self.assertEqual(b.write_batch_if_range(
            {"k": "2"}, [{"op": "delete", "key": "k"}]), [2])
        self.assertIsNone(a.get("k"))
        a.recover()
        self.assertEqual(a.stats(), {"pages": 1, "records": 2, "keys": 0})
        # after a reset the range is empty, so a non-empty expectation fails
        a.init()
        self.assertIsNone(b.write_batch_if_range(
            {"k": "2"}, [{"op": "put", "key": "x", "value": "9"}]))
        # but an empty-range expectation commits on the reset store
        self.assertEqual(b.write_batch_if_range(
            {}, [{"op": "put", "key": "x", "value": "9"}]), [1])
        self.assertEqual(b.stats(), {"pages": 1, "records": 1, "keys": 1})

    def test_readers_see_only_before_or_after_states(self) -> None:
        self.store.put("shared", "old")
        stop = threading.Event()
        errors: list[BaseException] = []

        def writer() -> None:
            local = PageStore(self.root)
            try:
                i = 0
                while not stop.is_set():
                    local.write_batch_if_range(
                        {"shared": f"v{i}"},
                        [{"op": "put", "key": "tmp", "value": "z"},
                         {"op": "delete", "key": "tmp"},
                         {"op": "put", "key": "shared", "value": f"v{i + 1}"}],
                        start="s", end="t")
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

        self.store.put("shared", "v0")
        threads = [threading.Thread(target=writer),
                   threading.Thread(target=reader),
                   threading.Thread(target=reader)]
        for thread in threads:
            thread.start()
        threading.Event().wait(0.5)
        stop.set()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(errors, [])
        self.assertEqual(self.store.verify()["status"], "ok")

    def test_snapshot_taken_before_is_frozen(self) -> None:
        self.store.put("a", "1")
        before = self.store.snapshot()
        self.store.write_batch_if_range(
            {"a": "1"}, [{"op": "put", "key": "a", "value": "2"}])
        self.assertEqual(before.scan(), [("a", "1")])
        self.assertEqual(before.stats(), {"pages": 1, "records": 1, "keys": 1})
        after = self.store.snapshot()
        self.assertEqual(after.scan(), [("a", "2")])
        self.assertEqual(after.stats()["records"], 2)


if __name__ == "__main__":
    unittest.main()
