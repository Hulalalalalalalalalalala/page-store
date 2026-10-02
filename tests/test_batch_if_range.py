"""Tests for PageStore.write_batch_if_range range-conditional batch commits.

The range comparison and the commit are one serial operation shared with all
other writes, resets, recoveries and compactions.  The committed batch is
indistinguishable on disk from a write_batch frame; a failed comparison
returns None without consuming a sequence number or changing a single byte
of the page file (a half-written tail included), while keys outside the
range are irrelevant to the comparison even when the batch itself writes
them.
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

# Two workers both scan an (initially) identical range and insert a key into
# it; exactly one may commit.
RACE_WORKER = r"""
import json, os, sys
sys.path.insert(0, os.environ["PYTHONPATH"])
from page_store.core import PageStore
root, wid = sys.argv[1], int(sys.argv[2])
store = PageStore(root)
result = store.write_batch_if_range(
    {"guard": "on"},
    [{"op": "put", "key": f"won-{wid}", "value": "x"}],
    start="a", end="z")
print(json.dumps(result))
"""


def encode(record: dict) -> bytes:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


def put_record(key: str, value: str) -> bytes:
    return encode({"op": "put", "key": key, "value": value})


OPS = [{"op": "put", "key": "k", "value": "v"}]


class RangeBatchTestBase(unittest.TestCase):
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


class RangeValidationTests(RangeBatchTestBase):
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

    def test_expected_values_must_be_strings(self) -> None:
        for bad_value in (None, 1, True, b"v", ["v"], {"v": 1}, object()):
            self.assert_rejected({"k": bad_value})

    def test_bounds_must_be_strings_or_none(self) -> None:
        for bad in (1, b"a", True, [], object()):
            self.assert_rejected({}, start=bad)
            self.assert_rejected({}, end=bad)

    def test_empty_string_bound_is_legal(self) -> None:
        # must not raise; empty range matches empty expectation
        self.assertEqual(self.store.write_batch_if_range(
            {}, OPS, start="", end=""), [1])

    def test_condition_keys_must_lie_inside_the_range(self) -> None:
        self.assert_rejected({"k": "v"}, start="l")        # k < start
        self.assert_rejected({"k": "v"}, end="k")          # k >= end
        self.assert_rejected({"k": "v"}, start="l", end="z")
        self.assert_rejected({"k": "v"}, start="a", end="k")
        # equality with start is inside; a key below an exclusive end is too
        self.store.put("k", "v")
        self.assertEqual(self.store.write_batch_if_range(
            {"k": "v"}, [{"op": "delete", "key": "k"}], start="k"), [2])
        self.store.put("ka", "v")
        self.assertEqual(self.store.write_batch_if_range(
            {"ka": "v"}, OPS, start="ka", end="kb"), [4])

    def test_empty_range_rejects_any_condition_key(self) -> None:
        # start >= end is an empty range, so no key can be inside it
        self.assert_rejected({"": "v"}, start="m", end="m")
        self.assert_rejected({"m": "v"}, start="z", end="a")
        self.assert_rejected({"l": "v"}, start="m", end="m")

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
        with self.assertRaises(ValueError):
            store.write_batch_if_range(None, OPS)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            store.write_batch_if_range({}, [])
        with self.assertRaises(ValueError):
            store.write_batch_if_range({}, OPS, start=1)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            store.write_batch_if_range({"k": "v"}, OPS, start="l")
        self.assertFalse(missing.exists())

    def test_failed_validation_consumes_no_number(self) -> None:
        self.store.put("a", "1")
        with self.assertRaises(ValueError):
            self.store.write_batch_if_range({"a": 1}, OPS)  # type: ignore[dict-item]
        self.assertEqual(self.store.stats(),
                         {"pages": 1, "records": 1, "keys": 1})
        self.assertEqual(
            self.store.write_batch_if_range({}, OPS, start="z"), [2])


class RangeComparisonTests(RangeBatchTestBase):
    def seed(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        self.store.put("c", "3")

    def test_exact_live_mapping_commits(self) -> None:
        self.seed()
        # dict order of expected is irrelevant
        self.assertEqual(self.store.write_batch_if_range(
            {"b": "2", "a": "1"},
            [{"op": "put", "key": "b", "value": "20"}],
            start="a", end="c"), [4])
        self.assertEqual(self.store.get("b"), "20")

    def test_open_bounds_cover_everything(self) -> None:
        self.store.put("a", "0")
        self.store.put("z", "9")
        self.assertEqual(self.store.write_batch_if_range(
            {"a": "0", "z": "9"}, [{"op": "delete", "key": "a"}]), [3])
        self.assertIsNone(self.store.get("a"))

    def test_inserted_key_blocks_commit(self) -> None:
        self.seed()
        self.store.put("aa", "x")
        self.assertIsNone(self.store.write_batch_if_range(
            {"a": "1", "b": "2"},
            [{"op": "delete", "key": "a"}], start="a", end="c"))
        self.assertEqual(self.store.scan("a", "c"),
                         [("a", "1"), ("aa", "x"), ("b", "2")])

    def test_deleted_key_blocks_commit(self) -> None:
        self.seed()
        self.store.delete("a")
        self.assertIsNone(self.store.write_batch_if_range(
            {"a": "1", "b": "2"}, OPS, start="a", end="c"))
        self.assertEqual(self.store.stats()["records"], 4)

    def test_changed_value_blocks_commit(self) -> None:
        self.seed()
        self.store.put("b", "two")
        self.assertIsNone(self.store.write_batch_if_range(
            {"a": "1", "b": "2"}, OPS, start="a", end="c"))

    def test_extra_expected_key_blocks_commit(self) -> None:
        self.seed()
        # expecting a key that is not live is itself a mismatch
        self.assertIsNone(self.store.write_batch_if_range(
            {"a": "1", "b": "2", "bb": "x"}, OPS, start="a", end="c"))

    def test_restored_state_matches_again(self) -> None:
        self.seed()
        self.store.put("b", "temp")
        self.store.put("b", "2")
        self.assertEqual(self.store.write_batch_if_range(
            {"a": "1", "b": "2"}, OPS, start="a", end="c"), [6])

    def test_change_outside_range_does_not_block(self) -> None:
        self.seed()
        # insert, change and delete keys outside [a, c)
        self.store.put("z", "9")
        self.store.put("z", "99")
        self.store.delete("z")
        self.assertEqual(self.store.write_batch_if_range(
            {"a": "1", "b": "2"},
            [{"op": "put", "key": "z", "value": "out"}],
            start="a", end="c"), [7])
        self.assertEqual(self.store.get("z"), "out")
        # c sits at the exclusive end and is also outside
        self.assertEqual(self.store.write_batch_if_range(
            {"a": "1", "b": "2"}, OPS, start="a", end="c"), [8])

    def test_empty_expected_requires_empty_range(self) -> None:
        self.seed()
        self.assertEqual(self.store.write_batch_if_range(
            {}, [{"op": "put", "key": "m", "value": "1"}],
            start="m", end="z"), [4])
        # now m lives in the range, so the emptiness condition fails
        self.assertIsNone(self.store.write_batch_if_range(
            {}, OPS, start="m", end="z"))
        # keys at/outside the bounds do not count
        self.assertEqual(self.store.write_batch_if_range(
            {}, OPS, start="z"), [5])

    def test_start_ge_end_is_empty_range(self) -> None:
        self.seed()
        self.assertEqual(self.store.write_batch_if_range(
            {}, OPS, start="c", end="a"), [4])
        self.assertEqual(self.store.write_batch_if_range(
            {}, OPS, start="b", end="b"), [5])

    def test_half_open_end_boundary_excludes_end_key(self) -> None:
        self.seed()
        # range [a, c): c is excluded; changing c does not matter
        self.store.put("c", "30")
        self.assertEqual(self.store.write_batch_if_range(
            {"a": "1", "b": "2"}, OPS, start="a", end="c"), [5])

    def test_batch_may_modify_keys_outside_range(self) -> None:
        self.seed()
        self.assertEqual(self.store.write_batch_if_range(
            {"a": "1", "b": "2"},
            [{"op": "put", "key": "outside", "value": "x"},
             {"op": "delete", "key": "c"},
             {"op": "delete", "key": "missing"}],
            start="a", end="c"), [4, 5, 6])
        self.assertEqual(self.store.get("outside"), "x")
        self.assertIsNone(self.store.get("c"))

    def test_duplicate_keys_not_merged_and_missing_delete_counts(self) -> None:
        self.seed()
        seqs = self.store.write_batch_if_range(
            {"a": "1", "b": "2", "c": "3"},
            [{"op": "put", "key": "a", "value": "x"},
             {"op": "put", "key": "a", "value": "y"},
             {"op": "delete", "key": "ghost"}],
            start="a")
        self.assertEqual(seqs, [4, 5, 6])
        self.assertEqual(self.store.get("a"), "y")
        self.assertEqual(self.store.stats()["records"], 6)


class RangeAtomicityTests(RangeBatchTestBase):
    def test_failed_comparison_leaves_file_and_sequence_untouched(self) -> None:
        self.store.put("a", "1")
        before = self.data
        self.assertIsNone(self.store.write_batch_if_range(
            {"a": "other"}, [{"op": "put", "key": "b", "value": "2"}],
            start="a", end="z"))
        self.assertEqual(self.data, before)  # byte-for-byte unchanged
        self.assertEqual(self.store.stats(),
                         {"pages": 1, "records": 1, "keys": 1})
        self.assertEqual(self.store.put("b", "2"), 2)

    def test_failed_comparison_keeps_half_written_tail(self) -> None:
        self.store.put("a", "1")
        frame = encode({"op": "put", "key": "torn", "value": "x" * 100})
        self.write(self.data + frame[:len(frame) - 5])
        torn = self.data
        fresh = PageStore(self.root)
        self.assertIsNone(fresh.write_batch_if_range(
            {"a": "other"}, [{"op": "put", "key": "b", "value": "2"}],
            start="a", end="z"))
        self.assertEqual(self.data, torn)
        # the next successful commit discards the tail, as usual
        self.assertEqual(fresh.write_batch_if_range(
            {"a": "1"}, [{"op": "put", "key": "b", "value": "2"}],
            start="a", end="z"), [2])
        self.assertNotIn(b"torn", self.data)
        self.assertEqual(fresh.scan(), [("a", "1"), ("b", "2")])

    def test_half_written_tail_is_not_part_of_the_comparison(self) -> None:
        self.store.put("a", "1")
        frame = encode({"op": "put", "key": "aa", "value": "intruder"})
        # a torn intruder frame would be inside the range if it counted
        self.write(self.data + frame[:len(frame) - 3])
        fresh = PageStore(self.root)
        self.assertEqual(fresh.write_batch_if_range(
            {"a": "1"}, [{"op": "put", "key": "b", "value": "2"}],
            start="a", end="z"), [2])
        self.assertEqual(fresh.scan(), [("a", "1"), ("b", "2")])

    def test_cross_page_batch_persists_or_vanishes_whole(self) -> None:
        ops = [{"op": "put", "key": f"k{i:04d}", "value": "v" * 2000}
               for i in range(6)]
        seqs = self.store.write_batch_if_range({}, ops)
        self.assertEqual(seqs, list(range(1, 7)))
        self.assertGreater(len(self.data), 3 * PAGE_SIZE)
        fresh = PageStore(self.root)
        self.assertEqual(fresh.stats()["records"], 6)
        for i in range(6):
            self.assertEqual(fresh.get(f"k{i:04d}"), "v" * 2000)
        self.assertEqual(fresh.verify()["status"], "ok")

    def test_conditions_are_not_recorded(self) -> None:
        self.store.put("guard", "on")
        self.store.write_batch_if_range(
            {"guard": "on"}, [{"op": "put", "key": "a", "value": "1"}],
            start="a", end="z")
        self.assertEqual(self.store.stats(),
                         {"pages": 1, "records": 2, "keys": 2})
        self.assertEqual(self.store.recover()["records"], 2)
        self.assertEqual(self.store.verify()["complete_records"], 2)
        result = self.store.compact()
        self.assertEqual(result["records_before"], 2)
        self.assertEqual(result["records_after"], 2)
        self.assertEqual(self.store.scan(), [("a", "1"), ("guard", "on")])

    def test_corrupt_middle_raises_even_when_comparison_fails(self) -> None:
        blob = (put_record("a", "1") + b"\x00\x00\x00\x09broken"
                + put_record("b", "2"))
        self.write(blob)
        fresh = PageStore(self.root)
        # the expected mapping deliberately cannot match the range
        with self.assertRaises(RuntimeError) as caught:
            fresh.write_batch_if_range(
                {"a": "no-such-value"},
                [{"op": "put", "key": "c", "value": "3"}],
                start="a", end="z")
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
             {"op": "delete", "key": "old"}],
            start="a", end="z"), [3, 4])
        self.assertEqual(fresh.scan(), [("new", "3")])
        self.assertEqual(fresh.verify()["status"], "ok")


class RangeSerialOrderingTests(RangeBatchTestBase):
    def test_concurrent_scan_then_insert_only_one_wins_threads(self) -> None:
        self.store.put("guard", "on")
        barrier = threading.Barrier(2)
        results: list[list[int] | None] = []

        def contender(i: int) -> None:
            local = PageStore(self.root)
            barrier.wait(timeout=10)
            results.append(local.write_batch_if_range(
                {"guard": "on"},
                [{"op": "put", "key": f"won-{i}", "value": "x"}],
                start="a", end="z"))

        threads = [threading.Thread(target=contender, args=(i,))
                   for i in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        winners = [r for r in results if r is not None]
        self.assertEqual(len(winners), 1)
        self.assertEqual(winners[0], [2])
        self.assertEqual(self.store.stats()["records"], 2)

    def test_concurrent_scan_then_insert_only_one_wins_processes(self) -> None:
        self.store.put("guard", "on")
        procs = [subprocess.Popen(
            [sys.executable, "-c", RACE_WORKER, str(self.root), str(w)],
            cwd=REPO_ROOT, env={**os.environ, "PYTHONPATH": ENVPATH},
            stdout=subprocess.PIPE, text=True) for w in range(2)]
        outcomes = []
        for proc in procs:
            out, err = proc.communicate(timeout=60)
            self.assertEqual(proc.returncode, 0, err)
            outcomes.append(json.loads(out))
        self.assertEqual(len([r for r in outcomes if r is not None]), 1)
        self.assertEqual(self.store.stats()["records"], 2)
        self.assertEqual(self.store.verify()["status"], "ok")

    def test_serialises_with_compact_reset_and_recover(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        a.put("k", "1")
        self.assertEqual(b.write_batch_if_range(
            {"k": "1"}, [{"op": "put", "key": "k", "value": "2"}],
            start="a", end="z"), [2])
        result = a.compact()
        self.assertEqual(result["records_after"], 1)
        self.assertEqual(b.write_batch_if_range(
            {"k": "2"}, [{"op": "delete", "key": "k"}],
            start="a", end="z"), [2])
        a.recover()
        self.assertEqual(a.stats(), {"pages": 1, "records": 2, "keys": 0})
        # after a reset the range is empty, so expecting k fails
        a.init()
        self.assertIsNone(b.write_batch_if_range(
            {"k": "2"}, [{"op": "put", "key": "x", "value": "9"}],
            start="a", end="z"))
        self.assertEqual(b.write_batch_if_range(
            {}, [{"op": "put", "key": "x", "value": "9"}],
            start="a", end="z"), [1])

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
                        start="a", end="z")
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
                    self.assertNotIn("tmp", dict(local.snapshot().scan()))
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        self.store.put("shared", "v0")
        threads = [threading.Thread(target=writer),
                   threading.Thread(target=reader),
                   threading.Thread(target=reader)]
        for thread in threads:
            thread.start()
        import time
        time.sleep(0.5)
        stop.set()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(errors, [])
        self.assertEqual(self.store.verify()["status"], "ok")


if __name__ == "__main__":
    unittest.main()
