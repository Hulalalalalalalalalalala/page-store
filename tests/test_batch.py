"""Tests for PageStore.write_batch atomic batch writes.

A batch is validated wholesale before the page file is touched, lands as a
single serial point (one frame that may span several pages), and survives a
crash as either the whole batch or none of it.  These tests cover validation,
sequence numbering, the operation-counting convention used by stats/recover/
verify, compaction/reset interactions, multi-process serial ordering and
reader visibility.
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
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from page_store.core import PAGE_SIZE, PageStore  # noqa: E402

ENVPATH = str(REPO_ROOT) + os.pathsep + os.environ.get("PYTHONPATH", "")

WORKER = r"""
import json, os, sys
sys.path.insert(0, os.environ["PYTHONPATH"])
from page_store.core import PageStore

root, wid, batches, per = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
store = PageStore(root)
seqs = []
for i in range(batches):
    ops = [{"op": "put", "key": f"w{wid:02d}-b{i:03d}-o{j:02d}",
            "value": f"v{wid:02d}-{i:03d}-{j:02d}"} for j in range(per)]
    ops.append({"op": "put", "key": "shared", "value": f"{wid:02d}:{i:03d}"})
    seqs.extend(store.write_batch(ops))
print(json.dumps(seqs))
"""

# Appends a complete record, then a batch frame cut short, and exits hard
# without fsync-confirming: exactly the on-disk state of a process killed
# inside an unconfirmed batch write.
TORN_BATCH_WORKER = r"""
import os, sys
sys.path.insert(0, os.environ["PYTHONPATH"])
from page_store.core import PageStore
root = sys.argv[1]
store = PageStore(root)
store.put("a", "1")
frame = store._encode({"op": "batch", "ops": [
    {"op": "put", "key": f"k{i:03d}", "value": "x" * 80} for i in range(120)]})
with open(os.path.join(root, "pages.dat"), "ab") as handle:
    handle.write(frame[:len(frame) // 2])
    handle.flush()
    os.fsync(handle.fileno())
os._exit(0)
"""


def encode(record: dict) -> bytes:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


def put_record(key: str, value: str) -> bytes:
    return encode({"op": "put", "key": key, "value": value})


def batch_frame(ops: list[dict]) -> bytes:
    return encode({"op": "batch", "ops": ops})


def outer_frames(data: bytes) -> list[dict]:
    """Parse the outer length-prefixed frames (batch payloads stay nested)."""
    out, offset = [], 0
    while offset + 4 <= len(data):
        size = int.from_bytes(data[offset:offset + 4], "big")
        out.append(json.loads(data[offset + 4:offset + 4 + size].decode("utf-8")))
        offset += 4 + size
    assert offset == len(data), "tail bytes left over"
    return out


class BatchTestBase(unittest.TestCase):
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

    def run_cli(self, *argv: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "page_store", "--root", str(self.root), *argv],
            cwd=REPO_ROOT, capture_output=True, text=True)


class BatchSuccessTests(BatchTestBase):
    def test_returns_consecutive_numbers_from_current_count(self) -> None:
        self.assertEqual(self.store.put("a", "1"), 1)
        seqs = self.store.write_batch(
            [{"op": "put", "key": "b", "value": "2"},
             {"op": "delete", "key": "a"},
             {"op": "put", "key": "c", "value": "3"}])
        self.assertEqual(seqs, [2, 3, 4])
        self.assertEqual(self.store.put("d", "4"), 5)
        self.assertEqual(self.store.scan(), [("b", "2"), ("c", "3"), ("d", "4")])

    def test_first_batch_on_empty_store_starts_at_one(self) -> None:
        seqs = self.store.write_batch(
            [{"op": "put", "key": "a", "value": "1"},
             {"op": "delete", "key": "missing"}])
        self.assertEqual(seqs, [1, 2])
        # deleting a missing key still takes a number and leaves no key
        self.assertEqual(self.store.scan(), [("a", "1")])
        self.assertEqual(self.store.stats(), {"pages": 1, "records": 2, "keys": 1})

    def test_operations_apply_in_input_order_without_merging(self) -> None:
        seqs = self.store.write_batch(
            [{"op": "put", "key": "k", "value": "v1"},
             {"op": "put", "key": "k", "value": "v2"},
             {"op": "delete", "key": "k"},
             {"op": "put", "key": "k", "value": "v3"}])
        self.assertEqual(seqs, [1, 2, 3, 4])  # duplicate keys are not merged
        self.assertEqual(self.store.get("k"), "v3")

    def test_batch_may_span_multiple_pages_but_is_one_frame(self) -> None:
        ops = [{"op": "put", "key": f"k{i:04d}", "value": "x" * 120}
               for i in range(200)]
        seqs = self.store.write_batch(ops)
        self.assertEqual(seqs, list(range(1, 201)))
        raw = self.data
        # the whole batch is one outer frame spanning several pages
        self.assertGreater(len(raw), PAGE_SIZE * 3)
        frames = outer_frames(raw)
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0]["op"], "batch")
        self.assertEqual(len(frames[0]["ops"]), 200)
        self.assertGreater(
            int.from_bytes(raw[:4], "big"), PAGE_SIZE)
        self.assertEqual(self.store.stats(),
                         {"pages": (len(raw) + PAGE_SIZE - 1) // PAGE_SIZE,
                          "records": 200, "keys": 200})

    def test_two_large_operations_can_exceed_one_page_together(self) -> None:
        # Each operation fits the single-record limit; together they cross a
        # page boundary and the batch must still succeed.
        big = "y" * 3000
        seqs = self.store.write_batch(
            [{"op": "put", "key": "a", "value": big},
             {"op": "put", "key": "b", "value": big}])
        self.assertEqual(seqs, [1, 2])
        self.assertGreater(len(self.data), PAGE_SIZE)
        self.assertEqual(self.store.get("a"), big)
        self.assertEqual(self.store.get("b"), big)

    def test_persisted_and_visible_from_a_fresh_instance_without_recover(self) -> None:
        ops = [{"op": "put", "key": f"k{i}", "value": f"v{i}"} for i in range(20)]
        ops.append({"op": "delete", "key": "k5"})
        self.store.write_batch(ops)
        fresh = PageStore(self.root)
        self.assertEqual(fresh.stats()["records"], 21)
        self.assertIsNone(fresh.get("k5"))
        self.assertEqual(fresh.get("k19"), "v19")
        self.assertEqual(fresh.verify()["status"], "ok")

    def test_long_lived_instance_sees_batch_from_other_instance(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        a.put("a", "1")
        b.write_batch([{"op": "put", "key": "b", "value": "2"},
                       {"op": "put", "key": "c", "value": "3"}])
        # A's next read/write observes the batch immediately, no recover first
        self.assertEqual(a.get("b"), "2")
        self.assertEqual(a.stats(), {"pages": 1, "records": 3, "keys": 3})
        self.assertEqual(a.put("d", "4"), 4)
        self.assertEqual(b.get("d"), "4")


class BatchValidationTests(BatchTestBase):
    def assert_rejected(self, operations: object) -> None:
        before = self.data
        with self.assertRaises(ValueError):
            self.store.write_batch(operations)  # type: ignore[arg-type]
        self.assertEqual(self.data, before)  # page file untouched

    def test_non_list_and_empty_list_rejected(self) -> None:
        self.assert_rejected(None)
        self.assert_rejected("x")
        self.assert_rejected({"op": "put", "key": "k", "value": "v"})
        self.assert_rejected(({"op": "put", "key": "k", "value": "v"},))
        self.assert_rejected([])

    def test_non_dict_elements_rejected(self) -> None:
        good = {"op": "put", "key": "k", "value": "v"}
        for bad in (None, 1, "put", ["put"], ()):
            self.assert_rejected([good, bad])
            self.assert_rejected([bad, good])

    def test_unknown_op_rejected(self) -> None:
        self.assert_rejected([{"op": "fetch", "key": "k"}])
        self.assert_rejected([{"op": "PUT", "key": "k", "value": "v"}])
        self.assert_rejected([{"op": "batch", "key": "k"}])
        self.assert_rejected([{"op": ""}])

    def test_missing_fields_rejected(self) -> None:
        self.assert_rejected([{"op": "put", "key": "k"}])
        self.assert_rejected([{"op": "put", "value": "v"}])
        self.assert_rejected([{"op": "delete"}])
        self.assert_rejected([{"op": "delete", "value": "v"}])
        self.assert_rejected([{"key": "k", "value": "v"}])

    def test_extra_fields_rejected(self) -> None:
        self.assert_rejected([{"op": "put", "key": "k", "value": "v", "x": 1}])
        self.assert_rejected([{"op": "delete", "key": "k", "value": "v"}])
        self.assert_rejected([{"op": "delete", "key": "k", "when": 1}])

    def test_bad_keys_and_values_rejected(self) -> None:
        for bad_key in (None, 1, b"k", "", object()):
            self.assert_rejected([{"op": "put", "key": bad_key, "value": "v"}])
            self.assert_rejected([{"op": "delete", "key": bad_key}])
        for bad_value in (None, 1, b"v", object()):
            self.assert_rejected([{"op": "put", "key": "k", "value": bad_value}])

    def test_oversized_single_operation_rejected_even_when_batch_is_small(self) -> None:
        self.assert_rejected(
            [{"op": "put", "key": "big", "value": "x" * (PAGE_SIZE + 1)}])
        # a large value alone dominates the JSON payload; JSON encoding
        # overhead pushes even PAGE_SIZE-ish strings over the payload limit
        self.assert_rejected(
            [{"op": "put", "key": "big", "value": "x" * PAGE_SIZE}])
        # an oversized operation late in an otherwise valid batch still
        # rejects the *whole* batch before storage
        good = [{"op": "put", "key": f"k{i}", "value": "v"} for i in range(10)]
        self.assert_rejected(
            good + [{"op": "put", "key": "big", "value": "x" * 5000}])

    def test_failed_validation_consumes_no_number_and_changes_nothing(self) -> None:
        self.store.put("a", "1")
        with self.assertRaises(ValueError):
            self.store.write_batch(
                [{"op": "put", "key": "b", "value": "2"},
                 {"op": "put", "key": "c"}])  # missing value
        self.assertEqual(self.store.stats(), {"pages": 1, "records": 1, "keys": 1})
        # the next successful batch numbers as if the failed call never happened
        self.assertEqual(
            self.store.write_batch([{"op": "put", "key": "b", "value": "2"}]),
            [2])
        self.assertIsNone(self.store.get("c"))


class BatchAtomicityTests(BatchTestBase):
    def test_torn_batch_frame_after_crash_is_wholly_absent(self) -> None:
        self.store.put("a", "1")
        ops = [{"op": "put", "key": f"k{i:03d}", "value": "x" * 60}
               for i in range(100)]
        frame = batch_frame(ops)
        self.assertGreater(len(frame), PAGE_SIZE)
        # simulate a process killed mid-write: only part of the one frame
        self.write(self.data + frame[:len(frame) - 7])
        fresh = PageStore(self.root)
        self.assertEqual(fresh.scan(), [("a", "1")])
        # pages follow the original file-size convention: the torn tail
        # still occupies pages physically, but the batch takes no record
        torn_size = len(self.data)
        self.assertEqual(
            fresh.stats(),
            {"pages": (torn_size + PAGE_SIZE - 1) // PAGE_SIZE,
             "records": 1, "keys": 1})
        # recover drops the unconfirmed tail; no batch number is consumed
        outcome = fresh.recover()
        self.assertEqual(outcome["records"], 1)
        self.assertTrue(outcome["truncated"])
        self.assertEqual(fresh.stats(), {"pages": 1, "records": 1, "keys": 1})
        seqs = fresh.write_batch([{"op": "put", "key": "b", "value": "2"},
                                  {"op": "delete", "key": "a"}])
        self.assertEqual(seqs, [2, 3])
        self.assertEqual(fresh.scan(), [("b", "2")])
        self.assertNotIn(b"k000", self.data)

    def test_torn_length_prefix_is_wholly_absent(self) -> None:
        self.store.put("a", "1")
        frame = batch_frame([{"op": "put", "key": "b", "value": "2"}])
        self.write(self.data + frame[:2])  # only two of four prefix bytes
        fresh = PageStore(self.root)
        self.assertEqual(fresh.stats()["records"], 1)
        self.assertEqual(fresh.put("c", "3"), 2)

    def test_dead_process_half_written_batch_is_wholly_absent(self) -> None:
        proc = subprocess.run(
            [sys.executable, "-c", TORN_BATCH_WORKER, str(self.root)],
            cwd=REPO_ROOT, env={**os.environ, "PYTHONPATH": ENVPATH},
            capture_output=True, text=True, timeout=30)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        other = PageStore(self.root)
        # the unconfirmed batch takes no number and none of its keys show
        self.assertEqual(other.stats()["records"], 1)
        self.assertEqual(other.scan(), [("a", "1")])
        self.assertEqual(
            other.write_batch([{"op": "put", "key": "b", "value": "2"},
                               {"op": "delete", "key": "a"}]),
            [2, 3])
        parsed = outer_frames(self.data)
        self.assertEqual([f["op"] for f in parsed], ["put", "batch"])
        self.assertEqual(parsed[1]["ops"],
                         [{"op": "put", "key": "b", "value": "2"},
                          {"op": "delete", "key": "a"}])
        self.assertEqual(other.verify()["status"], "ok")

    def test_next_write_overwrites_the_half_written_batch(self) -> None:
        ops = [{"op": "put", "key": f"k{i:03d}", "value": "x" * 60}
               for i in range(80)]
        frame = batch_frame(ops)
        self.write(frame[:len(frame) - 3])
        fresh = PageStore(self.root)
        self.assertEqual(fresh.put("a", "1"), 1)
        # no torn bytes survive in front of the confirmed record
        parsed = outer_frames(self.data)
        self.assertEqual(parsed, [{"op": "put", "key": "a", "value": "1"}])

    def test_write_failure_rolls_back_whole_batch(self) -> None:
        self.store.put("a", "1")
        old = self.data
        with mock.patch("page_store.core.os.fsync", side_effect=OSError("boom")):
            with self.assertRaises(OSError):
                self.store.write_batch(
                    [{"op": "put", "key": "b", "value": "2"},
                     {"op": "put", "key": "c", "value": "3"}])
        self.assertEqual(self.data, old)
        self.assertEqual(self.store.scan(), [("a", "1")])
        self.assertEqual(self.store.stats(), {"pages": 1, "records": 1, "keys": 1})
        # a later batch takes the numbers the failed batch would have taken
        self.assertEqual(
            self.store.write_batch([{"op": "put", "key": "b", "value": "2"}]),
            [2])

    @unittest.skipIf(hasattr(os, "geteuid") and os.geteuid() == 0,
                     "root bypasses permission bits")
    def test_io_error_on_append_raises_oserror(self) -> None:
        os.chmod(self.store.path, 0)
        try:
            with self.assertRaises(OSError):
                self.store.write_batch(
                    [{"op": "put", "key": "b", "value": "2"}])
        finally:
            os.chmod(self.store.path, 0o600)

    def test_middle_corruption_rejects_batch_and_preserves_file(self) -> None:
        blob = (put_record("a", "1") + b"\x00\x00\x00\x09broken"
                + put_record("b", "2"))
        self.write(blob)
        fresh = PageStore(self.root)
        with self.assertRaises(RuntimeError) as caught:
            fresh.write_batch([{"op": "put", "key": "c", "value": "3"}])
        self.assertEqual(str(caught.exception), "corrupt_middle")
        self.assertEqual(self.data, blob)

    def test_missing_root_raises_filenotfound_after_validation(self) -> None:
        # a valid batch still fails with FileNotFoundError, not ValueError,
        # when there is no store
        missing = self.root / "nope"
        with self.assertRaises(FileNotFoundError):
            PageStore(missing).write_batch(
                [{"op": "put", "key": "k", "value": "v"}])
        self.assertFalse(missing.exists())


class BatchCountingTests(BatchTestBase):
    def test_stats_counts_operations_not_frames(self) -> None:
        self.store.put("a", "1")
        self.store.write_batch(
            [{"op": "put", "key": "b", "value": "2"},
             {"op": "delete", "key": "b"},
             {"op": "put", "key": "c", "value": "3"}])
        self.assertEqual(self.store.stats(), {"pages": 1, "records": 4, "keys": 2})

    def test_recover_counts_operations(self) -> None:
        self.store.write_batch(
            [{"op": "put", "key": "a", "value": "1"},
             {"op": "put", "key": "b", "value": "2"},
             {"op": "delete", "key": "a"}])
        outcome = self.store.recover()
        self.assertFalse(outcome["truncated"])
        self.assertEqual(outcome["records"], 3)
        self.assertEqual(outcome["pages"], 1)
        self.assertEqual(self.store.scan(), [("b", "2")])

    def test_verify_counts_operations_and_stays_read_only(self) -> None:
        ops = [{"op": "put", "key": f"k{i:03d}", "value": "x" * 60}
               for i in range(60)]
        self.store.write_batch(ops)
        before = self.data
        outcome = self.store.verify()
        self.assertEqual(outcome["status"], "ok")
        self.assertEqual(outcome["complete_records"], 60)
        self.assertEqual(self.data, before)

    def test_verify_torn_batch_is_incomplete_tail(self) -> None:
        head = put_record("a", "1")
        frame = batch_frame(
            [{"op": "put", "key": f"k{i:03d}", "value": "x" * 60}
             for i in range(40)])
        self.write(head + frame[:len(frame) // 2])
        outcome = self.store.verify()
        self.assertEqual(outcome["status"], "incomplete_tail")
        self.assertEqual(outcome["complete_records"], 1)
        self.assertEqual(outcome["tail_partial_bytes"], len(frame) // 2)

    def test_batch_does_not_implicitly_compact_history(self) -> None:
        self.store.put("a", "1")
        self.store.put("a", "2")
        self.store.delete("a")
        before_batch = self.data
        self.store.write_batch([{"op": "put", "key": "b", "value": "3"}])
        raw = self.data
        # the pre-batch history is physically still there, ahead of the frame
        self.assertTrue(raw.startswith(before_batch))
        frames = outer_frames(raw)
        self.assertEqual([f["op"] for f in frames],
                         ["put", "put", "delete", "batch"])
        result = self.store.compact()
        self.assertEqual(result["records_before"], 4)  # 3 bare + 1 batch op
        self.assertEqual(result["records_after"], 1)

    def test_old_format_store_needs_no_migration(self) -> None:
        # a page file written entirely by the pre-batch version of the store
        blob = put_record("old", "1") + put_record("old", "2")
        self.write(blob)
        fresh = PageStore(self.root)
        self.assertEqual(fresh.stats(), {"pages": 1, "records": 2, "keys": 1})
        self.assertEqual(
            fresh.write_batch([{"op": "put", "key": "new", "value": "3"},
                               {"op": "delete", "key": "old"}]),
            [3, 4])
        self.assertEqual(fresh.scan(), [("new", "3")])
        self.assertEqual(fresh.verify()["status"], "ok")
        fresh.recover()
        self.assertEqual(fresh.scan(), [("new", "3")])


class BatchCompactResetTests(BatchTestBase):
    def test_compact_rewrites_batch_state_as_ascending_puts(self) -> None:
        self.store.put("x", "0")
        self.store.write_batch(
            [{"op": "put", "key": "a", "value": "1"},
             {"op": "put", "key": "b", "value": "2"},
             {"op": "delete", "key": "x"},
             {"op": "put", "key": "a", "value": "one"}])
        result = self.store.compact()
        self.assertEqual(result["records_before"], 5)
        self.assertEqual(result["records_after"], 2)
        self.assertEqual(result["keys"], 2)
        self.assertEqual(outer_frames(self.data),
                         [{"op": "put", "key": "a", "value": "one"},
                          {"op": "put", "key": "b", "value": "2"}])
        # post-compaction batches number from the post-compaction count
        self.assertEqual(
            self.store.write_batch([{"op": "put", "key": "c", "value": "3"},
                                    {"op": "delete", "key": "a"}]),
            [3, 4])
        self.assertEqual(self.store.scan(), [("b", "2"), ("c", "3")])

    def test_reset_then_batches_start_at_one(self) -> None:
        self.store.write_batch(
            [{"op": "put", "key": "a", "value": "1"} for _ in range(5)])
        self.store.init()
        self.assertEqual(self.store.stats(), {"pages": 0, "records": 0, "keys": 0})
        self.assertEqual(
            self.store.write_batch([{"op": "put", "key": "z", "value": "9"}]),
            [1])
        self.assertEqual(
            PageStore(self.root).write_batch(
                [{"op": "delete", "key": "z"}]), [2])

    def test_snapshot_before_batch_is_frozen_after_reflects_new_state(self) -> None:
        self.store.put("a", "1")
        before = self.store.snapshot()
        self.store.write_batch(
            [{"op": "put", "key": "a", "value": "2"},
             {"op": "put", "key": "b", "value": "3"},
             {"op": "delete", "key": "a"}])
        self.assertEqual(before.scan(), [("a", "1")])
        self.assertEqual(before.stats(), {"pages": 1, "records": 1, "keys": 1})
        after = self.store.snapshot()
        self.assertEqual(after.scan(), [("b", "3")])
        self.assertEqual(after.stats(), {"pages": 1, "records": 4, "keys": 1})


class BatchSerialOrderingTests(BatchTestBase):
    def spawn_worker(self, wid: int, batches: int, per: int) -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, "-c", WORKER, str(self.root),
             str(wid), str(batches), str(per)],
            cwd=REPO_ROOT, env={**os.environ, "PYTHONPATH": ENVPATH},
            stdout=subprocess.PIPE, text=True)

    def test_sequence_numbers_dense_and_unique_across_processes(self) -> None:
        n_workers, batches, per = 6, 10, 4
        procs = [self.spawn_worker(w, batches, per) for w in range(n_workers)]
        all_seqs: list[int] = []
        for proc in procs:
            out, err = proc.communicate(timeout=60)
            self.assertEqual(proc.returncode, 0, err)
            seqs = json.loads(out)
            self.assertEqual(sorted(seqs), seqs)  # per-process ascending
            all_seqs.extend(seqs)
        total = n_workers * batches * (per + 1)
        self.assertEqual(sorted(all_seqs), list(range(1, total + 1)))
        # the file is a complete prefix of whole outer frames, no torn tail
        frames = outer_frames(self.data)
        operations = sum(len(f["ops"]) for f in frames)
        self.assertTrue(all(f["op"] == "batch" for f in frames))
        self.assertEqual(operations, total)
        fresh = PageStore(self.root)
        self.assertEqual(fresh.stats()["records"], total)
        self.assertEqual(fresh.verify()["status"], "ok")
        self.assertEqual(len(fresh.scan()), n_workers * batches * per + 1)

    def test_batches_serialise_with_single_writes_and_compaction(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        self.assertEqual(a.put("a", "1"), 1)
        self.assertEqual(b.write_batch(
            [{"op": "put", "key": "b", "value": "2"},
             {"op": "put", "key": "c", "value": "3"}]), [2, 3])
        result = a.compact()
        self.assertEqual(result["records_after"], 3)
        self.assertEqual(b.put("d", "4"), 4)
        self.assertEqual(a.write_batch(
            [{"op": "delete", "key": "d"}, {"op": "put", "key": "e", "value": "5"}]),
            [5, 6])
        self.assertEqual(a.scan(),
                         [("a", "1"), ("b", "2"), ("c", "3"), ("e", "5")])
        self.assertEqual(b.stats()["records"], 6)

    def test_concurrent_readers_never_see_intra_batch_states(self) -> None:
        n_writers = 3
        stop = threading.Event()
        errors: list[BaseException] = []

        def writer(tid: int) -> None:
            local = PageStore(self.root)
            try:
                i = 0
                while not stop.is_set():
                    # within one batch the temp key is born and deleted, and
                    # shared steps through three values: none of those
                    # intermediate states is a legal reader observation
                    local.write_batch([
                        {"op": "put", "key": f"tmp-{tid}", "value": "z"},
                        {"op": "delete", "key": f"tmp-{tid}"},
                        {"op": "put", "key": "shared", "value": f"a-{tid}-{i}"},
                        {"op": "put", "key": "shared", "value": f"b-{tid}-{i}"},
                        {"op": "put", "key": f"r{tid}-k{i:04d}",
                         "value": f"{tid}-{i:04d}"},
                    ])
                    i += 1
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        def reader() -> None:
            local = PageStore(self.root)
            try:
                while not stop.is_set():
                    live = dict(local.scan())
                    self.assertFalse(
                        any(k.startswith("tmp-") for k in live), live)
                    if "shared" in live:
                        self.assertRegex(live["shared"], r"^b-\d+-\d+$")
                    snap = local.snapshot()
                    self.assertFalse(
                        any(k.startswith("tmp-") for k, _ in snap.scan()))
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = [threading.Thread(target=writer, args=(t,))
                   for t in range(n_writers)]
        threads += [threading.Thread(target=reader) for _ in range(3)]
        for thread in threads:
            thread.start()
        time.sleep(0.5)
        stop.set()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(errors, [])
        self.assertEqual(self.store.verify()["status"], "ok")


class BatchCliTests(BatchTestBase):
    def test_no_batch_subcommand(self) -> None:
        result = self.run_cli("write-batch")
        self.assertEqual(result.returncode, 2)
        result = self.run_cli("batch")
        self.assertEqual(result.returncode, 2)

    def test_existing_commands_and_report_unchanged(self) -> None:
        self.store.write_batch(
            [{"op": "put", "key": "a", "value": "1"},
             {"op": "put", "key": "b", "value": "2"}])
        stats = self.run_cli("stats")
        self.assertEqual(stats.returncode, 0)
        self.assertEqual(json.loads(stats.stdout),
                         {"keys": 2, "pages": 1, "records": 2})
        scan = self.run_cli("scan")
        self.assertEqual(json.loads(scan.stdout), [["a", "1"], ["b", "2"]])
        report = json.loads(self.run_cli("report").stdout)
        self.assertEqual(report["readiness"],
                         {"append": True, "recover": True,
                          "compaction": True, "snapshotRead": False})
        verify = self.run_cli("verify")
        self.assertEqual(verify.returncode, 0)
        self.assertEqual(json.loads(verify.stdout)["complete_records"], 2)


if __name__ == "__main__":
    unittest.main()
