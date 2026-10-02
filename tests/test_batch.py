"""Tests for PageStore.write_batch atomic batch writes.

Covers input validation, serial ordering and sequence numbers, page
spanning, the all-or-nothing crash boundary (a batch is framed on disk by a
``{"op": "batch", "n": k}`` marker followed by ``bput``/``bdelete`` member
frames), mid-file corruption inside an uncommitted batch, stats/recover/
verify/compact accounting, and same-machine thread/process concurrency.
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
import json, sys
from page_store.core import PageStore
root, wid, batches = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
store = PageStore(root)
seqs = []
for b in range(batches):
    ops = [{"op": "put", "key": f"w{wid:02d}-b{b:03d}-k{i:02d}",
            "value": f"{wid}:{b}:{i}"} for i in range(3)]
    ops.append({"op": "delete", "key": "shared"})
    seqs.extend(store.write_batch(ops))
print(json.dumps(seqs))
"""

# Appends most (not all) of a 100-member batch to an existing store with raw
# unbuffered bytes, fsyncs and hard-exits mid-write: write_batch never
# returns, so on reopen the whole batch must be absent and numbering must
# continue after the records the parent test already confirmed.
DEAD_BATCH_WORKER = r"""
import json, os, sys
root = sys.argv[1]
def enc(rec):
    payload = json.dumps(rec, sort_keys=True).encode()
    return len(payload).to_bytes(4, "big") + payload
blob = enc({"op": "batch", "n": 100}) + b"".join(
    enc({"op": "bput", "key": f"k{i:03d}", "value": "v" * 20})
    for i in range(100))
with open(os.path.join(root, "pages.dat"), "ab") as handle:
    handle.write(blob[:len(blob) - 5000])
    handle.flush()
    os.fsync(handle.fileno())
os._exit(0)
"""


def encode(record: dict) -> bytes:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


def marker(n: int) -> bytes:
    return encode({"op": "batch", "n": n})


def member_put(key: str, value: str) -> bytes:
    return encode({"op": "bput", "key": key, "value": value})


def member_delete(key: str) -> bytes:
    return encode({"op": "bdelete", "key": key})


def parse_frames(data: bytes) -> list[tuple[str, dict]]:
    """Parse every frame, expanding complete batch markers."""
    out: list[tuple[str, dict]] = []
    offset = 0
    while offset < len(data):
        size = int.from_bytes(data[offset:offset + 4], "big")
        rec = json.loads(data[offset + 4:offset + 4 + size].decode("utf-8"))
        offset += 4 + size
        if rec.get("op") != "batch":
            out.append((rec["op"], rec))
            continue
        out.append(("batch", rec))
        for _ in range(rec["n"]):
            msize = int.from_bytes(data[offset:offset + 4], "big")
            member = json.loads(
                data[offset + 4:offset + 4 + msize].decode("utf-8"))
            assert member["op"] in ("bput", "bdelete")
            out.append((member["op"], member))
            offset += 4 + msize
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
        return (self.root / "pages.dat").read_bytes()

    def write(self, blob: bytes) -> None:
        (self.root / "pages.dat").write_bytes(blob)

    def run_cli(self, *argv: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "page_store", "--root", str(self.root), *argv],
            cwd=REPO_ROOT, capture_output=True, text=True)


class BatchValidationTests(BatchTestBase):
    def assert_rejected(self, operations: object) -> None:
        before = self.data
        with self.assertRaises(ValueError):
            self.store.write_batch(operations)  # type: ignore[arg-type]
        # Whole-list validation precedes storage: no frame, no number.
        self.assertEqual(self.data, before)
        self.assertEqual(self.store.stats()["records"], 0)

    def test_non_list_and_empty_list_rejected(self) -> None:
        for bad in (None, "", "x", {}, (), 1, object()):
            self.assert_rejected(bad)
        self.assert_rejected([])

    def test_non_dict_elements_rejected(self) -> None:
        for bad in (None, 1, "put", ["x"], (), object()):
            self.assert_rejected([bad])
        # a dict later in the list still rejects the whole batch
        self.assert_rejected(
            [{"op": "put", "key": "a", "value": "1"}, "x"])

    def test_unknown_op_rejected(self) -> None:
        for op in (None, "", "get", "batch", "bput", "BATCH", 1):
            self.assert_rejected([{"op": op, "key": "k", "value": "v"}])
            self.assert_rejected([{"op": op, "key": "k"}])

    def test_missing_fields_rejected(self) -> None:
        self.assert_rejected([{"op": "put", "key": "k"}])
        self.assert_rejected([{"op": "put", "value": "v"}])
        self.assert_rejected([{"op": "delete"}])
        self.assert_rejected([{"op": "put"}])
        self.assert_rejected([{}])

    def test_extra_fields_rejected(self) -> None:
        self.assert_rejected(
            [{"op": "put", "key": "k", "value": "v", "extra": 1}])
        self.assert_rejected(
            [{"op": "delete", "key": "k", "value": "v"}])
        self.assert_rejected(
            [{"op": "delete", "key": "k", "when": 1}])

    def test_bad_keys_and_values_rejected(self) -> None:
        for key in (None, "", b"k", 1, object()):
            self.assert_rejected([{"op": "put", "key": key, "value": "v"}])
            self.assert_rejected([{"op": "delete", "key": key}])
        for value in (None, b"v", 1, object()):
            self.assert_rejected(
                [{"op": "put", "key": "k", "value": value}])

    def test_oversized_member_rejected_without_consuming_number(self) -> None:
        self.assert_rejected(
            [{"op": "put", "key": "ok", "value": "x" * (PAGE_SIZE + 1)}])
        # an oversized member late in the list rejects the whole list
        good = [{"op": "put", "key": f"k{i}", "value": "v"} for i in range(5)]
        good.append({"op": "put", "key": "big", "value": "x" * (PAGE_SIZE + 1)})
        self.assert_rejected(good)
        self.assertEqual(
            self.store.write_batch(
                [{"op": "put", "key": "only", "value": "1"}]),
            [1])

    def test_a_batch_is_its_only_member_no_marker_frame_before_validation(
            self) -> None:
        self.assertEqual(self.data, b"")


class BatchOrderingTests(BatchTestBase):
    def test_returns_consecutive_numbers_from_record_count_plus_one(self) -> None:
        self.assertEqual(self.store.put("a", "0"), 1)
        seqs = self.store.write_batch([
            {"op": "put", "key": "b", "value": "1"},
            {"op": "delete", "key": "missing"},
            {"op": "put", "key": "b", "value": "2"},
        ])
        self.assertEqual(seqs, [2, 3, 4])
        self.assertEqual(self.store.put("c", "3"), 5)

    def test_members_apply_in_input_order_without_merging(self) -> None:
        seqs = self.store.write_batch([
            {"op": "put", "key": "k", "value": "v1"},
            {"op": "put", "key": "k", "value": "v2"},
            {"op": "delete", "key": "k"},
            {"op": "put", "key": "k", "value": "v3"},
        ])
        self.assertEqual(seqs, [1, 2, 3, 4])
        self.assertEqual(self.store.get("k"), "v3")
        self.assertEqual(self.store.scan(), [("k", "v3")])
        # every member, including the overwritten value and the delete, is on
        # disk and the marker is not counted as a record
        frames = parse_frames(self.data)
        self.assertEqual([op for op, _ in frames],
                         ["batch", "bput", "bput", "bdelete", "bput"])
        self.assertEqual(self.store.stats(),
                         {"pages": 1, "records": 4, "keys": 1})

    def test_delete_of_missing_key_takes_a_number(self) -> None:
        seqs = self.store.write_batch([{"op": "delete", "key": "ghost"}])
        self.assertEqual(seqs, [1])
        self.assertIsNone(self.store.get("ghost"))
        self.assertEqual(self.store.stats(),
                         {"pages": 1, "records": 1, "keys": 0})

    def test_batch_may_span_multiple_pages(self) -> None:
        operations = [
            {"op": "put", "key": f"k{i:04d}", "value": "x" * 100}
            for i in range(200)]
        operations[150] = {"op": "delete", "key": "k0010"}
        seqs = self.store.write_batch(operations)
        self.assertEqual(seqs, list(range(1, 201)))
        self.assertGreaterEqual(self.store.stats()["pages"], 3)
        fresh = PageStore(self.root)
        self.assertEqual(fresh.verify()["status"], "ok")
        self.assertEqual(fresh.stats()["records"], 200)
        self.assertEqual(fresh.stats()["keys"], 198)  # one deleted, one over? no
        self.assertIsNone(fresh.get("k0010"))
        self.assertEqual(fresh.get("k0000"), "x" * 100)

    def test_fresh_and_long_lived_instances_see_after_state_immediately(
            self) -> None:
        other = PageStore(self.root)
        other.put("old", "1")
        self.store.write_batch([
            {"op": "put", "key": "a", "value": "1"},
            {"op": "put", "key": "b", "value": "2"},
            {"op": "delete", "key": "old"},
        ])
        # no recover() needed on either instance
        self.assertEqual(other.get("a"), "1")
        self.assertIsNone(other.get("old"))
        self.assertEqual(other.scan(), [("a", "1"), ("b", "2")])
        self.assertEqual(other.stats()["records"], 4)
        fresh = PageStore(self.root)
        self.assertEqual(fresh.scan(), [("a", "1"), ("b", "2")])

    def test_snapshot_sees_only_before_or_after(self) -> None:
        before = self.store.snapshot()
        self.store.write_batch([
            {"op": "put", "key": "a", "value": "1"},
            {"op": "put", "key": "b", "value": "2"},
        ])
        after = self.store.snapshot()
        self.assertEqual(before.scan(), [])
        self.assertEqual(before.stats()["records"], 0)
        self.assertEqual(after.scan(), [("a", "1"), ("b", "2")])
        self.assertEqual(after.stats()["records"], 2)
        self.assertEqual(before.scan(), [])  # the old snapshot stays frozen


class TornBatchCrashTests(BatchTestBase):
    """A crashed batch writer leaves either the whole batch or none of it."""

    def setUp(self) -> None:
        super().setUp()
        self.store.put("keep", "1")
        self.prefix = self.data

    def append_torn(self, blob: bytes) -> None:
        with (self.root / "pages.dat").open("ab") as handle:
            handle.write(blob)
            handle.flush()
            os.fsync(handle.fileno())

    def test_marker_alone_is_an_uncommitted_tail(self) -> None:
        self.append_torn(marker(3))
        fresh = PageStore(self.root)
        self.assertEqual(fresh.stats()["records"], 1)
        self.assertIsNone(fresh.get("k0"))
        outcome = fresh.recover()
        self.assertEqual(outcome,
                         {"pages": 1, "records": 1, "truncated": True})
        self.assertEqual(self.data, self.prefix)
        self.assertEqual(fresh.put("after", "2"), 2)

    def test_marker_with_some_members_is_discarded_as_a_unit(self) -> None:
        self.append_torn(marker(3) + member_put("k0", "v0"))
        fresh = PageStore(self.root)
        self.assertIsNone(fresh.get("k0"))
        self.assertEqual(fresh.stats()["records"], 1)
        # the next append cuts at the marker: members never linger
        self.assertEqual(fresh.put("after", "2"), 2)
        self.assertEqual(
            self.data,
            self.prefix + encode({"op": "put", "key": "after", "value": "2"}))
        self.assertEqual(PageStore(self.root).verify()["status"], "ok")

    def test_partial_final_member_is_discarded_as_a_unit(self) -> None:
        tail = marker(2) + member_put("k0", "v0") + b"\x00\x00\x00\x30short"
        self.append_torn(tail)
        fresh = PageStore(self.root)
        outcome = fresh.recover()
        self.assertTrue(outcome["truncated"])
        self.assertEqual(outcome["records"], 1)
        self.assertEqual(self.data, self.prefix)
        self.assertIsNone(fresh.get("k0"))

    def test_crash_inside_the_marker_itself_is_a_plain_tail(self) -> None:
        # Only three prefix bytes of the marker landed, yet the writer's
        # member frames happen to follow them byte-for-byte.  Member ops are
        # invalid outside a marker, so this is a half-written tail, not
        # mid-file corruption.
        full = (marker(3) + member_put("k0", "v0")
                + member_put("k1", "v1") + member_put("k2", "v2"))
        self.append_torn(full[:3] + full[len(marker(3)):])
        fresh = PageStore(self.root)
        self.assertEqual(fresh.stats()["records"], 1)
        self.assertEqual(fresh.verify()["status"], "incomplete_tail")
        self.assertEqual(fresh.put("after", "2"), 2)
        self.assertEqual(
            self.data,
            self.prefix + encode({"op": "put", "key": "after", "value": "2"}))

    def test_dead_process_mid_batch_leaves_no_batch_and_takes_no_number(
            self) -> None:
        proc = subprocess.run(
            [sys.executable, "-c", DEAD_BATCH_WORKER, str(self.root)],
            cwd=REPO_ROOT, env={**os.environ, "PYTHONPATH": ENVPATH},
            capture_output=True, text=True, timeout=30)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        fresh = PageStore(self.root)
        self.assertEqual(fresh.verify()["status"], "incomplete_tail")
        self.assertEqual(fresh.stats()["records"], 1)
        self.assertIsNone(fresh.get("k000"))
        # the next batch cleans the residue and starts at record 2
        self.assertEqual(
            fresh.write_batch([{"op": "put", "key": "after", "value": "x"}]),
            [2])
        self.assertEqual(fresh.verify()["status"], "ok")
        self.assertEqual(fresh.get("keep"), "1")
        self.assertEqual(fresh.get("after"), "x")

    def test_reopen_after_full_batch_keeps_the_whole_batch(self) -> None:
        self.store.write_batch([
            {"op": "put", "key": "a", "value": "1"},
            {"op": "put", "key": "b", "value": "2"},
        ])
        reopened = PageStore(self.root)
        self.assertEqual(reopened.scan(),
                         [("a", "1"), ("b", "2"), ("keep", "1")])
        self.assertEqual(reopened.stats()["records"], 3)
        self.assertEqual(reopened.verify()["status"], "ok")
        self.assertEqual(reopened.put("c", "3"), 4)

    def test_failed_write_rolls_back_the_entire_batch(self) -> None:
        before = self.data
        operations = [
            {"op": "put", "key": f"k{i}", "value": "v"} for i in range(10)]
        # The batch's fsync fails; the rollback's own fsync then succeeds.
        with mock.patch("page_store.core.os.fsync",
                        side_effect=[OSError("boom"), None, None]):
            with self.assertRaises(OSError):
                self.store.write_batch(operations)
        self.assertEqual(self.data, before)
        self.assertEqual(self.store.stats()["records"], 1)
        self.assertEqual(self.store.put("after", "2"), 2)
        self.assertEqual(self.store.get("k0"), None)


class CorruptMiddleWithBatchesTests(BatchTestBase):
    GARBAGE = b"\x00\x00\x00\x09broken!!"

    def setUp(self) -> None:
        super().setUp()
        self.store.put("keep", "1")

    def test_corruption_inside_torn_batch_before_plain_record_rejected(
            self) -> None:
        blob = (self.data + marker(5) + member_put("k0", "v0")
                + self.GARBAGE
                + encode({"op": "put", "key": "later", "value": "x"}))
        self.write(blob)
        fresh = PageStore(self.root)
        with self.assertRaises(RuntimeError) as caught:
            fresh.put("z", "1")
        self.assertEqual(str(caught.exception), "corrupt_middle")
        self.assertEqual(self.data, blob)  # byte-for-byte untouched
        with self.assertRaises(RuntimeError):
            fresh.recover()
        self.assertEqual(self.data, blob)
        with self.assertRaises(RuntimeError):
            fresh.compact()
        self.assertEqual(self.data, blob)
        # the committed prefix stays readable
        self.assertEqual(fresh.get("keep"), "1")

    def test_verify_flags_corruption_inside_torn_batch(self) -> None:
        prefix_end = len(self.data)
        blob = (self.data + marker(5) + member_put("k0", "v0")
                + self.GARBAGE
                + encode({"op": "put", "key": "later", "value": "x"}))
        self.write(blob)
        outcome = self.store.verify()
        self.assertEqual(outcome["status"], "corrupt_middle")
        self.assertEqual(outcome["error"], "corrupt_middle")
        self.assertEqual(outcome["first_error_offset"],
                         prefix_end + len(marker(5)) + len(member_put("k0", "v0")))
        self.assertEqual(outcome["complete_records"], 1)  # the earlier keep put
        self.assertEqual(self.data, blob)

    def test_damaged_middle_member_with_members_past_it_is_corruption(
            self) -> None:
        # A contiguous appender cannot leave whole members beyond a broken
        # one: members 1..4 are whole, member 5 is garbage, member 6 whole.
        blob = (self.data + marker(6)
                + member_put("k0", "v0") + member_put("k1", "v1")
                + self.GARBAGE
                + member_put("k5", "v5"))
        self.write(blob)
        fresh = PageStore(self.root)
        with self.assertRaises(RuntimeError) as caught:
            fresh.write_batch([{"op": "put", "key": "z", "value": "1"}])
        self.assertEqual(str(caught.exception), "corrupt_middle")
        self.assertEqual(self.data, blob)
        with self.assertRaises(RuntimeError):
            fresh.recover()
        self.assertEqual(self.data, blob)
        outcome = fresh.verify()
        self.assertEqual(outcome["status"], "corrupt_middle")
        self.assertEqual(
            outcome["first_error_offset"],
            len(self.data) - len(self.GARBAGE) - len(member_put("k5", "v5")))
        self.assertEqual(outcome["complete_records"], 1)  # the keep put only

    def test_corruption_after_a_complete_batch_leaves_the_batch_committed(
            self) -> None:
        self.store.write_batch([
            {"op": "put", "key": "a", "value": "1"},
            {"op": "delete", "key": "keep"},
        ])
        prefix_end = len(self.data)
        blob = self.data + self.GARBAGE + encode(
            {"op": "put", "key": "later", "value": "x"})
        self.write(blob)
        fresh = PageStore(self.root)
        self.assertEqual(fresh.get("a"), "1")
        self.assertIsNone(fresh.get("keep"))
        with self.assertRaises(RuntimeError):
            fresh.put("z", "1")
        outcome = fresh.verify()
        self.assertEqual(outcome["status"], "corrupt_middle")
        self.assertEqual(outcome["first_error_offset"], prefix_end)
        self.assertEqual(outcome["complete_records"], 3)  # keep + 2 members


class VerifyRecoverCompactBatchTests(BatchTestBase):
    def test_verify_counts_members_not_marker(self) -> None:
        self.store.write_batch([
            {"op": "put", "key": "a", "value": "1"},
            {"op": "put", "key": "b", "value": "2"},
            {"op": "delete", "key": "a"},
        ])
        outcome = self.store.verify()
        self.assertEqual(outcome["status"], "ok")
        self.assertEqual(outcome["complete_records"], 3)
        self.assertEqual(outcome["first_error_offset"], None)
        self.assertEqual(outcome["tail_partial_bytes"], 0)

    def test_verify_torn_batch_is_incomplete_tail_from_marker(self) -> None:
        blob = self.data + marker(4) + member_put("a", "1")
        self.write(blob)
        outcome = self.store.verify()
        self.assertEqual(outcome["status"], "incomplete_tail")
        self.assertEqual(outcome["complete_records"], 0)
        self.assertEqual(outcome["tail_partial_bytes"],
                         len(marker(4)) + len(member_put("a", "1")))
        # verify stays read-only
        self.assertEqual(self.data, blob)

    def test_compact_rewrites_committed_batches_as_ascending_puts(self) -> None:
        self.store.write_batch([
            {"op": "put", "key": "b", "value": "2"},
            {"op": "put", "key": "a", "value": "1"},
            {"op": "delete", "key": "b"},
        ])
        self.store.put("c", "3")
        torn = marker(2) + member_put("z", "9")
        self.write(self.data + torn)
        result = self.store.compact()
        self.assertEqual(result["records_before"], 4)
        self.assertEqual(result["records_after"], 2)
        self.assertEqual(result["keys"], 2)
        self.assertEqual(result["discarded_tail_bytes"], len(torn))
        self.assertEqual(parse_frames(self.data),
                         [("put", {"op": "put", "key": "a", "value": "1"}),
                          ("put", {"op": "put", "key": "c", "value": "3"})])
        # post-compaction numbering starts at the compacted record count
        self.assertEqual(
            self.store.write_batch([{"op": "put", "key": "d", "value": "4"}]),
            [3])

    def test_recover_then_more_batches_number_densely(self) -> None:
        self.store.write_batch([
            {"op": "put", "key": "a", "value": "1"},
            {"op": "put", "key": "b", "value": "2"},
        ])
        self.write(self.data + marker(3) + member_put("x", "0"))
        outcome = self.store.recover()
        self.assertTrue(outcome["truncated"])
        self.assertEqual(outcome["records"], 2)
        self.assertEqual(
            self.store.write_batch([
                {"op": "delete", "key": "b"},
                {"op": "put", "key": "c", "value": "3"},
            ]),
            [3, 4])
        self.assertEqual(self.store.scan(), [("a", "1"), ("c", "3")])


class BatchConcurrencyTests(BatchTestBase):
    def spawn_worker(self, wid: int, batches: int) -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, "-c", WORKER, str(self.root),
             str(wid), str(batches)],
            cwd=REPO_ROOT, env={**os.environ, "PYTHONPATH": ENVPATH},
            stdout=subprocess.PIPE, text=True)

    def test_batches_from_many_processes_share_one_dense_sequence(self) -> None:
        n_workers, batches = 5, 7
        members_per_batch = 4
        procs = [self.spawn_worker(w, batches) for w in range(n_workers)]
        all_seqs: list[int] = []
        for proc in procs:
            out, err = proc.communicate(timeout=60)
            self.assertEqual(proc.returncode, 0, err)
            seqs = json.loads(out)
            self.assertEqual(sorted(seqs), seqs)
            all_seqs.extend(seqs)
        total = n_workers * batches * members_per_batch
        self.assertEqual(sorted(all_seqs), list(range(1, total + 1)))
        # the file is a complete frame prefix: every marker fully populated
        frames = parse_frames(self.data)
        members = [op for op, _ in frames if op in ("bput", "bdelete")]
        self.assertEqual(len(members), total)
        fresh = PageStore(self.root)
        self.assertEqual(fresh.verify()["status"], "ok")
        self.assertEqual(fresh.stats()["records"], total)

    def test_threaded_batches_and_singles_keep_a_dense_serial_order(self) -> None:
        n_threads, per_thread = 4, 25
        barrier = threading.Barrier(n_threads + 1)
        errors: list[BaseException] = []

        def worker(tid: int) -> None:
            local = PageStore(self.root)
            barrier.wait()
            try:
                for i in range(per_thread):
                    if i % 2 == 0:
                        local.write_batch([
                            {"op": "put", "key": f"t{tid}-{i:03d}-a",
                             "value": f"{tid}:{i}:a"},
                            {"op": "delete", "key": f"t{tid}-{i:03d}-a"},
                            {"op": "put", "key": f"t{tid}-{i:03d}-b",
                             "value": f"{tid}:{i}:b"},
                        ])
                    else:
                        local.put(f"t{tid}-{i:03d}-s", f"{tid}:{i}")
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = [threading.Thread(target=worker, args=(t,))
                   for t in range(n_threads)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(errors, [])
        members_per_round = 3 * (per_thread // 2 + per_thread % 2) \
            + per_thread // 2  # 3 members per even round, one put per odd
        total = n_threads * members_per_round
        frames = parse_frames(self.data)
        self.assertEqual(
            self.store.verify()["status"], "ok")
        self.assertEqual(self.store.stats()["records"], total)
        self.assertEqual(len([o for o, _ in frames
                              if o in ("put", "bput", "bdelete")]),
                         total)
        self.assertEqual(self.store.put("after", "z"), total + 1)

    def test_concurrent_snapshots_see_whole_batches_only(self) -> None:
        n_batches, width = 30, 5
        stop = threading.Event()
        errors: list[BaseException] = []

        def writer() -> None:
            local = PageStore(self.root)
            for j in range(n_batches):
                local.write_batch([
                    {"op": "put", "key": f"b{j:03d}-k{i:02d}",
                     "value": f"{j}:{i}"} for i in range(width)])

        def reader() -> None:
            local = PageStore(self.root)
            try:
                while not stop.is_set():
                    snap = local.snapshot()
                    for j in range(n_batches):
                        present = sum(
                            1 for i in range(width)
                            if snap.get(f"b{j:03d}-k{i:02d}") is not None)
                        self.assertIn(present, (0, width))
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        readers = [threading.Thread(target=reader) for _ in range(3)]
        for thread in readers:
            thread.start()
        writer()
        time.sleep(0.05)
        stop.set()
        for thread in readers:
            thread.join(timeout=30)
        self.assertEqual(errors, [])
        self.assertEqual(len(self.store.scan()), n_batches * width)


class CliCompatibilityTests(BatchTestBase):
    def test_no_batch_subcommand_and_existing_outputs_unchanged(self) -> None:
        self.store.write_batch([
            {"op": "put", "key": "a", "value": "1"},
            {"op": "delete", "key": "a"},
        ])
        stats = self.run_cli("stats")
        self.assertEqual(stats.returncode, 0)
        self.assertEqual(json.loads(stats.stdout),
                         {"keys": 0, "pages": 1, "records": 2})
        self.assertEqual(self.run_cli("scan").returncode, 0)
        verify = self.run_cli("verify")
        self.assertEqual(json.loads(verify.stdout)["status"], "ok")
        bad = self.run_cli("write_batch")
        self.assertEqual(bad.returncode, 2)
        report = json.loads(self.run_cli("report").stdout)
        self.assertEqual(
            report["readiness"],
            {"append": True, "recover": True, "compaction": True,
             "snapshotRead": False})


if __name__ == "__main__":
    unittest.main()
