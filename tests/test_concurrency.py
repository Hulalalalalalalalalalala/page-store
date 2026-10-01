"""Tests for multi-instance / multi-process concurrency on a shared root.

The coordination happens through a lock file next to pages.dat, so these
tests exercise real separate processes as well as several PageStore
instances and threads inside one process.
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

WORKER = r"""
import json, os, sys
from page_store.core import PageStore

root, wid, count = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
store = PageStore(root)
seqs = []
for i in range(count):
    seqs.append(store.put(f"w{wid:02d}-k{i:04d}", f"v{wid:02d}-{i:04d}"))
    seqs.append(store.put("shared", f"{wid:02d}:{i:04d}"))
print(json.dumps(seqs))
"""

TAIL_WORKER = r"""
import json, os, sys
sys.path.insert(0, os.environ["PYTHONPATH"])
from page_store.core import PageStore
root = sys.argv[1]
store = PageStore(root)
store.put("a", "1")
frame = store._encode({"op": "put", "key": "b", "value": "2"})
with open(os.path.join(root, "pages.dat"), "ab") as handle:
    handle.write(frame[:len(frame) - 3])
    handle.flush()
    os.fsync(handle.fileno())
os._exit(0)
"""


def encode(record: dict) -> bytes:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


def frames(data: bytes) -> list[dict]:
    out, offset = [], 0
    while offset + 4 <= len(data):
        size = int.from_bytes(data[offset:offset + 4], "big")
        out.append(json.loads(data[offset + 4:offset + 4 + size].decode("utf-8")))
        offset += 4 + size
    assert offset == len(data), "tail bytes left over"
    return out


class ConcurrencyTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.store = PageStore(self.root)
        self.store.init()

    def spawn_worker(self, wid: int, count: int) -> subprocess.Popen:
        return subprocess.Popen(
            [sys.executable, "-c", WORKER, str(self.root), str(wid), str(count)],
            cwd=REPO_ROOT, env={**os.environ, "PYTHONPATH": ENVPATH},
            stdout=subprocess.PIPE, text=True)

    @property
    def data(self) -> bytes:
        return (self.root / "pages.dat").read_bytes()


class MultiProcessAppendTests(ConcurrencyTestBase):
    def test_sequence_numbers_dense_and_unique_across_processes(self) -> None:
        n_workers, per_worker = 6, 25
        calls_per_worker = per_worker * 2  # one distinct key + one shared key
        procs = [self.spawn_worker(w, per_worker) for w in range(n_workers)]
        all_seqs: list[int] = []
        for proc in procs:
            out, err = proc.communicate(timeout=60)
            self.assertEqual(proc.returncode, 0, err)
            seqs = json.loads(out)
            self.assertEqual(len(seqs), calls_per_worker)
            self.assertEqual(sorted(seqs), list(seqs))  # per-process ascending
            all_seqs.extend(seqs)
        # Every successful call got a distinct, dense number from the running
        # record count: nothing skipped, nothing repeated, no failed call took
        # a slot.  Interleaving across processes is expected, so compare the
        # union rather than per-process values.
        total = n_workers * calls_per_worker
        self.assertEqual(sorted(all_seqs), list(range(1, total + 1)))
        # Shared-key puts happened too; the file is a complete prefix of frames.
        parsed = frames(self.data)
        self.assertEqual(len(parsed), total)
        live: dict[str, str] = {}
        for rec in parsed:
            if rec["op"] == "put":
                live[rec["key"]] = rec["value"]
            else:
                live.pop(rec["key"], None)
        # No distinct-key change was lost.
        for w in range(n_workers):
            for i in range(per_worker):
                self.assertEqual(live[f"w{w:02d}-k{i:04d}"], f"v{w:02d}-{i:04d}")
        self.assertRegex(live["shared"], r"^\d{2}:\d{4}$")
        # A fresh instance agrees and stats are internally consistent.
        fresh = PageStore(self.root)
        self.assertEqual(fresh.stats()["records"], len(parsed))
        self.assertEqual(len(fresh.scan()), n_workers * per_worker + 1)
        self.assertEqual(fresh.verify()["status"], "ok")

    def test_cli_puts_from_many_processes_share_the_sequence(self) -> None:
        n = 12
        procs = [subprocess.Popen(
            [sys.executable, "-m", "page_store", "--root", str(self.root),
             "put", f"cli-{i:02d}", f"val-{i}"],
            cwd=REPO_ROOT, stdout=subprocess.PIPE, text=True) for i in range(n)]
        numbers = []
        for proc in procs:
            out, _ = proc.communicate(timeout=30)
            self.assertEqual(proc.returncode, 0)
            numbers.append(int(out.strip()))
        self.assertEqual(sorted(numbers), list(range(1, n + 1)))
        self.assertEqual({k for k, _ in self.store.scan()},
                         {f"cli-{i:02d}" for i in range(n)})


class MultipleInstancesTests(ConcurrencyTestBase):
    def test_two_instances_share_one_ordering(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        self.assertEqual(a.put("a", "1"), 1)
        self.assertEqual(b.put("b", "2"), 2)
        self.assertEqual(a.put("a", "3"), 3)
        self.assertEqual(b.delete("b"), 4)
        self.assertEqual(a.get("a"), "3")
        self.assertIsNone(b.get("b"))
        self.assertEqual(b.scan(), [("a", "3")])
        self.assertEqual(a.stats(), {"pages": 1, "records": 4, "keys": 1})
        self.assertEqual(b.stats(), {"pages": 1, "records": 4, "keys": 1})

    def test_sequence_continues_after_compact_seen_from_other_instance(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        a.put("a", "1")
        b.put("x", "0")
        a.put("a", "2")
        b.delete("x")
        result = a.compact()  # one live key -> one record
        self.assertEqual(result["records_after"], 1)
        # b must observe the atomic swap (a new inode) and renumber from 1.
        self.assertEqual(b.put("b", "3"), 2)
        self.assertEqual(a.put("c", "4"), 3)
        self.assertEqual(b.delete("a"), 4)
        self.assertEqual(b.scan(), [("b", "3"), ("c", "4")])
        self.assertEqual(a.stats()["records"], 4)

    def test_recover_in_one_instance_is_seen_by_the_other(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        a.put("a", "1")
        tail = encode({"op": "put", "key": "b", "value": "2"})
        (self.root / "pages.dat").write_bytes(self.data + tail[:len(tail) - 2])
        outcome = b.recover()
        self.assertTrue(outcome["truncated"])
        self.assertEqual(outcome["records"], 1)
        # a resyncs from the same (truncated) inode before appending.
        self.assertEqual(a.put("c", "3"), 2)
        self.assertEqual(b.scan(), [("a", "1"), ("c", "3")])

    def test_put_after_middle_corruption_preserves_file(self) -> None:
        # A damaged region with a complete record past it must not be
        # overwritten or skipped by an append: corrupt_middle, file untouched.
        blob = (encode({"op": "put", "key": "a", "value": "1"})
                + b"\x00\x00\x00\x09broken"
                + encode({"op": "put", "key": "b", "value": "2"}))
        (self.root / "pages.dat").write_bytes(blob)
        fresh = PageStore(self.root)
        with self.assertRaises(RuntimeError) as caught:
            fresh.put("c", "3")
        self.assertEqual(str(caught.exception), "corrupt_middle")
        self.assertEqual(self.data, blob)
        # read paths behave exactly as before too
        self.assertEqual(fresh.get("a"), "1")  # prefix still readable
        with self.assertRaises(RuntimeError):
            fresh.compact()
        self.assertEqual(self.data, blob)

    def test_half_written_tail_from_dead_process_is_overwritten(self) -> None:
        proc = subprocess.run(
            [sys.executable, "-c", TAIL_WORKER, str(self.root)],
            cwd=REPO_ROOT, env={**os.environ, "PYTHONPATH": ENVPATH},
            capture_output=True, text=True, timeout=30)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        other = PageStore(self.root)
        # The dead writer's unconfirmed frame takes no number and is invisible.
        self.assertEqual(other.put("c", "3"), 2)
        parsed = frames(self.data)
        self.assertEqual(parsed,
                         [{"op": "put", "key": "a", "value": "1"},
                          {"op": "put", "key": "c", "value": "3"}])
        self.assertEqual(other.verify()["status"], "ok")


class ThreadedSerialPointTests(ConcurrencyTestBase):
    def test_writers_and_compactor_interleave_without_losing_records(self) -> None:
        n_threads, per_thread = 4, 40
        barrier = threading.Barrier(n_threads + 1)

        def writer(tid: int) -> None:
            local = PageStore(self.root)
            barrier.wait()
            for i in range(per_thread):
                seq = local.put(f"t{tid}-k{i:03d}", f"{tid}:{i}")
                self.assertIsInstance(seq, int)
                local.put(f"t{tid}-live", f"{tid}:{i}")  # overwritten each round

        threads = [threading.Thread(target=writer, args=(t,))
                   for t in range(n_threads)]
        for thread in threads:
            thread.start()
        compactor = PageStore(self.root)
        barrier.wait()
        for _ in range(25):
            time.sleep(0.002)
            compactor.compact()
        for thread in threads:
            thread.join(timeout=30)
        # The file is always a complete prefix: no torn frames, and a final
        # compaction converges on exactly the surviving keys.
        parsed = frames(self.data)
        self.store.compact()
        parsed = frames(self.data)
        self.assertEqual(len(parsed), n_threads * (per_thread + 1))
        expected = {f"t{t}-k{i:03d}": f"{t}:{i}"
                    for t in range(n_threads) for i in range(per_thread)}
        expected.update({f"t{t}-live": f"{t}:{per_thread - 1}"
                         for t in range(n_threads)})
        self.assertEqual(dict(self.store.scan()), expected)
        # Numbering continues from the post-compaction record count.
        self.assertEqual(self.store.put("after", "z"), len(parsed) + 1)

    def test_concurrent_readers_always_see_complete_states(self) -> None:
        stop = threading.Event()
        writers = [threading.Thread(
            target=lambda t=t: self._write_loop(t, stop)) for t in range(3)]
        errors: list[BaseException] = []

        def reader() -> None:
            local = PageStore(self.root)
            try:
                while not stop.is_set():
                    live = dict(local.scan())
                    snap = local.snapshot()
                    # Each visible key must carry one of the values that some
                    # writer actually committed -- never a mixed/torn state.
                    for key, value in live.items():
                        tid, i = value.split("-")
                        self.assertEqual(key, f"r{tid}-k{int(i):03d}")
                    # snapshot is a complete point-in-time state too
                    self.assertEqual(sorted(snap.scan()), snap.scan())
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        readers = [threading.Thread(target=reader) for _ in range(3)]
        for thread in writers + readers:
            thread.start()
        time.sleep(0.4)
        stop.set()
        for thread in writers + readers:
            thread.join(timeout=30)
        self.assertEqual(errors, [])

    def _write_loop(self, tid: int, stop: threading.Event) -> None:
        local = PageStore(self.root)
        i = 0
        while not stop.is_set():
            local.put(f"r{tid}-k{i:03d}", f"{tid}-{i:03d}")
            i += 1

    def test_recover_serialisable_with_concurrent_appends(self) -> None:
        stop = threading.Event()
        errors: list[BaseException] = []

        def writer() -> None:
            local = PageStore(self.root)
            try:
                i = 0
                while not stop.is_set():
                    local.put(f"g-k{i:05d}", str(i))
                    i += 1
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        def healer() -> None:
            local = PageStore(self.root)
            try:
                while not stop.is_set():
                    outcome = local.recover()
                    # With no crashed writers there is never a tail to cut.
                    self.assertFalse(outcome["truncated"])
                    time.sleep(0.001)
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        pool = [threading.Thread(target=writer) for _ in range(3)]
        pool.append(threading.Thread(target=healer))
        for thread in pool:
            thread.start()
        time.sleep(0.4)
        stop.set()
        for thread in pool:
            thread.join(timeout=30)
        self.assertEqual(errors, [])
        self.assertEqual(self.store.verify()["status"], "ok")


class SnapshotAcrossInstancesTests(ConcurrencyTestBase):
    def test_snapshot_frozen_against_other_instance_process_and_compact(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        a.put("a", "1")
        a.put("b", "2")
        snap = a.snapshot()
        b.put("c", "3")
        a.put("a", "updated")
        b.delete("b")
        proc = subprocess.run(
            [sys.executable, "-m", "page_store", "--root", str(self.root),
             "put", "d", "4"],
            cwd=REPO_ROOT, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        a.compact()
        b.recover()
        self.assertEqual(snap.scan(), [("a", "1"), ("b", "2")])
        self.assertEqual(snap.stats(), {"pages": 1, "records": 2, "keys": 2})
        # the live store moved on
        self.assertEqual(a.scan(),
                         [("a", "updated"), ("c", "3"), ("d", "4")])


class ReinitVisibilityTests(ConcurrencyTestBase):
    """A long-lived instance must observe another instance's init() reset.

    init truncates pages.dat in place (same inode), so once the rewritten
    file reaches the old length a stale cache can no longer be told apart
    by size or inode alone.
    """

    def test_stale_instance_sees_reset_when_rewrite_reaches_old_length(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        a.put("old", "1")
        a.put("old2", "2")
        self.assertEqual(a.get("old"), "1")  # populate a's cache
        length_before = len(self.data)
        b.init()
        # Rewrite enough that pages.dat is at least as long as before the
        # reset: size alone cannot reveal the truncation.
        seq = 1
        while len(self.data) < length_before:
            self.assertEqual(b.put(f"new{seq}", f"v{seq}"), seq)
            seq += 1
        self.assertIsNone(a.get("old"))
        self.assertIsNone(a.get("old2"))
        self.assertEqual(a.get("new1"), "v1")
        fresh = PageStore(self.root)
        self.assertEqual(a.scan(), fresh.scan())
        self.assertEqual(a.stats(), fresh.stats())
        self.assertEqual(a.snapshot().scan(), fresh.scan())

    def test_repeated_resets_are_all_observed(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        for round_no in range(4):
            b.init()
            for i in range(round_no + 2):
                self.assertEqual(b.put(f"r{round_no}-k{i}", f"{round_no}:{i}"), i + 1)
            expected = [(f"r{round_no}-k{i}", f"{round_no}:{i}")
                        for i in range(round_no + 2)]
            self.assertEqual(a.scan(), expected)
            self.assertEqual(a.stats()["records"], round_no + 2)
            self.assertEqual(a.stats()["keys"], round_no + 2)
            # a's own writes number from the current record count: no gaps,
            # no skipped numbers, no revived keys from before the reset.
            self.assertEqual(a.put(f"r{round_no}-a", "x"), round_no + 3)
            self.assertEqual(b.get(f"r{round_no}-a"), "x")

    def test_reset_to_empty_then_first_write_numbers_from_one(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        for i in range(5):
            a.put(f"k{i}", "v")
        a.stats()  # cache the non-empty state
        b.init()
        # Before any new write the store reads as empty for the stale instance.
        self.assertEqual(a.stats(), {"pages": 0, "records": 0, "keys": 0})
        self.assertEqual(a.scan(), [])
        self.assertIsNone(a.get("k0"))
        self.assertEqual(a.put("first", "1"), 1)
        self.assertEqual(b.put("second", "2"), 2)
        self.assertEqual(a.delete("first"), 3)
        self.assertEqual(a.scan(), [("second", "2")])
        self.assertEqual(b.stats(), {"pages": 1, "records": 3, "keys": 1})

    def test_snapshot_from_before_reset_is_frozen_new_snapshot_reflects_reset(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        a.put("old", "1")
        before = a.snapshot()
        b.init()
        b.put("new", "2")
        self.assertEqual(before.scan(), [("old", "1")])
        self.assertEqual(before.stats(), {"pages": 1, "records": 1, "keys": 1})
        after = a.snapshot()
        self.assertEqual(after.scan(), [("new", "2")])
        self.assertEqual(after.stats(), {"pages": 1, "records": 1, "keys": 1})

    def test_reset_by_separate_process_is_observed(self) -> None:
        a = PageStore(self.root)
        a.put("old", "1")
        a.put("old2", "2")
        a.get("old")  # populate the cache
        for command in (["init"], ["put", "new", "3"], ["put", "new2", "4"]):
            proc = subprocess.run(
                [sys.executable, "-m", "page_store", "--root", str(self.root),
                 *command],
                cwd=REPO_ROOT, capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIsNone(a.get("old"))
        self.assertEqual(a.get("new"), "3")
        self.assertEqual(a.scan(), [("new", "3"), ("new2", "4")])
        self.assertEqual(a.stats()["records"], 2)
        # Appends from the stale instance continue the current numbering.
        self.assertEqual(a.put("third", "5"), 3)

    def test_compact_after_reset_seen_by_stale_instance(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        a.put("old", "1")
        a.stats()
        b.init()
        b.put("x", "1")
        b.put("x", "2")
        b.delete("x")
        b.put("y", "3")
        result = a.compact()
        self.assertEqual(result["records_before"], 4)
        self.assertEqual(result["records_after"], 1)
        self.assertEqual(a.scan(), [("y", "3")])
        self.assertEqual(b.put("z", "4"), 2)
        self.assertEqual(a.scan(), [("y", "3"), ("z", "4")])


class ValidationTests(ConcurrencyTestBase):
    def test_put_and_delete_reject_bad_keys_and_values(self) -> None:
        for bad in (None, 1, b"k", "", object()):
            with self.assertRaises(ValueError):
                self.store.put(bad, "v")  # type: ignore[arg-type]
            with self.assertRaises(ValueError):
                self.store.delete(bad)  # type: ignore[arg-type]
        for bad in (None, 1, b"v", object()):
            with self.assertRaises(ValueError):
                self.store.put("k", bad)  # type: ignore[arg-type]
        # rejected calls consume no sequence number
        self.assertEqual(self.store.put("ok", "1"), 1)

    def test_oversized_record_still_value_error_and_no_number(self) -> None:
        with self.assertRaises(ValueError):
            self.store.put("big", "x" * (PAGE_SIZE + 1))
        self.assertEqual(self.store.put("ok", "1"), 1)


class CoordinationFileTests(ConcurrencyTestBase):
    def test_missing_store_raises_without_creating_lock_file(self) -> None:
        missing = self.root / "nope"
        bare = self.root / "bare"
        bare.mkdir()
        a_file = self.root / "a-file"
        a_file.write_bytes(b"")
        with self.assertRaises(FileNotFoundError):
            PageStore(missing).get("x")
        with self.assertRaises(FileNotFoundError):
            PageStore(bare).put("x", "y")
        with self.assertRaises(FileNotFoundError):
            PageStore(a_file).scan()
        self.assertFalse(missing.exists())
        self.assertFalse((bare / ".pages.dat.lock").exists())
        self.assertFalse((a_file / ".pages.dat.lock").exists())

    def test_lock_file_is_coordination_not_data(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        lock = self.root / ".pages.dat.lock"
        self.assertTrue(lock.exists())
        # junk inside the coordination file is ignored
        lock.write_bytes(b"not a record")
        self.assertEqual(self.store.stats(), {"pages": 1, "records": 2, "keys": 2})
        result = self.store.compact()
        self.assertEqual(result["records_before"], 2)
        self.assertEqual(result["records_after"], 2)
        self.assertEqual(self.store.verify()["status"], "ok")
        # deleting it is transparent: it is recreated as needed
        lock.unlink()
        self.assertEqual(self.store.put("c", "3"), 3)
        self.assertTrue(lock.exists())


if __name__ == "__main__":
    unittest.main()
