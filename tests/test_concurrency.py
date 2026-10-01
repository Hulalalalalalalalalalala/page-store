"""Tests for multi-instance and multi-process concurrent PageStore access.

Concurrency contract exercised here:

* concurrent puts/deletes are equivalent to some serial order;
* successful writes return sequence numbers that start at one and grow
  strictly from the current record count -- unique, no gaps, never reused;
* distinct-key changes are never lost, same-key reads see the last change;
* compact/recover share the writers' mutual-exclusion order, compaction
  atomically swaps one serialisation-point state and numbering continues
  from the post-compact record count;
* readers always see a whole before/after state, and a snapshot never moves;
* the coordination file holds no records and is invisible to stats/verify.
"""

from __future__ import annotations

import json
import multiprocessing
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from page_store.core import LOCK_FILE, PAGE_SIZE, PageStore  # noqa: E402


def encode(record: dict) -> bytes:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


def put_record(key: str, value: str) -> bytes:
    return encode({"op": "put", "key": key, "value": value})


def frames(data: bytes) -> list[dict]:
    out, offset = [], 0
    while offset < len(data):
        size = int.from_bytes(data[offset:offset + 4], "big")
        out.append(json.loads(data[offset + 4:offset + 4 + size].decode("utf-8")))
        offset += 4 + size
    return out


def fold(records: list[dict]) -> dict[str, str]:
    live: dict[str, str] = {}
    for record in records:
        if record["op"] == "put":
            live[record["key"]] = record["value"]
        else:
            live.pop(record["key"], None)
    return live


# ---------------------------------------------------------------------------
# Top-level worker functions (each child process builds its own PageStore).
# ---------------------------------------------------------------------------

def worker_unique_puts(root: str, wid: int, n: int, outq) -> None:
    store = PageStore(root)
    seqs = []
    for i in range(n):
        seqs.append(store.put(f"w{wid:02d}-k{i:04d}", f"v{wid}-{i}"))
    outq.put((wid, seqs))


def worker_mixed_ops(root: str, wid: int, n: int, outq) -> None:
    store = PageStore(root)
    seqs, values = [], set()
    for i in range(n):
        value = f"w{wid}-{i}"
        seqs.append(store.put("shared", value))
        values.add(value)
        if wid == 0 and i % 5 == 0:
            seqs.append(store.delete(f"priv-{i}"))  # deletes of absent keys still number
    outq.put((wid, seqs, values))


def worker_phased(root: str, wid: int, n: int, barrier, outq) -> None:
    store = PageStore(root)
    seqs_a = [store.put(f"a-w{wid:02d}-k{i:04d}", f"{wid}:{i}") for i in range(n)]
    barrier.wait()  # phase A finished
    barrier.wait()  # compaction finished
    seqs_c = [store.put(f"c-w{wid:02d}-k{i:04d}", f"{wid}:{i}") for i in range(n)]
    outq.put((wid, seqs_a, seqs_c))


def compactor_phased(root: str, barrier) -> None:
    store = PageStore(root)
    barrier.wait()  # writers finished phase A
    store.compact()
    barrier.wait()  # release writers into phase C


def compactor_loop(root: str, event) -> None:
    store = PageStore(root)
    while not event.is_set():
        store.compact()


def recoverer_loop(root: str, event, outq) -> None:
    store = PageStore(root)
    runs = 0
    while not event.is_set():
        store.recover()
        runs += 1
    outq.put(runs)


def reader_loop(root: str, event, errors) -> None:
    store = PageStore(root)
    last_keys = 0
    while not event.is_set():
        # writers only add distinct keys and compact keeps every live key, so
        # the key count can never shrink; each individual call must also be
        # internally consistent (records >= keys).  Two separate calls are
        # allowed to straddle a serial point, so they are never compared.
        try:
            stats = store.stats()
            assert stats["records"] >= stats["keys"]
            assert stats["keys"] >= last_keys
            last_keys = stats["keys"]
            snap = store.snapshot()  # one capture: its views always agree
            snap_stats = snap.stats()
            assert len(snap.scan()) == snap_stats["keys"] == snap_stats["keys"]
            assert snap_stats["records"] >= snap_stats["keys"]
        except BaseException as exc:  # report any violation back to the parent
            errors.put(repr(exc))
            return


# ---------------------------------------------------------------------------


class ConcurrencyTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.store = PageStore(self.root)
        self.store.init()
        self.ctx = multiprocessing.get_context("fork")

    @property
    def data(self) -> bytes:
        return (self.root / "pages.dat").read_bytes()

    def write(self, blob: bytes) -> None:
        (self.root / "pages.dat").write_bytes(blob)

    def run_cli(self, *argv: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "page_store", "--root", str(self.root), *argv],
            cwd=REPO_ROOT, capture_output=True, text=True)


class MultiProcessAppendTests(ConcurrencyTestBase):
    WORKERS = 6
    PER_WORKER = 25

    def test_sequence_numbers_unique_dense_and_all_changes_survive(self) -> None:
        outq = self.ctx.Queue()
        procs = [self.ctx.Process(target=worker_unique_puts,
                                  args=(str(self.root), wid, self.PER_WORKER, outq))
                 for wid in range(self.WORKERS)]
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join()
        for proc in procs:
            self.assertEqual(proc.exitcode, 0)

        total = self.WORKERS * self.PER_WORKER
        per_worker = {}
        for _ in procs:
            wid, seqs = outq.get()
            self.assertEqual(seqs, sorted(seqs))  # strictly increasing per writer
            per_worker[wid] = seqs
        # the concurrent result equals one serial append order: every serial
        # position 1..total was handed out exactly once
        self.assertEqual(sorted(sum(per_worker.values(), [])),
                         list(range(1, total + 1)))

        records = frames(self.data)
        self.assertEqual(len(records), total)  # no torn tail after confirmed writes
        self.assertEqual(self.store.verify()["status"], "ok")
        live = fold(records)
        self.assertEqual(len(live), total)  # distinct keys: nothing lost
        self.assertEqual(dict(self.store.scan()), live)
        self.assertEqual(self.store.stats(),
                         {"pages": (len(self.data) + PAGE_SIZE - 1) // PAGE_SIZE,
                          "records": total, "keys": total})

    def test_mixed_puts_and_deletes_equivalent_to_serial_order(self) -> None:
        outq = self.ctx.Queue()
        procs = [self.ctx.Process(target=worker_mixed_ops,
                                  args=(str(self.root), wid, 20, outq))
                 for wid in range(4)]
        for proc in procs:
            proc.start()
        all_values = set()
        seqs = []
        for _ in procs:
            wid, worker_seqs, values = outq.get()
            seqs.extend(worker_seqs)
            all_values |= values
        for proc in procs:
            proc.join()
            self.assertEqual(proc.exitcode, 0)

        total_ops = 4 * 20 + 4  # four absent-key deletes from worker 0
        self.assertEqual(sorted(seqs), list(range(1, total_ops + 1)))
        records = frames(self.data)
        self.assertEqual(len(records), total_ops)
        live = fold(records)
        # last change wins on the shared key, and it is a value some write had
        self.assertEqual(self.store.get("shared"), live["shared"])
        self.assertIn(live["shared"], all_values)
        # the absent-key deletes leave no private keys behind
        self.assertFalse(any(k.startswith("priv-") for k in live))
        self.assertEqual(dict(self.store.scan()), live)

    def test_failed_writes_consume_no_sequence_numbers(self) -> None:
        # oversized values are rejected before touching the serial point
        with self.assertRaises(ValueError):
            self.store.put("too-big", "x" * (PAGE_SIZE + 100))
        with self.assertRaises(ValueError):
            self.store.put("", "v")

        procs = []
        outq = self.ctx.Queue()

        def bad_then_good(root: str, outq) -> None:
            store = PageStore(root)
            try:
                store.put(123, "v")  # type: ignore[arg-type]
            except ValueError:
                pass
            outq.put(store.put("good", "v"))

        proc = self.ctx.Process(target=bad_then_good, args=(str(self.root), outq))
        proc.start()
        proc.join()
        self.assertEqual(proc.exitcode, 0)
        self.assertEqual(outq.get(), 1)  # rejected writes neither numbered nor showed up
        self.assertEqual(self.store.scan(), [("good", "v")])


class SameProcessInstanceTests(ConcurrencyTestBase):
    def test_two_instances_and_threads_share_one_order(self) -> None:
        other = PageStore(self.root)  # same directory, second instance

        results: list[list[int]] = [[], []]

        def writer(store: PageStore, idx: int, prefix: str) -> None:
            for i in range(60):
                results[idx].append(store.put(f"{prefix}-{i:03d}", "v"))

        t1 = threading.Thread(target=writer, args=(self.store, 0, "a"))
        t2 = threading.Thread(target=writer, args=(other, 1, "b"))
        t1.start(); t2.start(); t1.join(); t2.join()

        self.assertEqual(sorted(results[0] + results[1]), list(range(1, 121)))
        self.assertEqual(len(self.store.scan()), 120)
        self.assertEqual(len(other.scan()), 120)
        stats = self.store.stats()
        self.assertEqual(stats["records"], 120)
        self.assertEqual(stats["keys"], 120)
        self.assertTrue((self.root / LOCK_FILE).exists())

    def test_validation_types(self) -> None:
        for bad_key in (None, 123, b"k", ""):
            with self.assertRaises(ValueError):
                self.store.put(bad_key, "v")  # type: ignore[arg-type]
            with self.assertRaises(ValueError):
                self.store.delete(bad_key)  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            self.store.put("k", None)   # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            self.store.put("k", 5)      # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            self.store.put("k", b"v")   # type: ignore[arg-type]
        # rejected writes are invisible and consumed no number
        self.assertEqual(self.store.put("ok", "v"), 1)


class CompactConcurrencyTests(ConcurrencyTestBase):
    def test_compact_then_writers_continue_from_new_record_count(self) -> None:
        workers, per = 4, 20
        barrier = self.ctx.Barrier(workers + 1)
        outq = self.ctx.Queue()
        writers = [self.ctx.Process(target=worker_phased,
                                    args=(str(self.root), wid, per, barrier, outq))
                   for wid in range(workers)]
        compactor = self.ctx.Process(target=compactor_phased,
                                     args=(str(self.root), barrier))
        for proc in writers + [compactor]:
            proc.start()
        for proc in writers + [compactor]:
            proc.join()
        for proc in writers + [compactor]:
            self.assertEqual(proc.exitcode, 0)

        a_seqs, c_seqs = [], []
        for _ in writers:
            _, sa, sc = outq.get()
            a_seqs.extend(sa)
            c_seqs.extend(sc)
        # phase A: dense 1..80; compact collapses to the 80 live keys;
        # phase C numbers continue at 81, all from the swapped serial point
        self.assertEqual(sorted(a_seqs), list(range(1, workers * per + 1)))
        self.assertEqual(sorted(c_seqs),
                         list(range(workers * per + 1, 2 * workers * per + 1)))

        records = frames(self.data)
        self.assertEqual(len(records), 2 * workers * per)
        live = fold(records)
        self.assertEqual(len(live), 2 * workers * per)
        self.assertEqual(dict(self.store.scan()), live)
        report = self.store.compact()  # idempotent from the new state
        self.assertEqual(report["records_before"], 2 * workers * per)
        self.assertEqual(report["records_after"], 2 * workers * per)
        self.assertEqual(self.store.put("after", "z"), 2 * workers * per + 1)

    def test_compact_racing_writers_loses_nothing(self) -> None:
        workers, per = 6, 30
        event = self.ctx.Event()
        outq = self.ctx.Queue()
        errors = self.ctx.Queue()
        compactor = self.ctx.Process(target=compactor_loop, args=(str(self.root), event))
        readers = [self.ctx.Process(target=reader_loop,
                                    args=(str(self.root), event, errors))
                   for _ in range(2)]
        compactor.start()
        for proc in readers:
            proc.start()
        writers = [self.ctx.Process(target=worker_unique_puts,
                                    args=(str(self.root), wid, per, outq))
                   for wid in range(workers)]
        for proc in writers:
            proc.start()
        for proc in writers:
            proc.join()
            self.assertEqual(proc.exitcode, 0)
        event.set()
        compactor.join()
        for proc in readers:
            proc.join()
        self.assertEqual(compactor.exitcode, 0)
        self.assertTrue(errors.empty(), msg=errors.get() if not errors.empty() else "")

        total = workers * per
        while not outq.empty():
            outq.get()
        # whole serial-point states only: no torn tail, every distinct key live
        self.assertEqual(self.store.verify()["status"], "ok")
        records = frames(self.data)
        self.assertEqual(len(records), len(fold(records)))
        self.assertEqual(len(self.store.scan()), total)

    def test_recover_racing_confirmed_writers_truncates_nothing(self) -> None:
        workers, per = 4, 20
        event = self.ctx.Event()
        write_q = self.ctx.Queue()
        recover_q = self.ctx.Queue()
        recoverer = self.ctx.Process(target=recoverer_loop,
                                     args=(str(self.root), event, recover_q))
        recoverer.start()
        writers = [self.ctx.Process(target=worker_unique_puts,
                                    args=(str(self.root), wid, per, write_q))
                   for wid in range(workers)]
        for proc in writers:
            proc.start()
        for proc in writers:
            proc.join()
            self.assertEqual(proc.exitcode, 0)
        event.set()
        recoverer.join()
        self.assertEqual(recoverer.exitcode, 0)
        self.assertGreater(recover_q.get(), 0)  # recover ran at least once
        self.assertEqual(self.store.verify()["status"], "ok")
        self.assertEqual(len(self.store.scan()), workers * per)


class ReadAtomicityAndSnapshotTests(ConcurrencyTestBase):
    def test_snapshot_is_fixed_across_other_processes(self) -> None:
        self.store.put("a", "1")
        snap = self.store.snapshot()

        def child(root: str) -> None:
            store = PageStore(root)
            store.put("a", "2")
            store.put("b", "3")
            store.compact()
            store.put("c", "4")
            store.recover()

        proc = self.ctx.Process(target=child, args=(str(self.root),))
        proc.start()
        proc.join()
        self.assertEqual(proc.exitcode, 0)

        self.assertEqual(snap.scan(), [("a", "1")])
        self.assertEqual(snap.stats(), {"pages": 1, "records": 1, "keys": 1})
        # the originating instance reads the post-change state
        self.assertEqual(self.store.get("a"), "2")
        self.assertEqual(self.store.get("b"), "3")
        self.assertEqual(self.store.get("c"), "4")

    def test_reads_never_see_half_change_in_process(self) -> None:
        stop = threading.Event()
        bad: list[BaseException] = []

        def reader(store: PageStore) -> None:
            last_keys = 0
            while not stop.is_set():
                get = None
                try:
                    # each individual call is one linearised read: scan is
                    # already a folded, sorted state of one serial point, and
                    # stats describes that same point internally.  Separate
                    # calls may straddle serial points, so they are not
                    # compared to each other -- only that the single live key
                    # can never disappear once written.
                    scan = store.scan()
                    assert scan == sorted(scan)
                    assert all(isinstance(k, str) and isinstance(v, str)
                               for k, v in scan)
                    stats = store.stats()
                    assert stats["records"] >= stats["keys"]
                    assert stats["keys"] in (0, 1)
                    assert stats["keys"] >= last_keys
                    last_keys = stats["keys"]
                    get = store.get("shared")
                except BaseException as exc:
                    bad.append(exc)
                    return
                if get is not None and not get.startswith("v-"):
                    bad.append(AssertionError(f"impossible value {get!r}"))
                    return

        other = PageStore(self.root)
        threads = [threading.Thread(target=reader, args=(s,))
                   for s in (self.store, other)]
        for t in threads:
            t.start()
        for i in range(200):
            (self.store if i % 2 else other).put("shared", f"v-{i}")
        stop.set()
        for t in threads:
            t.join()
        self.assertEqual(bad, [])


class CoordinationFileTests(ConcurrencyTestBase):
    def test_lock_file_created_beside_pages_dat(self) -> None:
        self.assertTrue((self.root / LOCK_FILE).is_file())

    def test_coordination_file_holds_no_records(self) -> None:
        for i in range(5):
            self.store.put(f"k{i}", "v")
        # stats and verify describe pages.dat only
        stats = self.store.stats()
        self.assertEqual(stats, {"pages": 1, "records": 5, "keys": 5})
        outcome = self.store.verify()
        self.assertEqual(outcome["scanned_end_offset"],
                         (self.root / "pages.dat").stat().st_size)
        # compact/recover neither count nor consume the coordination file
        result = self.store.compact()
        self.assertEqual(result["records_before"], 5)
        self.assertEqual(result["records_after"], 5)
        self.assertTrue((self.root / LOCK_FILE).exists())
        recovery = self.store.recover()
        self.assertFalse(recovery["truncated"])
        self.assertTrue((self.root / LOCK_FILE).exists())
        # pages.dat stays exactly five encoded records
        self.assertEqual(len(frames(self.data)), 5)

    def test_coordination_failure_is_oserror(self) -> None:
        # replace the coordination file with a directory: locking cannot start
        lock = self.root / LOCK_FILE
        lock.unlink()
        lock.mkdir()
        with self.assertRaises(OSError):
            self.store.put("k", "v")
        with self.assertRaises(OSError):
            self.store.compact()
        with self.assertRaises(OSError):
            self.store.recover()
        # pages.dat was not touched
        self.assertEqual(self.data, b"")


class CrashedWriterTailTests(ConcurrencyTestBase):
    def test_next_process_appender_drops_half_written_tail(self) -> None:
        head = put_record("a", "1")
        tail = put_record("b", "2")
        self.write(head + tail[:len(tail) - 4])  # writer died mid-record

        result = self.run_cli("put", "c", "3")  # a brand-new process
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(result.stdout.strip(), "2")  # numbering from confirmed count
        self.assertEqual(self.data, head + put_record("c", "3"))
        self.assertEqual(self.store.scan(), [("a", "1"), ("c", "3")])
        self.assertEqual(self.store.verify()["status"], "ok")

    def test_appender_preserves_mid_file_corruption(self) -> None:
        blob = (put_record("a", "1") + b"\x00\x00\x00\x09broken"
                + put_record("b", "2"))
        self.write(blob)
        result = self.run_cli("put", "c", "3")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.data, blob)  # untouched, nothing appended
        self.assertEqual(self.store.verify()["status"], "corrupt_middle")

    def test_tail_after_concurrent_compact_is_retaken_cleanly(self) -> None:
        # a crashed tail plus a stale compact scratch file: serial state wins
        head = put_record("a", "1") + put_record("b", "2")
        tail = put_record("z", "9")
        self.write(head + tail[:3])
        (self.root / ".pages.dat.compact.tmp").write_bytes(b"stale")
        result = self.store.compact()
        self.assertEqual(result["records_before"], 2)
        self.assertEqual(result["discarded_tail_bytes"], 3)
        self.assertEqual(self.data, put_record("a", "1") + put_record("b", "2"))
        self.assertFalse((self.root / ".pages.dat.compact.tmp").exists())


if __name__ == "__main__":
    unittest.main()
