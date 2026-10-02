"""Tests for using inherited PageStore instances after fork.

Both ``os.fork`` and the multiprocessing ``fork`` start method copy only the
calling thread into the child.  An inherited store therefore needs help:
its in-process gate lock could be owned by a thread the child never got, its
copied coordination-lock file descriptor shares the parent's open file
description, and its cached serial point is the parent's.  These tests pin
the required behaviour -- no permanent waits, a single serial ordering
shared with freshly created instances, and independent life after either
side exits.

Child bodies always leave via ``os._exit``: ordinary interpreter shutdown in
a fork child would try to join the parent's other (non-existent) threads.
Results come back through a pipe as small JSON payloads.
"""

from __future__ import annotations

import json
import multiprocessing
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from page_store.core import PageStore  # noqa: E402


class ForkTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.store = PageStore(self.root)
        self.store.init()

    # ------------------------------------------------------------- helpers

    def _fork_child(self, body, timeout: float = 20.0):
        """Run ``body`` (a no-arg callable) in a forked child.

        Returns the JSON-decoded payload the child sent: ``{"ok": true,
        "result": ...}`` or ``{"ok": false, "exc": "..."}``.  Fails the test
        if the child exits uncleanly or does not finish in time.
        """
        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:  # child
            os.close(read_fd)
            try:
                payload = json.dumps({"ok": True, "result": body()}).encode()
            except BaseException as error:  # noqa: BLE001 - report anything
                payload = json.dumps(
                    {"ok": False,
                     "exc": f"{type(error).__name__}: {error}"}).encode()
            try:
                view = memoryview(payload)
                while view:
                    written = os.write(write_fd, view)
                    view = view[written:]
            except OSError:
                pass
            os._exit(0)
        os.close(write_fd)
        chunks: list[bytes] = []
        deadline = time.time() + timeout
        while True:
            done_pid, status = os.waitpid(pid, os.WNOHANG)
            if done_pid == pid:
                while True:
                    part = os.read(read_fd, 65536)
                    if not part:
                        break
                    chunks.append(part)
                os.close(read_fd)
                code = os.waitstatus_to_exitcode(status)
                self.assertEqual(code, 0, f"child exited with {code}")
                return json.loads(b"".join(chunks).decode())
            if time.time() > deadline:
                os.kill(pid, 9)
                os.waitpid(pid, 0)
                os.close(read_fd)
                self.fail(f"child {body!r} did not finish within {timeout}s")
            time.sleep(0.02)

    def assertChildOk(self, payload) -> object:
        self.assertTrue(payload["ok"], f"child raised: {payload.get('exc')}")
        return payload["result"]

    def _release(self, write_fd: int) -> None:
        os.write(write_fd, b"go")
        os.close(write_fd)


class InheritedInstanceBasicTests(ForkTestBase):
    def test_child_reads_parent_state_and_extends_sequence(self) -> None:
        self.assertEqual(self.store.put("parent", "p"), 1)
        payload = self._fork_child(
            lambda: [self.store.get("parent"), self.store.put("child", "c")])
        self.assertEqual(self.assertChildOk(payload), ["p", 2])
        # The parent sees the child's confirmed write without rebuild/recover.
        self.assertEqual(self.store.get("child"), "c")
        self.assertEqual(self.store.put("parent2", "p2"), 3)
        self.assertEqual(PageStore(self.root).verify()["status"], "ok")

    def test_multiple_instances_created_before_fork_both_work(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        a.put("a", "1")  # warms a's lock descriptor
        # b has never been used: it has no descriptor yet and must lazily open
        # its own in the child without creating anything extra.
        payload = self._fork_child(lambda: (
            b.put("b", "2"), a.get("a"), b.get("b"), a.put("a", "3")))
        self.assertEqual(self.assertChildOk(payload), [2, "1", "2", 3])
        self.assertEqual(self.store.scan(), [("a", "3"), ("b", "2")])

    def test_child_can_recover_init_compact_verify_snapshot(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")

        def body():
            snap = self.store.snapshot()
            compacted = self.store.compact()
            after_compact = self.store.put("c", "3")
            self.store.init()
            after_reset = self.store.put("fresh", "x")
            recovered = self.store.recover()
            return [snap.scan(), compacted["records_after"], after_compact,
                    after_reset, recovered["truncated"],
                    self.store.verify()["status"]]

        payload = self._fork_child(body)
        self.assertEqual(
            self.assertChildOk(payload),
            [[["a", "1"], ["b", "2"]], 2, 3, 1, False, "ok"])
        self.assertEqual(self.store.scan(), [("fresh", "x")])
        self.assertEqual(self.store.put("late", "y"), 2)

    def test_missing_root_in_child_still_raises_and_creates_nothing(self) -> None:
        missing = self.root / "nope"
        unused = PageStore(missing)  # constructed, never touched
        payload = self._fork_child(lambda: _raises(unused.put, "k", "v"))
        result = self.assertChildOk(payload)
        self.assertEqual(result[0], "FileNotFoundError")
        self.assertFalse(missing.exists())

    def test_inherited_instance_in_multiprocessing_fork_worker(self) -> None:
        ctx = multiprocessing.get_context("fork")
        shared = self.store  # captured by the forked worker's memory

        def worker(per_worker: int, out_queue) -> None:
            seqs = []
            for i in range(per_worker):
                seqs.append(shared.put(f"w-k{i:03d}", f"{i}"))
                seqs.append(shared.put("shared", f"{os.getpid()}:{i}"))
            out_queue.put(seqs)

        n_workers, per = 3, 15
        queue = ctx.Queue()
        procs = [ctx.Process(target=worker, args=(per, queue))
                 for _ in range(n_workers)]
        parent_seqs = [self.store.put(f"p-k{i:03d}", str(i))
                       for i in range(per)]
        for proc in procs:
            proc.start()
        for proc in procs:
            proc.join(timeout=30)
            self.assertEqual(proc.exitcode, 0)
        child_seqs = [queue.get(timeout=10) for _ in range(n_workers)]
        all_seqs = parent_seqs + [s for batch in child_seqs for s in batch]
        total = per + n_workers * per * 2
        self.assertEqual(sorted(all_seqs), list(range(1, total + 1)))
        self.assertEqual(self.store.stats()["records"], total)
        self.assertEqual(PageStore(self.root).verify()["status"], "ok")


class ForkWhileParentThreadsActiveTests(ForkTestBase):
    def test_child_not_blocked_when_parent_thread_held_gate_at_fork(self) -> None:
        # Simulate a parent thread being inside a storage call at fork time:
        # it holds the instance gate, a lock the copied child thread set does
        # not contain anyone able to release it.
        warmed = PageStore(self.root)
        warmed.put("warm", "0")  # record 1, warms the gate state
        started = threading.Event()

        def hold_gate() -> None:
            warmed._gate.acquire()
            started.set()
            time.sleep(3.0)
            warmed._gate.release()

        thread = threading.Thread(target=hold_gate)
        thread.start()
        self.addCleanup(thread.join)
        started.wait()
        time.sleep(0.1)
        # Before the fix this child waited forever; the helper fails on timeout.
        payload = self._fork_child(
            lambda: warmed.put("child", "c"), timeout=8.0)
        self.assertEqual(self.assertChildOk(payload), 2)
        self.assertEqual(warmed.put("parent", "p"), 3)

    def test_child_waits_for_parent_operation_in_flight_then_continues(self) -> None:
        # A parent thread holds the coordination flock on the store's warm
        # descriptor at fork time.  The inherited descriptor shares the
        # parent's open file description, so the child must reopen its own:
        # its put has to queue behind the parent's section rather than race
        # through it, then continue once the parent unlocks.
        import fcntl

        warmed = PageStore(self.root)
        warmed.put("warm", "1")
        ready = threading.Event()

        def hold_lock() -> None:
            fcntl.flock(warmed._lock_fd, fcntl.LOCK_EX)
            ready.set()
            time.sleep(1.5)
            fcntl.flock(warmed._lock_fd, fcntl.LOCK_UN)

        thread = threading.Thread(target=hold_lock)
        thread.start()
        self.addCleanup(thread.join)
        ready.wait()
        time.sleep(0.1)

        def body():
            start = time.time()
            seq = warmed.put("child", "c")
            return [seq, time.time() - start]

        payload = self._fork_child(body, timeout=8.0)
        seq, waited = self.assertChildOk(payload)
        # init left 0 records; warmed.put was 1, so the child gets 2 -- but
        # only after queuing behind the parent's in-flight section.
        self.assertEqual(seq, 2)
        self.assertGreaterEqual(waited, 1.2)

    def test_child_continues_after_lock_holder_process_exits(self) -> None:
        # The waiter must not depend on the holder unlocking politely: when
        # the process actually occupying the serial point dies, the kernel
        # drops its flock and the waiter proceeds.
        store = self.store
        store.put("x", "1")

        def body():
            holder = os.fork()
            if holder == 0:  # sibling: grab the lock, then die holding it
                fd = os.open(str(store._lock_path), os.O_RDWR)
                import fcntl as _fcntl
                _fcntl.flock(fd, _fcntl.LOCK_EX)
                time.sleep(1.2)
                os._exit(0)
            time.sleep(0.3)
            start = time.time()
            seq = store.put("after-death", "v")
            return [seq, time.time() - start]

        payload = self._fork_child(body, timeout=10.0)
        seq, waited = self.assertChildOk(payload)
        self.assertEqual(seq, 2)
        self.assertGreaterEqual(waited, 0.7)

    def test_stress_fork_while_parent_writer_thread_runs(self) -> None:
        # The forking thread (main) is never inside a storage call, but
        # another parent thread continuously is.  Many children use the
        # inherited instance concurrently with the parent: none may hang,
        # and the resulting numbers must stay dense and unique.
        stop = threading.Event()
        parent_seqs: list[int] = []
        errors: list[BaseException] = []

        def writer() -> None:
            i = 0
            try:
                while not stop.is_set():
                    parent_seqs.append(self.store.put(f"p{i:05d}", str(i)))
                    i += 1
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        thread = threading.Thread(target=writer)
        thread.start()
        child_counts: list[int] = []
        try:
            for i in range(15):
                payload = self._fork_child(
                    lambda i=i: self.store.put(f"c{i:03d}", str(i)),
                    timeout=10.0)
                child_counts.append(self.assertChildOk(payload))
        finally:
            stop.set()
            thread.join(timeout=10)
        self.assertEqual(errors, [])
        self.store.compact()  # serialises; also asserts no torn frames
        used = sorted(parent_seqs + child_counts)
        self.assertEqual(used, list(range(1, len(used) + 1)))
        self.assertEqual(self.store.verify()["status"], "ok")


class NestedForkTests(ForkTestBase):
    def test_grandchild_uses_inherited_instance(self) -> None:
        store = self.store
        store.put("root", "0")

        def child_body():
            grand = os.fork()
            if grand == 0:  # grandchild: fork hook must run again
                try:
                    seq = store.put("grand", "g")
                    os._exit(0 if seq == 2 else 1)
                except BaseException:  # noqa: BLE001
                    os._exit(2)
            _, status = os.waitpid(grand, 0)
            if os.waitstatus_to_exitcode(status) != 0:
                return None
            return [store.get("grand"), store.put("child", "c")]

        payload = self._fork_child(child_body)
        self.assertEqual(self.assertChildOk(payload), ["g", 3])
        self.assertEqual(self.store.scan(),
                         [("child", "c"), ("grand", "g"), ("root", "0")])

    def test_refork_after_calls_does_not_let_anyone_jump_ahead(self) -> None:
        # A child completes calls, then forks again; the grandchild must not
        # inherit a descriptor that lets it bypass an operation its parent
        # still has in flight (it gets its own reset instance).
        import fcntl

        store = self.store
        store.put("a", "1")

        def child_body():
            store.get("a")  # ordinary call before re-forking
            ready = os.pipe()
            grand = os.fork()
            if grand == 0:  # grandchild waits for signal, then contends
                os.close(ready[1])
                os.read(ready[0], 1)
                try:
                    os._exit(store.put("grand", "g"))  # exits with seq
                except BaseException:  # noqa: BLE001
                    os._exit(20)
            os.close(ready[0])
            # Parent (of the grandchild) takes the lock first, holds it, then
            # releases the grandchild into the contention.
            fcntl.flock(store._lock_fd, fcntl.LOCK_EX)
            os.write(ready[1], b"go")
            time.sleep(1.0)
            own = store.put("child", "c")
            fcntl.flock(store._lock_fd, fcntl.LOCK_UN)
            os.close(ready[1])
            _, status = os.waitpid(grand, 0)
            return [own, os.waitstatus_to_exitcode(status)]

        payload = self._fork_child(child_body, timeout=10.0)
        own, grand_seq = self.assertChildOk(payload)
        self.assertEqual(sorted([own, grand_seq]), [2, 3])


class SerialOrderingAcrossForkTests(ForkTestBase):
    def _two_signalled_children(self, body_a, body_b):
        """Fork two children; each blocks on a pipe until told to go."""
        ra, wa = os.pipe()
        rb, wb = os.pipe()

        def runner(read_fd: int, body) -> None:
            os.read(read_fd, 1)
            try:
                result = body()
                os._exit(0 if result else 1)
            except BaseException:  # noqa: BLE001
                os._exit(2)

        pa = os.fork()
        if pa == 0:  # child A
            os.close(wa); os.close(rb); os.close(wb)
            runner(ra, body_a)
        pb = os.fork()
        if pb == 0:  # child B
            os.close(wb); os.close(ra); os.close(wa)
            runner(rb, body_b)
        os.close(ra); os.close(rb)
        return pa, pb, wa, wb

    def _reap(self, pid: int) -> int:
        _, status = os.waitpid(pid, 0)
        return os.waitstatus_to_exitcode(status)

    def test_compare_and_set_only_one_forked_writer_wins(self) -> None:
        store = self.store
        store.put("k", "old")

        def try_a():
            got = store.write_batch_if({"k": "old"},
                                       [{"op": "put", "key": "k",
                                         "value": "A"}])
            return got is not None

        def try_b():
            got = store.write_batch_if({"k": "old"},
                                       [{"op": "put", "key": "k",
                                         "value": "B"}])
            return got is not None

        pa, pb, wa, wb = self._two_signalled_children(try_a, try_b)
        self._release(wa)
        self._release(wb)
        code_a, code_b = self._reap(pa), self._reap(pb)
        self.assertEqual(sorted([code_a, code_b]), [0, 1])
        self.assertIn(store.get("k"), ("A", "B"))
        self.assertEqual(store.stats()["records"], 2)  # exactly one batch op

    def test_range_condition_detects_key_inserted_in_sibling(self) -> None:
        store = self.store
        store.write_batch([{"op": "put", "key": "a", "value": "1"}])

        def inserter():
            return store.write_batch(
                [{"op": "put", "key": "b", "value": "2"}]) is not None

        def guarded():
            # Expects an empty (a, c) range; the sibling inserts "b" into it
            # first (signalled ordering), so this must fail.
            failed = store.write_batch_if_range(
                {}, [{"op": "put", "key": "d", "value": "4"}],
                start="a", end="c") is None
            # Retrying against the new state (a=1 and the inserted b=2 both
            # lie inside (a, c)) succeeds.
            retried = store.write_batch_if_range(
                {"a": "1", "b": "2"},
                [{"op": "put", "key": "e", "value": "5"}],
                start="a", end="c")
            return failed and isinstance(retried, list)

        # The inserter must commit first: fork it separately, wait, then fork
        # the guarded child.
        payload = self._fork_child(inserter)
        self.assertChildOk(payload)
        self.assertEqual(self.store.get("b"), "2")
        payload = self._fork_child(guarded)
        self.assertTrue(self.assertChildOk(payload))

    def test_batches_keep_contiguous_sequence_segments(self) -> None:
        store = self.store
        store.put("one", "1")

        def body():
            return store.write_batch([
                {"op": "put", "key": "k1", "value": "v1"},
                {"op": "delete", "key": "one"},
                {"op": "put", "key": "k2", "value": "v2"},
            ])

        payload = self._fork_child(body)
        self.assertEqual(self.assertChildOk(payload), [2, 3, 4])
        self.assertEqual(store.put("after", "z"), 5)
        self.assertEqual(store.scan(),
                         [("after", "z"), ("k1", "v1"), ("k2", "v2")])


class ResetAndCompactVisibilityTests(ForkTestBase):
    def test_parent_reads_child_reset_on_next_call_and_renumbers(self) -> None:
        store = self.store
        store.put("old", "1")
        snap = store.snapshot()  # captured before the reset

        payload = self._fork_child(lambda: _reset_and_write(store))
        self.assertEqual(self.assertChildOk(payload), 1)
        # The next parent call sees the new inode/state without recover().
        self.assertIsNone(store.get("old"))
        self.assertEqual(store.stats(), {"pages": 1, "records": 1, "keys": 1})
        self.assertEqual(store.put("parent", "p"), 2)
        # The pre-fork snapshot stays frozen across the reset.
        self.assertEqual(snap.scan(), [("old", "1")])

    def test_parent_resumes_numbering_after_child_compaction(self) -> None:
        store = self.store
        for i in range(4):
            store.put(f"k{i}", "v")
        store.put("k0", "shadow")  # overwritten -> discarded by compact
        self.assertEqual(store.stats()["records"], 5)

        def body():
            result = store.compact()
            seq = store.put("new", "x")
            return [result["records_after"], seq]

        payload = self._fork_child(body)
        records_after, seq = self.assertChildOk(payload)
        self.assertEqual(records_after, 4)
        self.assertEqual(seq, 5)
        # Parent detects the swapped inode on its next call and continues.
        self.assertEqual(store.put("tail", "y"), 6)
        self.assertEqual(store.stats()["records"], 6)
        self.assertEqual(store.get("k0"), "shadow")

    def test_snapshot_taken_before_fork_is_frozen(self) -> None:
        store = self.store
        store.put("keep", "1")
        snap = store.snapshot()

        def body():
            store.put("keep", "2")
            store.compact()
            store.init()
            store.put("other", "3")
            return store.snapshot().scan()

        payload = self._fork_child(body)
        self.assertEqual(self.assertChildOk(payload), [["other", "3"]])
        self.assertEqual(snap.scan(), [("keep", "1")])
        self.assertEqual(snap.stats(), {"pages": 1, "records": 1, "keys": 1})


class ExitIndependenceTests(ForkTestBase):
    def test_parent_keeps_using_store_after_child_exit(self) -> None:
        store = self.store
        payload = self._fork_child(
            lambda: [store.put("child", "c"), store.get("child")])
        self.assertEqual(self.assertChildOk(payload), [1, "c"])
        self.assertEqual(store.put("parent", "p"), 2)
        self.assertEqual(store.scan(), [("child", "c"), ("parent", "p")])

    def test_repeated_fork_and_exit_cycles_never_strand_the_lock(self) -> None:
        # Each child's independent descriptor is closed on exit without
        # affecting anyone else's lock; repeated fork/exit cycles must not
        # strand the serial point or leak sequence numbers.
        for i in range(5):
            payload = self._fork_child(
                lambda i=i: self.store.put(f"c{i:02d}", str(i)))
            self.assertEqual(self.assertChildOk(payload), i + 1)
        self.assertEqual(self.store.verify()["status"], "ok")
        self.assertEqual(self.store.stats()["records"], 5)


# ---------------------------------------------------------------- functions

def _raises(fn, *args):
    """Return the exception type name raised by fn(*args); None if it didn't."""
    try:
        fn(*args)
    except BaseException as error:  # noqa: BLE001
        return [type(error).__name__, str(error)]
    return ["NoException", ""]


def _reset_and_write(store: PageStore) -> int:
    store.init()
    return store.put("child-after-reset", "x")


if __name__ == "__main__":
    unittest.main()
