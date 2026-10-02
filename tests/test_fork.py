"""Tests for fork safety: inherited PageStore objects across os.fork and the
multiprocessing ``fork`` start method.

A forked child keeps using the very instances it inherited -- no rebuild and
no ``recover``.  The child must never wait on a coordination lock held by a
parent thread that was not copied into the child; it waits only for storage
operations a live parent thread/process is actually still holding, and
proceeds when that operation completes or the holder exits.  Inherited and
post-fork instances share one serial order: dense unique sequence numbers,
atomic batches, both conditional commits, init, recover and compact keep
their existing semantics, reads and snapshots see complete before/after
states, and a reset/compaction by one side is observed on the other side's
next call while earlier snapshots stay frozen.
"""

from __future__ import annotations

import json
import multiprocessing
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

from page_store.core import PageStore  # noqa: E402

FORK_AVAILABLE = hasattr(os, "fork") and sys.platform.startswith("linux")


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


@unittest.skipUnless(FORK_AVAILABLE, "os.fork semantics tested on Linux")
class ForkTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.store = PageStore(self.root)
        self.store.init()

    def fork_and_read(self, build_payload):
        """Fork; the child calls ``build_payload(store)`` and reports its JSON.

        Returns ``(pid, read_fd)`` in the parent.  The child never returns.
        """
        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(read_fd)
            try:
                payload = build_payload(self.store)
                os.write(write_fd, json.dumps(payload).encode("utf-8"))
            except BaseException as error:  # noqa: BLE001
                os.write(write_fd, json.dumps(
                    {"__child_error__": repr(error)}).encode("utf-8"))
            finally:
                os.close(write_fd)
                os._exit(0)
        os.close(write_fd)
        return pid, read_fd

    def wait_child(self, pid: int, fd: int, timeout: float = 20.0):
        """Wait for the child and decode its single JSON report."""
        chunks = []
        deadline = time.time() + timeout
        while True:
            wpid, status = os.waitpid(pid, os.WNOHANG)
            if wpid:
                self.assertTrue(
                    os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0,
                    f"child failed status={status}")
                while True:
                    chunk = os.read(fd, 65536)
                    if not chunk:
                        break
                    chunks.append(chunk)
                os.close(fd)
                self.assertTrue(chunks, "child reported nothing (it hung/died)")
                payload = json.loads(b"".join(chunks))
                if isinstance(payload, dict) and "__child_error__" in payload:
                    self.fail(payload["__child_error__"])
                return payload
            self.assertLess(time.time(), deadline,
                            "child did not finish (permanent lock wait?)")
            time.sleep(0.02)


class InheritedInstanceForkTests(ForkTestBase):
    def test_os_fork_children_use_inherited_instance_with_dense_sequence(self) -> None:
        self.assertEqual(self.store.put("a", "1"), 1)
        children = []
        for tag in ("x1", "x2"):
            pid, fd = self.fork_and_read(
                lambda s, tag=tag: [s.put(f"{tag}-{i}", str(i))
                                   for i in range(5)])
            children.append((pid, fd))
        all_seqs = [1]
        for pid, fd in children:
            all_seqs.extend(self.wait_child(pid, fd))
        self.assertEqual(sorted(all_seqs), list(range(1, 12)))
        parsed = frames((self.root / "pages.dat").read_bytes())
        self.assertEqual(len(parsed), 11)
        # The parent keeps using the inherited-by-the-test instance too.
        self.assertEqual(self.store.put("parent", "p"), 12)

    def test_multiple_pre_fork_instances_and_grandchild_share_one_order(self) -> None:
        second = PageStore(self.root)
        self.assertEqual(self.store.put("a", "1"), 1)
        self.assertEqual(second.put("b", "2"), 2)

        def body(s):
            # Re-fork before touching either inherited instance.
            gpid, gfd = self.fork_and_read(lambda s2: second.put("grand", "g"))
            gseq = self.wait_child(gpid, gfd)
            return {"self": s.put("child", "c"),
                    "second": second.put("child2", "d"), "grand": gseq}

        pid, fd = self.fork_and_read(body)
        seqs = self.wait_child(pid, fd)
        self.assertEqual(sorted(seqs.values()), [3, 4, 5])
        self.assertEqual(self.store.put("parent", "p"), 6)
        self.assertEqual(second.put("parent2", "q"), 7)

    def test_forked_children_contend_on_one_conditional_batch(self) -> None:
        self.store.put("k", "old")
        children = []
        for _ in range(4):
            pid, fd = self.fork_and_read(
                lambda s: s.write_batch_if(
                    {"k": "old"},
                    [{"op": "put", "key": "k", "value": "new"}]))
            children.append((pid, fd))
        outcomes = [self.wait_child(pid, fd) for pid, fd in children]
        winners = [o for o in outcomes if o is not None]
        self.assertEqual(len(winners), 1, outcomes)
        self.assertEqual(winners[0], [2])  # the single winning record
        self.assertEqual(self.store.get("k"), "new")
        # Failed conditional batches consumed no number.
        self.assertEqual(self.store.put("after", "z"), 3)

    def test_range_condition_discovers_key_inserted_by_sibling(self) -> None:
        children = []
        for _ in range(3):
            pid, fd = self.fork_and_read(
                lambda s: s.write_batch_if_range(
                    {}, [{"op": "put", "key": "p-key", "value": "x"}],
                    start="p", end="q"))
            children.append((pid, fd))
        outcomes = [self.wait_child(pid, fd) for pid, fd in children]
        self.assertEqual(sum(o is not None for o in outcomes), 1, outcomes)
        self.assertEqual(self.store.scan("p", "q"), [("p-key", "x")])

    def test_inherited_instance_new_after_fork_share_ordering_and_semantics(self) -> None:
        self.store.put("a", "1")

        def body(s):
            fresh = PageStore(self.root)  # created in the child
            batch = fresh.write_batch(
                [{"op": "put", "key": "b", "value": "2"},
                 {"op": "delete", "key": "a"},
                 {"op": "put", "key": "c", "value": "3"}])
            return {"batch": batch,
                    "inherited_get": s.get("a"),
                    "fresh_scan": fresh.scan(),
                    "recover": fresh.recover(),
                    "verify": s.verify()["status"]}

        pid, fd = self.fork_and_read(body)
        result = self.wait_child(pid, fd)
        # The batch occupies a consecutive block 2..4.
        self.assertEqual(result["batch"], [2, 3, 4])
        self.assertIsNone(result["inherited_get"])  # delete visible at once
        self.assertEqual([tuple(pair) for pair in result["fresh_scan"]],
                         [("b", "2"), ("c", "3")])
        self.assertEqual(result["recover"]["truncated"], False)
        self.assertEqual(result["verify"], "ok")
        self.assertEqual(self.store.put("parent", "p"), 5)
        self.assertEqual(self.store.scan(),
                         [("b", "2"), ("c", "3"), ("parent", "p")])


class LockLifetimeAcrossForkTests(ForkTestBase):
    def test_child_waits_for_live_parent_op_then_proceeds_densely(self) -> None:
        self.assertEqual(self.store.put("seed", "s"), 1)
        # A different parent thread holds the coordination lock across the
        # fork; the forking thread itself is never inside a storage method.
        held, release = threading.Event(), threading.Event()

        def holder() -> None:
            with self.store._locked(exclusive=True):
                held.set()
                release.wait(10)

        thread = threading.Thread(target=holder, daemon=True)
        thread.start()
        held.wait()

        def body(s):
            # Grandchild forked while the parent's op is still in flight:
            # both descendants queue behind that live serial operation.
            gpid, gfd = self.fork_and_read(
                lambda s2: (lambda started=time.time():
                            [self.store.put("grand", "g"),
                             time.time() - started])())
            started = time.time()
            cseq = s.put("child", "c")  # blocks until release
            gresult = self.wait_child(gpid, gfd)
            return {"cseq": cseq, "gseq": gresult[0],
                    "waited": time.time() - started, "gwaited": gresult[1]}

        pid, fd = self.fork_and_read(body)

        # While the live parent op is held the descendants must be waiting,
        # not deadlocked on the thread they never inherited.
        time.sleep(0.8)
        wpid, _ = os.waitpid(pid, os.WNOHANG)
        self.assertEqual(wpid, 0, "child finished while parent op still held")
        release.set()
        result = self.wait_child(pid, fd)
        thread.join()
        self.assertEqual(sorted([result["cseq"], result["gseq"]]), [2, 3])
        self.assertGreaterEqual(min(result["waited"], result["gwaited"]), 0.6)
        # Neither descendant overtook the parent's in-flight serial point.
        self.assertEqual(self.store.put("parent", "p"), 4)

    def test_holder_process_exit_unblocks_inherited_waiter(self) -> None:
        # A holder that exits while holding the lock releases it via the
        # kernel; an inherited child that was (or becomes) a waiter must still
        # proceed rather than wait on a dead process.
        script = r"""
import json, os, sys, threading, time
sys.path.insert(0, %r)
from page_store.core import PageStore

root = sys.argv[1]
store = PageStore(root); store.init()
store.put("a", "1")
held, release = threading.Event(), threading.Event()
def hold():
    with store._locked(exclusive=True):
        held.set(); release.wait(30)
threading.Thread(target=hold, daemon=True).start()
held.wait()
pid = os.fork()
if pid == 0:
    started = time.time()
    seq = store.put("b", "2")
    print(json.dumps([seq, round(time.time() - started, 3)]))
    sys.stdout.flush()
    os._exit(0)
time.sleep(0.5)
os._exit(7)  # parent dies still holding the coordination lock
""" % str(REPO_ROOT)
        proc = subprocess.run(
            [sys.executable, "-W", "ignore", "-c", script, str(self.root)],
            capture_output=True, text=True, timeout=20)
        self.assertIn("[2,", proc.stdout.strip(), proc.stderr)
        self.assertEqual(PageStore(self.root).scan(),
                         [("a", "1"), ("b", "2")])

    def test_either_side_exiting_leaves_the_other_usable(self) -> None:
        self.store.put("a", "1")
        pid, fd = self.fork_and_read(lambda s: s.get("a"))
        self.assertEqual(self.wait_child(pid, fd), "1")
        # Child has exited; parent continues.
        self.assertEqual(self.store.put("after-child", "x"), 2)
        # A lingering child and the parent both keep working.
        pid, fd = self.fork_and_read(
            lambda s: (time.sleep(0.2), s.put("lingering", "l"))[1])
        self.assertEqual(self.wait_child(pid, fd), 3)
        self.assertEqual(self.store.put("parent-last", "z"), 4)


class StateVisibilityAcrossForkTests(ForkTestBase):
    def negotiate(self, child_fn):
        """Two-phase fork: child signals ready, waits, then runs after parent."""
        ready_r, ready_w = os.pipe()
        go_r, go_w = os.pipe()
        res_r, res_w = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(ready_r); os.close(go_w); os.close(res_r)
            try:
                payload = child_fn(self.store, ready_w, go_r)
                os.write(res_w, json.dumps(payload).encode("utf-8"))
            except BaseException as error:  # noqa: BLE001
                os.write(res_w, json.dumps(
                    {"__child_error__": repr(error)}).encode("utf-8"))
            finally:
                os.close(res_w)
                os._exit(0)
        os.close(ready_w); os.close(go_r); os.close(res_w)
        os.read(ready_r, 5)
        os.close(ready_r)

        def resume():
            os.write(go_w, b"g")
            os.close(go_w)
            payload = self.wait_child(pid, res_r)
            return payload

        return resume

    def test_reset_observed_on_next_call_with_frozen_snapshot_and_renumber(self) -> None:
        self.store.put("a", "1")
        self.store.put("x", "0")
        self.store.delete("x")  # 3 confirmed records, one live key

        def reset_child(s, ready_w, go_r):
            snap = s.snapshot()  # captured before the parent resets
            os.write(ready_w, b"ready")
            os.read(go_r, 1)
            return {"snap_scan": snap.scan(),
                    "snap_stats": snap.stats(),
                    "stats_after_reset": s.stats(),
                    "first_seq": s.put("c", "3"),
                    "scan": s.scan()}

        resume = self.negotiate(reset_child)
        self.store.init()  # reset: fresh empty inode
        payload = resume()
        # The pre-reset snapshot is frozen...
        self.assertEqual([tuple(pair) for pair in payload["snap_scan"]],
                         [("a", "1")])
        self.assertEqual(payload["snap_stats"],
                         {"pages": 1, "records": 3, "keys": 1})
        # ...but the next call sees the empty reset state and renumbers.
        self.assertEqual(payload["stats_after_reset"],
                         {"pages": 0, "records": 0, "keys": 0})
        self.assertEqual(payload["first_seq"], 1)
        self.assertEqual([tuple(pair) for pair in payload["scan"]],
                         [("c", "3")])
        self.assertEqual(self.store.stats()["records"], 1)

    def test_compaction_by_parent_seen_by_inherited_child(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "0")
        self.store.delete("b")
        self.store.put("c", "3")  # 4 records, 2 live keys

        def compact_child(s, ready_w, go_r):
            before = s.snapshot()  # 4 records pre-compaction
            os.write(ready_w, b"ready")
            os.read(go_r, 1)
            return {"before_scan": before.scan(),
                    "before_records": before.stats()["records"],
                    "scan": s.scan(),
                    "stats": s.stats(),
                    "seq": s.put("d", "4")}

        resume = self.negotiate(compact_child)
        compact = self.store.compact()  # 2 live keys -> 2 records
        self.assertEqual(compact["records_after"], 2)
        payload = resume()
        self.assertEqual(payload["before_records"], 4)
        self.assertEqual([tuple(pair) for pair in payload["before_scan"]],
                         [("a", "1"), ("c", "3")])
        self.assertEqual(payload["stats"]["records"], 2)
        self.assertEqual(payload["seq"], 3)  # continues post-compaction
        self.assertEqual(self.store.stats()["records"], 3)

    def test_half_written_tail_left_by_dead_forked_child_is_recovered(self) -> None:
        self.assertEqual(self.store.put("a", "1"), 1)

        def crashing(s):
            frame = encode({"op": "put", "key": "b", "value": "2"})
            with open(s.path, "ab") as handle:  # bypass the serial point
                handle.write(frame[:len(frame) - 3])
                handle.flush()
                os.fsync(handle.fileno())
            return "done"

        pid, fd = self.fork_and_read(crashing)
        self.assertEqual(self.wait_child(pid, fd), "done")
        data_with_tail = (self.root / "pages.dat").read_bytes()
        # The half-written tail takes no number and is invisible.
        self.assertEqual(self.store.stats()["records"], 1)
        self.assertIsNone(self.store.get("b"))
        # A failed conditional commit leaves the tail byte-for-byte in place.
        outcome = self.store.write_batch_if(
            {"missing": "x"}, [{"op": "put", "key": "z", "value": "1"}])
        self.assertIsNone(outcome)
        self.assertEqual((self.root / "pages.dat").read_bytes(), data_with_tail)
        # The next successful write discards the tail under the usual rule.
        self.assertEqual(self.store.put("c", "3"), 2)
        parsed = frames((self.root / "pages.dat").read_bytes())
        self.assertEqual(parsed,
                         [{"op": "put", "key": "a", "value": "1"},
                          {"op": "put", "key": "c", "value": "3"}])
        self.assertEqual(self.store.verify()["status"], "ok")

    def test_recover_in_parent_seen_by_inherited_child(self) -> None:
        self.store.put("a", "1")
        tail = encode({"op": "put", "key": "b", "value": "2"})
        (self.root / "pages.dat").write_bytes(
            (self.root / "pages.dat").read_bytes() + tail[:len(tail) - 2])

        def child(s, ready_w, go_r):
            os.write(ready_w, b"ready")
            os.read(go_r, 1)
            return {"recover": s.recover(),
                    "put": s.put("c", "3"),
                    "scan": s.scan()}

        resume = self.negotiate(child)
        recovered = self.store.recover()
        self.assertTrue(recovered["truncated"])
        payload = resume()
        self.assertEqual(payload["recover"]["truncated"], False)
        self.assertEqual(payload["put"], 2)
        self.assertEqual([tuple(pair) for pair in payload["scan"]],
                         [("a", "1"), ("c", "3")])

    def test_invalid_arguments_in_child_raise_before_touching_store(self) -> None:
        self.assertEqual(self.store.put("a", "1"), 1)

        def child(s):
            for call in (
                lambda: s.put("", "x"),
                lambda: s.put("k", 1),  # type: ignore[arg-type]
                lambda: s.write_batch([{"op": "nope"}]),
                lambda: s.write_batch_if({"k": 1}, [{"op": "delete", "key": "z"}]),
                lambda: s.write_batch_if_range(
                    {"a": "1"}, [{"op": "delete", "key": "z"}], start="b"),
            ):
                try:
                    call()
                except ValueError:
                    continue
                return "no ValueError"
            return "ok"

        pid, fd = self.fork_and_read(child)
        self.assertEqual(self.wait_child(pid, fd), "ok")
        self.assertEqual(self.store.stats()["records"], 1)
        self.assertEqual(self.store.put("after", "z"), 2)

    def test_missing_store_raises_file_not_found_without_creation(self) -> None:
        missing = self.root / "gone"

        def child(s):
            try:
                PageStore(missing).put("k", "v")
            except FileNotFoundError:
                return "file-not-found"
            return "wrong"

        pid, fd = self.fork_and_read(child)
        self.assertEqual(self.wait_child(pid, fd), "file-not-found")
        self.assertFalse(missing.exists())


@unittest.skipUnless(FORK_AVAILABLE, "multiprocessing fork is Linux/POSIX")
class MultiprocessingForkTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.store = PageStore(self.root)
        self.store.init()
        self.ctx = multiprocessing.get_context("fork")

    def test_inherited_instance_under_multiprocessing_fork(self) -> None:
        queue = self.ctx.Queue()
        store, n_per = self.store, 10

        def worker(wid: int) -> None:
            seqs = [store.put(f"w{wid}-k{i}", f"{wid}:{i}")
                    for i in range(n_per)]
            queue.put(seqs)

        procs = [self.ctx.Process(target=worker, args=(w,)) for w in range(4)]
        for proc in procs:
            proc.start()
        all_seqs = []
        for _ in procs:
            all_seqs.extend(queue.get(timeout=30))
        for proc in procs:
            proc.join(timeout=10)
            self.assertEqual(proc.exitcode, 0)
        self.assertEqual(sorted(all_seqs), list(range(1, 4 * n_per + 1)))
        self.assertEqual(self.store.verify()["status"], "ok")
        self.assertEqual(self.store.put("parent", "p"), 4 * n_per + 1)

    def test_fork_workers_share_condition_and_range_locks(self) -> None:
        self.store.put("k", "old")
        queue = self.ctx.Queue()
        store = self.store

        def cond_worker() -> None:
            queue.put(store.write_batch_if(
                {"k": "old"},
                [{"op": "put", "key": "k", "value": "new"}]))

        procs = [self.ctx.Process(target=cond_worker) for _ in range(4)]
        for proc in procs:
            proc.start()
        outcomes = [queue.get(timeout=30) for _ in procs]
        for proc in procs:
            proc.join(timeout=10)
            self.assertEqual(proc.exitcode, 0)
        self.assertEqual(sum(o is not None for o in outcomes), 1, outcomes)
        self.assertEqual(store.get("k"), "new")


@unittest.skipUnless(FORK_AVAILABLE, "os.fork stress test on Linux")
class ConcurrentThreadsAndForkStressTests(ForkTestBase):
    def test_forks_while_other_threads_write_never_hang_and_stay_dense(self) -> None:
        stop = threading.Event()
        errors: list[BaseException] = []

        def writer(tid: int) -> None:
            i = 0
            try:
                while not stop.is_set():
                    self.store.put(f"t{tid}-{i}", str(i))
                    i += 1
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        threads = [threading.Thread(target=writer, args=(t,)) for t in range(3)]
        for thread in threads:
            thread.start()

        child_seqs = []
        deadline = time.time() + 1.5
        child_index = 0
        while time.time() < deadline:
            child_index += 1
            pid, fd = self.fork_and_read(
                lambda s, i=child_index: s.put("child", str(i)))
            seq = self.wait_child(pid, fd, timeout=15)
            self.assertIsInstance(seq, int)
            child_seqs.append(seq)
            time.sleep(0.01)

        stop.set()
        for thread in threads:
            thread.join(timeout=10)
        self.assertEqual(errors, [])

        # Successful writes form a complete prefix; every child number is a
        # distinct, in-range slot of the file's dense global numbering.
        parsed = frames((self.root / "pages.dat").read_bytes())
        file_numbers = set(range(1, len(parsed) + 1))
        self.assertTrue(set(child_seqs) <= file_numbers)
        after = self.store.compact()["records_after"]
        self.assertEqual(self.store.put("final", "f"), after + 1)
        self.assertEqual(len(child_seqs), len(set(child_seqs)))


if __name__ == "__main__":
    unittest.main()
