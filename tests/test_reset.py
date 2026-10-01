"""Tests for init() as a serial state change visible to every live instance.

A successful init clears all previously confirmed records: it is a reset on
equal footing with a compaction.  Other long-lived PageStore instances -- in
the same process or in another one -- must observe the new serial point on
their next read, stats call, snapshot capture or append without being closed
or told to recover, even when the post-reset file grows as long as or longer
than the pre-reset file.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from page_store.core import PageStore  # noqa: E402

ENVPATH = str(REPO_ROOT) + os.pathsep + os.environ.get("PYTHONPATH", "")

INIT_WORKER = r"""
import sys
from page_store.core import PageStore
root = sys.argv[1]
PageStore(root).init()
"""


def encode(record: dict) -> bytes:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


def put_record(key: str, value: str) -> bytes:
    return encode({"op": "put", "key": key, "value": value})


def frames(data: bytes) -> list[dict]:
    out, offset = [], 0
    while offset + 4 <= len(data):
        size = int.from_bytes(data[offset:offset + 4], "big")
        out.append(json.loads(data[offset + 4:offset + 4 + size].decode("utf-8")))
        offset += 4 + size
    assert offset == len(data), "tail bytes left over"
    return out


class ResetTestBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.init_store = PageStore(self.root)
        self.init_store.init()

    @property
    def data(self) -> bytes:
        return (self.root / "pages.dat").read_bytes()

    def run_cli(self, *argv: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "page_store", "--root", str(self.root), *argv],
            cwd=REPO_ROOT, capture_output=True, text=True)


class ResetVisibilityTests(ResetTestBase):
    def test_longer_post_reset_file_old_instance_drops_old_and_sees_new(self) -> None:
        # The original bug: the new file is LONGER than the cleared one, so a
        # same-inode length check alone cannot tell that the state was reset.
        a, b = PageStore(self.root), PageStore(self.root)
        a.put("old", "small")
        self.assertEqual(a.get("old"), "small")  # populate A's cache
        pre_len = self.data and len(self.data)

        b.init()
        self.assertEqual(b.put("new", "x" * 200), 1)
        self.assertGreaterEqual(len(self.data), pre_len)

        # A reads the post-reset state with no close/reopen/recover hint.
        self.assertIsNone(a.get("old"))
        self.assertEqual(a.get("new"), "x" * 200)
        fresh = PageStore(self.root)
        self.assertEqual(a.scan(), fresh.scan())
        self.assertEqual(a.scan(), [("new", "x" * 200)])
        self.assertEqual(a.stats(), fresh.stats())
        self.assertEqual(a.stats(), {"pages": 1, "records": 1, "keys": 1})
        # The file holds exactly the post-reset record, no pre-reset bytes.
        self.assertEqual(frames(self.data),
                         [{"op": "put", "key": "new", "value": "x" * 200}])

    def test_shorter_post_reset_file_is_seen_too(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        for i in range(20):
            a.put(f"old-{i:02d}", "y" * 100)
        a.scan()  # populate A's cache with the large state
        b.init()
        self.assertEqual(self.data, b"")
        self.assertIsNone(a.get("old-00"))
        self.assertEqual(a.scan(), [])
        # a small append after the reset makes the file far shorter than before
        self.assertEqual(b.put("new", "n"), 1)
        self.assertEqual(a.get("old-19"), None)
        self.assertEqual(a.scan(), [("new", "n")])
        self.assertEqual(a.stats(), {"pages": 1, "records": 1, "keys": 1})

    def test_repeated_resets_never_revive_old_keys(self) -> None:
        a = PageStore(self.root)
        for era in range(6):
            resetter = PageStore(self.root)
            resetter.init()
            # right after a reset, before any write, every counter is zero
            self.assertEqual(a.stats(), {"pages": 0, "records": 0, "keys": 0})
            self.assertEqual(a.scan(), [])
            for i in range(3):
                seq = resetter.put(f"era{era}-k{i}", f"{era}:{i}")
                self.assertEqual(seq, i + 1)
            expected = [(f"era{era}-k{i}", f"{era}:{i}") for i in range(3)]
            self.assertEqual(a.scan(), expected)
            self.assertEqual(a.stats(), {"pages": 1, "records": 3, "keys": 3})
            for past in range(era):
                self.assertIsNone(a.get(f"era{past}-k0"))
        # a fresh instance converges on exactly the last era
        self.assertEqual(PageStore(self.root).scan(),
                         [(f"era5-k{i}", f"5:{i}") for i in range(3)])

    def test_sequence_continues_from_current_record_count_without_skips(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        for i in range(5):
            a.put(f"old-{i}", str(i))
        self.assertEqual(a.put("old-5", "5"), 6)  # A cached seq == 6
        b.init()
        self.assertEqual(b.put("new-0", "0"), 1)
        self.assertEqual(b.put("new-1", "1"), 2)
        # the stale instance must not jump back to its cached 6
        self.assertEqual(a.put("new-2", "2"), 3)
        self.assertEqual(a.delete("new-0"), 4)
        self.assertEqual(b.scan(), [("new-1", "1"), ("new-2", "2")])
        self.assertEqual(a.stats(), {"pages": 1, "records": 4, "keys": 2})
        # deleting a key that only existed before the reset still takes seq 1
        c = PageStore(self.root)
        c.init()
        self.assertEqual(c.delete("old-0"), 1)
        self.assertIsNone(PageStore(self.root).get("old-0"))

    def test_init_clears_and_leaves_no_scratch_file(self) -> None:
        for i in range(4):
            self.init_store.put(f"k{i}", "v" * 50)
        self.init_store.init()
        self.assertEqual(self.data, b"")
        self.assertFalse((self.root / ".pages.dat.init.tmp").exists())
        self.assertEqual(self.init_store.stats(),
                         {"pages": 0, "records": 0, "keys": 0})
        self.assertEqual(self.init_store.verify()["status"], "ok")

    def test_init_creates_missing_dirs_and_rebuilds_bare_dir(self) -> None:
        nested = self.root / "deep" / "nested"
        store = PageStore(nested)
        store.init()  # the only operation allowed to create paths
        self.assertEqual(store.put("a", "1"), 1)
        self.assertEqual(store.get("a"), "1")

        bare = self.root / "bare"
        bare.mkdir()
        other = PageStore(bare)
        other.init()  # directory exists but pages.dat did not
        self.assertEqual(other.stats(), {"pages": 0, "records": 0, "keys": 0})
        self.assertEqual(other.put("a", "1"), 1)


class ResetSnapshotTests(ResetTestBase):
    def test_snapshot_before_reset_is_frozen_after_reset_reflects_new_state(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        a.put("old", "1")
        a.put("gone", "2")
        before = a.snapshot()
        b.init()
        middle = a.snapshot()
        self.assertEqual(middle.stats(), {"pages": 0, "records": 0, "keys": 0})
        self.assertEqual(middle.scan(), [])
        b.put("new", "9")
        after = a.snapshot()

        # pre-reset capture keeps the pre-reset key/values and stats
        self.assertEqual(before.scan(), [("gone", "2"), ("old", "1")])
        self.assertEqual(before.stats(), {"pages": 1, "records": 2, "keys": 2})
        self.assertEqual(before.get("old"), "1")
        # post-reset capture reflects the new state
        self.assertEqual(after.scan(), [("new", "9")])
        self.assertEqual(after.stats(), {"pages": 1, "records": 1, "keys": 1})
        self.assertIsNone(after.get("old"))
        self.assertEqual(after.get("new"), "9")

    def test_compact_after_reset_compacts_only_live_keys(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        a.put("old", "1")
        a.put("old", "2")
        b.init()
        b.put("a", "1")
        b.put("a", "one")
        b.put("b", "2")
        b.delete("b")
        result = a.compact()
        self.assertEqual(result["records_before"], 4)
        self.assertEqual(result["records_after"], 1)
        self.assertEqual(result["keys"], 1)
        self.assertEqual(frames(self.data),
                         [{"op": "put", "key": "a", "value": "one"}])
        self.assertIsNone(a.get("old"))
        # appends number from the post-compaction record count
        self.assertEqual(a.put("c", "3"), 2)
        self.assertEqual(PageStore(self.root).scan(),
                         [("a", "one"), ("c", "3")])

    def test_recover_and_half_written_tail_after_reset(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        a.put("old", "1")
        b.init()
        b.put("a", "1")
        tail = put_record("b", "2")
        (self.root / "pages.dat").write_bytes(self.data + tail[:len(tail) - 2])
        # the half-written tail stays invisible through the stale instance
        self.assertIsNone(a.get("b"))
        outcome = a.recover()
        self.assertTrue(outcome["truncated"])
        self.assertEqual(outcome["records"], 1)
        self.assertIsNone(a.get("old"))
        self.assertEqual(a.put("c", "3"), 2)

    def test_middle_corruption_after_reset_still_raises_and_preserves_file(self) -> None:
        a, b = PageStore(self.root), PageStore(self.root)
        a.put("old", "1")
        b.init()
        blob = (put_record("a", "1") + b"\x00\x00\x00\x09broken"
                + put_record("b", "2"))
        (self.root / "pages.dat").write_bytes(blob)
        with self.assertRaises(RuntimeError) as caught:
            a.put("c", "3")
        self.assertEqual(str(caught.exception), "corrupt_middle")
        self.assertEqual(self.data, blob)
        with self.assertRaises(RuntimeError):
            a.compact()
        with self.assertRaises(RuntimeError):
            a.recover()
        self.assertEqual(self.data, blob)


class ResetCrossProcessTests(ResetTestBase):
    def test_init_then_put_in_other_processes_is_seen_by_live_instance(self) -> None:
        live = PageStore(self.root)
        live.put("old", "small")
        self.assertEqual(live.get("old"), "small")
        pre_len = len(self.data)

        init_proc = subprocess.run(
            [sys.executable, "-c", INIT_WORKER, str(self.root)],
            cwd=REPO_ROOT, env={**os.environ, "PYTHONPATH": ENVPATH},
            capture_output=True, text=True, timeout=30)
        self.assertEqual(init_proc.returncode, 0, init_proc.stderr)
        put_proc = self.run_cli("put", "new", "x" * 200)
        self.assertEqual(put_proc.returncode, 0, put_proc.stderr)
        self.assertEqual(int(put_proc.stdout.strip()), 1)
        self.assertGreaterEqual(len(self.data), pre_len)

        self.assertIsNone(live.get("old"))
        self.assertEqual(live.get("new"), "x" * 200)
        self.assertEqual(live.scan(), PageStore(self.root).scan())
        self.assertEqual(live.stats(),
                         {"pages": 1, "records": 1, "keys": 1})

    def test_cli_init_output_and_exit_code_unchanged(self) -> None:
        self.init_store.put("a", "1")
        result = self.run_cli("init")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, f"initialised {self.root / 'pages.dat'}\n")
        self.assertEqual(result.stderr, "")
        self.assertEqual(self.data, b"")
        stats = self.run_cli("stats")
        self.assertEqual(json.loads(stats.stdout),
                         {"keys": 0, "pages": 0, "records": 0})

    def test_concurrent_init_put_and_reads_observe_complete_states_only(self) -> None:
        n_eras, keys_per_era = 12, 5
        stop = threading.Event()
        errors: list[BaseException] = []

        def resetter() -> None:
            local = PageStore(self.root)
            try:
                for era in range(n_eras):
                    local.init()
                    for i in range(keys_per_era):
                        local.put(f"e{era:02d}:k{i:02d}", f"{era}:{i}")
            except BaseException as error:  # noqa: BLE001
                errors.append(error)
            finally:
                stop.set()

        def reader() -> None:
            local = PageStore(self.root)
            pattern = re.compile(r"^e(\d{2}):k(\d{2})$")
            try:
                while not stop.is_set():
                    # each individually locked read is one complete serial state
                    items = local.scan()
                    eras = {m.group(1) for k, _ in items
                            if (m := pattern.match(k))}
                    self.assertLessEqual(len(eras), 1)  # no mixing across resets
                    for key, value in items:
                        m = pattern.match(key)
                        self.assertIsNotNone(m)
                        era, i = int(m.group(1)), int(m.group(2))  # type: ignore[union-attr]
                        self.assertEqual(value, f"{era}:{i}")
                    # a snapshot is an internally consistent complete state
                    snap = local.snapshot()
                    self.assertEqual(sorted(snap.scan()), snap.scan())
                    stats = snap.stats()
                    self.assertEqual(stats["keys"], len(snap.scan()))
                    self.assertEqual(stats["records"], stats["keys"])
                    self.assertEqual(stats["pages"], 1 if stats["records"] else 0)
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        thread = threading.Thread(target=resetter)
        readers = [threading.Thread(target=reader) for _ in range(3)]
        thread.start()
        for t in readers:
            t.start()
        thread.join(timeout=30)
        stop.set()
        for t in readers:
            t.join(timeout=30)
        self.assertEqual(errors, [])
        # the final state is exactly the last era, agreed by old and new instances
        expected = [(f"e{n_eras - 1:02d}:k{i:02d}",
                     f"{n_eras - 1}:{i}") for i in range(keys_per_era)]
        self.assertEqual(PageStore(self.root).scan(), expected)


if __name__ == "__main__":
    unittest.main()
