"""Tests for PageStore.snapshot read-only point-in-time views."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from page_store.core import PageStore, Snapshot  # noqa: E402


def encode(record: dict) -> bytes:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


def put_record(key: str, value: str) -> bytes:
    return encode({"op": "put", "key": key, "value": value})


class SnapshotTestBase(unittest.TestCase):
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


class SnapshotContentsTests(SnapshotTestBase):
    def test_snapshot_type_and_get(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        snap = self.store.snapshot()
        self.assertIsInstance(snap, Snapshot)
        self.assertEqual(snap.get("a"), "1")
        self.assertEqual(snap.get("b"), "2")
        self.assertIsNone(snap.get("missing"))

    def test_last_put_wins_and_deletes_are_none(self) -> None:
        self.store.put("a", "1")
        self.store.put("a", "2")
        self.store.put("gone", "x")
        self.store.delete("gone")
        snap = self.store.snapshot()
        self.assertEqual(snap.get("a"), "2")
        self.assertIsNone(snap.get("gone"))
        self.assertIsNone(snap.get("never"))

    def test_scan_is_sorted_and_half_open(self) -> None:
        for key, value in [("c", "3"), ("a", "1"), ("b", "2"), ("d", "4")]:
            self.store.put(key, value)
        snap = self.store.snapshot()
        self.assertEqual(snap.scan(),
                         [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")])
        self.assertEqual(snap.scan(start="b"),
                         [("b", "2"), ("c", "3"), ("d", "4")])  # start inclusive
        self.assertEqual(snap.scan(end="c"),
                         [("a", "1"), ("b", "2")])  # end exclusive
        self.assertEqual(snap.scan(start="b", end="d"),
                         [("b", "2"), ("c", "3")])
        self.assertEqual(snap.scan(start="d", end="d"), [])
        self.assertEqual(snap.scan(start="z", end="a"), [])
        self.assertEqual(snap.scan(start="z"), [])
        self.assertEqual(snap.scan(end="a"), [])

    def test_empty_store_snapshot(self) -> None:
        snap = self.store.snapshot()
        self.assertIsNone(snap.get("anything"))
        self.assertEqual(snap.scan(), [])
        self.assertEqual(snap.stats(), {"pages": 0, "records": 0, "keys": 0})


class SnapshotIsolationTests(SnapshotTestBase):
    def test_later_puts_and_deletes_do_not_mix_in(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        snap = self.store.snapshot()
        self.store.put("c", "3")
        self.store.put("a", "updated")
        self.store.delete("b")
        # the live view moves on...
        self.assertEqual(self.store.get("a"), "updated")
        self.assertIsNone(self.store.get("b"))
        self.assertEqual(self.store.scan(), [("a", "updated"), ("c", "3")])
        # ...the snapshot stays fixed
        self.assertEqual(snap.get("a"), "1")
        self.assertEqual(snap.get("b"), "2")
        self.assertIsNone(snap.get("c"))
        self.assertEqual(snap.scan(), [("a", "1"), ("b", "2")])
        self.assertEqual(snap.stats(), {"pages": 1, "records": 2, "keys": 2})

    def test_stats_stable_across_writes_and_repeated_reads(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        self.store.delete("a")
        snap = self.store.snapshot()
        first = snap.stats()
        for _ in range(3):
            self.store.put("a", "x")
            self.store.put("k", "v")
            self.store.delete("k")
        # stats must remain equal and independent objects
        self.assertEqual(snap.stats(), {"pages": 1, "records": 3, "keys": 1})
        self.assertEqual(snap.stats(), first)
        self.assertIsNot(snap.stats(), first)
        # methods are order-independent and repeatable
        self.assertEqual(snap.scan(), snap.scan())
        self.assertEqual(snap.get("b"), snap.get("b"))

    def test_recover_after_snapshot_leaves_snapshot_untouched(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        snap = self.store.snapshot()
        # append a half-written tail and recover the live store
        tail = put_record("c", "3")
        self.write(self.data + tail[:len(tail) - 3])
        outcome = self.store.recover()
        self.assertTrue(outcome["truncated"])
        self.store.put("d", "4")
        self.assertEqual(snap.get("a"), "1")
        self.assertEqual(snap.get("b"), "2")
        self.assertIsNone(snap.get("c"))
        self.assertIsNone(snap.get("d"))
        self.assertEqual(snap.scan(), [("a", "1"), ("b", "2")])
        self.assertEqual(snap.stats(), {"pages": 1, "records": 2, "keys": 2})

    def test_snapshot_discards_half_written_tail_at_capture(self) -> None:
        head = put_record("a", "1") + put_record("b", "2")
        tail = put_record("c", "3")
        self.write(head + tail[:len(tail) - 2])
        snap = self.store.snapshot()
        self.assertIsNone(snap.get("c"))
        self.assertEqual(snap.scan(), [("a", "1"), ("b", "2")])
        self.assertEqual(snap.stats()["records"], 2)

    def test_two_snapshots_are_independent(self) -> None:
        self.store.put("a", "1")
        old = self.store.snapshot()
        self.store.put("a", "2")
        new = self.store.snapshot()
        self.assertEqual(old.get("a"), "1")
        self.assertEqual(new.get("a"), "2")
        self.assertEqual(old.scan(), [("a", "1")])
        self.assertEqual(new.scan(), [("a", "2")])


class SnapshotErrorTests(SnapshotTestBase):
    def test_missing_root_raises(self) -> None:
        with self.assertRaises(FileNotFoundError):
            PageStore(self.root / "nope").snapshot()

    def test_root_pointing_at_file_raises(self) -> None:
        target = self.root / "a-file"
        target.write_bytes(b"")
        with self.assertRaises(FileNotFoundError):
            PageStore(target).snapshot()

    def test_missing_pages_dat_raises(self) -> None:
        # root exists as a directory but pages.dat was never created
        bare = self.root / "bare"
        bare.mkdir()
        with self.assertRaises(FileNotFoundError):
            PageStore(bare).snapshot()

    def test_snapshot_creates_no_files(self) -> None:
        missing = self.root / "nope"
        with self.assertRaises(FileNotFoundError):
            PageStore(missing).snapshot()
        self.assertFalse(missing.exists())


class SnapshotCliRegressionTests(SnapshotTestBase):
    def run_cli(self, *argv: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "page_store", "--root", str(self.root), *argv],
            cwd=REPO_ROOT, capture_output=True, text=True)

    def test_no_snapshot_subcommand_and_outputs_unchanged(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        self.assertEqual(self.run_cli("get", "a").stdout, "1\n")
        scan = self.run_cli("scan")
        self.assertEqual(json.loads(scan.stdout), [["a", "1"], ["b", "2"]])
        stats = self.run_cli("stats")
        self.assertEqual(json.loads(stats.stdout),
                         {"keys": 2, "pages": 1, "records": 2})
        report = json.loads(self.run_cli("report").stdout)
        # snapshotRead is an API-only behaviour; the frozen report schema
        # and its readiness flags are unchanged
        self.assertFalse(report["readiness"]["snapshotRead"])
        bad = self.run_cli("snapshot")
        self.assertEqual(bad.returncode, 2)


if __name__ == "__main__":
    unittest.main()
