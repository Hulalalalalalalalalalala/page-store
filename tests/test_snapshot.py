"""Tests for PageStore.snapshot stable read views."""

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


class SnapshotIsolationTests(SnapshotTestBase):
    def test_get_scan_reflect_capture_time(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        snap = self.store.snapshot()
        self.assertIsInstance(snap, Snapshot)
        self.assertEqual(snap.get("a"), "1")
        self.assertEqual(snap.get("b"), "2")
        self.assertIsNone(snap.get("c"))
        self.assertEqual(snap.scan(), [("a", "1"), ("b", "2")])

    def test_later_puts_and_deletes_do_not_reach_snapshot(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        snap = self.store.snapshot()
        self.store.put("a", "9")
        self.store.put("c", "3")
        self.store.delete("b")
        # live view moved on...
        self.assertIsNone(self.store.get("b"))
        self.assertEqual(self.store.get("a"), "9")
        self.assertEqual(self.store.scan(), [("a", "9"), ("c", "3")])
        # ...the snapshot did not
        self.assertEqual(snap.get("a"), "1")
        self.assertEqual(snap.get("b"), "2")
        self.assertIsNone(snap.get("c"))
        self.assertEqual(snap.scan(), [("a", "1"), ("b", "2")])

    def test_deleted_and_unknown_keys_return_none(self) -> None:
        self.store.put("a", "1")
        self.store.delete("a")
        snap = self.store.snapshot()
        self.assertIsNone(snap.get("a"))
        self.assertIsNone(snap.get("missing"))
        self.assertEqual(snap.scan(), [])

    def test_repeated_reads_are_stable_and_unordered(self) -> None:
        self.store.put("a", "1")
        snap = self.store.snapshot()
        for _ in range(3):
            self.assertEqual(snap.get("a"), "1")
            self.assertEqual(snap.scan(), [("a", "1")])
            self.assertEqual(snap.stats(), {"pages": 1, "records": 1, "keys": 1})
            self.store.put("a", "x")
            self.store.delete("a")

    def test_recover_does_not_change_snapshot(self) -> None:
        self.store.put("a", "1")
        snap = self.store.snapshot()
        # append a half-written tail, then recover truncates it
        with self.store.path.open("ab") as handle:
            handle.write(b"\x00\x00")
        self.store.recover()
        self.store.put("b", "2")
        self.assertEqual(snap.get("a"), "1")
        self.assertIsNone(snap.get("b"))
        self.assertEqual(snap.scan(), [("a", "1")])
        self.assertEqual(snap.stats(), {"pages": 1, "records": 1, "keys": 1})


class SnapshotRangeTests(SnapshotTestBase):
    def seed(self) -> None:
        for k, v in [("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")]:
            self.store.put(k, v)

    def test_scan_bounds(self) -> None:
        self.seed()
        snap = self.store.snapshot()
        self.assertEqual(snap.scan(start="b"), [("b", "2"), ("c", "3"), ("d", "4")])
        self.assertEqual(snap.scan(end="c"), [("a", "1"), ("b", "2")])
        self.assertEqual(snap.scan(start="b", end="d"), [("b", "2"), ("c", "3")])
        self.assertEqual(snap.scan(start="a", end="a"), [])
        self.assertEqual(snap.scan(start="c", end="a"), [])
        self.assertEqual(snap.scan(start="zzz"), [])
        self.assertEqual(snap.scan(end=""), [])


class SnapshotStatsTests(SnapshotTestBase):
    def test_stats_match_live_counters_at_capture(self) -> None:
        self.store.put("a", "1")
        self.store.put("a", "2")
        self.store.put("b", "3")
        self.store.delete("a")
        snap = self.store.snapshot()
        self.assertEqual(snap.stats(), {"pages": 1, "records": 4, "keys": 1})
        self.store.put("c", "4")
        self.assertEqual(snap.stats(), {"pages": 1, "records": 4, "keys": 1})
        self.assertEqual(dict(snap.stats()), snap.stats())  # fresh copy, safe to mutate

    def test_empty_store(self) -> None:
        snap = self.store.snapshot()
        self.assertEqual(snap.stats(), {"pages": 0, "records": 0, "keys": 0})
        self.assertEqual(snap.scan(), [])
        self.assertIsNone(snap.get("anything"))


class SnapshotHalfWrittenTailTests(SnapshotTestBase):
    def test_snapshot_excludes_half_written_tail(self) -> None:
        head = put_record("a", "1") + put_record("b", "2")
        tail = put_record("c", "3")
        self.store.path.write_bytes(head + tail[:len(tail) - 3])
        snap = self.store.snapshot()
        self.assertEqual(snap.get("c"), None)
        self.assertEqual(snap.scan(), [("a", "1"), ("b", "2")])
        self.assertEqual(snap.stats()["records"], 2)
        # no mutation as a side effect
        self.assertEqual(self.store.path.read_bytes(), head + tail[:len(tail) - 3])


class SnapshotPathTests(SnapshotTestBase):
    def test_missing_root_raises(self) -> None:
        with self.assertRaises(FileNotFoundError):
            PageStore(self.root / "nope").snapshot()

    def test_root_is_a_file_raises(self) -> None:
        target = self.root / "afile"
        target.write_bytes(b"")
        with self.assertRaises(FileNotFoundError):
            PageStore(target).snapshot()

    def test_missing_pages_dat_raises_without_creating_it(self) -> None:
        bare = self.root / "empty"
        bare.mkdir()
        store = PageStore(bare)
        with self.assertRaises(FileNotFoundError):
            store.snapshot()
        self.assertFalse((bare / "pages.dat").exists())


class CliFrozenTests(SnapshotTestBase):
    """New API only; existing CLI output stays byte-identical."""

    def run_cli(self, *argv: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "page_store", "--root", str(self.root), *argv],
            cwd=REPO_ROOT, capture_output=True, text=True)

    def test_report_still_declares_snapshot_read_false(self) -> None:
        result = self.run_cli("report")
        self.assertEqual(result.returncode, 0)
        report = json.loads(result.stdout)
        self.assertFalse(report["readiness"]["snapshotRead"])

    def test_stats_unchanged(self) -> None:
        self.store.put("a", "1")
        self.store.delete("a")
        result = self.run_cli("stats")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(json.loads(result.stdout), {"keys": 0, "pages": 1, "records": 2})


if __name__ == "__main__":
    unittest.main()
