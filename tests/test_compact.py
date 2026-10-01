"""Tests for PageStore.compact page-file compaction and its CLI."""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from page_store.core import PageStore  # noqa: E402


def encode(record: dict) -> bytes:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


def put_record(key: str, value: str) -> bytes:
    return encode({"op": "put", "key": key, "value": value})


def delete_record(key: str) -> bytes:
    return encode({"op": "delete", "key": key})


class CompactTestBase(unittest.TestCase):
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


class CompactContentsTests(CompactTestBase):
    def test_rewrites_live_keys_sorted_and_minimal(self) -> None:
        self.store.put("c", "3")
        self.store.put("a", "1")
        self.store.put("b", "2")
        self.store.put("a", "old")  # overwritten below
        self.store.put("a", "new")
        self.store.put("gone", "x")
        self.store.delete("gone")
        outcome = self.store.compact()
        self.assertEqual(outcome, {"pages_before": 1, "pages_after": 1,
                                   "records_before": 7, "records_after": 3,
                                   "keys": 3, "discarded_tail_bytes": 0})
        expected = put_record("a", "new") + put_record("b", "2") + put_record("c", "3")
        self.assertEqual(self.data, expected)
        self.assertEqual(self.store.scan(),
                         [("a", "new"), ("b", "2"), ("c", "3")])

    def test_empty_store_returns_all_zeros(self) -> None:
        outcome = self.store.compact()
        self.assertEqual(outcome, {"pages_before": 0, "pages_after": 0,
                                   "records_before": 0, "records_after": 0,
                                   "keys": 0, "discarded_tail_bytes": 0})
        self.assertEqual(self.data, b"")

    def test_same_live_state_gives_same_bytes(self) -> None:
        other_root = self.root / "other"
        other = PageStore(other_root)
        other.init()
        # different histories, same live state
        self.store.put("a", "1")
        self.store.put("b", "2")
        self.store.delete("b")
        self.store.put("b", "2")
        other.put("b", "2")
        other.put("a", "1")
        self.store.compact()
        other.compact()
        self.assertEqual(self.data, other.path.read_bytes())
        self.assertEqual(self.data, put_record("a", "1") + put_record("b", "2"))

    def test_compact_is_idempotent(self) -> None:
        self.store.put("a", "1")
        self.store.put("a", "2")
        self.store.compact()
        first = self.data
        outcome = self.store.compact()
        self.assertEqual(self.data, first)
        self.assertEqual(outcome["records_before"], 1)
        self.assertEqual(outcome["records_after"], 1)

    def test_discards_half_written_tail(self) -> None:
        head = put_record("a", "1") + put_record("b", "2")
        tail = put_record("c", "3")
        cut = tail[:len(tail) - 2]
        self.write(head + cut)
        outcome = self.store.compact()
        self.assertEqual(outcome["records_before"], 2)
        self.assertEqual(outcome["discarded_tail_bytes"], len(cut))
        self.assertEqual(self.data, put_record("a", "1") + put_record("b", "2"))
        self.assertIsNone(self.store.get("c"))

    def test_no_leftover_temporary_file(self) -> None:
        self.store.put("a", "1")
        self.store.compact()
        self.assertEqual(sorted(p.name for p in self.root.iterdir()), ["pages.dat"])


class CompactStateTests(CompactTestBase):
    """Every read path sees the compacted state afterwards."""

    def setUp(self) -> None:
        super().setUp()
        self.store.put("b", "2")
        self.store.put("a", "1")
        self.store.put("a", "new")
        self.store.delete("b")
        self.store.put("c", "3")
        self.outcome = self.store.compact()

    def test_get_and_scan(self) -> None:
        self.assertEqual(self.store.get("a"), "new")
        self.assertIsNone(self.store.get("b"))
        self.assertEqual(self.store.scan(), [("a", "new"), ("c", "3")])

    def test_snapshot(self) -> None:
        snap = self.store.snapshot()
        self.assertEqual(snap.scan(), [("a", "new"), ("c", "3")])
        self.assertEqual(snap.stats(), {"pages": 1, "records": 2, "keys": 2})

    def test_stats(self) -> None:
        self.assertEqual(self.store.stats(),
                         {"pages": 1, "records": 2, "keys": 2})

    def test_recover_is_clean(self) -> None:
        outcome = self.store.recover()
        self.assertEqual(outcome, {"pages": 1, "records": 2, "truncated": False})

    def test_verify_is_ok(self) -> None:
        outcome = self.store.verify()
        self.assertEqual(outcome["status"], "ok")
        self.assertEqual(outcome["complete_records"], 2)

    def test_sequence_continues_from_new_record_count(self) -> None:
        self.assertEqual(self.store.put("d", "4"), 3)
        self.assertEqual(self.store.delete("a"), 4)
        self.assertEqual(self.store.scan(), [("c", "3"), ("d", "4")])


class CompactCorruptMiddleTests(CompactTestBase):
    def build_corrupt_middle(self) -> bytes:
        return (put_record("a", "1") + b"\x00\x00\x00\x09broken"
                + put_record("b", "2"))

    def test_raises_and_preserves_file(self) -> None:
        blob = self.build_corrupt_middle()
        self.write(blob)
        with self.assertRaises(RuntimeError) as caught:
            self.store.compact()
        self.assertEqual(str(caught.exception), "corrupt_middle")
        self.assertEqual(self.data, blob)  # content and size untouched

    def test_cli_corrupt_middle(self) -> None:
        blob = self.build_corrupt_middle()
        self.write(blob)
        offset = len(put_record("a", "1"))
        result = self.run_cli("compact")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, f"error: corrupt_middle at offset {offset}\n")
        self.assertEqual(self.data, blob)


class CompactPathErrorTests(CompactTestBase):
    def test_missing_root_raises(self) -> None:
        with self.assertRaises(FileNotFoundError):
            PageStore(self.root / "nope").compact()

    def test_root_pointing_at_file_raises(self) -> None:
        target = self.root / "a-file"
        target.write_bytes(b"")
        with self.assertRaises(FileNotFoundError):
            PageStore(target).compact()

    def test_missing_pages_dat_raises(self) -> None:
        bare = self.root / "bare"
        bare.mkdir()
        with self.assertRaises(FileNotFoundError):
            PageStore(bare).compact()

    def test_cli_missing_store(self) -> None:
        missing = self.root / "nope"
        result = subprocess.run(
            [sys.executable, "-m", "page_store", "--root", str(missing), "compact"],
            cwd=REPO_ROOT, capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr,
                         f"error: no store at {missing / 'pages.dat'}\n")
        self.assertFalse(missing.exists())  # compact creates nothing


class CompactCliTests(CompactTestBase):
    def test_cli_success_single_json_line(self) -> None:
        self.store.put("b", "2")
        self.store.put("a", "1")
        self.store.delete("b")
        result = self.run_cli("compact")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stderr, "")
        self.assertEqual(result.stdout.count("\n"), 1)
        self.assertEqual(json.loads(result.stdout),
                         {"pages_before": 1, "pages_after": 1,
                          "records_before": 3, "records_after": 1,
                          "keys": 1, "discarded_tail_bytes": 0})
        # fixed field order: pages, records, keys, discarded tail bytes
        self.assertEqual(list(json.loads(result.stdout).keys()),
                         ["pages_before", "pages_after",
                          "records_before", "records_after",
                          "keys", "discarded_tail_bytes"])

    def test_cli_empty_store_all_zeros(self) -> None:
        result = self.run_cli("compact")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(json.loads(result.stdout),
                         {"pages_before": 0, "pages_after": 0,
                          "records_before": 0, "records_after": 0,
                          "keys": 0, "discarded_tail_bytes": 0})

    def test_report_readiness_compaction_true(self) -> None:
        report = json.loads(self.run_cli("report").stdout)
        self.assertTrue(report["readiness"]["compaction"])
        self.assertFalse(report["readiness"]["snapshotRead"])
        self.assertEqual(report["readiness"],
                         {"append": True, "recover": True,
                          "compaction": True, "snapshotRead": False})

    def test_existing_commands_unchanged_after_compact(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        self.store.compact()
        self.assertEqual(self.run_cli("get", "a").stdout, "1\n")
        scan = self.run_cli("scan")
        self.assertEqual(json.loads(scan.stdout), [["a", "1"], ["b", "2"]])
        stats = self.run_cli("stats")
        self.assertEqual(json.loads(stats.stdout),
                         {"keys": 2, "pages": 1, "records": 2})
        recover = self.run_cli("recover")
        self.assertEqual(json.loads(recover.stdout),
                         {"pages": 1, "records": 2, "truncated": False})
        verify = self.run_cli("verify")
        self.assertEqual(verify.returncode, 0)
        self.assertEqual(json.loads(verify.stdout)["status"], "ok")

    def test_usage_errors_still_exit_2(self) -> None:
        self.assertEqual(self.run_cli("bogus").returncode, 2)
        self.assertEqual(self.run_cli("put", "only-key").returncode, 2)


if __name__ == "__main__":
    unittest.main()
