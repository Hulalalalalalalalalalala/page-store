"""Tests for PageStore.compact live-key compaction and its CLI."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from page_store.core import PAGE_SIZE, PageStore  # noqa: E402


def encode(record: dict) -> bytes:
    payload = json.dumps(record, sort_keys=True).encode("utf-8")
    return len(payload).to_bytes(4, "big") + payload


def put_record(key: str, value: str) -> bytes:
    return encode({"op": "put", "key": key, "value": value})


def del_record(key: str) -> bytes:
    return encode({"op": "delete", "key": key})


def frames(data: bytes) -> list[dict]:
    out, offset = [], 0
    while offset < len(data):
        size = int.from_bytes(data[offset:offset + 4], "big")
        out.append(json.loads(data[offset + 4:offset + 4 + size].decode("utf-8")))
        offset += 4 + size
    return out


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


class CompactRewriteTests(CompactTestBase):
    def build_history(self) -> None:
        self.store.put("c", "3")
        self.store.put("a", "1")
        self.store.put("b", "old")
        self.store.put("a", "1new")
        self.store.put("b", "2")
        self.store.delete("c")
        self.store.put("a", "1")  # back to the original value

    def test_rewrites_as_ascending_puts_and_counts(self) -> None:
        self.build_history()
        before = self.data
        result = self.store.compact()
        expected_bytes = put_record("a", "1") + put_record("b", "2")
        self.assertEqual(self.data, expected_bytes)
        self.assertEqual(frames(self.data),
                         [{"op": "put", "key": "a", "value": "1"},
                          {"op": "put", "key": "b", "value": "2"}])
        self.assertEqual(result, {
            "pages_before": (len(before) + PAGE_SIZE - 1) // PAGE_SIZE,
            "pages_after": 1,
            "records_before": 7,
            "records_after": 2,
            "keys": 2,
            "discarded_tail_bytes": 0,
        })
        # no scratch file is left behind
        self.assertFalse((self.root / ".pages.dat.compact.tmp").exists())

    def test_large_state_page_counts_shrink(self) -> None:
        # many overwrites across several pages; the compacted image is smaller
        for round_ in range(8):
            for i in range(60):
                self.store.put(f"k{i:03d}", f"round{round_}-" + "x" * 40)
        pages_before = self.store.stats()["pages"]
        result = self.store.compact()
        new_size = len(self.data)
        self.assertEqual(result["pages_before"], pages_before)
        self.assertEqual(result["pages_after"],
                         (new_size + PAGE_SIZE - 1) // PAGE_SIZE)
        self.assertEqual(result["records_before"], 480)
        self.assertEqual(result["records_after"], 60)
        self.assertEqual(result["keys"], 60)
        self.assertLess(result["pages_after"], pages_before)

    def test_empty_store_returns_all_zero(self) -> None:
        result = self.store.compact()
        self.assertEqual(result, {
            "pages_before": 0, "pages_after": 0,
            "records_before": 0, "records_after": 0,
            "keys": 0, "discarded_tail_bytes": 0,
        })
        self.assertEqual(self.data, b"")

    def test_all_keys_deleted_compacts_to_empty_file(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        self.store.delete("a")
        self.store.delete("b")
        result = self.store.compact()
        self.assertEqual(self.data, b"")
        self.assertEqual(result["records_before"], 4)
        self.assertEqual(result["records_after"], 0)
        self.assertEqual(result["keys"], 0)
        self.assertEqual(result["pages_after"], 0)

    def test_half_written_tail_is_discarded(self) -> None:
        head = put_record("a", "1") + put_record("b", "2")
        tail = put_record("c", "3")
        self.write(head + tail[:len(tail) - 3])
        result = self.store.compact()
        self.assertEqual(self.data,
                         put_record("a", "1") + put_record("b", "2"))
        self.assertEqual(result["records_before"], 2)
        self.assertEqual(result["discarded_tail_bytes"], len(tail) - 3)

    def test_deterministic_bytes_for_same_live_state(self) -> None:
        self.build_history()
        first = self.store.compact()
        image = self.data

        other_root = self.root / "other"
        other = PageStore(other_root)
        other.init()
        # a completely different history that converges on the same live state
        other.put("b", "2")
        other.put("temp", "v")
        other.delete("temp")
        other.put("a", "one")
        other.put("a", "1")
        other_result = other.compact()
        self.assertEqual(other.path.read_bytes(), image)
        self.assertEqual(other_result["records_after"], first["records_after"])

    def test_repeated_compaction_is_idempotent(self) -> None:
        self.build_history()
        self.store.compact()
        image = self.data
        again = self.store.compact()
        self.assertEqual(self.data, image)
        self.assertEqual(again["records_before"], 2)
        self.assertEqual(again["records_after"], 2)
        self.assertEqual(again["discarded_tail_bytes"], 0)


class CompactStateAfterTests(CompactTestBase):
    def test_all_reads_see_new_state(self) -> None:
        self.store.put("a", "1")
        self.store.put("a", "old")
        self.store.put("b", "2")
        self.store.delete("b")
        self.store.put("c", "3")
        tail = put_record("d", "4")
        self.write(self.data + tail[:len(tail) - 2])
        self.store.compact()

        self.assertEqual(self.store.get("a"), "old")
        self.assertIsNone(self.store.get("b"))
        self.assertEqual(self.store.get("c"), "3")
        self.assertIsNone(self.store.get("d"))
        self.assertEqual(self.store.scan(), [("a", "old"), ("c", "3")])
        snap = self.store.snapshot()
        self.assertEqual(snap.scan(), [("a", "old"), ("c", "3")])
        self.assertEqual(snap.stats(), {"pages": 1, "records": 2, "keys": 2})
        self.assertEqual(self.store.stats(),
                         {"pages": 1, "records": 2, "keys": 2})
        recovery = self.store.recover()
        self.assertFalse(recovery["truncated"])
        self.assertEqual(recovery["records"], 2)
        outcome = self.store.verify()
        self.assertEqual(outcome["status"], "ok")
        self.assertIsNone(outcome["first_error_offset"])

    def test_sequence_numbers_continue_from_new_record_count(self) -> None:
        self.store.put("a", "1")
        self.store.put("x", "0")
        self.store.put("a", "2")
        self.store.delete("x")
        self.store.compact()  # one live key -> one record
        self.assertEqual(self.store.put("b", "3"), 2)
        self.assertEqual(self.store.delete("a"), 3)
        self.assertEqual(self.store.scan(), [("b", "3")])

    def test_sequence_after_tail_discard(self) -> None:
        self.store.put("a", "1")
        tail = put_record("b", "2")
        self.write(self.data + tail[:len(tail) - 2])
        self.store.compact()  # only one confirmed record survives
        self.assertEqual(self.store.put("c", "3"), 2)


class CompactCorruptionTests(CompactTestBase):
    def build_corrupt_middle(self) -> bytes:
        return (put_record("a", "1") + b"\x00\x00\x00\x09broken"
                + put_record("b", "2") + b"\xff\xfe garbage "
                + put_record("c", "3"))

    def test_corrupt_middle_raises_and_preserves_file(self) -> None:
        blob = self.build_corrupt_middle()
        self.write(blob)
        with self.assertRaises(RuntimeError) as caught:
            self.store.compact()
        self.assertEqual(str(caught.exception), "corrupt_middle")
        self.assertEqual(self.data, blob)  # byte-for-byte untouched

    def test_cli_corrupt_middle(self) -> None:
        blob = self.build_corrupt_middle()
        self.write(blob)
        offset = len(put_record("a", "1"))
        result = self.run_cli("compact")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr,
                         f"error: corrupt_middle at offset {offset}\n")
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

    def test_cli_no_store_messages(self) -> None:
        for name in ("nope", "bare"):
            target = self.root / name
            if name == "bare":
                target.mkdir()
            result = subprocess.run(
                [sys.executable, "-m", "page_store", "--root", str(target),
                 "compact"],
                cwd=REPO_ROOT, capture_output=True, text=True)
            self.assertEqual(result.returncode, 1)
            self.assertEqual(result.stdout, "")
            self.assertEqual(result.stderr,
                             f"error: no store at {target / 'pages.dat'}\n")

    @unittest.skipIf(hasattr(os, "geteuid") and os.geteuid() == 0,
                     "root bypasses permission bits")
    def test_cli_io_error(self) -> None:
        self.store.put("a", "1")
        os.chmod(self.store.path, 0)
        try:
            result = self.run_cli("compact")
        finally:
            os.chmod(self.store.path, 0o600)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, "error: io_error\n")


class CompactAtomicityTests(CompactTestBase):
    def test_replace_failure_leaves_complete_old_file(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        old = self.data
        with mock.patch("page_store.core.os.replace", side_effect=OSError("boom")):
            with self.assertRaises(OSError):
                self.store.compact()
        # complete old state only; the scratch file was cleaned up
        self.assertEqual(self.data, old)
        self.assertFalse((self.root / ".pages.dat.compact.tmp").exists())
        self.assertEqual(self.store.scan(), [("a", "1"), ("b", "2")])

    def test_write_failure_leaves_complete_old_file(self) -> None:
        self.store.put("a", "1")
        old = self.data
        # fsync of the new image fails before the swap: nothing is replaced
        with mock.patch("page_store.core.os.fsync", side_effect=OSError("boom")):
            with self.assertRaises(OSError):
                self.store.compact()
        self.assertEqual(self.data, old)
        self.assertFalse((self.root / ".pages.dat.compact.tmp").exists())


class CompactCliTests(CompactTestBase):
    def test_success_outputs_one_ordered_json_line(self) -> None:
        self.store.put("b", "2")
        self.store.put("a", "old")
        self.store.put("a", "1")
        result = self.run_cli("compact")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stderr, "")
        line = result.stdout.rstrip("\n")
        self.assertNotIn("\n", line)
        payload = json.loads(line)
        self.assertEqual(list(payload), [
            "pages_before", "pages_after", "records_before",
            "records_after", "keys", "discarded_tail_bytes"])
        self.assertEqual(payload, {
            "pages_before": 1, "pages_after": 1,
            "records_before": 3, "records_after": 2,
            "keys": 2, "discarded_tail_bytes": 0,
        })

    def test_empty_store_cli_all_zero(self) -> None:
        result = self.run_cli("compact")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(json.loads(result.stdout), {
            "pages_before": 0, "pages_after": 0,
            "records_before": 0, "records_after": 0,
            "keys": 0, "discarded_tail_bytes": 0,
        })

    def test_usage_error_still_exits_2(self) -> None:
        result = self.run_cli("bogus")
        self.assertEqual(result.returncode, 2)

    def test_report_flips_only_compaction_flag(self) -> None:
        report = json.loads(self.run_cli("report").stdout)
        self.assertTrue(report["readiness"]["compaction"])
        self.assertFalse(report["readiness"]["snapshotRead"])
        self.assertTrue(report["readiness"]["append"])
        self.assertTrue(report["readiness"]["recover"])


if __name__ == "__main__":
    unittest.main()
