"""Tests for PageStore.recover crash-recovery boundaries and its CLI."""

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


class RecoverTestBase(unittest.TestCase):
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


class CorruptMiddleTests(RecoverTestBase):
    """Invalid regions followed by complete records must be preserved."""

    def build_corrupt_middle(self) -> bytes:
        # valid, garbage, valid, garbage, valid: two invalid regions,
        # each with complete records still recoverable afterwards.
        return (put_record("a", "1") + b"\x00\x00\x00\x09broken"
                + put_record("b", "2") + b"\xff\xfe garbage "
                + put_record("c", "3"))

    def test_recover_raises_and_preserves_file(self) -> None:
        blob = self.build_corrupt_middle()
        self.write(blob)
        with self.assertRaises(RuntimeError) as caught:
            self.store.recover()
        self.assertEqual(str(caught.exception), "corrupt_middle")
        self.assertEqual(self.data, blob)  # content and size untouched

    def test_verify_still_reports_corrupt_middle(self) -> None:
        blob = self.build_corrupt_middle()
        self.write(blob)
        outcome = self.store.verify()
        self.assertEqual(outcome["status"], "corrupt_middle")
        self.assertEqual(outcome["error"], "corrupt_middle")
        self.assertEqual(outcome["first_error_offset"], len(put_record("a", "1")))
        self.assertEqual(self.data, blob)  # verify stays read-only

    def test_cli_corrupt_middle(self) -> None:
        blob = self.build_corrupt_middle()
        self.write(blob)
        offset = len(put_record("a", "1"))
        result = subprocess.run(
            [sys.executable, "-m", "page_store", "--root", str(self.root), "recover"],
            cwd=REPO_ROOT, capture_output=True, text=True)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stdout, "")
        self.assertEqual(result.stderr, f"error: corrupt_middle at offset {offset}\n")
        self.assertEqual(self.data, blob)


class IncompleteTailTests(RecoverTestBase):
    """A half-written tail is still truncated exactly as before."""

    def test_truncates_half_written_tail(self) -> None:
        head = put_record("a", "1") + put_record("b", "2")
        tail = put_record("c", "3")
        self.write(head + tail[:len(tail) - 2])  # payload cut short
        outcome = self.store.recover()
        self.assertEqual(outcome, {"pages": 1, "records": 2, "truncated": True})
        self.assertEqual(self.data, head)

    def test_truncates_partial_length_prefix(self) -> None:
        head = put_record("a", "1")
        self.write(head + b"\x00\x00")  # fewer than 4 prefix bytes remain
        outcome = self.store.recover()
        self.assertEqual(outcome, {"pages": 1, "records": 1, "truncated": True})
        self.assertEqual(self.data, head)

    def test_append_after_truncate_continues_after_last_record(self) -> None:
        head = put_record("a", "1")
        self.write(head + b"\x00\x00\x00\x40short")
        self.store.recover()
        self.store.put("b", "2")
        self.assertEqual(self.data, head + put_record("b", "2"))
        self.assertEqual(self.store.scan(), [("a", "1"), ("b", "2")])


class CleanFileTests(RecoverTestBase):
    def test_no_tail_returns_not_truncated_and_leaves_file(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        before = self.data
        outcome = self.store.recover()
        self.assertEqual(outcome, {"pages": 1, "records": 2, "truncated": False})
        self.assertEqual(self.data, before)

    def test_empty_file(self) -> None:
        outcome = self.store.recover()
        self.assertEqual(outcome, {"pages": 0, "records": 0, "truncated": False})
        self.assertEqual(self.data, b"")


class CliRegressionTests(RecoverTestBase):
    def run_cli(self, *argv: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "page_store", "--root", str(self.root), *argv],
            cwd=REPO_ROOT, capture_output=True, text=True)

    def test_cli_recover_clean_and_tail(self) -> None:
        self.store.put("a", "1")
        clean = self.run_cli("recover")
        self.assertEqual(clean.returncode, 0)
        self.assertEqual(json.loads(clean.stdout),
                         {"pages": 1, "records": 1, "truncated": False})
        with self.store.path.open("ab") as handle:
            handle.write(b"\x00\x00")
        tailed = self.run_cli("recover")
        self.assertEqual(tailed.returncode, 0)
        self.assertEqual(json.loads(tailed.stdout),
                         {"pages": 1, "records": 1, "truncated": True})

    def test_cli_verify_exit_codes_unchanged(self) -> None:
        self.store.put("a", "1")
        ok = self.run_cli("verify")
        self.assertEqual(ok.returncode, 0)
        self.assertEqual(json.loads(ok.stdout)["status"], "ok")
        corrupt = put_record("a", "1") + b"junk" + put_record("b", "2")
        self.write(corrupt)
        bad = self.run_cli("verify")
        self.assertEqual(bad.returncode, 4)
        self.assertEqual(json.loads(bad.stdout)["status"], "corrupt_middle")
        missing = subprocess.run(
            [sys.executable, "-m", "page_store", "--root",
             str(self.root / "nope"), "verify"],
            cwd=REPO_ROOT, capture_output=True, text=True)
        self.assertEqual(missing.returncode, 2)


if __name__ == "__main__":
    unittest.main()
