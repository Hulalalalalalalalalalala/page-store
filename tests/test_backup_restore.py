"""Tests for PageStore.backup and PageStore.restore logical backup."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import threading
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


def image(items: list[list[str]], version: object = 1) -> bytes:
    return json.dumps({"version": version, "items": items}).encode("utf-8")


class BackupRestoreTestBase(unittest.TestCase):
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


class BackupContentsTests(BackupRestoreTestBase):
    def test_empty_store(self) -> None:
        blob = self.store.backup()
        self.assertIsInstance(blob, bytes)
        blob.decode("utf-8")  # must be valid UTF-8
        self.assertEqual(json.loads(blob), {"version": 1, "items": []})

    def test_top_level_shape_and_scan_order(self) -> None:
        self.store.put("c", "3")
        self.store.put("a", "1")
        self.store.put("b", "old")
        self.store.put("b", "2")
        self.store.delete("c")
        parsed = json.loads(self.store.backup())
        self.assertEqual(set(parsed), {"version", "items"})
        self.assertEqual(parsed["version"], 1)
        self.assertEqual(type(parsed["version"]), int)  # not a bool
        self.assertEqual(parsed["items"], [["a", "1"], ["b", "2"]])
        self.assertEqual([tuple(pair) for pair in parsed["items"]],
                         self.store.scan())

    def test_strings_preserved_verbatim(self) -> None:
        self.store.put("unicode", "héllo☃世界")
        self.store.put("escapes", "a\"b\\c\nd")
        self.store.put("empty", "")
        parsed = json.loads(self.store.backup())
        self.assertEqual(parsed["items"],
                         [["empty", ""], ["escapes", "a\"b\\c\nd"],
                          ["unicode", "héllo☃世界"]])

    def test_same_state_same_bytes(self) -> None:
        self.store.put("b", "2")
        self.store.put("temp", "v")
        self.store.delete("temp")
        self.store.put("a", "one")
        self.store.put("a", "1")
        first = self.store.backup()

        other = PageStore(self.root / "other")
        other.init()
        other.put("a", "1")
        other.put("b", "2")
        self.assertEqual(other.backup(), first)
        # a repeated capture is byte-identical too
        self.assertEqual(self.store.backup(), first)

    def test_only_live_keys_are_saved(self) -> None:
        self.store.put("dead", "x")
        self.store.put("a", "1")
        self.store.delete("dead")
        self.store.put("a", "2")
        parsed = json.loads(self.store.backup())
        self.assertEqual(parsed["items"], [["a", "2"]])


class BackupSerialPointTests(BackupRestoreTestBase):
    def test_half_written_tail_ignored_and_file_untouched(self) -> None:
        self.store.put("a", "1")
        head = self.data
        tail = put_record("b", "2")
        self.write(head + tail[:len(tail) - 3])
        before = self.data
        parsed = json.loads(self.store.backup())
        self.assertEqual(parsed["items"], [["a", "1"]])
        # not truncated, no sequence number consumed
        self.assertEqual(self.data, before)
        self.assertEqual(self.store.put("c", "3"), 2)

    def test_corrupt_middle_raises_and_preserves_file(self) -> None:
        blob = (put_record("a", "1") + b"\x00\x00\x00\x09broken"
                + put_record("b", "2"))
        self.write(blob)
        with self.assertRaises(RuntimeError) as caught:
            self.store.backup()
        self.assertEqual(str(caught.exception), "corrupt_middle")
        self.assertEqual(self.data, blob)

    def test_missing_root_raises_and_creates_nothing(self) -> None:
        missing = self.root / "nope"
        with self.assertRaises(FileNotFoundError):
            PageStore(missing).backup()
        self.assertFalse(missing.exists())

    def test_root_pointing_at_file_raises(self) -> None:
        target = self.root / "a-file"
        target.write_bytes(b"")
        with self.assertRaises(FileNotFoundError):
            PageStore(target).backup()

    def test_missing_pages_dat_raises(self) -> None:
        bare = self.root / "bare"
        bare.mkdir()
        with self.assertRaises(FileNotFoundError):
            PageStore(bare).backup()
        self.assertEqual(os.listdir(bare), [])


class RestoreTests(BackupRestoreTestBase):
    def test_round_trip_between_stores(self) -> None:
        self.store.put("b", "2")
        self.store.put("a", "1")
        self.store.put("gone", "x")
        self.store.delete("gone")
        blob = self.store.backup()

        other = PageStore(self.root / "other")
        other.init()
        other.put("old", "state")
        result = other.restore(blob)
        self.assertEqual(other.scan(), [("a", "1"), ("b", "2")])
        self.assertEqual(result, {"pages": 1, "records": 2, "keys": 2})
        self.assertEqual(result, other.stats())
        # a backup of the restored store reproduces the original bytes
        self.assertEqual(other.backup(), blob)

    def test_restore_replaces_and_never_merges(self) -> None:
        self.store.put("keep-out", "old")
        self.store.put("a", "stale")
        result = self.store.restore(image([["a", "new"], ["z", "26"]]))
        self.assertEqual(self.store.scan(), [("a", "new"), ("z", "26")])
        self.assertIsNone(self.store.get("keep-out"))
        self.assertEqual(result, {"pages": 1, "records": 2, "keys": 2})

    def test_restore_overwrites_half_written_tail(self) -> None:
        self.store.put("a", "1")
        tail = put_record("b", "2")
        self.write(self.data + tail[:len(tail) - 3])
        self.store.restore(image([["c", "3"]]))
        self.assertEqual(self.store.scan(), [("c", "3")])
        self.assertEqual(self.data, put_record("c", "3"))

    def test_restore_overwrites_corrupt_middle(self) -> None:
        blob = (put_record("a", "1") + b"\x00\x00\x00\x09broken"
                + put_record("b", "2"))
        self.write(blob)
        result = self.store.restore(image([["c", "3"], ["d", "4"]]))
        self.assertEqual(self.store.scan(), [("c", "3"), ("d", "4")])
        self.assertEqual(result["records"], 2)
        self.assertEqual(self.store.verify()["status"], "ok")

    def test_empty_items_clears_store(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        result = self.store.restore(image([]))
        self.assertEqual(result, {"pages": 0, "records": 0, "keys": 0})
        self.assertEqual(self.store.stats(),
                         {"pages": 0, "records": 0, "keys": 0})
        self.assertEqual(self.store.scan(), [])
        self.assertEqual(self.data, b"")
        self.assertEqual(self.store.put("n", "1"), 1)

    def test_sequence_continues_from_entry_count(self) -> None:
        self.store.put("old", "x")
        self.store.put("old2", "y")
        self.store.restore(image([["a", "1"], ["b", "2"], ["c", "3"]]))
        self.assertEqual(self.store.put("d", "4"), 4)
        self.assertEqual(self.store.delete("a"), 5)

    def test_unordered_items_accepted_and_written_sorted(self) -> None:
        self.store.restore(image([["b", "2"], ["a", "1"], ["c", "3"]]))
        self.assertEqual(self.store.scan(),
                         [("a", "1"), ("b", "2"), ("c", "3")])
        self.assertEqual(self.data, put_record("a", "1")
                         + put_record("b", "2") + put_record("c", "3"))

    def test_pages_computed_from_new_file_size(self) -> None:
        value = "x" * (PAGE_SIZE - 100)
        result = self.store.restore(
            image([["k1", value], ["k2", value], ["k3", value]]))
        size = len(self.data)
        self.assertGreater(size, PAGE_SIZE)
        self.assertEqual(result["pages"],
                         (size + PAGE_SIZE - 1) // PAGE_SIZE)
        self.assertEqual(result["records"], 3)
        self.assertEqual(result["keys"], 3)
        self.assertEqual(self.store.stats(), result)


class RestoreValidationTests(BackupRestoreTestBase):
    def assert_invalid(self, blob: object) -> None:
        before = self.data
        with self.assertRaises(ValueError, msg=repr(blob)):
            self.store.restore(blob)  # type: ignore[arg-type]
        # the page file is byte-for-byte untouched and no number consumed
        self.assertEqual(self.data, before)

    def test_non_bytes_rejected(self) -> None:
        self.store.put("a", "1")
        for bad in ('{"version":1,"items":[]}', 123, None,
                    bytearray(image([])), ["version"]):
            self.assert_invalid(bad)
        self.assertEqual(self.store.put("b", "2"), 2)

    def test_invalid_utf8_and_json_rejected(self) -> None:
        self.store.put("a", "1")
        self.assert_invalid(b"\xff\xfe")
        self.assert_invalid(b"{")
        self.assert_invalid(b'{"version":1,"items":[')
        self.assertEqual(self.store.put("b", "2"), 2)

    def test_duplicate_object_fields_rejected(self) -> None:
        self.store.put("a", "1")
        self.assert_invalid(b'{"version":1,"version":1,"items":[]}')
        self.assert_invalid(b'{"version":1,"items":[],"items":[]}')

    def test_top_level_shape_enforced(self) -> None:
        self.store.put("a", "1")
        self.assert_invalid(b"[]")
        self.assert_invalid(b"1")
        self.assert_invalid(b'"text"')
        self.assert_invalid(b"null")
        self.assert_invalid(b'{"version":1}')            # missing items
        self.assert_invalid(b'{"items":[]}')             # missing version
        self.assert_invalid(b'{"version":1,"items":[],"extra":0}')

    def test_version_must_be_integer_one(self) -> None:
        self.store.put("a", "1")
        for bad in (b'{"version":true,"items":[]}',
                    b'{"version":false,"items":[]}',
                    b'{"version":1.0,"items":[]}',
                    b'{"version":"1","items":[]}',
                    b'{"version":0,"items":[]}',
                    b'{"version":2,"items":[]}',
                    b'{"version":null,"items":[]}'):
            self.assert_invalid(bad)

    def test_items_must_be_an_array(self) -> None:
        self.store.put("a", "1")
        self.assert_invalid(b'{"version":1,"items":{}}')
        self.assert_invalid(b'{"version":1,"items":"a"}')
        self.assert_invalid(b'{"version":1,"items":1}')

    def test_entries_must_be_exact_string_pairs(self) -> None:
        self.store.put("a", "1")
        self.assert_invalid(image([["only-key"]]))
        self.assert_invalid(image([["a", "1", "extra"]]))
        self.assert_invalid(image(["not-a-pair"]))
        self.assert_invalid(image([[1, "v"]]))
        self.assert_invalid(image([["a", 1]]))
        self.assert_invalid(image([["a", None]]))
        self.assert_invalid(image([["", "v"]]))           # empty key
        self.assert_invalid(image([[None, "v"]]))

    def test_duplicate_keys_rejected(self) -> None:
        self.store.put("a", "1")
        self.assert_invalid(image([["k", "1"], ["k", "2"]]))
        self.assert_invalid(image([["k", "1"], ["k", "1"]]))

    def test_single_entry_size_limit(self) -> None:
        self.store.put("a", "1")
        # the same 4096-byte payload limit as a single put
        too_big = "x" * PAGE_SIZE
        self.assert_invalid(image([["k", too_big]]))
        # a value that still fits the limit is accepted
        fits = "x" * (PAGE_SIZE - 100)
        self.store.restore(image([["k", fits]]))
        self.assertEqual(self.store.get("k"), fits)

    def test_validation_precedes_target_access(self) -> None:
        # invalid input + missing store: ValueError wins, nothing created
        missing = self.root / "nope"
        with self.assertRaises(ValueError):
            PageStore(missing).restore(b"not json")
        self.assertFalse(missing.exists())

    def test_failed_restore_consumes_no_sequence_number(self) -> None:
        self.store.put("a", "1")
        with self.assertRaises(ValueError):
            self.store.restore(image([["k", "1"], ["k", "2"]]))
        self.assertEqual(self.store.put("b", "2"), 2)


class RestorePathErrorTests(BackupRestoreTestBase):
    def test_missing_root_raises_and_creates_nothing(self) -> None:
        missing = self.root / "nope"
        with self.assertRaises(FileNotFoundError):
            PageStore(missing).restore(image([]))
        self.assertFalse(missing.exists())

    def test_root_pointing_at_file_raises(self) -> None:
        target = self.root / "a-file"
        target.write_bytes(b"")
        with self.assertRaises(FileNotFoundError):
            PageStore(target).restore(image([]))

    def test_missing_pages_dat_raises(self) -> None:
        bare = self.root / "bare"
        bare.mkdir()
        with self.assertRaises(FileNotFoundError):
            PageStore(bare).restore(image([]))
        self.assertEqual(os.listdir(bare), [])


class RestoreAtomicityTests(BackupRestoreTestBase):
    def test_replace_failure_leaves_complete_old_file(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        old = self.data
        with mock.patch("page_store.core.os.replace",
                        side_effect=OSError("boom")):
            with self.assertRaises(OSError):
                self.store.restore(image([["c", "3"]]))
        self.assertEqual(self.data, old)
        self.assertFalse((self.root / ".pages.dat.restore.tmp").exists())
        self.assertEqual(self.store.scan(), [("a", "1"), ("b", "2")])
        self.assertEqual(self.store.put("c", "3"), 3)

    def test_write_failure_leaves_complete_old_file(self) -> None:
        self.store.put("a", "1")
        old = self.data
        with mock.patch("page_store.core.os.fsync",
                        side_effect=OSError("boom")):
            with self.assertRaises(OSError):
                self.store.restore(image([["c", "3"]]))
        self.assertEqual(self.data, old)
        self.assertFalse((self.root / ".pages.dat.restore.tmp").exists())
        self.assertEqual(self.store.scan(), [("a", "1")])


class RestoreVisibilityTests(BackupRestoreTestBase):
    def test_other_instance_sees_new_state_without_recover(self) -> None:
        other = PageStore(self.root)
        self.store.put("a", "1")
        self.assertEqual(other.scan(), [("a", "1")])  # warms its cache
        self.store.restore(image([["b", "2"], ["c", "3"]]))
        self.assertEqual(other.scan(), [("b", "2"), ("c", "3")])
        self.assertEqual(other.stats(),
                         {"pages": 1, "records": 2, "keys": 2})
        # and its next append numbers from the restored record count
        self.assertEqual(other.put("d", "4"), 3)

    def test_old_snapshot_is_unaffected(self) -> None:
        self.store.put("a", "1")
        snap = self.store.snapshot()
        self.store.restore(image([["b", "2"]]))
        self.assertEqual(snap.scan(), [("a", "1")])
        self.assertEqual(snap.stats(), {"pages": 1, "records": 1, "keys": 1})

    def test_restore_survives_reopen_and_fork_inherited_instance(self) -> None:
        self.store.restore(image([["a", "1"], ["b", "2"]]))
        if not hasattr(os, "fork"):
            self.skipTest("no os.fork")
        pid = os.fork()
        if pid == 0:
            try:
                # the inherited instance must see the restored state on
                # its next operation, with no recover
                ok = self.store.scan() == [("a", "1"), ("b", "2")]
                os._exit(0 if ok else 1)
            finally:
                os._exit(1)
        _, status = os.waitpid(pid, 0)
        self.assertEqual(status, 0)

    def test_restore_shares_serial_order_with_concurrent_puts(self) -> None:
        writer = PageStore(self.root)
        errors: list[BaseException] = []

        def putter() -> None:
            try:
                for i in range(50):
                    writer.put(f"w{i:02d}", str(i))
            except BaseException as error:  # pragma: no cover
                errors.append(error)

        thread = threading.Thread(target=putter)
        thread.start()
        for _ in range(5):
            self.store.restore(image([["r", "v"]]))
        thread.join()
        self.assertEqual(errors, [])
        live = dict(self.store.scan())
        # a restore never merges: keys written before the last restore are
        # gone, so the surviving writer keys form a suffix of its sequence
        self.assertEqual(live["r"], "v")
        writer_keys = sorted(k for k in live if k.startswith("w"))
        self.assertEqual(writer_keys,
                         [f"w{i:02d}"
                          for i in range(50 - len(writer_keys), 50)])


class BackupRestoreCliRegressionTests(BackupRestoreTestBase):
    def run_cli(self, *argv: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "page_store", "--root", str(self.root),
             *argv],
            cwd=REPO_ROOT, capture_output=True, text=True)

    def test_report_schema_and_readiness_unchanged(self) -> None:
        report = json.loads(self.run_cli("report").stdout)
        self.assertEqual(report["readiness"],
                         {"append": True, "recover": True,
                          "compaction": True, "snapshotRead": False})
        self.assertEqual(report["components"], ["pages", "directory"])

    def test_no_backup_or_restore_subcommand(self) -> None:
        for name in ("backup", "restore"):
            result = self.run_cli(name)
            self.assertEqual(result.returncode, 2)


if __name__ == "__main__":
    unittest.main()
