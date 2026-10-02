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


def backup_bytes(items: list[list[str]], version: int = 1) -> bytes:
    return json.dumps({"version": version, "items": items},
                      separators=(",", ":")).encode("utf-8")


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
    def test_empty_store_backups_empty_items(self) -> None:
        self.assertEqual(self.store.backup(), b'{"version":1,"items":[]}')

    def test_top_level_shape_and_scan_order(self) -> None:
        self.store.put("c", "3")
        self.store.put("a", "1")
        self.store.put("b", "2")
        blob = self.store.backup()
        parsed = json.loads(blob.decode("utf-8"))
        self.assertEqual(set(parsed), {"version", "items"})
        self.assertEqual(parsed["version"], 1)
        self.assertEqual(parsed["items"], [["a", "1"], ["b", "2"], ["c", "3"]])

    def test_only_live_keys_and_latest_values(self) -> None:
        self.store.put("a", "old")
        self.store.put("gone", "x")
        self.store.put("b", "2")
        self.store.delete("gone")
        self.store.put("a", "1")
        parsed = json.loads(self.store.backup())
        self.assertEqual(parsed["items"], [["a", "1"], ["b", "2"]])

    def test_strings_preserved_exactly(self) -> None:
        values = ["", "with space", "quote\"back\\slash", "汉字", "🎉",
                  "line\nbreak", "\t", "null", "1", "true"]
        for i, value in enumerate(values):
            self.store.put(f"k{i:02d}", value)
        parsed = json.loads(self.store.backup())
        self.assertEqual([v for _, v in parsed["items"]], values)

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

    def test_backup_is_deterministic_across_calls(self) -> None:
        self.store.put("a", "1")
        self.assertEqual(self.store.backup(), self.store.backup())


class BackupSerialPointTests(BackupRestoreTestBase):
    def test_half_written_tail_ignored_and_file_untouched(self) -> None:
        self.store.put("a", "1")
        head = self.data
        tail = put_record("b", "2")
        self.write(head + tail[:len(tail) - 3])
        before = self.data
        parsed = json.loads(self.store.backup())
        self.assertEqual(parsed["items"], [["a", "1"]])
        # not truncated, no sequence number advanced
        self.assertEqual(self.data, before)
        self.assertEqual(self.store.put("c", "3"), 2)
        self.assertEqual(self.store.scan(), [("a", "1"), ("c", "3")])

    def test_corrupt_middle_raises_and_file_untouched(self) -> None:
        blob = (put_record("a", "1") + b"\x00\x00\x00\x09broken"
                + put_record("b", "2"))
        self.write(blob)
        with self.assertRaises(RuntimeError) as caught:
            self.store.backup()
        self.assertEqual(str(caught.exception), "corrupt_middle")
        self.assertEqual(self.data, blob)

    def test_path_errors_raise_and_create_nothing(self) -> None:
        with self.assertRaises(FileNotFoundError):
            PageStore(self.root / "nope").backup()
        self.assertFalse((self.root / "nope").exists())
        target = self.root / "a-file"
        target.write_bytes(b"")
        with self.assertRaises(FileNotFoundError):
            PageStore(target).backup()
        bare = self.root / "bare"
        bare.mkdir()
        with self.assertRaises(FileNotFoundError):
            PageStore(bare).backup()
        self.assertEqual(list(bare.iterdir()), [])


class RestoreValidationTests(BackupRestoreTestBase):
    def assert_invalid(self, data: object) -> None:
        before = self.data
        stats = self.store.stats()
        with self.assertRaises(ValueError):
            self.store.restore(data)  # type: ignore[arg-type]
        # page file untouched, no sequence number consumed
        self.assertEqual(self.data, before)
        self.assertEqual(self.store.stats(), stats)

    def test_non_bytes_rejected(self) -> None:
        self.store.put("a", "1")
        for bad in ('{"version":1,"items":[]}', bytearray(b"{}"), None, 1,
                    ["version", 1], {"version": 1, "items": []}):
            self.assert_invalid(bad)

    def test_invalid_utf8_rejected(self) -> None:
        self.assert_invalid(b'{"version":1,"items":[["a","\xff"]]}')
        self.assert_invalid(b"\xff\xfe")

    def test_invalid_json_rejected(self) -> None:
        for bad in (b"", b"{", b"nope", b"[1,2]", b'"text"', b"1",
                    b'{"version":1,"items":[]', b"null"):
            self.assert_invalid(bad)

    def test_duplicate_object_fields_rejected(self) -> None:
        self.assert_invalid(b'{"version":1,"version":1,"items":[]}')
        self.assert_invalid(b'{"version":1,"items":[],"items":[]}')

    def test_top_level_fields_must_be_exactly_version_and_items(self) -> None:
        self.assert_invalid(b'{"version":1}')
        self.assert_invalid(b'{"items":[]}')
        self.assert_invalid(b'{"version":1,"items":[],"extra":0}')
        self.assert_invalid(b"{}")

    def test_version_must_be_integer_one(self) -> None:
        for bad in (b'{"version":2,"items":[]}', b'{"version":0,"items":[]}',
                    b'{"version":"1","items":[]}',
                    b'{"version":1.0,"items":[]}',
                    b'{"version":true,"items":[]}',
                    b'{"version":false,"items":[]}',
                    b'{"version":null,"items":[]}'):
            self.assert_invalid(bad)

    def test_items_must_be_an_array(self) -> None:
        for bad in (b'{"version":1,"items":{}}', b'{"version":1,"items":"x"}',
                    b'{"version":1,"items":1}', b'{"version":1,"items":null}'):
            self.assert_invalid(bad)

    def test_entries_must_be_string_pairs(self) -> None:
        for bad in (b'{"version":1,"items":[["a"]]}',
                    b'{"version":1,"items":[["a","1","x"]]}',
                    b'{"version":1,"items":["a"]}',
                    b'{"version":1,"items":[[1,"1"]]}',
                    b'{"version":1,"items":[["a",1]]}',
                    b'{"version":1,"items":[[null,"1"]]}',
                    b'{"version":1,"items":[["a",null]]}',
                    b'{"version":1,"items":[["","1"]]}',
                    b'{"version":1,"items":[[true,"1"]]}',
                    b'{"version":1,"items":[{"key":"a","value":"1"}]}'):
            self.assert_invalid(bad)

    def test_duplicate_keys_rejected(self) -> None:
        self.assert_invalid(b'{"version":1,"items":[["a","1"],["a","2"]]}')
        self.assert_invalid(
            b'{"version":1,"items":[["a","1"],["b","2"],["a","1"]]}')

    def test_entry_payload_limit_matches_put(self) -> None:
        # largest value that still fits one record is accepted
        value = "x" * PAGE_SIZE
        while len(encode({"op": "put", "key": "k", "value": value})) - 4 \
                > PAGE_SIZE:
            value = value[:-1]
        self.store.restore(backup_bytes([["k", value]]))
        self.assertEqual(self.store.get("k"), value)
        # one byte more is rejected
        self.assert_invalid(backup_bytes([["k", value + "x"]]))

    def test_validation_precedes_target_access(self) -> None:
        # invalid data raises ValueError even when the store is missing
        missing = PageStore(self.root / "nope")
        with self.assertRaises(ValueError):
            missing.restore(b"not json")
        self.assertFalse((self.root / "nope").exists())


class RestoreEffectTests(BackupRestoreTestBase):
    def test_replaces_store_wholesale_without_merging(self) -> None:
        self.store.put("old", "x")
        self.store.put("keep", "old-value")
        result = self.store.restore(
            backup_bytes([["keep", "new"], ["fresh", "1"]]))
        self.assertEqual(self.store.scan(), [("fresh", "1"), ("keep", "new")])
        self.assertIsNone(self.store.get("old"))
        self.assertEqual(result, {"pages": 1, "records": 2, "keys": 2})
        self.assertEqual(self.store.stats(), result)

    def test_empty_items_clears_store(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        result = self.store.restore(b'{"version":1,"items":[]}')
        self.assertEqual(result, {"pages": 0, "records": 0, "keys": 0})
        self.assertEqual(self.store.scan(), [])
        self.assertEqual(self.data, b"")
        self.assertEqual(self.store.put("c", "3"), 1)

    def test_out_of_order_items_accepted_and_stored_sorted(self) -> None:
        self.store.restore(backup_bytes([["c", "3"], ["a", "1"], ["b", "2"]]))
        self.assertEqual(self.store.scan(),
                         [("a", "1"), ("b", "2"), ("c", "3")])
        # the restored image is canonical: backing it up matches a fresh store
        other = PageStore(self.root / "other")
        other.init()
        other.restore(self.store.backup())
        self.assertEqual(other.path.read_bytes(), self.data)

    def test_restore_overwrites_half_written_tail(self) -> None:
        self.store.put("a", "1")
        tail = put_record("b", "2")
        self.write(self.data + tail[:len(tail) - 3])
        result = self.store.restore(backup_bytes([["z", "9"]]))
        self.assertEqual(result["records"], 1)
        self.assertEqual(self.store.scan(), [("z", "9")])
        self.assertEqual(self.store.verify()["status"], "ok")

    def test_restore_overwrites_corrupt_middle(self) -> None:
        blob = (put_record("a", "1") + b"\x00\x00\x00\x09broken"
                + put_record("b", "2"))
        self.write(blob)
        result = self.store.restore(backup_bytes([["z", "9"]]))
        self.assertEqual(result, {"pages": 1, "records": 1, "keys": 1})
        self.assertEqual(self.store.scan(), [("z", "9")])
        self.assertEqual(self.store.verify()["status"], "ok")

    def test_sequence_numbers_continue_from_entry_count(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "2")
        self.store.put("c", "3")
        self.store.restore(backup_bytes([["x", "1"], ["y", "2"]]))
        self.assertEqual(self.store.put("z", "3"), 3)
        self.assertEqual(self.store.delete("x"), 4)

    def test_pages_reflect_new_file_size(self) -> None:
        items = [[f"k{i:03d}", "v" * 100] for i in range(80)]
        result = self.store.restore(backup_bytes(items))
        size = len(self.data)
        self.assertEqual(result["pages"], (size + PAGE_SIZE - 1) // PAGE_SIZE)
        self.assertGreater(result["pages"], 1)
        self.assertEqual(result["records"], 80)
        self.assertEqual(self.store.stats(), result)

    def test_round_trip_through_backup(self) -> None:
        self.store.put("a", "1")
        self.store.put("b", "汉字")
        self.store.put("c", "")
        other = PageStore(self.root / "other")
        other.init()
        result = other.restore(self.store.backup())
        self.assertEqual(result, {"pages": 1, "records": 3, "keys": 3})
        self.assertEqual(other.scan(), self.store.scan())
        self.assertEqual(other.backup(), self.store.backup())

    def test_old_snapshot_is_unaffected(self) -> None:
        self.store.put("a", "1")
        snap = self.store.snapshot()
        self.store.restore(backup_bytes([["b", "2"]]))
        self.assertEqual(snap.scan(), [("a", "1")])
        self.assertEqual(snap.stats(), {"pages": 1, "records": 1, "keys": 1})
        self.assertEqual(self.store.scan(), [("b", "2")])

    def test_long_lived_second_instance_sees_new_state(self) -> None:
        self.store.put("a", "1")
        other = PageStore(self.root)
        self.assertEqual(other.get("a"), "1")  # build its cache
        self.store.restore(backup_bytes([["b", "2"], ["c", "3"]]))
        self.assertIsNone(other.get("a"))
        self.assertEqual(other.scan(), [("b", "2"), ("c", "3")])
        self.assertEqual(other.stats(), {"pages": 1, "records": 2, "keys": 2})
        self.assertEqual(other.put("d", "4"), 3)

    def test_path_errors_raise_and_create_nothing(self) -> None:
        image = backup_bytes([["a", "1"]])
        with self.assertRaises(FileNotFoundError):
            PageStore(self.root / "nope").restore(image)
        self.assertFalse((self.root / "nope").exists())
        target = self.root / "a-file"
        target.write_bytes(b"")
        with self.assertRaises(FileNotFoundError):
            PageStore(target).restore(image)
        bare = self.root / "bare"
        bare.mkdir()
        with self.assertRaises(FileNotFoundError):
            PageStore(bare).restore(image)
        self.assertEqual(list(bare.iterdir()), [])


class RestoreAtomicityTests(BackupRestoreTestBase):
    def test_replace_failure_leaves_complete_old_file(self) -> None:
        self.store.put("a", "1")
        old = self.data
        with mock.patch("page_store.core.os.replace",
                        side_effect=OSError("boom")):
            with self.assertRaises(OSError):
                self.store.restore(backup_bytes([["b", "2"]]))
        self.assertEqual(self.data, old)
        self.assertFalse((self.root / ".pages.dat.restore.tmp").exists())
        self.assertEqual(self.store.scan(), [("a", "1")])
        self.assertEqual(self.store.put("c", "3"), 2)

    def test_write_failure_leaves_complete_old_file(self) -> None:
        self.store.put("a", "1")
        old = self.data
        with mock.patch("page_store.core.os.fsync",
                        side_effect=OSError("boom")):
            with self.assertRaises(OSError):
                self.store.restore(backup_bytes([["b", "2"]]))
        self.assertEqual(self.data, old)
        self.assertFalse((self.root / ".pages.dat.restore.tmp").exists())
        self.assertEqual(self.store.scan(), [("a", "1")])


class RestoreConcurrencyTests(BackupRestoreTestBase):
    def test_concurrent_restore_and_puts_are_serial(self) -> None:
        for i in range(20):
            self.store.put(f"k{i:02d}", str(i))
        image = backup_bytes([["only", "1"]])
        errors: list[BaseException] = []

        def run_puts() -> None:
            try:
                for i in range(20, 40):
                    self.store.put(f"k{i:02d}", str(i))
            except BaseException as error:  # pragma: no cover
                errors.append(error)

        writer = threading.Thread(target=run_puts)
        writer.start()
        self.store.restore(image)
        writer.join()
        self.assertEqual(errors, [])
        # whichever serial order won, the state is complete and consistent:
        # every confirmed record is visible and sequence numbers are dense
        live = self.store.scan()
        stats = self.store.stats()
        self.assertEqual(stats["keys"], len(live))
        self.assertEqual(self.store.verify()["status"], "ok")
        self.assertEqual(
            self.store.put("last", "x"), stats["records"] + 1)


class BackupRestoreCliRegressionTests(BackupRestoreTestBase):
    def run_cli(self, *argv: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "page_store", "--root", str(self.root),
             *argv],
            cwd=REPO_ROOT, capture_output=True, text=True)

    def test_no_new_subcommands_and_report_unchanged(self) -> None:
        self.store.put("a", "1")
        for name in ("backup", "restore"):
            result = self.run_cli(name)
            self.assertEqual(result.returncode, 2)
        report = json.loads(self.run_cli("report").stdout)
        self.assertEqual(report["readiness"], {
            "append": True, "recover": True,
            "compaction": True, "snapshotRead": False})
        self.assertEqual(report["components"], ["pages", "directory"])

    def test_existing_cli_outputs_unchanged(self) -> None:
        self.store.put("a", "1")
        self.store.restore(backup_bytes([["b", "2"]]))
        self.assertEqual(self.run_cli("get", "b").stdout, "2\n")
        self.assertEqual(json.loads(self.run_cli("stats").stdout),
                         {"keys": 1, "pages": 1, "records": 1})
        self.assertEqual(json.loads(self.run_cli("scan").stdout), [["b", "2"]])


@unittest.skipUnless(hasattr(os, "fork"), "fork required")
class RestoreForkTests(BackupRestoreTestBase):
    def test_fork_inherited_instance_sees_restored_state(self) -> None:
        self.store.put("a", "1")
        self.assertEqual(self.store.get("a"), "1")  # build this instance's cache
        # another instance replaces the whole store, moving the serial point
        PageStore(self.root).restore(backup_bytes([["b", "2"]]))
        pid = os.fork()
        if pid == 0:  # child: the inherited instance resyncs on its next op
            try:
                ok = self.store.scan() == [("b", "2")] \
                    and self.store.put("c", "3") == 2
            finally:
                os._exit(0 if ok else 1)
        _, status = os.waitpid(pid, 0)
        self.assertTrue(os.WIFEXITED(status))
        self.assertEqual(os.WEXITSTATUS(status), 0)
        # the parent's long-lived instance saw the same restored state
        self.assertEqual(self.store.scan(), [("b", "2"), ("c", "3")])


if __name__ == "__main__":
    unittest.main()
