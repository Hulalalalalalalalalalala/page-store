"""An append-only page store with an in-memory ordered directory.

Multiple :class:`PageStore` instances -- whether in the same process or in
different processes -- may share one directory.  A coordination file next to
``pages.dat`` is locked with ``flock`` while an operation runs: writes
(``put``/``delete``/``compact``/``recover``) take an exclusive lock and reads
(``get``/``scan``/``stats``/``snapshot``/``verify``) a shared one, so the
interleaving is always equivalent to some serial order and every read sees a
state from either before or after a whole change.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import threading
from pathlib import Path

__all__ = ["PageStore", "Snapshot"]

PAGE_SIZE = 4096
LOG_FILE = "pages.dat"
LOCK_FILE = f".{LOG_FILE}.lock"

#: Per-directory in-process locks, keyed by the resolved directory path.
#: ``flock`` does not serialise two independent file descriptors held by the
#: same process, so same-process instances/threads are ordered here before an
#: flock is ever taken.
_thread_locks: dict[str, threading.RLock] = {}
_thread_locks_guard = threading.Lock()


def _thread_lock_for(directory: Path) -> threading.RLock:
    key = str(directory.resolve())
    with _thread_locks_guard:
        lock = _thread_locks.get(key)
        if lock is None:
            lock = threading.RLock()
            _thread_locks[key] = lock
        return lock


@contextlib.contextmanager
def _coordinated(directory: Path, exclusive: bool):
    """Serialise same-process callers, then take the cross-process flock."""
    thread_lock = _thread_lock_for(directory)
    thread_lock.acquire()
    try:
        handle = os.open(directory / LOCK_FILE, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(handle, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            yield
        finally:
            try:
                fcntl.flock(handle, fcntl.LOCK_UN)
            finally:
                os.close(handle)
    finally:
        thread_lock.release()


def _write_all(handle: int, blob: bytes) -> None:
    written = 0
    while written < len(blob):
        written += os.write(handle, blob[written:])


class Snapshot:
    """A read-only view of a store's live keys fixed at one point in time.

    The view keeps its own in-memory copy, so later puts, deletes, compaction
    and recovery of the originating :class:`PageStore` never affect it.
    """

    def __init__(self, live: dict[str, str], pages: int, records: int) -> None:
        self._live = dict(live)
        self._stats = {"pages": pages, "records": records, "keys": len(live)}

    def get(self, key: str) -> str | None:
        return self._live.get(key)

    def scan(self, start: str | None = None, end: str | None = None) -> list[tuple[str, str]]:
        items = sorted(self._live.items())
        return [(k, v) for k, v in items
                if (start is None or k >= start) and (end is None or k < end)]

    def stats(self) -> dict:
        return dict(self._stats)


class PageStore:
    """A page store rooted at ``root``, safe for multi-process concurrency."""

    def __init__(self, root: str | Path) -> None:
        self.directory = Path(root)
        self.path = self.directory / LOG_FILE

    def init(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path.write_bytes(b"")
        # the coordination file is not store data: it holds no records and is
        # never read by compact/recover.
        (self.directory / LOCK_FILE).touch(exist_ok=True)

    def _require_path(self) -> None:
        if not self.path.is_file():
            raise FileNotFoundError(f"no store at {self.path}; run init first")

    @staticmethod
    def _encode(record: dict) -> bytes:
        payload = json.dumps(record, sort_keys=True).encode("utf-8")
        return len(payload).to_bytes(4, "big") + payload

    def _parse(self, data: bytes) -> tuple[list[dict], int]:
        """Whole records along length-prefix boundaries; stop at a tail gap."""
        records: list[dict] = []
        offset, size = 0, len(data)
        while offset < size:
            record = self._record_at(data, offset)
            if record is None:
                break  # a half-written tail record is discarded
            records.append(json.loads(data[offset + 4:record["end"]].decode("utf-8")))
            offset = record["end"]
        return records, offset

    @staticmethod
    def _fold_live(records: list[dict]) -> dict[str, str]:
        live: dict[str, str] = {}
        for record in records:
            if record["op"] == "put":
                live[record["key"]] = record["value"]
            else:
                live.pop(record["key"], None)
        return live

    def _read_state(self) -> tuple[list[dict], int, int]:
        """Confirmed records, end of the confirmed region, file size."""
        data = self.path.read_bytes()
        records, confirmed_end = self._parse(data)
        return records, confirmed_end, len(data)

    def put(self, key: str, value: str) -> int:
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")
        if not isinstance(value, str):
            raise ValueError("value must be a string")
        return self._append({"op": "put", "key": key, "value": value})

    def delete(self, key: str) -> int:
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")
        return self._append({"op": "delete", "key": key})

    def _append(self, record: dict) -> int:
        frame = self._encode(record)
        if len(frame) - 4 > PAGE_SIZE:
            raise ValueError(f"record exceeds one page ({len(frame) - 4} > {PAGE_SIZE})")
        self._require_path()
        with _coordinated(self.directory, exclusive=True):
            handle = os.open(self.path, os.O_RDWR)
            try:
                size = os.fstat(handle).st_size
                data = b""
                remaining = size
                while remaining:
                    chunk = os.read(handle, remaining)
                    if not chunk:
                        break
                    data += chunk
                    remaining -= len(chunk)
                records, confirmed_end = self._parse(data)
                if confirmed_end < size:
                    # A dead writer may have left bytes past the last serial
                    # point.  A plain half-written tail is dropped before we
                    # continue; complete records past it are mid-file damage,
                    # which must leave the file untouched.
                    if self._has_record_after(data, confirmed_end):
                        raise RuntimeError("corrupt_middle")
                    os.ftruncate(handle, confirmed_end)
                    os.fsync(handle)
                sequence = len(records) + 1
                os.lseek(handle, confirmed_end, os.SEEK_SET)
                _write_all(handle, frame)
                os.fsync(handle)
            finally:
                os.close(handle)
        return sequence

    def get(self, key: str) -> str | None:
        self._require_path()
        with _coordinated(self.directory, exclusive=False):
            records, _, _ = self._read_state()
        return self._fold_live(records).get(key)

    def scan(self, start: str | None = None, end: str | None = None) -> list[tuple[str, str]]:
        self._require_path()
        with _coordinated(self.directory, exclusive=False):
            records, _, _ = self._read_state()
        items = sorted(self._fold_live(records).items())
        return [(k, v) for k, v in items if (start is None or k >= start) and (end is None or k < end)]

    def snapshot(self) -> Snapshot:
        """Capture the live key state at call time as an immutable read-only view.

        Only records that are complete and confirmed at call time are seen; a
        half-written tail record is discarded, just as by the live reads.
        Raises ``FileNotFoundError`` like the other read operations when the
        root is missing, points at a file, or ``pages.dat`` is absent.  The
        captured view never changes, even across later puts, deletes,
        recovery or compaction by this or any other process.
        """
        self._require_path()
        with _coordinated(self.directory, exclusive=False):
            records, _, size = self._read_state()
            live = self._fold_live(records)
            pages = (size + PAGE_SIZE - 1) // PAGE_SIZE
            return Snapshot(live, pages, len(records))

    def compact(self) -> dict:
        """Rewrite live key/value pairs as the fewest ascending ``put`` records.

        Confirmed records are replayed strictly along length-prefix
        boundaries.  Old values and delete records are discarded; a
        half-written tail is discarded too.  The result is written to a
        temporary file and atomically swapped in, so an interrupted
        compaction leaves either the complete old file or the complete new
        one -- never a mixture.  The swap is one serialisation point shared
        with puts, deletes and recovery, so the returned counters all
        describe the same state and later appends number from the new record
        count.

        Raises ``FileNotFoundError`` when the root is missing, points at a
        file, or ``pages.dat`` is absent; ``RuntimeError("corrupt_middle")``
        when a complete record can still be found past a damaged region (the
        file is left untouched); and ``OSError`` on coordination, read/write
        or atomic-replace failures.
        """
        self._require_path()
        with _coordinated(self.directory, exclusive=True):
            data = self.path.read_bytes()
            size = len(data)
            records, confirmed_end = self._parse(data)
            records_before = len(records)
            if confirmed_end < size and self._has_record_after(data, confirmed_end):
                raise RuntimeError("corrupt_middle")  # leave the file untouched
            discarded = size - confirmed_end
            live = self._fold_live(records)
            frames = [self._encode({"op": "put", "key": key, "value": live[key]})
                      for key in sorted(live)]
            pages_before = (size + PAGE_SIZE - 1) // PAGE_SIZE
            new_size = sum(map(len, frames))
            tmp = self.directory / f".{LOG_FILE}.compact.tmp"
            try:
                with tmp.open("wb") as handle:
                    handle.write(b"".join(frames))
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp, self.path)
                dirfd = os.open(self.directory, os.O_RDONLY)
                try:
                    os.fsync(dirfd)
                finally:
                    os.close(dirfd)
            except BaseException:
                with contextlib.suppress(OSError):
                    tmp.unlink()
                raise
            records_after = len(frames)
            return {"pages_before": pages_before,
                    "pages_after": (new_size + PAGE_SIZE - 1) // PAGE_SIZE,
                    "records_before": records_before,
                    "records_after": records_after,
                    "keys": len(live),
                    "discarded_tail_bytes": discarded}

    def recover(self) -> dict:
        """Reopen the page file, dropping a half-written tail record.

        Records are scanned strictly along length-prefix boundaries.  The first
        boundary that is not a whole, parseable record marks a half-written
        tail: scanning stops and every byte from that boundary on is removed, so
        later appends continue right after the last confirmed record.  Recovery
        shares the write serialisation point with puts, deletes and compaction.

        If a complete record can still be re-synchronised past the stop offset,
        the damage is mid-file corruption rather than a half-written tail:
        ``RuntimeError("corrupt_middle")`` is raised and the file is left
        byte-for-byte untouched.
        """
        self._require_path()
        with _coordinated(self.directory, exclusive=True):
            data = self.path.read_bytes()
            size = len(data)
            records, offset = self._parse(data)
            truncated = offset < size
            if truncated:
                if self._has_record_after(data, offset):
                    raise RuntimeError("corrupt_middle")
                with self.path.open("r+b") as handle:
                    handle.truncate(offset)
                    handle.flush()
                    os.fsync(handle.fileno())
            return {"pages": (offset + PAGE_SIZE - 1) // PAGE_SIZE,
                    "records": len(records), "truncated": truncated}

    def stats(self) -> dict:
        self._require_path()
        with _coordinated(self.directory, exclusive=False):
            records, _, size = self._read_state()
        pages = (size + PAGE_SIZE - 1) // PAGE_SIZE
        return {"pages": pages, "records": len(records),
                "keys": len(self._fold_live(records))}

    @staticmethod
    def _record_at(data: bytes, offset: int) -> dict | None:
        """Parse one record at ``offset``; ``None`` when it is not whole and valid."""
        size = len(data)
        if offset + 4 > size:
            return None
        rec_size = int.from_bytes(data[offset:offset + 4], "big")
        if offset + 4 + rec_size > size:
            return None
        try:
            json.loads(data[offset + 4:offset + 4 + rec_size].decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            return None
        return {"end": offset + 4 + rec_size}

    def _has_record_after(self, data: bytes, offset: int) -> bool:
        """Whether any valid record can be re-synchronised past ``offset``."""
        return any(self._record_at(data, start) is not None
                   for start in range(offset + 1, len(data) - 3))

    def verify(self) -> dict:
        """Read-only check of the page file; never mutates the store."""
        result = {"status": "ok", "complete_records": 0, "valid_pages": 0,
                  "first_error_offset": None, "tail_partial_bytes": 0,
                  "scanned_end_offset": None, "error": None}
        if not self.path.is_file():
            result.update(status="error", error="invalid_path")
            return result
        try:
            with _coordinated(self.directory, exclusive=False):
                data = self.path.read_bytes()
        except OSError:
            result.update(status="error", error="read_error")
            return result
        size = len(data)
        result["scanned_end_offset"] = size
        offset = 0
        while offset < size:
            record = self._record_at(data, offset)
            if record is None:
                break
            offset = record["end"]
            result["complete_records"] += 1
        result["valid_pages"] = offset // PAGE_SIZE
        if offset == size:
            return result
        if self._has_record_after(data, offset):
            result.update(status="corrupt_middle", error="corrupt_middle",
                          first_error_offset=offset)
        else:
            result.update(status="incomplete_tail", tail_partial_bytes=size - offset)
        return result
