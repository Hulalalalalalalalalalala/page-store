"""An append-only page store with an in-memory ordered directory.

Several :class:`PageStore` instances -- in one process or in several -- may
share the same root directory.  A coordination file next to ``pages.dat``
(``.pages.dat.lock``) is locked while the serial point is read or moved, so
the outcome of concurrent writers is equivalent to running their calls one
after another: each successful put/delete gets a strictly increasing record
sequence number, a failed or unconfirmed call never consumes a number, and
readers always see a complete before/after state.
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


class Snapshot:
    """A read-only view of a store's serial-point state fixed at capture time.

    The view keeps its own in-memory copy, so later puts, deletes, recovery
    and compaction of the originating :class:`PageStore` never affect it, and
    concurrent writers in other processes cannot either.
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
    """An append-only page store rooted at ``root``.

    Multiple instances pointing at the same directory, including instances in
    different processes, coordinate through a lock file so that their
    interleaved calls have the same result as a serial ordering.
    """

    def __init__(self, root: str | Path) -> None:
        self.directory = Path(root)
        self.path = self.directory / LOG_FILE
        self._lock_path = self.directory / LOCK_FILE
        # Serialises this instance's own locked sections (also against other
        # instances in the same process via the file lock) and guards the
        # cached serial-point state.
        self._gate = threading.RLock()
        self._lock_fd: int | None = None
        # Cached serial point: live directory, next sequence number (= number
        # of confirmed records), offset just past those records, and the
        # identity of the file they were read from.
        self._live: dict[str, str] | None = None
        self._seq = 0
        self._end = 0
        self._identity: tuple[int, int, int, int, int] | None = None

    # ------------------------------------------------------------------ init

    def init(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        with self._locked(exclusive=True, require=False):
            # Install a *fresh empty inode* rather than truncating the old one.
            # A reset is a serial state change on equal footing with a compaction:
            # other instances detect a new inode (independent of the file's new
            # length) and rebuild from an empty image, so pre-reset writes can
            # neither be resurrected from a long-lived cache nor shadow records
            # appended after the reset -- even when the new file grows as long
            # as or longer than the old one.  The temp file is fsynced and the
            # swap is followed by a directory fsync, so an interrupted init
            # leaves either the complete old file or the complete empty one.
            tmp = self.directory / f".{LOG_FILE}.init.tmp"
            try:
                with tmp.open("wb") as handle:
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
                # The old file may or may not still be the one the cache was
                # built from; force a full rescan on the next operation.
                self._invalidate()
                raise
            self._invalidate()
            self._resync()

    # ------------------------------------------------------------- locking

    @contextlib.contextmanager
    def _locked(self, exclusive: bool, require: bool = True):
        """Hold the cross-process coordination lock for one serial operation."""
        with self._gate:
            # Fail before touching the coordination file: a missing root, a
            # root that is a file, or an absent pages.dat must not create
            # anything (and would otherwise raise NotADirectoryError).  init is
            # the one call allowed to create pages.dat itself.
            if require:
                self._require_store()
            if self._lock_fd is None or os.fstat(self._lock_fd).st_nlink == 0:
                # The coordination file only serialises access; it is never
                # counted as a record or treated as data by compact.  If it
                # was removed out from under us, reopen the current path so
                # everyone converges on one lock inode again.
                if self._lock_fd is not None:
                    with contextlib.suppress(OSError):
                        os.close(self._lock_fd)
                self._lock_fd = os.open(self._lock_path, os.O_RDWR | os.O_CREAT, 0o600)
            fcntl.flock(
                self._lock_fd,
                fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            try:
                yield
            finally:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)

    def _require_store(self) -> None:
        if not self.directory.is_dir() or not self.path.is_file():
            raise FileNotFoundError(f"no store at {self.path}; run init first")

    def _identity_of(self) -> tuple[int, int, int, int, int]:
        stat = self.path.stat()
        return (stat.st_dev, stat.st_ino, stat.st_size,
                stat.st_mtime_ns, stat.st_ctime_ns)

    def _invalidate(self) -> None:
        self._live = None
        self._identity = None

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

    def _scan_from(self, data: bytes, offset: int,
                   live: dict[str, str]) -> tuple[int, int]:
        """Replay whole records from ``offset`` into ``live``; return (end, count)."""
        size = len(data)
        count = 0
        while offset < size:
            record = self._record_at(data, offset)
            if record is None:
                break  # a half-written tail record is discarded
            decoded = json.loads(data[offset + 4:record["end"]].decode("utf-8"))
            if decoded["op"] == "put":
                live[decoded["key"]] = decoded["value"]
            else:
                live.pop(decoded["key"], None)
            offset = record["end"]
            count += 1
        return offset, count

    def _resync(self) -> None:
        """Rebuild or extend the cached serial point from the current page file.

        Must be called while holding the coordination lock.  When the file has
        been replaced (another instance compacted) the whole image is read;
        otherwise only bytes past the previously confirmed offset are replayed,
        which also discards a half-written tail left by a crashed writer.
        """
        self._require_store()
        identity = self._identity_of()
        data = self.path.read_bytes()
        replaced = self._live is None or self._identity is None or \
            identity[:2] != self._identity[:2]
        if replaced or len(data) < self._end:
            # New file (another instance compacted) or an externally
            # truncated inode: rebuild the serial point from the beginning.
            live: dict[str, str] = {}
            offset, count = self._scan_from(data, 0, live)
            self._live = live
            self._seq = count
            self._end = offset
        else:
            # Same inode: replay only what other processes appended beyond the
            # last confirmed boundary (including an earlier half-write).
            offset, count = self._scan_from(data, self._end, self._live)
            self._seq += count
            self._end = offset
        self._identity = identity

    def _state(self, exclusive: bool) -> tuple[dict[str, str], int, int]:
        with self._locked(exclusive=exclusive):
            self._resync()
            # Copy so callers can never mutate the serial-point directory.
            return dict(self._live), self._seq, self._identity[2]  # type: ignore[index]

    # ------------------------------------------------------------- writing

    @staticmethod
    def _encode(record: dict) -> bytes:
        payload = json.dumps(record, sort_keys=True).encode("utf-8")
        return len(payload).to_bytes(4, "big") + payload

    def _append(self, record: dict) -> int:
        frame = self._encode(record)
        if len(frame) - 4 > PAGE_SIZE:
            raise ValueError(f"record exceeds one page ({len(frame) - 4} > {PAGE_SIZE})")
        with self._locked(exclusive=True):
            # Another process may have appended or compacted since we last
            # synced; bring the serial point up to date first.
            self._resync()
            assert self._live is not None and self._identity is not None
            if self._identity[2] - self._end > 0:
                trailing = self.path.read_bytes()[self._end:]
                if self._record_past(trailing):
                    # A complete record exists beyond the unparseable region:
                    # the file is corrupt in the middle.  Never overwrite or
                    # skip those records; leave the file byte-for-byte untouched.
                    raise RuntimeError("corrupt_middle")
            try:
                with self.path.open("r+b") as handle:
                    # Cut at the confirmed boundary so a half-written tail
                    # left by a crashed (unconfirmed) writer is overwritten
                    # rather than sealed in front of the new record.
                    if self._identity[2] - self._end > 0:
                        handle.truncate(self._end)
                    handle.seek(self._end)
                    handle.write(frame)
                    handle.flush()
                    os.fsync(handle.fileno())
            except OSError:
                # Unconfirmed call: best-effort rollback to the confirmed
                # boundary so it occupies no sequence number and stays
                # invisible, then drop the cache for a fresh scan.
                with contextlib.suppress(OSError):
                    with self.path.open("r+b") as rollback:
                        rollback.truncate(self._end)
                        rollback.flush()
                        os.fsync(rollback.fileno())
                self._invalidate()
                raise
            # Confirmed: the append is now part of the serial point.
            decoded = json.loads(frame[4:].decode("utf-8"))
            if decoded["op"] == "put":
                self._live[decoded["key"]] = decoded["value"]
            else:
                self._live.pop(decoded["key"], None)
            self._seq += 1
            self._end += len(frame)
            try:
                self._identity = self._identity_of()
            except OSError:
                self._invalidate()
            return self._seq

    @staticmethod
    def _check_key(key: object) -> None:
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")

    def put(self, key: str, value: str) -> int:
        self._check_key(key)
        if not isinstance(value, str):
            raise ValueError("value must be a string")
        return self._append({"op": "put", "key": key, "value": value})

    def delete(self, key: str) -> int:
        self._check_key(key)
        return self._append({"op": "delete", "key": key})

    # -------------------------------------------------------------- reading

    def _live_state(self) -> dict[str, str]:
        return self._state(exclusive=False)[0]

    def get(self, key: str) -> str | None:
        return self._live_state().get(key)

    def scan(self, start: str | None = None, end: str | None = None) -> list[tuple[str, str]]:
        live = self._live_state()
        items = sorted(live.items())
        return [(k, v) for k, v in items if (start is None or k >= start) and (end is None or k < end)]

    def snapshot(self) -> Snapshot:
        """Capture the live key state at call time as an immutable read-only view.

        The capture is taken under the coordination lock, so only the complete
        before/after state of any concurrent put, delete, recover or compact is
        ever seen.  Raises ``FileNotFoundError`` like the other read operations
        when the root is missing, points at a file, or ``pages.dat`` is absent.
        """
        with self._locked(exclusive=False):
            self._resync()
            assert self._live is not None and self._identity is not None
            pages = (self._identity[2] + PAGE_SIZE - 1) // PAGE_SIZE
            return Snapshot(self._live, pages, self._seq)

    def stats(self) -> dict:
        live, seq, size = self._state(exclusive=False)
        return {"pages": (size + PAGE_SIZE - 1) // PAGE_SIZE,
                "records": seq, "keys": len(live)}

    # ------------------------------------------------------------ compaction

    def compact(self) -> dict:
        """Rewrite live key/value pairs as the fewest ascending ``put`` records.

        Confirmed records are replayed strictly along length-prefix
        boundaries.  Old values and delete records are discarded; a
        half-written tail is discarded too.  The result is written to a
        temporary file and atomically swapped in, so an interrupted
        compaction leaves either the complete old file or the complete new
        one -- never a mixture.

        The whole operation is one serial point: it is mutually exclusive with
        concurrent puts, deletes and recoveries (here or in other processes),
        and ``pages_before``, ``pages_after``, ``records_before``,
        ``records_after``, ``keys`` and ``discarded_tail_bytes`` all describe
        that same point.  Later appends number from the post-compaction record
        count.

        Raises ``FileNotFoundError`` when the root is missing, points at a
        file, or ``pages.dat`` is absent; ``RuntimeError("corrupt_middle")``
        when a complete record can still be found past a damaged region (the
        file is left untouched); and ``OSError`` on read/write failures.
        """
        with self._locked(exclusive=True):
            self._resync()
            assert self._live is not None
            data = self.path.read_bytes()
            size = len(data)
            offset, records_before = 0, 0
            live: dict[str, str] = {}
            while offset < size:
                record = self._record_at(data, offset)
                if record is None:
                    break  # a half-written tail record is discarded
                chunk = data[offset + 4:record["end"]]
                decoded = json.loads(chunk.decode("utf-8"))
                if decoded["op"] == "put":
                    live[decoded["key"]] = decoded["value"]
                else:
                    live.pop(decoded["key"], None)
                offset = record["end"]
                records_before += 1
            if offset < size and self._has_record_after(data, offset):
                raise RuntimeError("corrupt_middle")  # leave the file untouched
            discarded = size - offset
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
                # The old file may or may not still be the one the cache was
                # built from; force a full rescan on the next operation.
                self._invalidate()
                raise
            # The atomic swap is the new serial point.
            self._live = dict(live)
            self._seq = len(frames)
            self._end = new_size
            self._identity = self._identity_of()
            records_after = len(frames)
            return {"pages_before": pages_before,
                    "pages_after": (new_size + PAGE_SIZE - 1) // PAGE_SIZE,
                    "records_before": records_before,
                    "records_after": records_after,
                    "keys": len(live),
                    "discarded_tail_bytes": discarded}

    # ------------------------------------------------------------- recovery

    def recover(self) -> dict:
        """Reopen the page file, dropping a half-written tail record.

        Records are scanned strictly along length-prefix boundaries.  The first
        boundary that is not a whole, parseable record marks a half-written
        tail: scanning stops and every byte from that boundary on is removed, so
        later appends continue right after the last confirmed record.

        If a complete record can still be re-synchronised past the stop offset,
        the damage is mid-file corruption rather than a half-written tail:
        ``RuntimeError("corrupt_middle")`` is raised and the file is left
        byte-for-byte untouched.

        Recovery shares the serial point with writes, so only a confirmed tail
        is ever truncated and concurrent appends cannot race the cut.
        """
        with self._locked(exclusive=True):
            self._require_store()
            data = self.path.read_bytes()
            size = len(data)
            offset, records = 0, 0
            while offset < size:
                record = self._record_at(data, offset)
                if record is None:
                    break  # partial prefix, declared length past EOF, or bad payload
                offset = record["end"]
                records += 1
            truncated = offset < size
            if truncated:
                if self._has_record_after(data, offset):
                    raise RuntimeError("corrupt_middle")
                with self.path.open("r+b") as handle:
                    handle.truncate(offset)
                    handle.flush()
                    os.fsync(handle.fileno())
            live: dict[str, str] = {}
            self._scan_from(data[:offset], 0, live)
            self._live = live
            self._seq = records
            self._end = offset
            self._identity = self._identity_of()
            return {"pages": (offset + PAGE_SIZE - 1) // PAGE_SIZE,
                    "records": records, "truncated": truncated}

    def _has_record_after(self, data: bytes, offset: int) -> bool:
        """Whether any valid record can be re-synchronised past ``offset``."""
        return any(self._record_at(data, start) is not None
                   for start in range(offset + 1, len(data) - 3))

    def _record_past(self, region: bytes) -> bool:
        """Whether a complete record hides anywhere in a post-boundary region."""
        return any(self._record_at(region, start) is not None
                   for start in range(0, len(region) - 3))

    # --------------------------------------------------------------- verify

    def verify(self) -> dict:
        """Read-only check of the page file; never mutates the store."""
        result = {"status": "ok", "complete_records": 0, "valid_pages": 0,
                  "first_error_offset": None, "tail_partial_bytes": 0,
                  "scanned_end_offset": None, "error": None}
        if not self.directory.is_dir() or not self.path.is_file():
            result.update(status="error", error="invalid_path")
            return result
        with self._locked(exclusive=False):
            try:
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
