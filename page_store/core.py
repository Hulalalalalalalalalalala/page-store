"""An append-only page store with an in-memory ordered directory."""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path

__all__ = ["PageStore", "Snapshot"]

PAGE_SIZE = 4096
LOG_FILE = "pages.dat"


class Snapshot:
    """A read-only view of a store's live keys fixed at one point in time.

    The view keeps its own in-memory copy, so later puts, deletes and
    recovery of the originating :class:`PageStore` never affect it.
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
    """A single-process page store rooted at ``root``."""

    def __init__(self, root: str | Path) -> None:
        self.directory = Path(root)
        self.path = self.directory / LOG_FILE

    def init(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path.write_bytes(b"")

    def _append(self, record: dict) -> int:
        frame = self._encode(record)
        if len(frame) - 4 > PAGE_SIZE:
            raise ValueError(f"record exceeds one page ({len(frame) - 4} > {PAGE_SIZE})")
        with self.path.open("ab") as handle:
            handle.write(frame)
            handle.flush()
            os.fsync(handle.fileno())
        return sum(1 for _ in self._records())

    @staticmethod
    def _encode(record: dict) -> bytes:
        payload = json.dumps(record, sort_keys=True).encode("utf-8")
        return len(payload).to_bytes(4, "big") + payload

    def _records(self) -> list[dict]:
        if not self.path.is_file():
            raise FileNotFoundError(f"no store at {self.path}; run init first")
        data, offset, out = self.path.read_bytes(), 0, []
        while offset + 4 <= len(data):
            size = int.from_bytes(data[offset:offset + 4], "big")
            chunk = data[offset + 4:offset + 4 + size]
            if len(chunk) < size:
                break  # a half-written tail record is discarded
            out.append(json.loads(chunk.decode("utf-8")))
            offset += 4 + size
        return out

    def put(self, key: str, value: str) -> int:
        if not key:
            raise ValueError("key must be non-empty")
        return self._append({"op": "put", "key": key, "value": value})

    def delete(self, key: str) -> int:
        return self._append({"op": "delete", "key": key})

    def _live(self) -> dict[str, str]:
        live: dict[str, str] = {}
        for record in self._records():
            if record["op"] == "put":
                live[record["key"]] = record["value"]
            else:
                live.pop(record["key"], None)
        return live

    def get(self, key: str) -> str | None:
        return self._live().get(key)

    def scan(self, start: str | None = None, end: str | None = None) -> list[tuple[str, str]]:
        items = sorted(self._live().items())
        return [(k, v) for k, v in items if (start is None or k >= start) and (end is None or k < end)]

    def snapshot(self) -> Snapshot:
        """Capture the live key state at call time as an immutable read-only view.

        Only records that are complete and confirmed at call time are seen; a
        half-written tail record is discarded, just as by the live reads.
        Raises ``FileNotFoundError`` like the other read operations when the
        root is missing, points at a file, or ``pages.dat`` is absent.
        """
        records = self._records()
        live: dict[str, str] = {}
        for record in records:
            if record["op"] == "put":
                live[record["key"]] = record["value"]
            else:
                live.pop(record["key"], None)
        pages = (self.path.stat().st_size + PAGE_SIZE - 1) // PAGE_SIZE
        return Snapshot(live, pages, len(records))

    def compact(self) -> dict:
        """Rewrite live key/value pairs as the fewest ascending ``put`` records.

        Confirmed records are replayed strictly along length-prefix
        boundaries.  Old values and delete records are discarded; a
        half-written tail is discarded too.  The result is written to a
        temporary file and atomically swapped in, so an interrupted
        compaction leaves either the complete old file or the complete new
        one -- never a mixture.

        Raises ``FileNotFoundError`` when the root is missing, points at a
        file, or ``pages.dat`` is absent; ``RuntimeError("corrupt_middle")``
        when a complete record can still be found past a damaged region (the
        file is left untouched); and ``OSError`` on read/write failures.
        """
        if not self.directory.is_dir() or not self.path.is_file():
            raise FileNotFoundError(f"no store at {self.path}; run init first")
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
        later appends continue right after the last confirmed record.

        If a complete record can still be re-synchronised past the stop offset,
        the damage is mid-file corruption rather than a half-written tail:
        ``RuntimeError("corrupt_middle")`` is raised and the file is left
        byte-for-byte untouched.
        """
        if not self.directory.is_dir() or not self.path.is_file():
            raise FileNotFoundError(f"no store at {self.path}; run init first")
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
        return {"pages": (offset + PAGE_SIZE - 1) // PAGE_SIZE,
                "records": records, "truncated": truncated}

    def stats(self) -> dict:
        live = self._live()
        return {"pages": (self.path.stat().st_size + PAGE_SIZE - 1) // PAGE_SIZE if self.path.is_file() else 0,
                "records": len(self._records()), "keys": len(live)}

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
        if not self.directory.is_dir() or not self.path.is_file():
            result.update(status="error", error="invalid_path")
            return result
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
