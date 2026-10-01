"""An append-only page store with an in-memory ordered directory."""

from __future__ import annotations

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
        payload = json.dumps(record, sort_keys=True).encode("utf-8")
        if len(payload) > PAGE_SIZE:
            raise ValueError(f"record exceeds one page ({len(payload)} > {PAGE_SIZE})")
        with self.path.open("ab") as handle:
            handle.write(len(payload).to_bytes(4, "big") + payload)
            handle.flush()
            os.fsync(handle.fileno())
        return sum(1 for _ in self._records()) 

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

    def compact(self) -> dict:
        """Rewrite the live keys as a minimal run of put records, key-ascending.

        Only complete, confirmed records are considered: a half-written tail
        is discarded and its byte count reported as ``discarded_tail_bytes``.
        Old values and delete records are dropped, so the new file holds
        exactly one put record per live key, sorted by key.  The same live
        state always yields the same record order and the same ``pages.dat``
        content.

        The new content is written to a sibling temporary file, fsynced and
        atomically renamed over ``pages.dat``, so an interrupted compaction
        leaves either the complete old state or the complete new state,
        never a mix.  Afterwards the record sequence continues from the new
        complete-record count.

        Raises ``FileNotFoundError`` when the root is missing, points at a
        file, or ``pages.dat`` is absent; ``RuntimeError("corrupt_middle")``
        (leaving the file untouched) when complete records follow a corrupt
        region; and ``OSError`` on read or write failure.
        """
        if not self.directory.is_dir() or not self.path.is_file():
            raise FileNotFoundError(f"no store at {self.path}; run init first")
        data = self.path.read_bytes()
        size = len(data)
        offset, records = 0, []
        while offset < size:
            record = self._record_at(data, offset)
            if record is None:
                break  # partial prefix, declared length past EOF, or bad payload
            records.append(json.loads(data[offset + 4:record["end"]].decode("utf-8")))
            offset = record["end"]
        if offset < size and self._has_record_after(data, offset):
            raise RuntimeError("corrupt_middle")
        live: dict[str, str] = {}
        for record in records:
            if record["op"] == "put":
                live[record["key"]] = record["value"]
            else:
                live.pop(record["key"], None)
        out = bytearray()
        for key in sorted(live):
            payload = json.dumps({"op": "put", "key": key, "value": live[key]},
                                 sort_keys=True).encode("utf-8")
            out += len(payload).to_bytes(4, "big") + payload
        temporary = self.directory / (LOG_FILE + ".tmp")
        with temporary.open("wb") as handle:
            handle.write(bytes(out))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.path)
        directory_fd = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return {"pages_before": (size + PAGE_SIZE - 1) // PAGE_SIZE,
                "pages_after": (len(out) + PAGE_SIZE - 1) // PAGE_SIZE,
                "records_before": len(records),
                "records_after": len(live),
                "keys": len(live),
                "discarded_tail_bytes": size - offset}

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
