"""An append-only page store with an in-memory ordered directory."""

from __future__ import annotations

import json
import os
from pathlib import Path

__all__ = ["PageStore", "verify_scan", "verify_error"]

PAGE_SIZE = 4096
LOG_FILE = "pages.dat"

#: Fixed key order of the single JSON line emitted by the ``verify`` subcommand.
VERIFY_KEYS = ("status", "complete_records", "valid_pages", "first_error_offset",
               "tail_partial_bytes", "scanned_end_offset", "error")


def _verify_result(status: str, complete_records: int, complete_end: int,
                   first_error_offset: int | None, tail_partial_bytes: int,
                   scanned_end_offset: int | None, error: str | None) -> dict:
    return {
        "status": status,
        "complete_records": complete_records,
        "valid_pages": complete_end // PAGE_SIZE,
        "first_error_offset": first_error_offset,
        "tail_partial_bytes": tail_partial_bytes,
        "scanned_end_offset": scanned_end_offset,
        "error": error,
    }


def verify_error(kind: str) -> dict:
    """A verify result for a path or read failure (counts zero, offsets null)."""
    return _verify_result("error", 0, 0, None, 0, None, kind)


def _declared_end(data: bytes, offset: int) -> int | None:
    """End offset implied by the length prefix at ``offset`` (no payload check)."""
    if offset + 4 > len(data):
        return None  # fewer than 4 bytes: a half-written length prefix
    size = int.from_bytes(data[offset:offset + 4], "big")
    if not 0 < size <= PAGE_SIZE:
        return None  # the format never frames an empty or larger-than-page record
    end = offset + 4 + size
    return None if end > len(data) else end


def _frame_end(data: bytes, offset: int) -> int | None:
    """End offset of one well-formed length-prefixed record at ``offset``, else None."""
    end = _declared_end(data, offset)
    if end is None:
        return None
    try:
        json.loads(data[offset + 4:end].decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None  # the frame is fully present but its payload is not a record
    return end


def verify_scan(data: bytes) -> dict:
    """Scan ``pages.dat`` bytes without mutating anything.

    Sequential framing either reaches end of file (``ok``), stops in a damaged
    tail (``incomplete_tail``), or stops at an interruption point before a
    suffix of complete records that resynchronises to end of file
    (``corrupt_middle``).

    ``good[offset]`` is true when the suffix at ``offset`` is exactly a run of
    well-formed records ending on end of file; computing it backwards is O(n)
    and it only drives the post-interruption resynchronisation check.
    """
    file_size = len(data)
    good = bytearray(file_size + 1)
    good[file_size] = 1
    for offset in range(file_size - 1, -1, -1):
        end = _declared_end(data, offset)  # cheap: the length prefix alone
        if end is not None and good[end]:
            try:
                json.loads(data[offset + 4:end].decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            good[offset] = 1

    offset, complete_records = 0, 0
    while offset != file_size:
        end = _frame_end(data, offset)
        if end is None:
            break
        complete_records += 1
        offset = end

    if offset == file_size:
        return _verify_result("ok", complete_records, offset, None, 0, file_size, None)

    for candidate in range(offset + 1, file_size):
        if good[candidate]:  # complete records exist behind the interruption
            return _verify_result("corrupt_middle", complete_records, offset,
                                  offset, 0, file_size, "corrupt_middle")

    return _verify_result("incomplete_tail", complete_records, offset, None,
                          file_size - offset, file_size, None)


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

    def recover(self) -> dict:
        records = self._records()
        return {"pages": (self.path.stat().st_size + PAGE_SIZE - 1) // PAGE_SIZE if self.path.is_file() else 0,
                "records": len(records), "truncated": False}

    def stats(self) -> dict:
        live = self._live()
        return {"pages": (self.path.stat().st_size + PAGE_SIZE - 1) // PAGE_SIZE if self.path.is_file() else 0,
                "records": len(self._records()), "keys": len(live)}

    def verify(self) -> dict:
        """Read-only integrity check; never raises for missing or unreadable data."""
        if not self.directory.is_dir() or not self.path.is_file():
            return verify_error("invalid_path")
        try:
            data = self.path.read_bytes()
        except OSError:
            return verify_error("read_error")
        return verify_scan(data)
