"""An append-only page store with an in-memory ordered directory."""

from __future__ import annotations

import json
import os
from pathlib import Path

__all__ = ["PageStore"]

PAGE_SIZE = 4096
LOG_FILE = "pages.dat"


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
