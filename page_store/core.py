"""An append-only page store with an in-memory ordered directory.

Several :class:`PageStore` instances -- in one process or in several -- may
share the same root directory.  A coordination file next to ``pages.dat``
(``.pages.dat.lock``) is locked while the serial point is read or moved, so
the outcome of concurrent writers is equivalent to running their calls one
after another: each successful put/delete gets a strictly increasing record
sequence number, a failed or unconfirmed call never consumes a number, and
readers always see a complete before/after state.

Batches
-------
``write_batch`` appends several put/delete records as one serial operation.
On the page file a batch is a one-frame *batch marker*
(``{"op": "batch", "n": k}``) followed by k member frames whose op is the
internal ``"bput"``/``"bdelete"`` kind (same key/value rules).  Member frames
are only valid *inside* a batch: met on their own -- e.g. after a crash while
the marker itself was still being written -- they are an unparseable tail,
never standalone records or evidence of mid-file corruption.  A marker
commits only when all ``n`` of its members are whole and present, applying
them in order; the marker is internal metadata that is never returned as a
record, never takes a sequence number, and is invisible to
stats/recover/verify counts.  Files written before batches existed hold only
plain put/delete frames and replay with no migration.
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
    def _record_at(data: bytes, offset: int) -> tuple[int, str, dict] | None:
        """Parse one frame at ``offset``.

        A frame is a four-byte big-endian length prefix followed by 1..4096
        UTF-8 bytes encoding a JSON object.  Two frame kinds are whole and
        valid:

        * a put/delete *record* whose ``op`` is ``"put"`` or ``"delete"`` and
          whose ``key`` is a non-empty string; a ``put`` record additionally
          carries a string ``value``; a ``delete`` ignores its ``value`` and
          both ops ignore every other field;
        * the internal batch marker ``{"op": "batch", "n": k}`` with ``n`` an
          int in 1..2**32-1, which promises exactly ``n`` whole member frames
          follow it.

        Member frames use the internal ``"bput"``/``"bdelete"`` ops and are
        deliberately *not* accepted here: a member frame met outside a complete
        marker is a break in the stream, the same as any other half-written
        tail.  Anything else -- a partial prefix, a declared length past EOF or
        outside 1..4096, bad UTF-8 or JSON, a scalar/array payload, an unknown
        op, or a missing/invalid field -- is a break in the stream rather than
        a frame, so callers treat it exactly like a half-written tail and
        never let a ``KeyError``/``TypeError`` escape.  Returns the boundary
        just past the frame, its kind (``"put"``/``"delete"``/``"batch"``) and
        its decoded object, or ``None``.
        """
        size = len(data)
        if offset + 4 > size:
            return None
        rec_size = int.from_bytes(data[offset:offset + 4], "big")
        if not 1 <= rec_size <= PAGE_SIZE or offset + 4 + rec_size > size:
            return None
        try:
            decoded = json.loads(
                data[offset + 4:offset + 4 + rec_size].decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            return None
        if not isinstance(decoded, dict):
            return None
        op = decoded.get("op")
        if op == "batch":
            count = decoded.get("n")
            # bool is an int subclass; reject it along with floats/strings.
            if (not isinstance(count, int) or isinstance(count, bool)
                    or not 1 <= count <= 0xFFFFFFFF or len(decoded) != 2):
                return None
            return offset + 4 + rec_size, "batch", decoded
        if op not in ("put", "delete"):
            return None
        key = decoded.get("key")
        if not isinstance(key, str) or not key:
            return None
        if op == "put" and not isinstance(decoded.get("value"), str):
            return None
        return offset + 4 + rec_size, op, decoded

    @staticmethod
    def _member_at(data: bytes, offset: int) -> tuple[int, str, dict] | None:
        """Parse one batch member frame (``"bput"``/``"bdelete"``) at ``offset``.

        Member frames share the framing and key/value rules of put/delete
        records but carry the batch-internal ops, so they are whole only as
        part of a batch promised by a marker; returns the boundary, the public
        op (``"put"``/``"delete"``) and the record, or ``None``.
        """
        size = len(data)
        if offset + 4 > size:
            return None
        rec_size = int.from_bytes(data[offset:offset + 4], "big")
        if not 1 <= rec_size <= PAGE_SIZE or offset + 4 + rec_size > size:
            return None
        try:
            decoded = json.loads(
                data[offset + 4:offset + 4 + rec_size].decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            return None
        if not isinstance(decoded, dict):
            return None
        raw_op = decoded.get("op")
        if raw_op == "bput":
            if set(decoded) != {"op", "key", "value"}:
                return None
            op = "put"
        elif raw_op == "bdelete":
            if set(decoded) != {"op", "key"}:
                return None
            op = "delete"
        else:
            return None
        key = decoded.get("key")
        if not isinstance(key, str) or not key:
            return None
        if op == "put" and not isinstance(decoded.get("value"), str):
            return None
        return offset + 4 + rec_size, op, decoded

    @classmethod
    def _record_past(cls, data: bytes, offset: int) -> bool:
        """Whether a valid record starts at any later offset past ``offset``.

        Only a frame that satisfies the full record rule can be evidence of
        mid-file corruption; a scalar JSON value or an invalid object buried in
        the damaged region proves nothing.  The first offsets tried immediately
        after the break also cover the case where the frame's own declared
        length runs past EOF (its whole payload region is searched).
        """
        size = len(data)
        return any(cls._record_at(data, start) is not None
                   for start in range(offset + 1, size - 3))

    def _scan_from(self, data: bytes, offset: int,
                   live: dict[str, str]) -> tuple[int, int]:
        """Replay committed operations from ``offset`` into ``live``.

        Returns the boundary just past the last committed operation and the
        number of put/delete records replayed.  Plain put/delete frames commit
        one at a time; a batch marker commits only when every one of its ``n``
        frames is whole and present, in which case all members apply in order.
        Stops at the first boundary that is not a committed operation -- a
        broken frame, or a batch marker without all its frames (a crashed
        batch writer) -- leaving ``live`` and the boundary exactly as they
        were before that marker; the caller classifies the remainder as an
        unconfirmed tail or mid-file corruption.  Batch markers are metadata:
        they take no sequence number.
        """
        size = len(data)
        count = 0
        while offset < size:
            parsed = self._record_at(data, offset)
            if parsed is None:
                break
            end, kind, decoded = parsed
            if kind != "batch":
                if kind == "put":
                    live[decoded["key"]] = decoded["value"]
                else:
                    live.pop(decoded["key"], None)
                offset = end
                count += 1
                continue
            # Batch marker: collect all n members before applying any of
            # them, so a partially present batch is invisible as a unit.
            batch_end, members = end, []
            for _ in range(decoded["n"]):
                if batch_end >= size:
                    return offset, count
                member = self._member_at(data, batch_end)
                if member is None:
                    return offset, count
                member_end, member_kind, member_rec = member
                members.append((member_kind, member_rec))
                batch_end = member_end
            for member_kind, member_rec in members:
                if member_kind == "put":
                    live[member_rec["key"]] = member_rec["value"]
                else:
                    live.pop(member_rec["key"], None)
            offset = batch_end
            count += len(members)
        return offset, count

    @classmethod
    def _batch_member_break(cls, data: bytes, offset: int) -> int:
        """Boundary after the contiguous whole members of a marker at ``offset``.

        The marker has already been parsed as valid.  Member frames are walked
        in order until a declared member is missing or broken; the returned
        offset is where that member was expected.  A crash while writing a
        batch leaves a marker, some whole members and at most one partial
        member, so on real crash residue the walk stops at (or inside) that
        last frame.
        """
        parsed = cls._record_at(data, offset)
        assert parsed is not None  # caller guarantees a valid marker frame
        end, _, decoded = parsed
        pos = end
        for _ in range(decoded["n"]):
            if pos >= len(data):
                break
            member = cls._member_at(data, pos)
            if member is None:
                break
            pos = member[0]
        return pos

    @classmethod
    def _frame_past(cls, data: bytes, offset: int) -> bool:
        """Whether a whole frame (record, marker or batch member) lies later.

        Like :meth:`_record_past`, but inside an uncommitted batch region the
        batch-internal ``bput``/``bdelete`` frames are valid grammar too:
        whole members found past a broken member cannot be crash residue,
        because the serial appender writes members contiguously and could not
        have reached the later ones without writing the broken one.
        """
        size = len(data)
        return any(cls._record_at(data, start) is not None
                   or cls._member_at(data, start) is not None
                   for start in range(offset + 1, size - 3))

    def _tail_disposition(
            self, data: bytes, offset: int) -> tuple[str, int] | None:
        """Classify the bytes past a confirmed boundary ``offset``.

        ``None`` when the file ends exactly at the boundary; otherwise
        ``("tail", offset)`` for one discardable unconfirmed write -- a plain
        half-written frame, or a batch marker whose batch did not fully land --
        and ``("corrupt", error_offset)`` when a whole frame can still be
        resynchronised past the damaged region.

        For a torn batch the marker is part of the unconfirmed write: its
        contiguous whole members are exactly the crash residue, so they never
        count as resync evidence on their own.  At the first missing member the
        true frame's own length prefix decides: fewer than four bytes left or a
        declared length running past EOF is a partial final member (torn) even
        if its payload bytes happen to frame by coincidence.  Anything else
        past that member -- a further whole member, a plain record, or a nested
        marker -- proves mid-file corruption, because a serial batch append
        writes members contiguously and could not have produced a whole later
        frame beyond the gap.
        """
        size = len(data)
        if offset >= size:
            return None
        parsed = self._record_at(data, offset)
        if parsed is not None and parsed[1] == "batch":
            member_at = self._batch_member_break(data, offset)
            if member_at >= size:
                return "tail", offset  # whole members then EOF: missing members
            remaining = size - member_at
            declared = int.from_bytes(
                data[member_at:member_at + 4], "big") if remaining >= 4 else 0
            if remaining < 4 or (1 <= declared <= PAGE_SIZE
                                 and member_at + 4 + declared > size):
                return "tail", offset  # partial final member of the batch
            if self._frame_past(data, member_at):
                return "corrupt", member_at
            return "tail", offset
        if self._record_past(data, offset):
            return "corrupt", offset
        return "tail", offset

    def _resync(self) -> None:
        """Rebuild or extend the cached serial point from the current page file.

        Must be called while holding the coordination lock.  When the file has
        been replaced (another instance compacted) the whole image is read;
        otherwise only bytes past the previously confirmed offset are replayed,
        which also leaves a half-written tail -- or a batch marker whose batch
        did not fully land -- outside the confirmed boundary.  Like the
        pre-batch behaviour, a damaged region never raises here: read paths see
        the committed prefix, and the write/recover/compact paths classify the
        remainder with :meth:`_tail_disposition`.
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

    def _append_frames(self, blob: bytes,
                       effects: list[tuple[str, str, str]]) -> list[int]:
        """Append one already-encoded serial operation and confirm it.

        ``blob`` is the exact byte string to append (a single record frame, or
        a batch marker followed by its member frames); ``effects`` lists the
        operations to apply to the live directory in order as
        ``(op, key, value)``.  The whole blob is one serial point visible to
        other instances and processes only after its fsync, and it is rolled
        back as a unit on failure.  Returns the confirmed sequence numbers.
        """
        with self._locked(exclusive=True):
            # Another process may have appended, crashed or compacted since we
            # last synced; bring the serial point up to date first.
            self._resync()
            assert self._live is not None and self._identity is not None
            size = self._identity[2]
            if size - self._end > 0:
                data = self.path.read_bytes()
                disposition = self._tail_disposition(data, self._end)
                if disposition is not None and disposition[0] == "corrupt":
                    # A complete frame exists beyond the unparseable region:
                    # the file is corrupt in the middle.  Never overwrite or
                    # skip those records; leave the file byte-for-byte untouched.
                    raise RuntimeError("corrupt_middle")
            try:
                with self.path.open("r+b") as handle:
                    # Cut at the confirmed boundary so a half-written tail left
                    # by a crashed (unconfirmed) writer -- including a batch
                    # that landed only in part -- is overwritten rather than
                    # sealed in front of the new operation.
                    if size - self._end > 0:
                        handle.truncate(self._end)
                    handle.seek(self._end)
                    handle.write(blob)
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
            for op, key, value in effects:
                if op == "put":
                    self._live[key] = value
                else:
                    self._live.pop(key, None)
            first = self._seq + 1
            self._seq += len(effects)
            self._end += len(blob)
            try:
                self._identity = self._identity_of()
            except OSError:
                self._invalidate()
            return list(range(first, self._seq + 1))

    @staticmethod
    def _check_key(key: object) -> None:
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")

    def put(self, key: str, value: str) -> int:
        self._check_key(key)
        if not isinstance(value, str):
            raise ValueError("value must be a string")
        record = {"op": "put", "key": key, "value": value}
        frame = self._encode(record)
        if len(frame) - 4 > PAGE_SIZE:
            raise ValueError(
                f"record exceeds one page ({len(frame) - 4} > {PAGE_SIZE})")
        return self._append_frames(frame, [("put", key, value)])[0]

    def delete(self, key: str) -> int:
        self._check_key(key)
        record = {"op": "delete", "key": key}
        return self._append_frames(
            self._encode(record), [("delete", key, "")])[0]

    def write_batch(self, operations: list[dict]) -> list[int]:
        """Append a non-empty list of put/delete operations as one batch.

        Each operation is exactly ``{"op": "put", "key": k, "value": v}`` or
        ``{"op": "delete", "key": k}`` with a non-empty string key and string
        value.  The whole list is validated before anything is stored, so a
        rejected batch leaves the page file and the sequence numbers
        untouched; on disk the batch is one marker frame carrying the member
        count, followed by the member frames (batch-internal ``bput``/
        ``bdelete`` ops), and may span several pages.  On success the members take effect strictly in input order
        (repeated keys are not merged and deleting a missing key still takes a
        number) and a list of consecutive sequence numbers -- one per member,
        starting at the current complete-record count plus one -- is returned.

        The batch is one serial point with puts, deletes, resets, compactions
        and recoveries in this and other processes: readers and snapshots only
        ever see the complete before- or after-batch state, and after a crash
        or a failed write a reopened store keeps either the entire batch or no
        trace of it (an unkept batch consumes no numbers).  A damaged region in
        the middle of the file rejects the batch with
        ``RuntimeError("corrupt_middle")`` and leaves the file untouched;
        missing-store and read/write failures raise as elsewhere.
        """
        if not isinstance(operations, list) or not operations:
            raise ValueError("operations must be a non-empty list")
        frames: list[bytes] = []
        effects: list[tuple[str, str, str]] = []
        for operation in operations:
            if not isinstance(operation, dict):
                raise ValueError("each operation must be a dict")
            op = operation.get("op")
            if op == "put":
                if set(operation) != {"op", "key", "value"}:
                    raise ValueError("put takes exactly op, key and value")
                key, value = operation.get("key"), operation.get("value")
                self._check_key(key)
                if not isinstance(value, str):
                    raise ValueError("value must be a string")
                # Batch-internal op: the frame is whole only under a marker.
                frame = self._encode(
                    {"op": "bput", "key": key, "value": value})
                effects.append(("put", key, value))
            elif op == "delete":
                if set(operation) != {"op", "key"}:
                    raise ValueError("delete takes exactly op and key")
                key = operation.get("key")
                self._check_key(key)
                frame = self._encode({"op": "bdelete", "key": key})
                effects.append(("delete", key, ""))
            else:
                raise ValueError("unknown op: expected put or delete")
            # The same single-record limit as put/delete, checked per member;
            # the marker's own small frame always fits a page.
            if len(frame) - 4 > PAGE_SIZE:
                raise ValueError(
                    f"record exceeds one page ({len(frame) - 4} > {PAGE_SIZE})")
            frames.append(frame)
        marker = self._encode({"op": "batch", "n": len(frames)})
        return self._append_frames(marker + b"".join(frames), effects)

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

        Committed operations are replayed strictly along length-prefix
        boundaries; batch members apply in order and batch markers are
        discarded.  Old values and delete records are discarded; one
        unconfirmed write at the tail -- a half-written record or a batch that
        did not fully land -- is discarded too.  The result is written to a
        temporary file and atomically swapped in, so an interrupted compaction
        leaves either the complete old file or the complete new one -- never a
        mixture.

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
            live: dict[str, str] = {}
            offset, records_before = self._scan_from(data, 0, live)
            disposition = self._tail_disposition(data, offset)
            if disposition is not None and disposition[0] == "corrupt":
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
        """Reopen the page file, dropping one unconfirmed write at the tail.

        Committed operations -- plain records and complete batches -- are
        scanned strictly along length-prefix boundaries, with batch markers
        skipped from the count.  The first boundary that is not a whole,
        committed operation marks an unconfirmed write: a half-written record,
        or a batch marker whose batch did not fully land; scanning stops and
        every byte from that boundary on is removed as a unit, so an unkept
        batch is invisible and takes no sequence number, and later appends
        continue right after the last confirmed operation.

        If a whole frame can still be re-synchronised past the stop offset
        (including a complete member beyond a gap inside a torn batch), the
        damage is mid-file corruption rather than a half-written tail:
        ``RuntimeError("corrupt_middle")`` is raised and the file is left
        byte-for-byte untouched.

        Recovery shares the serial point with writes, so only a confirmed tail
        is ever truncated and concurrent appends cannot race the cut.
        """
        with self._locked(exclusive=True):
            self._require_store()
            data = self.path.read_bytes()
            size = len(data)
            live: dict[str, str] = {}
            offset, records = self._scan_from(data, 0, live)
            disposition = self._tail_disposition(data, offset)
            if disposition is not None and disposition[0] == "corrupt":
                raise RuntimeError("corrupt_middle")
            truncated = offset < size
            if truncated:
                # One unconfirmed write: a plain half-written tail record, or
                # a batch marker whose batch did not fully land.  Cut the whole
                # region at the marker so an unkept batch takes no number.
                with self.path.open("r+b") as handle:
                    handle.truncate(offset)
                    handle.flush()
                    os.fsync(handle.fileno())
            self._live = live
            self._seq = records
            self._end = offset
            self._identity = self._identity_of()
            return {"pages": (offset + PAGE_SIZE - 1) // PAGE_SIZE,
                    "records": records, "truncated": truncated}

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
        # Committed operations only: a batch marker advances the prefix just
        # when all its members are whole, and the marker itself is never
        # counted; a marker short of members ends the committed prefix at the
        # marker, exactly like a half-written tail.
        offset, complete = self._scan_from(data, 0, {})
        result["complete_records"] = complete
        result["valid_pages"] = offset // PAGE_SIZE
        if offset == size:
            return result
        disposition = self._tail_disposition(data, offset)
        if disposition is not None and disposition[0] == "corrupt":
            result.update(status="corrupt_middle", error="corrupt_middle",
                          first_error_offset=disposition[1])
        else:
            result.update(status="incomplete_tail",
                          tail_partial_bytes=size - offset)
        return result
