"""An append-only page store with an in-memory ordered directory.

Several :class:`PageStore` instances -- in one process or in several -- may
share the same root directory.  A coordination file next to ``pages.dat``
(``.pages.dat.lock``) is locked while the serial point is read or moved, so
the outcome of concurrent writers is equivalent to running their calls one
after another: each successful put/delete (and every operation of a successful
batch) gets a strictly increasing record sequence number, a failed or
unconfirmed call -- including a whole unconfirmed batch -- never consumes a
number, and readers always see a complete before/after state.

Instances also survive ``fork`` (``os.fork`` and the multiprocessing ``fork``
start): a forked child keeps using the very objects it inherited, with no
rebuild or ``recover`` required.  Right after the fork the child drops the
lock file descriptor and threading primitives it copied from the parent --
which may carry a lock held by a thread the child never received -- and opens
its own fresh descriptor, so the child can only wait for storage operations
that a live parent thread or process is actually still holding; those are
released when the operation completes or the holder exits.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import threading
import weakref
from pathlib import Path

__all__ = ["PageStore", "Snapshot"]

PAGE_SIZE = 4096
LOG_FILE = "pages.dat"
LOCK_FILE = f".{LOG_FILE}.lock"

# --------------------------------------------------------------------- fork
#
# A forked child inherits every PageStore's coordination-file descriptor and
# gate lock.  The descriptor is a duplicate of the parent's open file
# description: if a parent thread held the flock at fork time, that lock is
# owned by a thread the child never received -- a child that kept using the
# inherited descriptor could block on it forever, and the inherited RLock may
# look held by a thread that does not exist in the child either.  The
# at-fork child hook resets every live instance before user code runs again;
# flock is tied to the open file description (not the pid), so closing the
# child's duplicate never disturbs a lock the parent genuinely still holds,
# while any lock of a process that exits is released by the kernel.

_INSTANCES: "set[weakref.ReferenceType[PageStore]]" = set()
_REGISTRY_LOCK = threading.RLock()


def _drop_instance(ref: "weakref.ReferenceType[PageStore]") -> None:
    with _REGISTRY_LOCK:
        _INSTANCES.discard(ref)


def _before_fork() -> None:
    # Held across the fork so the registry cannot change underneath the
    # single thread that survives in the child.
    _REGISTRY_LOCK.acquire()


def _after_fork_parent() -> None:
    _REGISTRY_LOCK.release()


def _after_fork_child() -> None:
    global _REGISTRY_LOCK
    # The copied lock may appear owned by a thread that stayed in the parent;
    # start with a fresh, uncontended one instead of trying to release it.
    _REGISTRY_LOCK = threading.RLock()
    for ref in list(_INSTANCES):
        store = ref()
        if store is not None:
            store._reset_after_fork()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(before=_before_fork,
                        after_in_parent=_after_fork_parent,
                        after_in_child=_after_fork_child)


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
        # The pid this instance has been prepared for; it differs after a
        # fork (see _after_fork_child / _reset_after_fork).
        self._pid = os.getpid()
        # Cached serial point: live directory, next sequence number (= number
        # of confirmed records), offset just past those records, and the
        # identity of the file they were read from.
        self._live: dict[str, str] | None = None
        self._seq = 0
        self._end = 0
        self._identity: tuple[int, int, int, int, int] | None = None
        with _REGISTRY_LOCK:
            _INSTANCES.add(weakref.ref(self, _drop_instance))

    def _reset_after_fork(self) -> None:
        """Detach this copied instance from the forking process's resources.

        Runs in the child immediately after a fork (and lazily via
        :meth:`_ensure_pid` if the hook somehow did not run).  The inherited
        coordination-file descriptor is closed without being unlocked and the
        gate is replaced: the descriptor shares the parent's open file
        description and the gate may be held by a thread the child never
        received.  The cached serial point is dropped too, since the parent
        (or a sibling) may move it while the child is alive; the first call
        rebuilds it under the child's own coordination lock.
        """
        fd = self._lock_fd
        self._lock_fd = None
        if fd is not None:
            # Closing this duplicate never releases a lock the parent still
            # holds: an flock lives on the shared open file description and is
            # released only once *all* its duplicates are closed (the parent
            # keeps its own).  It must not be unlocked explicitly, since
            # LOCK_UN on any duplicate would release the parent's live lock.
            with contextlib.suppress(OSError):
                os.close(fd)
        self._gate = threading.RLock()
        self._live = None
        self._seq = 0
        self._end = 0
        self._identity = None
        self._pid = os.getpid()

    def _ensure_pid(self) -> None:
        """Repair the instance if it finds itself in a different process.

        Belt-and-braces behind the ``register_at_fork`` hook: an instance
        created before the fork module was wired up, or a fork path that
        bypassed the hook, is repaired on its first locked section.  This is
        only safe to call when no other thread in *this* process can be
        inside the instance (e.g. the single surviving forking thread).
        """
        if self._pid != os.getpid():
            self._reset_after_fork()

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
        self._ensure_pid()
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
                self._lock_fd = os.open(
                    self._lock_path, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC,
                    0o600)
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
    def _record_at(data: bytes, offset: int) -> tuple[int, dict] | None:
        """Parse one record at ``offset``.

        A record is a four-byte big-endian length prefix followed by UTF-8
        bytes encoding a JSON object.  A bare record's payload is 1..4096
        bytes and its ``op`` is ``"put"`` or ``"delete"``; a ``put`` record
        carries a string ``value`` and a ``delete`` does not.  A batch
        record's ``op`` is ``"batch"`` and its ``ops`` is a non-empty list
        of those same put/delete objects; a batch frame may span several
        pages, so its payload is bounded only by the declared length rather
        than one page.  A ``delete`` ignores its ``value`` and bare ops
        ignore every other field.  Anything else -- a partial prefix, a
        declared length past EOF, a bare-record length outside 1..4096, bad
        UTF-8 or JSON, a scalar/array payload, an unknown op, a malformed
        batch, or a missing/invalid ``key``/``value`` -- is a break in the
        stream rather than a record, so callers treat it exactly like a
        half-written tail and never let a ``KeyError``/``TypeError`` escape.
        Returns the boundary just past the record and its decoded object, or
        ``None``.
        """
        size = len(data)
        if offset + 4 > size:
            return None
        rec_size = int.from_bytes(data[offset:offset + 4], "big")
        end = offset + 4 + rec_size
        if rec_size < 1 or end > size:
            return None
        try:
            decoded = json.loads(data[offset + 4:end].decode("utf-8"))
        except (UnicodeDecodeError, ValueError):
            return None
        if not isinstance(decoded, dict):
            return None
        op = decoded.get("op")
        if op == "batch":
            ops = decoded.get("ops")
            if not isinstance(ops, list) or not ops:
                return None
            for item in ops:
                if not PageStore._valid_item(item):
                    return None
            return end, decoded
        if op not in ("put", "delete"):
            return None
        # Bare put/delete records stay bounded to one page.
        if rec_size > PAGE_SIZE:
            return None
        key = decoded.get("key")
        if not isinstance(key, str) or not key:
            return None
        if op == "put" and not isinstance(decoded.get("value"), str):
            return None
        return end, decoded

    @staticmethod
    def _valid_item(item: object) -> bool:
        """Whether ``item`` is an exact put/delete operation dictionary."""
        if not isinstance(item, dict):
            return False
        op = item.get("op")
        if op == "put":
            if set(item) != {"op", "key", "value"}:
                return False
            return isinstance(item["key"], str) and bool(item["key"]) \
                and isinstance(item["value"], str)
        if op == "delete":
            if set(item) != {"op", "key"}:
                return False
            return isinstance(item["key"], str) and bool(item["key"])
        return False

    @staticmethod
    def _apply(live: dict[str, str], decoded: dict) -> int:
        """Replay one decoded record into ``live``; return its record count.

        A bare put/delete counts once; a batch applies its operations in
        order without merging and counts one per operation.
        """
        if decoded["op"] != "batch":
            if decoded["op"] == "put":
                live[decoded["key"]] = decoded["value"]
            else:
                live.pop(decoded["key"], None)
            return 1
        for item in decoded["ops"]:
            if item["op"] == "put":
                live[item["key"]] = item["value"]
            else:
                live.pop(item["key"], None)
        return len(decoded["ops"])

    @classmethod
    def _record_past(cls, data: bytes, offset: int) -> bool:
        """Whether a valid record starts at any later offset past ``offset``.

        Only a frame that satisfies the full record rule can be evidence of
        mid-file corruption; a scalar JSON value or an invalid object buried in
        the damaged region proves nothing.  The first offsets tried immediately
        after the break also cover the case where the frame's own declared
        length runs past EOF (its whole payload region is searched).

        Batch frames may span several pages, so the scan is O(n) cheap prefix
        checks: the full JSON decode runs only when the declared length fits
        and either stays within one page (a bare put/delete frame) or the
        payload opens like a JSON object carrying an ``op`` member, which a
        batch object must do within its first bytes.
        """
        size = len(data)
        for start in range(offset + 1, size - 3):
            rec_size = int.from_bytes(data[start:start + 4], "big")
            end = start + 4 + rec_size
            if rec_size < 1 or end > size:
                continue
            if rec_size > PAGE_SIZE:
                head = data[start + 4:start + 4 + 32]
                if not head.lstrip().startswith(b"{") or b'"op"' not in head:
                    continue
            if cls._record_at(data, start) is not None:
                return True
        return False

    def _scan_from(self, data: bytes, offset: int,
                   live: dict[str, str]) -> tuple[int, int]:
        """Replay whole records from ``offset`` into ``live``; return (end, count).

        Stops at the first boundary that is not a whole, valid record; the
        caller decides whether the remaining bytes are a half-written tail or
        mid-file corruption.
        """
        size = len(data)
        count = 0
        while offset < size:
            parsed = self._record_at(data, offset)
            if parsed is None:
                break
            offset, decoded = parsed
            count += self._apply(live, decoded)
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

    def _append_record(self, record: dict, count: int,
                       expected: dict[str, str | None] | None = None,
                       expect_range: "tuple[dict[str, str], str | None, str | None] | None" = None
                       ) -> list[int] | None:
        """Append one already-validated frame covering ``count`` records.

        Returns the consecutive sequence numbers assigned to those records,
        starting at one past the number of confirmed records.  A bare
        put/delete passes ``count == 1``; a batch frame passes the length of
        its operation list, and the whole frame lands -- or is rolled back --
        as one serial point, so a crash keeps either all of it or none.

        When ``expected`` is given, the frame is appended only if every
        condition holds at the serial point: a string must equal the key's
        current live value exactly, ``None`` requires the key to be absent
        (a deleted key is absent; an empty-string value is not).  When
        ``expect_range`` is given, it carries a required live mapping and a
        half-open ``[start, end)`` range, and the frame is appended only when
        the keys currently live inside that range equal the mapping exactly
        -- same keys, same values, in any order: an inserted key, a deleted
        key or a changed value all fail the condition, while keys outside the
        range are irrelevant.  A failed condition returns ``None`` instead: no
        number is consumed and the page file is left byte-for-byte untouched
        -- in particular a half-written tail is not truncated, since nothing
        was committed.  A half-written tail never takes part in the
        comparison, since only confirmed records are replayed into the
        serial point.
        """
        frame = self._encode(record)
        with self._locked(exclusive=True):
            # Another process may have appended or compacted since we last
            # synced; bring the serial point up to date first.
            self._resync()
            assert self._live is not None and self._identity is not None
            size = self._identity[2]
            if size - self._end > 0:
                data = self.path.read_bytes()
                if self._record_past(data, self._end):
                    # A complete, valid record exists beyond the unparseable
                    # region: the file is corrupt in the middle.  Never
                    # overwrite or skip those records; leave the file
                    # byte-for-byte untouched.
                    raise RuntimeError("corrupt_middle")
            if expected:
                # Conditions are checked against the serial point just
                # established, inside the same locked section as the commit,
                # so check-and-commit is one serial operation.  Live values
                # are always strings, so ``None`` from ``get`` means absent.
                for key, want in expected.items():
                    current = self._live.get(key)
                    if want is None:
                        if current is not None:
                            return None
                    elif current != want:
                        return None
            if expect_range is not None:
                wanted, rstart, rend = expect_range
                # Exact-equality check of the mapping of every key currently
                # live inside [start, end): same key set, same values.  Dict
                # equality ignores ordering, so the caller's key order is
                # irrelevant; a size mismatch catches an inserted or deleted
                # key.  Keys outside the range never enter the comparison.
                actual = {key: value for key, value in self._live.items()
                          if (rstart is None or key >= rstart)
                          and (rend is None or key < rend)}
                if actual != wanted:
                    return None
            try:
                with self.path.open("r+b") as handle:
                    # Cut at the confirmed boundary so a half-written tail
                    # left by a crashed (unconfirmed) writer is overwritten
                    # rather than sealed in front of the new record.
                    if size - self._end > 0:
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
            first = self._seq + 1
            self._apply(self._live, record)
            self._seq += count
            self._end += len(frame)
            try:
                self._identity = self._identity_of()
            except OSError:
                self._invalidate()
            return list(range(first, first + count))

    @staticmethod
    def _check_key(key: object) -> None:
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a non-empty string")

    def put(self, key: str, value: str) -> int:
        self._check_key(key)
        if not isinstance(value, str):
            raise ValueError("value must be a string")
        record = {"op": "put", "key": key, "value": value}
        payload_size = len(self._encode(record)) - 4
        if payload_size > PAGE_SIZE:
            raise ValueError(
                f"record exceeds one page ({payload_size} > {PAGE_SIZE})")
        return self._append_record(record, 1)[0]

    def delete(self, key: str) -> int:
        self._check_key(key)
        return self._append_record({"op": "delete", "key": key}, 1)[0]

    @classmethod
    def _normalize_operations(cls, operations: list[dict]) -> list[dict]:
        """Validate a batch operation list wholesale and normalise it.

        Raises ``ValueError`` for any malformed element or oversized single
        operation; nothing is stored and no sequence number is consumed.
        """
        if not isinstance(operations, list) or not operations:
            raise ValueError("operations must be a non-empty list")
        normalized: list[dict] = []
        for item in operations:
            if not cls._valid_item(item):
                raise ValueError(
                    "each operation must be a dict with op 'put' "
                    "(keys op, key, value) or 'delete' (keys op, key); "
                    "key must be a non-empty string and value a string")
            record = {"op": item["op"], "key": item["key"]}
            if item["op"] == "put":
                record["value"] = item["value"]
            if len(cls._encode(record)) - 4 > PAGE_SIZE:
                raise ValueError(
                    "an operation in the batch exceeds the one-record "
                    f"size limit ({PAGE_SIZE} bytes)")
            normalized.append(record)
        return normalized

    def write_batch(self, operations: list[dict]) -> list[int]:
        """Atomically append a non-empty batch of put/delete operations.

        Each element is an operation dict: a put has exactly the keys
        ``op``, ``key`` and ``value``; a delete has exactly ``op`` and
        ``key``.  Keys are non-empty strings and values are strings; every
        single operation must fit the existing one-record size limit.  The
        whole list is validated -- without touching the page file or
        consuming a sequence number -- before anything is stored.

        The batch takes effect as one serial point, operations applied in
        input order (duplicate keys are not merged and deleting a missing
        key still takes a number).  It may span several pages and is stored
        as one frame, so a failed write or a process crash leaves either the
        entire batch or none of it: an unconfirmed batch is a half-written
        tail on reopen and is discarded by the next write or ``recover``.
        Returns the operations' consecutive sequence numbers, starting at
        one past the current number of confirmed records.

        Raises ``ValueError`` for any malformed batch or oversized
        operation, ``FileNotFoundError`` when the root is missing, points
        at a file, or ``pages.dat`` is absent, ``RuntimeError
        ("corrupt_middle")`` when damage precedes complete records (the
        file is left untouched), and ``OSError`` on other read/write
        failures.
        """
        normalized = self._normalize_operations(operations)
        return self._append_record({"op": "batch", "ops": normalized},
                                   len(normalized))

    def write_batch_if(self, expected: dict[str, str | None],
                       operations: list[dict]) -> list[int] | None:
        """Commit a batch only if every condition in ``expected`` holds.

        ``expected`` maps keys to the state they must be in at the serial
        point: a string requires the key's current live value to be exactly
        equal, ``None`` requires the key to be absent (a deleted key is
        absent; an empty-string value is not).  Condition keys need not
        appear in ``operations``, and an empty ``expected`` dict commits
        unconditionally.  Both arguments are validated wholesale first --
        ``expected`` must be a dict with non-empty string keys and
        string-or-``None`` values, and ``operations`` follows the exact
        ``write_batch`` rules -- so any ``ValueError`` is raised before the
        store is touched, ahead of every storage error, and leaves the page
        file and sequence numbers unchanged.

        The condition check and the commit are one serial operation, sharing
        the coordination lock with every other write, reset, recovery and
        compaction across instances, threads and processes on this machine:
        two concurrent calls that both read the same old value and each try
        to replace it cannot both succeed.  If any condition fails the call
        returns ``None``: no sequence number is consumed and the page file
        is left byte-for-byte untouched, including any half-written tail a
        crashed writer left behind.  On success the batch lands exactly like
        a ``write_batch`` frame (conditions are not recorded and never count
        toward the record total) and the operations' consecutive sequence
        numbers are returned, starting at one past the number of confirmed
        records at that serial point.

        Raises ``ValueError`` for any malformed argument,
        ``FileNotFoundError`` when the root is missing, points at a file, or
        ``pages.dat`` is absent (no storage path is created),
        ``RuntimeError("corrupt_middle")`` when damage precedes complete
        records -- even when a condition would have failed -- and
        ``OSError`` on other read/write failures.
        """
        if not isinstance(expected, dict):
            raise ValueError(
                "expected must be a dict mapping keys to their required "
                "current value (a string) or None (key must be absent)")
        for key, want in expected.items():
            self._check_key(key)
            if want is not None and not isinstance(want, str):
                raise ValueError("expected values must be strings or None")
        normalized = self._normalize_operations(operations)
        return self._append_record({"op": "batch", "ops": normalized},
                                   len(normalized), expected=dict(expected))

    @staticmethod
    def _check_boundary(value: object, name: str) -> None:
        if value is not None and not isinstance(value, str):
            raise ValueError(f"{name} must be a string or None")

    def write_batch_if_range(self, expected: dict[str, str],
                             operations: list[dict],
                             start: str | None = None,
                             end: str | None = None) -> list[int] | None:
        """Commit a batch only if a whole key range matches ``expected``.

        ``expected`` maps non-empty string keys to the string values that
        must be the *entire* live content of the half-open range
        ``[start, end)`` at the serial point: the keys currently live in the
        range must be exactly its keys with exactly its values.  Dict order
        is irrelevant; an empty ``expected`` requires the range to contain no
        live keys at all.  Bounds follow the ``scan`` rules: ``start`` is
        inclusive and ``end`` exclusive, either may be ``None`` for an open
        bound, an empty string is a legal bound, and ``start >= end``
        describes an empty range.  Every condition key must itself lie inside
        the range.

        ``operations`` follows the exact ``write_batch`` rules and may touch
        keys outside the range; those writes never affect the comparison.
        All arguments are validated wholesale before the store is touched,
        so every ``ValueError`` is raised ahead of any storage error and
        leaves the page file and sequence numbers unchanged.

        The comparison and the commit are one serial operation under the
        coordination lock, shared with every other write, reset, recovery
        and compaction across instances, threads and processes: a key
        inserted by a scan-then-check transaction blocks the commit.  An
        insertion, deletion or value change anywhere in the range fails the
        condition (returning ``None``, byte-for-byte untouched, half-written
        tail included); changes outside the range do not, and a key that
        changed and was later restored to its original state matches again.
        On success the frame lands exactly like a ``write_batch`` frame --
        conditions are neither recorded nor counted -- and the operations'
        consecutive sequence numbers are returned, starting at one past the
        number of confirmed records at that serial point.

        Raises ``ValueError`` for any malformed argument (including a
        condition key outside the range), ``FileNotFoundError`` when the
        root is missing, points at a file, or ``pages.dat`` is absent (no
        storage path is created), ``RuntimeError("corrupt_middle")`` when
        damage precedes complete records -- even when the comparison would
        have failed -- and ``OSError`` on other read/write failures.
        """
        if not isinstance(expected, dict):
            raise ValueError(
                "expected must be a dict mapping the range's live keys to "
                "their required current string values; an empty dict "
                "requires the range to be empty")
        for key, want in expected.items():
            self._check_key(key)
            if not isinstance(want, str):
                raise ValueError("expected values must be strings")
        self._check_boundary(start, "start")
        self._check_boundary(end, "end")
        for key in expected:
            if (start is not None and key < start) or \
                    (end is not None and key >= end):
                raise ValueError(
                    f"condition key {key!r} lies outside the requested range")
        normalized = self._normalize_operations(operations)
        return self._append_record(
            {"op": "batch", "ops": normalized}, len(normalized),
            expect_range=(dict(expected), start, end))

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
                parsed = self._record_at(data, offset)
                if parsed is None:
                    break  # the continuous valid prefix ends here
                offset, decoded = parsed
                records_before += self._apply(live, decoded)
            if offset < size and self._record_past(data, offset):
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
            live: dict[str, str] = {}
            while offset < size:
                parsed = self._record_at(data, offset)
                if parsed is None:
                    break  # partial prefix, declared length past EOF/out of
                           # range, bad encoding/JSON, or an invalid payload
                offset, decoded = parsed
                records += self._apply(live, decoded)
            truncated = offset < size
            if truncated:
                if self._record_past(data, offset):
                    raise RuntimeError("corrupt_middle")
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
        offset = 0
        replay: dict[str, str] = {}
        while offset < size:
            parsed = self._record_at(data, offset)
            if parsed is None:
                break
            offset, decoded = parsed
            # A batch frame counts as one retained record per operation;
            # its wrapper metadata never counts.
            result["complete_records"] += self._apply(replay, decoded)
        result["valid_pages"] = offset // PAGE_SIZE
        if offset == size:
            return result
        if self._record_past(data, offset):
            result.update(status="corrupt_middle", error="corrupt_middle",
                          first_error_offset=offset)
        else:
            result.update(status="incomplete_tail", tail_partial_bytes=size - offset)
        return result
