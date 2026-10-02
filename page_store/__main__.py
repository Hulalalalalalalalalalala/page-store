"""Command line entry point: ``python3 -m page_store --root <dir> <subcommand>``.

Exit codes: 0 success, 1 a storage or verification error, 2 a usage error.
The read-only ``verify`` subcommand additionally uses 3 when the page file
cannot be read and 4 when records follow a corrupt region.
"""

from __future__ import annotations

import argparse
import json
import sys

from . import DOMAIN, SOURCE_CATEGORIES, __version__
from .core import PageStore

USAGE_ERROR = 2
STORAGE_ERROR = 1


def _corrupt_middle(store: PageStore) -> int:
    """Print the one-line corrupt_middle diagnostic; return the exit code.

    The offset is taken from a fresh read-only ``verify`` so it matches
    ``first_error_offset`` exactly; nothing is written to standard output.
    """
    offset = store.verify()["first_error_offset"]
    print(f"error: corrupt_middle at offset {offset}", file=sys.stderr)
    return STORAGE_ERROR


def _tags() -> list[str]:
    """Tags this domain claims: the comma-separated line that follows each named category heading."""
    import pathlib
    corpus = pathlib.Path(__file__).resolve().parent.parent / "corpus.md"
    if not corpus.is_file():
        return []
    wanted, tags, collect = set(SOURCE_CATEGORIES), [], False
    for line in corpus.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped in wanted:
            collect = True
            continue
        if not collect or not stripped:
            continue
        for token in stripped.split(","):
            token = token.strip().replace("\\", "")
            if token and token not in tags:
                tags.append(token)
        collect = False
    return tags


def _report(component_names: list[str], readiness: dict[str, bool]) -> str:
    import json
    return json.dumps({"domain": DOMAIN, "version": __version__, "sourceCategories": list(SOURCE_CATEGORIES),
                       "tags": _tags(), "components": component_names, "readiness": readiness},
                      ensure_ascii=False, sort_keys=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="page_store", description="追加页存储与崩溃恢复")
    parser.add_argument("--root", required=True, help="working directory")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init", help="create an empty store")
    put = sub.add_parser("put"); put.add_argument("key"); put.add_argument("value")
    get = sub.add_parser("get"); get.add_argument("key")
    delete = sub.add_parser("delete"); delete.add_argument("key")
    scan = sub.add_parser("scan"); scan.add_argument("--start"); scan.add_argument("--end")
    sub.add_parser("recover", help="reopen the page file")
    sub.add_parser("stats", help="print page, record and key counts")
    sub.add_parser("compact", help="rewrite live keys as ascending put records")
    sub.add_parser("report", help="print this domain's report as JSON")
    sub.add_parser("verify", help="verify the page file read-only")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    store = PageStore(args.root)
    try:
        if args.command == "init":
            store.init(); print(f"initialised {store.path}")
        elif args.command == "put":
            try:
                print(store.put(args.key, args.value))
            except RuntimeError as error:
                if str(error) == "corrupt_middle":
                    return _corrupt_middle(store)
                raise
        elif args.command == "get":
            value = store.get(args.key); print("" if value is None else value)
        elif args.command == "delete":
            try:
                print(store.delete(args.key))
            except RuntimeError as error:
                if str(error) == "corrupt_middle":
                    return _corrupt_middle(store)
                raise
        elif args.command == "scan":
            print(json.dumps(store.scan(args.start, args.end), ensure_ascii=False))
        elif args.command == "recover":
            try:
                print(json.dumps(store.recover(), sort_keys=True))
            except RuntimeError as error:
                if str(error) == "corrupt_middle":
                    return _corrupt_middle(store)
                raise
        elif args.command == "stats":
            print(json.dumps(store.stats(), sort_keys=True))
        elif args.command == "compact":
            try:
                result = store.compact()
            except FileNotFoundError:
                print(f"error: no store at {store.path}", file=sys.stderr)
                return STORAGE_ERROR
            except RuntimeError as error:
                if str(error) == "corrupt_middle":
                    return _corrupt_middle(store)
                raise
            except OSError:
                print("error: io_error", file=sys.stderr)
                return STORAGE_ERROR
            print(json.dumps(result))
        elif args.command == "report":
            print(_report(["pages", "directory"], {"append": True, "recover": True, "compaction": True, "snapshotRead": False}))
        elif args.command == "verify":
            outcome = store.verify()
            print(json.dumps(outcome, sort_keys=True))
            if outcome["status"] == "error":
                print(f"error: {outcome['error']}: {store.path}", file=sys.stderr)
                return USAGE_ERROR if outcome["error"] == "invalid_path" else 3
            if outcome["status"] == "corrupt_middle":
                print(f"error: corrupt record at offset {outcome['first_error_offset']}", file=sys.stderr)
                return 4
        return 0
    except FileNotFoundError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    except (KeyError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return USAGE_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
