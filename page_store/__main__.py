"""Command line entry point: ``python3 -m page_store --root <dir> <subcommand>``.

Exit codes: 0 success, 1 a storage or verification error, 2 a usage error.
"""

from __future__ import annotations

import argparse
import json
import sys

from . import DOMAIN, SOURCE_CATEGORIES, __version__
from .core import PageStore

USAGE_ERROR = 2

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
    sub.add_parser("report", help="print this domain's report as JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    store = PageStore(args.root)
    try:
        if args.command == "init":
            store.init(); print(f"initialised {store.path}")
        elif args.command == "put":
            print(store.put(args.key, args.value))
        elif args.command == "get":
            value = store.get(args.key); print("" if value is None else value)
        elif args.command == "delete":
            print(store.delete(args.key))
        elif args.command == "scan":
            print(json.dumps(store.scan(args.start, args.end), ensure_ascii=False))
        elif args.command == "recover":
            print(json.dumps(store.recover(), sort_keys=True))
        elif args.command == "stats":
            print(json.dumps(store.stats(), sort_keys=True))
        elif args.command == "report":
            print(_report(["pages", "directory"], {"append": True, "recover": True, "compaction": False, "snapshotRead": False}))
        return 0
    except FileNotFoundError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    except (KeyError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return USAGE_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
