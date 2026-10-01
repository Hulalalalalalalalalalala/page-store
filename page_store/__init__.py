"""page_store: 追加页存储与崩溃恢复."""

__version__ = "0.1.0"

#: The technical domain this package belongs to.
DOMAIN = "storage-engine"

#: Category headings in corpus.md whose tags this domain claims.
SOURCE_CATEGORIES = ("🧮 数据库 / OLAP / OLTP", "🪶 数据存储 / 文件格式")

from .core import PageStore, Snapshot  # noqa: E402  (re-exported after the constants above)

__all__ = ["PageStore", "Snapshot", "DOMAIN", "SOURCE_CATEGORIES", "__version__"]
