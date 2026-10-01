# page-store

键值写入追加到固定大小的页文件，内存中维护有序目录；重开时从页文件重建，半写页被丢弃。

## 依赖

仅标准库（Python 3.10+）。

## 安装与运行

无需安装，直接以模块方式运行（目录参数统一为 `--root`）：

```bash
python3 -m page_store --root ./state init
```

子命令：`init`、`put <key> <value>`、`get <key>`、`delete <key>`、`scan [--start S] [--end E]`、`recover`、`stats`、`report`、`verify`。

`verify` 只读校验页文件，不改动数据，也不影响后续 `recover`、`stats`、`report`。它在标准输出只输出一行 JSON，字段固定为：

- `status`：`ok`（全部记录完整）、`incomplete_tail`（仅尾部半写）、`corrupt_middle`（中断区后仍有记录）、`error`（路径或读取失败）。
- `complete_records`：连续解析得到的完整记录数；`incomplete_tail`/`corrupt_middle` 时只含中断点之前的部分。
- `valid_pages`：完整记录末偏移整除 4096 的页数。
- `first_error_offset`：`corrupt_middle` 时连续解析的中断点偏移，否则为 `null`。
- `tail_partial_bytes`：`incomplete_tail` 时尾部残缺字节数，否则为 0。
- `scanned_end_offset`：页文件大小；路径或读取失败无法确定时为 `null`。
- `error`：`null`、`invalid_path`（root 不存在、指向文件或缺少 `pages.dat`，退出码 2）、`read_error`（文件无法读取，退出码 3）或 `corrupt_middle`（退出码 4）。

失败时两项计数为 0，诊断只写标准错误。

## 公开接口

`page_store.PageStore(root)`：

- `init() -> None` 建立空存储。
- `put(key, value) -> int` 追加一条记录并返回记录序号。
- `get(key) -> bytes | None` 读取最后一次写入的值。
- `delete(key) -> int` 追加一条删除记录。
- `scan(start=None, end=None) -> list[tuple[bytes, bytes]]` 按键升序返回区间内的存活记录（半开区间）。
- `snapshot() -> Snapshot` 捕获调用时刻存活键值状态的只读快照；root 不存在、指向文件或缺少 `pages.dat` 时与其他读操作一样抛出 `FileNotFoundError`。
- `recover() -> dict` 重开页文件，返回 `{pages, records, truncated}`。
- `stats() -> dict` 返回页数、记录数与存活键数。
- `verify() -> dict` 只读校验页文件，返回上述固定字段的 JSON 口径字典。

`Snapshot` 只读快照（同一进程、同一 `PageStore` 实例内有效）：

- `get(key) -> str | None` 返回该键在捕获时刻最后一次 `put` 的值；已删除或从未写入返回 `None`。
- `scan(start=None, end=None) -> list[tuple[str, str]]` 按键升序返回 `[start, end)` 半开区间内的存活记录，省略边界表示开放区间，`start >= end` 时返回空列表。
- `stats() -> dict` 返回捕获时刻的 `{pages, records, keys}`：页数、连续完整记录数与存活键数。
- 快照创建后原存储继续 `put`、`delete` 或 `recover` 均不影响快照结果；快照只包含捕获时已确认的完整记录前缀（半写尾记录被排除）。

## 约定

- 所有写操作立即持久化；进程被杀死后 `recover`/`init` 之外的重开不得丢失已确认的写。
- 非法输入抛出 `ValueError`，未知标识抛出 `KeyError`。
- 退出码：0 成功，1 存储或校验错误，2 用法错误；`verify` 另用 3 表示页文件无法读取、4 表示中段损坏。

## 限制

- 没有页内二分查找，键索引常驻内存。
- 未实现压缩与页回收。
- 未实现并发写；快照读仅限同一进程、同一 `PageStore` 实例。

## 语料

`corpus.md` 是本项目对应的技术标签语料（GitHub 热门技术标签）。`report` 子命令输出本域声明覆盖的分类与标签。
