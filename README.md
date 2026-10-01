# page-store

键值写入追加到固定大小的页文件，内存中维护有序目录；重开时从页文件重建，半写页被丢弃。同一目录可被同进程的多个 `PageStore` 实例或多个进程并发打开，操作经同目录协调文件互斥，并发结果与某种串行顺序等价。

## 依赖

仅标准库（Python 3.10+）。

## 安装与运行

无需安装，直接以模块方式运行（目录参数统一为 `--root`）：

```bash
python3 -m page_store --root ./state init
```

子命令：`init`、`put <key> <value>`、`get <key>`、`delete <key>`、`scan [--start S] [--end E]`、`recover`、`stats`、`compact`、`report`、`verify`。

`compact` 把存活键值重写为按键升序、数量最少的 `put` 记录（旧值与删除记录不落盘，半写尾记录一并丢弃），经临时文件原子替换 `pages.dat`：压缩中断后只留下完整旧文件或完整新文件，不会混合。相同存活状态生成相同的记录顺序与文件内容，压缩后首次 `put`/`delete` 的序号从新的完整记录数继续递增。成功时标准输出只写一行 JSON，字段固定且顺序为：

- `pages_before` / `pages_after`：压缩前后占用页数（文件大小按 4096 向上取整）。
- `records_before` / `records_after`：压缩前确认的完整记录数 / 压缩后记录数（等于存活键数）。
- `keys`：存活键数。
- `discarded_tail_bytes`：被丢弃的半写尾字节数（无尾部时为 0）。空存储六个字段全为 0。

压缩失败时诊断只写标准错误并退出 1：root 不存在、指向文件或缺少 `pages.dat` 为 `error: no store at PATH`；中段损坏为 `error: corrupt_middle at offset N`（N 取 `verify` 的 `first_error_offset`，且文件保持不变）；读写失败为 `error: io_error`。

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
- `put(key, value) -> int` 追加一条记录并返回记录序号：成功时序号从当前确认记录数严格递增、全局唯一不重复（从 1 起）；失败或未确认的调用不占号、不可见。
- `get(key) -> bytes | None` 读取最后一次写入的值。
- `delete(key: str) -> int` 追加一条删除记录，返回序号，口径同 `put`。
- `scan(start=None, end=None) -> list[tuple[bytes, bytes]]` 按键升序返回区间内的存活记录（半开区间）。
- `snapshot() -> Snapshot` 捕获调用时刻的存活键值状态，得到只读快照；root 不存在、指向文件或缺少 `pages.dat` 时与其他读操作一样抛出 `FileNotFoundError`。
  - `get(key)` 返回该键在快照时刻最后一次 put 的值，已删除或从未写入返回 `None`。
  - `scan(start=None, end=None)` 按键升序返回 `list[tuple[str, str]]`，`start` 含、`end` 不含，省略边界为开放区间，`start >= end` 返回空列表。
  - `stats()` 返回 `{pages, records, keys}`，口径同存储的 `stats()`，且不随后续 put、delete、recover 变化。
- `recover() -> dict` 重开页文件，返回 `{pages, records, truncated}`。
- `compact() -> dict` 将存活键值原子重写为按键升序的最少 `put` 记录，返回 `{pages_before, pages_after, records_before, records_after, keys, discarded_tail_bytes}`；root 不存在、指向文件或缺少 `pages.dat` 时抛出 `FileNotFoundError`，中段损坏抛出 `RuntimeError("corrupt_middle")` 且文件不变，读写失败抛出 `OSError`。
- `stats() -> dict` 返回页数、记录数与存活键数。
- `verify() -> dict` 只读校验页文件，返回上述固定字段的 JSON 口径字典。

## 约定

- 所有写操作立即持久化；进程被杀死后 `recover`/`init` 之外的重开不得丢失已确认的写。崩溃留下的半写尾仍由 `recover` 或 `compact` 丢弃；下一次持锁的 `put`/`delete` 也会在追加前把它截掉，使序号紧贴最后确认记录。
- 多个 `PageStore` 实例（同进程多实例或多进程）共享一个目录时，`put`、`delete`、`compact`、`recover` 之间以同目录协调文件 `.pages.dat.lock` 上的排他 `flock` 串行化，`get`、`scan`、`stats`、`snapshot`、`verify` 取共享锁。因此并发结果等价于某种串行顺序：同 key 的最终 `get`/`scan`/`stats` 只见最后一次变更，异 key 变更不丢，`pages.dat` 的完整记录不会被覆盖、撕裂或跳过。
- `compact` 的原子替换是一个串行点：`pages_before`、`pages_after`、`records_before`、`records_after`、`keys`、`discarded_tail_bytes` 同属该点的同一状态，替换后序号从压缩后记录数继续。`recover` 只截断串行点确认的半写尾；截断区后仍能再同步出完整记录时抛 `RuntimeError("corrupt_middle")` 且文件字节不变。
- 读操作与 `snapshot` 只会看到某次变更前或变更后的完整状态；快照捕获后不随后续 `put`、`delete`、`recover`、`compact`（即使来自其他进程）改变。
- 协调文件 `.pages.dat.lock` 与压缩临时文件不计记录、不被 `compact` 当数据，`stats`/`verify` 只描述 `pages.dat`。
- `put`/`delete` 的 `key` 必须是非空字符串，`put` 的 `value` 必须是字符串，否则抛 `ValueError`；除 `init` 会建目录外，root 不存在、是文件或缺少 `pages.dat` 时抛 `FileNotFoundError`；协调、读写、原子替换失败抛 `OSError`；未知标识抛出 `KeyError`。
- 退出码：0 成功，1 存储或校验错误，2 用法错误；`verify` 另用 3 表示页文件无法读取、4 表示中段损坏。

## 限制

- 没有页内二分查找，键索引常驻内存。
- 跨进程互斥依赖 `flock(2)`（POSIX 语义；与 Linux/macOS 上的本地文件系统配合使用）。快照为只读对象，可在同进程任意实例上只读使用。

## 语料

`corpus.md` 是本项目对应的技术标签语料（GitHub 热门技术标签）。`report` 子命令输出本域声明覆盖的分类与标签。
