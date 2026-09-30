# page-store

键值写入追加到固定大小的页文件，内存中维护有序目录；重开时从页文件重建，半写页被丢弃。

## 依赖

仅标准库（Python 3.10+）。

## 安装与运行

无需安装，直接以模块方式运行（目录参数统一为 `--root`）：

```bash
python3 -m page_store --root ./state init
```

子命令：`init`、`put <key> <value>`、`get <key>`、`delete <key>`、`scan [--start S] [--end E]`、`recover`、`stats`、`verify`、`report`。

## 公开接口

`page_store.PageStore(root)`：

- `init() -> None` 建立空存储。
- `put(key, value) -> int` 追加一条记录并返回记录序号。
- `get(key) -> bytes | None` 读取最后一次写入的值。
- `delete(key) -> int` 追加一条删除记录。
- `scan(start=None, end=None) -> list[tuple[bytes, bytes]]` 按键升序返回区间内的存活记录（半开区间）。
- `recover() -> dict` 重开页文件，返回 `{pages, records, truncated}`。
- `stats() -> dict` 返回页数、记录数与存活键数。
- `verify() -> dict` 只读校验，不改动数据；`verify` 子命令在标准输出只输出一行 JSON，key 固定为
  `status`、`complete_records`、`valid_pages`、`first_error_offset`、`tail_partial_bytes`、
  `scanned_end_offset`、`error`：
  - `ok`：完整记录数计入 `complete_records`，`valid_pages` 为完整记录末偏移整除 4096 的页数，
    `tail_partial_bytes=0`，`error` 与 `first_error_offset` 为 null。
  - `incomplete_tail`：仅有尾部半写记录，计数只含此前完整记录，`tail_partial_bytes` 为准确残缺字节数。
  - `corrupt_middle`：中断区后仍有完整记录，`first_error_offset` 为连续解析中断点，计数只含中断前部分，
    `tail_partial_bytes=0`，`error=corrupt_middle`。
  - 三种结果的 `scanned_end_offset` 均为文件大小；`error=invalid_path`（root 不存在、指向文件或缺
    `pages.dat`）与 `error=read_error`（文件无法读取）时两项计数为 0、偏移为 null。
  - 退出码：0 成功，2 invalid_path，3 read_error，4 corrupt_middle；诊断只写标准错误。

## 约定

- 所有写操作立即持久化；进程被杀死后 `recover`/`init` 之外的重开不得丢失已确认的写。
- 非法输入抛出 `ValueError`，未知标识抛出 `KeyError`。
- 退出码：0 成功，1 存储或校验错误，2 用法错误（`verify` 另用 3 read_error、4 corrupt_middle）。

## 限制

- 没有页内二分查找，键索引常驻内存。
- 未实现压缩与页回收。
- 未实现并发写与快照读。

## 语料

`corpus.md` 是本项目对应的技术标签语料（GitHub 热门技术标签）。`report` 子命令输出本域声明覆盖的分类与标签。
