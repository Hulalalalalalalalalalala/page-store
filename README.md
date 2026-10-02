# page-store

键值写入追加到固定大小的页文件，内存中维护有序目录；重开时从页文件重建，半写页被丢弃。同一目录可由同进程的多个 `PageStore` 实例或多个进程同时打开：目录下的协调文件 `.pages.dat.lock` 在读取或推进串行点时加文件锁，使并发结果等价于某一串行顺序。

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

- `init() -> None` 建立空存储（建目录是唯一允许自动创建路径的操作）。每次成功 `init` 都是一次串行状态变更：经临时文件原子替换出全新的空 `pages.dat`，重置前的写入被清空、重置后确认的写入保留；同一目录下其他长期存活的实例或进程下一次读取、`stats`、`snapshot` 或追加写入即看到新状态，无须关闭重开或先 `recover`（即使新文件长度与重置前相同或更长）。重置后、首次写入前 `stats` 三项均为 0，首次 `put`/`delete` 返回 1，此后序号从当前完整记录数继续递增。
- `put(key, value) -> int` 追加一条记录并返回记录序号。`key` 必须是非空字符串、`value` 必须是字符串，否则抛 `ValueError`。
- `get(key) -> bytes | None` 读取最后一次写入的值。
- `delete(key) -> int` 追加一条删除记录；`key` 必须是非空字符串，否则抛 `ValueError`。
- `write_batch(operations) -> list[int]` 原子追加一批 put/delete 操作。`operations` 必须是非空列表，每个元素是操作字典：put 恰含 `op`/`key`/`value` 三个字段，delete 恰含 `op`/`key` 两个字段；`key` 是非空字符串、`value` 是字符串，且每条单操作的 JSON 负载都不超过既有的单条大小限制（4096 字节）。非列表、空列表、非字典元素、未知操作、字段缺失或多余、非法键值、单条超限均抛 `ValueError`。整批校验先于任何存储操作：校验失败时页文件与序号都不变。有效批次按输入顺序作为**一个串行点**整体生效，重复键不合并、删除不存在的键也占一个序号；返回与操作逐项对应的连续整数序号，从当前完整记录数加一开始。整批以一帧落盘、可跨多个页，因此写入失败或进程被杀后重开只可能整批保留或整批不存在——未确认的批次就是半写尾，不占序号，由下一次写入在确认边界截断或由 `recover`/`compact` 丢弃；成功返回后全部操作已持久化。
- `scan(start=None, end=None) -> list[tuple[bytes, bytes]]` 按键升序返回区间内的存活记录（半开区间）。
- `write_batch_if(expected, operations) -> list[int] | None` 按当前键值条件提交整批操作。`expected` 是字典：键为非空字符串，值为字符串或 `None`；字符串要求该键当前存活值逐字相等，`None` 要求该键不存在（已删除算不存在，空字符串值不等于不存在）。条件键可以不参与写入，空字典表示无条件提交。`operations` 的非空列表、操作字段、键值类型及单条 4096 字节负载上限与 `write_batch` 完全相同。两个参数的全部校验先于任何存储操作，非法输入一律抛 `ValueError`（优先于存储错误），页文件与序号不变。条件检查与提交在协调锁内作为**一个串行操作**完成，与既有写入、重置、恢复、压缩共享串行顺序：两个调用同时检查同一旧值并试图改成新值时只有一个成功。任一条件不满足返回 `None`：不占序号，页文件逐字节不变，已有半写尾也不会被截断。成功时整批与 `write_batch` 一样以一帧落盘（条件本身不记录、不计入记录数），返回从当时完整记录数加一开始、与操作逐项对应的连续序号。有效输入在 root 不存在、指向文件或缺少 `pages.dat` 时抛 `FileNotFoundError`（不创建路径），中段损坏即使条件不满足也抛 `RuntimeError("corrupt_middle")` 且文件不变，其他读写失败抛 `OSError`。
- `write_batch_if_range(expected, operations, start=None, end=None) -> list[int] | None` 按**完整键区间**的当前快照条件提交整批操作，让“扫描后区间内被插入键”也能阻止提交。`expected` 是非空字符串键到字符串值的字典，表示半开区间 `[start, end)` 内**全部**存活键值：提交时区间内存活的键必须恰好是字典的键、值必须逐字相等，字典顺序无关；空字典要求区间内没有任何键。边界只接受字符串或 `None`：`None` 表示开放边界，空字符串是合法边界，排序与半开语义沿用 `scan`，`start >= end` 时区间为空（此时不允许任何条件键）。条件键必须落在区间内（键 `< start` 或 `>= end` 均抛 `ValueError`）。`operations` 的非空列表、操作字段、键值类型及单条 4096 字节负载上限与 `write_batch` 完全相同，且可以修改区间外的键——区间外的增删改不影响比较结果。全部参数先于任何存储访问校验：非字典 `expected`、非法条件键值、非法边界、区间外条件键或非法操作列表一律抛 `ValueError`（优先于存储错误），不改文件、不占序号。区间比较与提交在协调锁内作为**同一个串行操作**完成，与同机其他实例、线程和进程的写入、重置、恢复及压缩共享串行顺序。区间内新增键、删除键或改值都导致不匹配而返回 `None`；键值变化后又恢复原状仍可匹配；区间外变化不影响结果。不匹配时不占序号、页文件逐字节不变（已有半写尾也保留），半写尾不参与比较；成功时整批按输入顺序整体生效（重复键不合并、删除缺失键仍占号），与 `write_batch` 一样以一帧落盘（条件不记录、不计入记录数），返回从当时完整记录数加一开始的连续序号，成功返回即持久化，中断的跨页批次重开后整体保留或整体不存在，读取不见批内中间状态。有效输入在 root 不存在、指向文件或缺少 `pages.dat` 时抛 `FileNotFoundError`（不创建路径），中段损坏即使条件不匹配也抛 `RuntimeError("corrupt_middle")` 且文件不变，其他读写失败抛 `OSError`。

多个实例/进程共享同一目录时，`put`、`delete`、`write_batch`、`compact`、`recover` 在协调文件上互斥串行：并发结果等价于某次串行交织；每次成功的 `put`/`delete` 及成功批次中的每条操作返回从当前记录数严格递增、不重复的记录序号，失败或未确认（进程崩溃）的调用——包括整个未确认批次——不占号、不可见，其半写尾由下一次写入在确认边界截断，或由 `recover`/`compact` 丢弃；`pages.dat` 的完整记录不会被覆盖、撕裂或跳过。`get`、`scan`、`stats`、`snapshot` 持共享锁，只可能看到某一次串行操作（含整个批次）变更前或变更后的完整状态，绝不会看到批次内部的中间状态。旧存储（仅含 put/delete 帧的页文件）无需迁移即可使用，批次也不会隐式压缩历史。
- `snapshot() -> Snapshot` 捕获调用时刻的存活键值状态，得到只读快照，捕获在协调锁内完成；root 不存在、指向文件或缺少 `pages.dat` 时与其他读操作一样抛出 `FileNotFoundError`。快照捕获后不随后续 put、delete、recover、compact（无论来自本实例还是其他进程）改变。
  - `get(key)` 返回该键在快照时刻最后一次 put 的值，已删除或从未写入返回 `None`。
  - `scan(start=None, end=None)` 按键升序返回 `list[tuple[str, str]]`，`start` 含、`end` 不含，省略边界为开放区间，`start >= end` 返回空列表。
  - `stats()` 返回 `{pages, records, keys}`，口径同存储的 `stats()`，且不随后续 put、delete、recover 变化。
- `recover() -> dict` 重开页文件，返回 `{pages, records, truncated}`。
- `compact() -> dict` 将存活键值原子重写为按键升序的最少 `put` 记录，返回 `{pages_before, pages_after, records_before, records_after, keys, discarded_tail_bytes}`；root 不存在、指向文件或缺少 `pages.dat` 时抛出 `FileNotFoundError`，中段损坏抛出 `RuntimeError("corrupt_middle")` 且文件不变，读写失败抛出 `OSError`。
- `stats() -> dict` 返回页数、记录数与存活键数。
- `verify() -> dict` 只读校验页文件，返回上述固定字段的 JSON 口径字典。

## 约定

- 所有写操作立即持久化；进程被杀死后 `recover`/`init` 之外的重开不得丢失已确认的写。
- `stats`、`recover`、`verify` 的记录计数按已保留的操作条数计算：一个批次贡献其操作条数，批次包装帧本身不计数；页数仍按文件大小以 4096 向上取整的原口径计算。`verify` 保持只读，半写尾（含半写的批次帧）仍报 `incomplete_tail`；`compact` 仍把存活键值重写为最少的升序 put 记录，压缩后的序号从压缩后记录数继续，批次不会触发隐式压缩。
- 非法输入抛出 `ValueError`，未知标识抛出 `KeyError`。
- 退出码：0 成功，1 存储或校验错误，2 用法错误；`verify` 另用 3 表示页文件无法读取、4 表示中段损坏。

## 限制

- 没有页内二分查找，键索引常驻内存。
- 并发协调依赖同机文件锁（`fcntl.flock`，协调文件 `.pages.dat.lock`），仅适用于同一台机器上共享目录的实例与进程，不支持网络文件系统之外的多机协调。协调文件不计记录、不会被 `compact` 当作数据；删除后下次操作自动重建。
- 快照读捕获的是本实例（及共享该目录的其他进程）在该时刻的完整串行点状态。

## 语料

`corpus.md` 是本项目对应的技术标签语料（GitHub 热门技术标签）。`report` 子命令输出本域声明覆盖的分类与标签。
