# Tail Cache 内存淘汰设计

状态：已达成一致（尚未实现）  
范围：仅 `TailCacheFileSystem` 写路径上对 `TAIL_CACHE` 的淘汰。

## 目标

1. 内存允许时，尽量按 `expectedMinRetentionMs` 保留数据。
2. 用全局 cache 上限、单文件上限控制 cache chunk 的目标用量；`TAIL_CACHE` 始终完整缓存本次 input，上限只决定写前淘汰目标，不作为写入准入条件；`FULL_CACHE` 在 dirty 时允许为保持连续写而超限，clean 时按配额和大文件规则处理。
3. 只淘汰已安全落盘的数据（已 write 且已 fsync）。
4. 以 chunk 为淘汰单位；元数据尽量少；写路径尽量轻。
5. 中水位用异步文件分档打分，让大文件更积极淘汰；分数整表替换，file 自行读取（写路径不刷分）。

## 非目标（当前阶段）

- 淘汰 `FULL_CACHE`（Index 也属于 `FULL_CACHE`）。
- 把异步淘汰当作写停滞后的唯一手段（异步可后续再加；先用单文件上限兜底）。
- `NO_FS` 模式下的配额失败、降级和淘汰语义；本期不处理，后续集中设计和实现。

## 计量单位

内存用量、水位和配额统一使用实际申请的 **字节数**（`long`），不再把容量配置转换成 chunk 个数：

```text
globalCommittedBytes = sum(cache entry 实际持有的 ByteBuf capacity) + 尚未完成分配的 reservation
ratio                = globalCommittedBytes / maxCacheSizeBytes
```

Tail 和普通 FULL 仍使用固定逻辑 `chunkSize` 分块，因此每个此类 chunk 通常按 `chunkSize` 字节计费。atomic FULL 使用单个实际大小的 chunk 0，按其实际 `ByteBuf.capacity()` 计费。

Tail 一次 write 需要新增多少固定大小 chunk，必须按实际写入区间和现有 chunks 计算，再乘以 `chunkSize` 得到需要预占的字节数：

- 先使用当前尾 chunk 尚未写入的空间。
- 从 input 大小中扣除这部分可写空间，再计算剩余数据实际覆盖多少个新 chunk。
- 权威口径是：write 目标区间覆盖、但当前 `chunks` map 中尚不存在的 chunk index 数量。
- 普通连续追加且已有尾 chunk 时，可按下式计算：

```text
tailWritableBytes = 当前尾 chunk 尚未写入的字节数；不存在或已写满时为 0
remainingBytes    = max(0, inputBytes - tailWritableBytes)
newChunks         = ceil(remainingBytes / chunkSize)
```

上述“扣除当前尾 chunk 剩余空间”的计算用于已初始化 Tail。未初始化 Tail 需要先取得将作为 cache 起点的逻辑 `offset`，按 `[offset, offset + dataBytes)` 实际覆盖的 chunk index 数预占；不能只按 `dataBytes` 计算。Tail 始终完整缓存本次 input，不拆分 input，也不截取后缀。

本方案区分全局 committed 字节和按需动态计算的 entry 实际持有字节：

- `globalCommittedBytes` 在 reservation 成功时增加，分配成功时不重复增加；reservation 回滚或 ByteBuf 释放时减少。它用于全局配额、水位和分档判断，允许在分配窗口内略高于实际已分配内存。
- `FileCacheEntry` 不维护 `cacheSizeBytes`。需要自身实际容量时动态计算：chunks 为空时为 `0`，否则为 `chunks.size() * 任意一个 chunk.capacity()`。同一 entry 内多个 chunk 的 capacity 必须一致；普通 Tail / FULL 都是固定 `chunkSize`，atomic FULL 只有一个实际大小的 chunk 0，因此该公式无需知道 file 是否 atomic。
- `TAIL_CACHE`、`FULL_CACHE` 都参与全局 committed 字节及配额占用；任何超限分配也必须完整计入，计数不截断到配置上限。
- `FULL_CACHE` 只参与统计及配额占用，不参与淘汰；无脏数据时，无法完整容纳的 FULL 按大文件规则退出 cache。`minRetainChunks` 只作用于 `TAIL_CACHE` body。
- 元数据、底层 FS 使用的临时 IO buffer、`CompositeByteBuf`、Tail pending / in-flight write 持有的临时引用以及 allocator 保留内存均不计入。chunk 从 cache 移除后即归还其 committed 配额；若 pending write 的 retained slice 仍持有底层内存，物理释放可以晚于配额归还。
- 因此全局指标表达已经承诺给 cache 的 ByteBuf capacity，而不是进程全部 direct memory 的硬上限，不要求与某一瞬间实际持有的内存完全相等。底层 FS IO、Tail pending write 和 atomic replacement 都可能造成可接受的短暂物理峰值；超限分支下 committed 字节也可以超过配置值。

### 配额预占、超限与分配失败

需要新 chunk 时：

1. 先计算本次操作实际需要新增的 ByteBuf capacity。Tail 还要按“现有 chunks + 本次新增 chunks”得到 append 后的预计总量，据此计算写前淘汰目标；FULL 不做淘汰。
2. Tail 在 entry 锁内先对现有头部 chunk 执行时间淘汰，再在未达到容量 / score 阈值时执行强制淘汰；两阶段都只能删除 durable chunk，不能破坏 append 后的 `minRetainChunks`，也不能删除与本次 append 共用的尾 chunk。
3. 淘汰结束后，在分配任何 `ByteBuf`、复制数据或向 `chunks` 发布 cache 内容之前，一次性登记本次所需的全部字节。Tail 可以先执行只读取 size、设置起止 offset 的轻量初始化。
4. `TAIL_CACHE` 的 reservation 始终登记全部新增 capacity，不因单文件或全局上限不足返回 `false`。即使可淘汰 chunk 不足、append 后仍超过上限，也完整缓存 input；后续写入时继续按相同规则自然收敛。
5. `FULL_CACHE` 若含 dirty 数据（atomic FULL 使用 generation dirty 状态），为避免 cache 出现空洞，忽略本次单文件 / 全局配额不足，登记全部新增字节并完整缓存 input。clean FULL 因全局配额不足，或完整文件超过单文件上限时返回 `false`；后者同时设置共享“大文件”标识。
6. helper 返回 `false` 时由 `writeInternal` 统一降级到本次非 cache 写；不向上抛 cache 配额不足异常。本次不得分配部分 chunk，也不得留下部分 cache 数据或 offset 变化。
7. 配额登记成功后再分配 chunk。若 direct buffer 分配失败，释放本次已经分配的 `ByteBuf`，完整回滚 reservation 和尚未发布的 cache 状态，并返回 `false`。不为 chunk 发布、字段提交等代码非预期异常或 JVM OOM 设计额外恢复契约。

配额登记需要提供原子结果，并用 reservation/token 之类的对象统一提交或回滚。reservation 成功时立即增加 `globalCommittedBytes`；实际分配成功后只把 chunk 发布到 entry，entry 容量由 chunks 动态计算，全局 committed 不重复增加；失败回滚 reservation。所有上限都必须是正数，不提供 `0` 表示无限或关闭的语义。Tail reservation 允许计数超过配置值；FULL 仅在 dirty 连续写分支允许超限。

clean `FULL_CACHE` 配额不足按可降级处理：`ASYNC` 下本次绕过 cache 后写 backing FS，`NO_CACHE` 本身已经是直写路径，均不因 cache 配额不足中断业务。Tail 不因配额不足降级。只有 `NO_FS` 下配额失败的语义后续集中设计和实现。

### `writeInternal` 的 cache 写路径

Tail 初始化本质上只是读取 size 并设置 offset，没有文件数据 preload，因此无需把“未初始化 Tail”和“已初始化 Tail”拆成两个 helper。建议按 cache 语义拆成以下三个返回 boolean 的函数：

1. **Tail append**
   - entry 未初始化时先沿用现有 Tail init 逻辑取得 size 并设置 `cacheStartOffset` / `cacheEndOffset`。
   - 初始化后，根据实际 offset、当前尾 chunk 剩余空间和 input 覆盖范围计算本次需要新增的 chunk 数。
   - 用现有容量加新增 chunk capacity 计算 append 后预计总量，并据此完成时间淘汰及容量 / score 阈值淘汰。
   - 淘汰结束后登记全部新增字节并完整追加；Tail 不因单文件或全局上限不足降级，不拆分 input，也不截取后缀。
2. **FULL preload + append**
   - 仅用于普通 FULL entry 未初始化的情况。
   - 先读取文件 `size`，按普通 FULL 固定分块后实际需要申请的 capacity 预占：`ceil((size + data.readableBytes()) / chunkSize) * chunkSize`；成功后才读取完整文件并追加本次 input。
   - `size` 与实际读取量不一致时，按 `actualReadBytes + data.readableBytes()` 重新计算上述实际 capacity 并调整 reservation；补申请失败则回滚并保持未初始化。
3. **FULL append**
   - 用于已初始化的普通 FULL，以及 atomic FULL replacement。
   - 普通 FULL 根据当前尾 chunk 和 input 计算增量配额；atomic 按下文的新旧 cache 配额转移规则处理。

实现时也可以只保留 Tail append 和 FULL append 两个入口，在 FULL append 内部根据 entry 是否初始化选择 preload 分支；关键是不要再按 Tail 是否初始化拆出重复函数。

任一路径返回 `false` 时，`writeInternal` 都把本次 `useCacheSnapshot` 调整为 `false`，保持 input 可供后续非 cache 写路径使用；helper 不得提前消费或释放这份 input。若 entry 已初始化，helper 也不得自行清空它，后续复用 `writeInternal` 现有的
`if (!useCacheSnapshot && entry != null && entry.cacheStartOffset >= 0)` 分支：先 flush/await 尚未下刷的数据，再由该分支统一释放 cache，最后执行本次非 cache write；不额外实现另一套降级清理逻辑。

### 实现注意：集中管理 chunk 生命周期

- 所有参与全局 committed 统计的 chunk 分配、插入、移除和释放必须走统一封装，业务路径不得直接修改全局计数或只操作 `chunks` map。
- `FileCacheEntry` 和 `SegmentFileCacheEntry` 都不维护 size 或聚合 size 字段；单 entry 自身容量按 `chunks.size() * 任意 chunk.capacity()` 动态计算，并在需要精确结果时持有 entry 锁。
- 扫描线程的排序容量与实际淘汰容量使用不同口径：
  - 普通 AsyncFile：排序和淘汰都使用自身 entry 的动态容量；Index 也是 AsyncFile，有自己的 cache entry，但 FULL index 不参与淘汰。
  - Segment：排序时动态计算 body 容量加其持有的所有 index cache 容量；真正执行淘汰和单文件容量判断时只使用 segment body 容量，不考虑 index。
- 统一封装负责同步维护全局 committed 配额；entry 和 segment 的容量只在需要时从 chunks / index entries 动态计算。
- append、preload、truncate、delete、reset、close及可预期的分配失败回滚等路径都必须复用同一套 reserve / commit / rollback / release 操作。
- `releaseAllChunks` 也必须通过统一释放入口逐个或批量归还配额，避免 ByteBuf 已释放但统计未减少，或统计减少但内存仍被持有。

## 安全水位（可淘汰上界）

```text
durableFsOffset = writtenToFsOffset - pendingFsyncBytes
```

- `writtenToFsOffset` 在 `FileCacheEntry` 上。
- `pendingFsyncBytes` 在对应的 `AbstractStorageFile` 上；两者成对使用。
- pending 偏大可接受（更保守：算出来的 durable 更小）。除已保证耐久的路径外（如 roll/truncate 前已 seal flush），不得在未 fsync 时把 pending 清 0。
- `atomicReplaceWrite` 本身会 force；该路径不维护 pending 可接受。

下标为 `i` 的 chunk 仅当满足以下条件才允许因淘汰删除：

```text
(i + 1) * chunkSize <= durableFsOffset
```

（并同时满足下文其它硬规则。）

## 谁可以被淘汰

| 类型 | 是否淘汰 |
|------|----------|
| `TAIL_CACHE` 的 file / segment body | 是 |
| `FULL_CACHE` | 否 |

## 硬规则（始终生效）

1. **单文件最少保留 chunk 数**（`minRetainChunks`，可配，如 1 / 3 / 4）：按本次 append 完成后的预计 chunks 判断尾部保护区；写前淘汰后加上本次新增 chunks 不得低于该数量（空/未初始化 entry 除外）。与本次 append 起点共用的现有尾 chunk 始终不可预先删除。
2. **单文件最大 cache 字节数**（`maxCacheSizePerFileBytes`）：Tail 的写前淘汰目标，不是写入准入上限；无法淘汰到目标时仍完整 append。FULL dirty 时可为连续写超限，clean 时无法完整容纳则按大文件规则退出 cache。
3. **仅 durable**：不得超过 `durableFsOffset`。
4. **时间淘汰**使用每个 chunk 的 close 时间戳（见下）；可以突破「尽量保留满 retention」的软目标，但仍遵守 `minRetainChunks` 与 durable。
5. 从 **头部**（最老 chunk）淘汰，推进 `cacheStartOffset`，`release` ByteBuf，并删除对应时间戳。

### Tail 始终 append 与 FULL 超限

所有判断和状态变化都在 entry 锁内完成：

1. **TAIL_CACHE**
   - 不区分本次 input 是否为“大 write”，也不以 dirty / clean 决定是否允许写入。
   - 先按 append 后预计总量执行写前淘汰，再登记全部新增 capacity 并完整缓存 input。
   - 无法达到单文件或全局目标时允许超限；后续 write 继续执行正常的写前淘汰，新数据进入安全水位后自然释放。
2. **dirty FULL**
   - cache 中存在尚未完整落盘的数据时不能清空旧 cache；atomic FULL 使用 generation dirty 状态作等价判断。
   - 新 input 必须继续完整写入 cache，避免 cache 中间出现空洞；本次允许突破单文件及全局上限。
   - 若完整文件已超过单文件上限，同时设置共享“大文件”标识；本次仍完成 cache 写，后续操作不再尝试使用该 cache。
3. **clean FULL 无法完整容纳**
   - 初始化时原文件已超过单文件上限，或本次追加后完整文件将超过单文件上限时，设置共享“大文件”标识并返回 `false`。
   - `writeInternal` 复用现有降级路径释放 clean cache 并执行非 cache 写；后续操作直接绕过 cache。
   - truncate 成功后清除大文件标识并允许重新尝试。动态调整上限时本期不主动设置或清除该标识。

clean FULL 的其它 reservation 失败按正常降级路径处理。任何超限分配的实际 capacity 都必须完整计入 `globalCommittedBytes`，entry 自身容量由其 chunks 如实反映。

## `FULL_CACHE` 与 atomic

- atomic 是一种特殊的 `FULL_CACHE`；Index 也按普通 `FULL_CACHE` 处理，不单独定义规则。
- `FULL_CACHE` 不参与淘汰。无脏数据时无法完整容纳的 FULL 按大文件规则退出 cache；存在脏数据时，为保证连续写允许当前 write 超限。
- atomic FULL 不使用固定大小 chunk：完整内容始终存放在 key 为 `0` 的单个 ByteBuf 中，capacity 等于实际文件大小；读、写、flush 和 preload 都使用该单块布局。系统默认同一 key 的读写实例类型一致，不为 atomic 与非 atomic 混用提供兼容或保护。
- `loadFullFileCache` 必须先读取文件 size，计算完整 preload 实际需要申请的 ByteBuf capacity，检查单文件上限并预占全局字节配额；只有预占成功后才读取文件数据。不得先把完整文件读入内存，再发现配额不足而丢弃，避免配额持续不足时反复产生无效文件 IO。
- preload 读文件或构建 chunks 失败时释放临时数据并回滚预占配额；若读取期间文件 size 发生变化，必须按最终实际申请的 capacity 重新校验，不能超配额发布 cache。
- 拆分后的 cache 初始化 / 追加 helper 返回 boolean。返回 `false` 表示本次无法建立完整 cache，调用方不得保留部分 cache 结果。
- `TAIL_CACHE` 始终完整缓存本次 input；单文件及全局上限只决定写前淘汰目标，reservation 允许超限，不因配额不足返回 `false`。
- `FULL_CACHE` 有脏数据时先完成本次 cache 写；若完整文件超过单文件上限，同时设置“大文件”标识供后续操作绕过 cache。无脏数据时全局配额不足返回 `false`，仅本次 write 绕过 cache，下次仍可重新尝试。
- `FULL_CACHE` 在无脏数据时因 `maxCacheSizePerFileBytes` 无法容纳完整文件，不修改 `file.cacheMode`，而是在共享 entry 上设置“大文件”标识并返回 `false`；已初始化 clean cache 由 `writeInternal` 的统一降级分支释放。
- `useCache` 除检查 mode 外直接读取共享 entry 的“大文件”标识。标识存在期间，该 key 的所有实例都不再建立或追加 cache，避免只修改单个 file 实例后其它写实例仍继续扩大旧 FULL cache。
- “大文件”标识只控制 cache 初始化和写入准入，不要求读路径立即绕过已经发布的 cache。特别是 dirty FULL 因本次连续写而设置标识后，现有 reader 仍可读取该 cache；后续写路径看到 `useCache == false` 时，复用 `writeInternal` 的统一降级分支，先下刷并 fsync，再释放 cache。这样不会在 backing FS 尚未追平时让 reader 读取旧数据。
- 若标识存在但当前没有 live cache，读路径自然直接访问 backing FS；reader 不负责主动清空共享 cache。
- truncate 成功后清除“大文件”标识，使后续操作可以按截断后的实际大小重新尝试建立 FULL cache；若仍超过 `maxCacheSizePerFileBytes`，初始化时会再次设置标识。

atomic replace 使用旧 cache 配额转移的方式；构建新内容期间允许新旧 ByteBuf 短暂同时存在，但全局 committed 配额只按替换后的新 cache 大小计：

```text
oldBytes = 当前 atomic chunk 0 的 capacity
newBytes = 新内容的实际大小
delta    = newBytes - oldBytes
```

1. 在 entry 锁内判断旧 atomic cache 是否 dirty。若 `newBytes > maxCacheSizePerFileBytes`，先设置共享“大文件”标识：旧 cache clean 时返回 `false` 并绕过 cache；旧 cache dirty 时仍继续完成本次 cache replacement，后续操作再绕过 cache。
2. 若 `delta > 0`，先向全局配额申请 `delta`。旧 cache clean 且申请失败时返回 `false`，由 `writeInternal` 统一处理；旧 cache dirty 时为保证连续写允许超限登记。
3. 配额满足或进入 dirty 超限分支后，保持旧 chunk 0 不变，按 `newBytes` 分配并写完整的新 chunk。这里申请的是新旧大小的配额差值，不是只分配 `delta` 大小的 ByteBuf。
4. 若新 chunk 分配或构建失败，释放本次新分配的内容并回滚 `delta` reservation，旧 cache 保持不变；helper 返回 `false` 后复用 `writeInternal` 现有的下刷、释放和非 cache 写路径。
5. 新 chunk 完整建立后再原子发布为 chunk 0，然后释放旧 chunk；原有 `oldBytes` committed 配额直接转移给新 chunk。
6. 若 `delta < 0`，替换成功并释放旧 chunk 后归还 `-delta` 配额。
7. 任一时刻都不发布部分 atomic FULL cache。新旧 ByteBuf 短暂共存造成的实际内存峰值不额外计入 committed 配额；这会使 `globalCommittedBytes` 在 replacement 窗口内短暂低估实际持有的 direct memory，属于本期明确接受的统计误差。该例外只影响瞬时峰值，不改变替换完成后的 committed 结果和正常配额判断。

atomic read / transfer / flush 始终从 chunk 0 按真实 byte offset 切片，不使用 `offset / chunkSize` 查找；这些分支可以在公共读写 helper 内按 `file.atomicReplace` 特殊化，不要求新增独立 API。

atomic truncate 也按完整重写处理：先把截断后仍有效的内容读入临时 buffer，按新旧 capacity 差额完成配额申请；保持旧 chunk 0 不变，分配并写完整的新单块 chunk 0，成功后再原子替换并释放旧 chunk，最后归还多余配额。构建失败时回滚新分配和 reservation，保留旧 cache。直接只改 `cacheEndOffset` 不会释放实际内存，因此不可作为 atomic truncate 的最终实现。

## 每个 chunk 的 close 时间戳

- 在 chunk **关闭（封口）**时记录时间：写满并切到下一 chunk 时。
- close ts 是 chunk 自身的元数据（实现上可使用包含 `ByteBuf` 和 close ts 的 chunk record），不维护独立、生命周期可能脱节的时间戳表；chunk 删除后时间戳自然一起消失。
- 当前最后一个 chunk 仍处于打开、可写状态，其 close ts 没有意义，也不需要记录。
- 除当前最后一个 chunk 外，其余 chunk 都已经封口，close ts 均有意义；时间淘汰直接使用这些时间戳。
- 是否允许回收首先由 `minRetainChunks` 保护区决定，而不是额外依赖“是否为最后一个 chunk”的判断；最后一个 chunk 自然包含在保护区内。
- truncate 落在一个 chunk 中间时，该 chunk 重新成为可写尾 chunk，应清除旧 close ts；之后再次写满封口时记录新的 close ts。
- reset、delete、cache 释放或重新初始化都会删除原 chunk，因此不需要额外清理时间戳。
- segment roll 不改变 cache chunk，也不影响 chunk 的 close ts；两者相互独立。
- 默认约 1MiB/chunk 时，每 chunk 一个 int/long 相对数据量可忽略。

## 全局 committed 字节与水位

维护 `globalCommittedBytes`，包含 `TAIL_CACHE` 和 `FULL_CACHE` 已实际持有的 capacity，以及已成功 reservation、尚未完成实际分配的 capacity。

### tenant 接口预留

- 当前应用中每个 tenant 本质上只有少量大文件，本期直接按文件维度控制和打分，不实现 tenant 维度的统计、注册、配额、上限或分档代码。
- 现有 open API 的 tenant 参数保留，作为后续扩展接口；本期内存控制逻辑不读取、不存储该参数。
- 现有 `maxCacheSizePerTenantBytes` 配置及相关 getter/setter、运行时代码可以删除。

水位为简单除法，需要时现算（不必缓存「当前处于哪一档」）：

```text
ratio = globalCommittedBytes / maxCacheSizeBytes
```

`maxCacheSizeBytes` 必须为正数，是全局水位计算和 FULL 正常 reservation 的基准。Tail 始终允许超限，dirty FULL 连续写也可超限，因此 `globalCommittedBytes` 和 `ratio` 都可以超过配置值及 `1`。

### 快路径：低于绝对用量时只看时间

若 `globalCommittedBytes < timeOnlyBelowBytes`：

- **不算**水位分档 / 中水位 score floor。
- 仅对本 write 的 entry 做 **时间淘汰**（若超过单文件目标上限则在 durable 和 `minRetainChunks` 允许范围内尽量压回）。

### 水位分档（阈值可配；下表为示例）

| 水位 | 示例 ratio | 写路径行为 |
|------|------------|------------|
| 低 | `< lowRatio`（如 0.7） | 仅时间淘汰（只动自己）。 |
| 中 | `[lowRatio, highRatio)` | 时间淘汰 + 基于 score 的 **驱逐下限 floor**（见中水位打分）。 |
| 高 | `≥ highRatio`（如 0.9） | 与中水位同一套算法，但 floor 取 **大值**（等效 score≈最大）；可选额外启发式（如 entry 很大时每新 chunk 再砍 K 个已 durable 旧 chunk）。 |

高水位是中水位 floor 算法的「大值形态」，不是另一套独立策略。

## 单文件写路径淘汰顺序（中水位）

仅针对正在写入的 `TAIL_CACHE` entry：

```text
minKeep         = minRetainChunks
existingChunks  = 写前当前 chunk 数
newChunks       = 本次 append 需要新建的 chunk 数
projectedChunks = existingChunks + newChunks
projectedBytes  = 当前 entry 容量 + newChunks * chunkSize
maxEvictable    = max(0, projectedChunks - minKeep)

score = scores.get(fileKey)   // 无条目 => ABSENT（未入选）

scoreFloor = 0
若 score 存在:
  scoreFloor = max(newChunks, round(maxEvictable * score))   // score ∈ (0, 1]
  scoreFloor = min(scoreFloor, maxEvictable)
若处于高水位:
  scoreFloor = max(scoreFloor, largeFloor(maxEvictable))  // 例如 ≈ maxEvictable

capacityFloor = ceil(max(0, projectedBytes - maxCacheSizePerFileBytes) / chunkSize)
floor         = min(maxEvictable, max(scoreFloor, capacityFloor))

effectiveRetention = expectedMinRetentionMs
若 score 存在:
  effectiveRetention *= (1 - α * score)   // 可选：时间轴压短

1) append 前对现有头部 chunk 做时间淘汰，条件：
   - projectedChunks - 本轮已淘汰数 > minKeep
   - chunk 有 close ts 且 age >= effectiveRetention
   - chunk 落在 durable 水位内
   - chunk 不是与本次 append 起点共用的尾 chunk
2) 若本轮已淘汰数 < floor：
   忽略 retention，继续淘汰满足 durable、minKeep 和共用尾 chunk 约束的现有头部 chunk，
   直到达到 floor 或无法再删
3) 登记全部新增 capacity 并完整 append input；即使最终仍超过 maxCacheSizePerFileBytes
   或 maxCacheSizeBytes，也不降级
```

score floor **有意在每次 write 重复计算和执行**，不是每个评分周期只执行一次：

- `projectedChunks`、`projectedBytes` 和 `maxEvictable` 均按本次 append 完成后的预计状态计算，但实际淘汰发生在 append 前，候选仅来自现有 chunks。
- 若本次数据只写入现有尾 chunk、没有创建新 chunk，则 `newChunks == 0`，不会凭空增加基于新增量的淘汰要求。
- 容量上升时，入选文件的首要目标是不再继续增长；本次每新建一个 chunk，floor 至少增加一个 chunk。
- `round(maxEvictable * score)` 可以让本轮淘汰数大于本次新增数，使被提名的大文件主动缩小；这是预期行为。
- 只有被提名的文件承担 score floor，其它文件仍可能增长，因此被提名文件单次净缩小超过一个 chunk 是允许的。
- `minRetainChunks` 按 append 后预计尾部计算，是维持 tail read 能力的最小保留量，也是所有 floor 淘汰的最终硬下界。
- 时间淘汰已经删除的 chunk 计入本轮淘汰数；只有未达到 `floor` 时才继续执行忽略 retention 的强制淘汰。
- 上述目标始终受 durable 和共用尾 chunk 约束；无法达到 floor 时仍完整 append。

### 未入选文件（重要）

异步打分 **未提名** 的文件：

- **没有**默认/最低 score，也 **没有** 基于 score 的 floor。
- 仍会做：
  - **时间淘汰**（遵守 `minRetainChunks` + durable）；
  - **单文件目标上限** `maxCacheSizePerFileBytes`（无法继续淘汰时允许 Tail 超限）。
- **不会**获得中水位分档带来的 floor，也不会因分档做时间压短。

## 中水位异步打分（文件分档）

在中水位周期性执行。构建新的 `Map<fileKey, score>` 并 **整体原子替换**。写路径只按自己的 key 读取当前 map（不向各个 file 推送刷分）。

### 调度与生命周期

- `TailCacheFileSystem` 自己创建并持有一个独立的单线程调度器，不复用文件 IO executor；默认扫描周期为 **1 分钟**。
- 扫描周期支持运行时动态调整且必须为正数。调度任务每轮完成后读取最新周期并安排下一轮，因此更新无需中断正在执行的扫描，并从下一轮调度开始生效。
- 每轮触发时重新读取当前 committed 水位；处于中水位时执行文件扫描、分档并原子替换 score map。其它水位不要求刷新，写路径继续按各自水位规则处理。
- 单轮扫描的异常只记录日志，不允许终止后续调度。
- `TailCacheFileSystem.shutdown()` 先停止并关闭该调度器，再关闭 delegate；shutdown 开始后不得再安排新一轮扫描。

### 步骤 A — 文件按用量降序

仅收集可淘汰的 `TAIL_CACHE` body 文件，按扫描时动态计算的排序容量从大到小排列：普通 AsyncFile 使用自身 chunks 容量；Segment 使用 body chunks 加其持有的所有 index cache 容量。Segment 被提名后仍只淘汰 body，不淘汰 index。

该聚合口径可能使“index 很大、body 很小”的 Segment 被持续提名，但实际只能清理少量 body；当前业务不存在这种数据分布，本期接受该退化，不额外引入 index 淘汰。

### 步骤 B — 按占全局用量的累计比例给文件分档

示例：档宽 10%（可配；档数 ≥2 可配）：

- 使用加入当前文件 **之前**的累计占比 `sumBeforeBytes / globalCommittedBytes` 决定当前文件所在档；分母仍是包含 `TAIL_CACHE` 和 `FULL_CACHE` 的全局 committed 字节。
- `sumBeforeBytes / globalCommittedBytes` 落入 `[0, 10%)` → **第 1 档**。
- `sumBeforeBytes / globalCommittedBytes` 落入 `[10%, 20%)` → **第 2 档**。
- `sumBeforeBytes / globalCommittedBytes` 落入 `[20%, 30%)` → **第 3 档**，以此类推。
- 当前文件整体只属于其起始累计比例对应的一档，不按 chunk 拆分到多个档；分档后再把该文件用量加入累计值。
- 累计值达到所有提名档覆盖的总比例后停止，剩余文件不提名。

例如共有 3 档、每档 10%，最大的文件单独占全局 60%：处理它时
`sumBeforeBytes == 0`，因此它属于第 1 档；加入后累计值已超过 30%，扫描结束，不再产生第 2、3 档文件。

同一档内的文件打分力度一视同仁。

### 步骤 C — 按档赋予固定分

示例（可配）：

| 提名结果 | Score |
|----------|-------|
| 第 1 档文件 | `1.0` |
| 第 2 档文件 | `0.5` |
| （第 3 档） | `0.25` |
| 未提名 | *map 中无条目* |

通过替换整张 score map 发布。

若尚未扫描或 map 中无该 key → 按未入选处理。

score map 只使用 `fileKey`，不增加 entry generation，也不在 key 的最后一个实例释放时主动清理旧 score。若同 key 在关闭后很快重新打开，新生命周期允许短暂继承旧 score，直到下一次整表刷新；最坏结果只是 cache 暂时被压得更低，属于可接受的命中率影响，不是正确性问题。所有淘汰仍必须遵守 durable 和 `minRetainChunks`。

## 全局 / 单文件上限（部分配置已存在）

- `maxCacheSizeBytes`：全局 cache ByteBuf capacity 目标上限、水位基准和 FULL 正常 reservation 上限；对 Tail 不作为写入准入限制；必须 `> 0`
- **`maxCacheSizePerFileBytes`**（新增）：单 cache entry 字节目标上限和 FULL 正常 reservation 上限；对 Tail 只决定写前容量淘汰目标；必须 `> 0`
- **`minRetainChunks`**（新增）：淘汰时单 entry 硬下限，必须 `>= 1`
- 所有上限配置始终参与统计和写前判断，任何配置都不使用 `0` 表示无限或关闭，并且必须满足 `minRetainChunks * chunkSize <= maxCacheSizePerFileBytes`；Tail 始终可超过目标值，FULL 仅在 dirty 连续写时允许超过
- 现有 `TailCacheFileSystemConfig` 中全局上限的 `0` 默认值需要在实现时改为有效的正数配置；初始化或动态更新得到非正数上限时直接拒绝
- 动态调整上限不主动设置或清除 FULL 的“大文件”标识，也不增加独立收敛流程；后续 reservation、写路径淘汰和 cache 释放自然使用新配置
- `expectedMinRetentionMs`：主要时间目标，必须 `>= 0`；`0` 表示 chunk 封口后立刻过期，不承载“关闭时间淘汰”等特殊语义
- 生产配置不提供关闭时间淘汰的语义；是否可淘汰仍同时受 durable 和 `minRetainChunks` 约束
- `timeOnlyBelowBytes`：全局 committed 字节低于此值时，跳过水位/score floor
- `scoreScanIntervalMs`：后台文件分档扫描周期，默认 `60_000ms`，支持运行时动态调整且必须 `> 0`

## 挂点

1. reservation 成功/回滚及 chunk 释放 → 更新 `globalCommittedBytes`；entry / segment size 不维护，扫描或容量判断时动态计算；Tail chunk **封口**时写入 close ts。
2. cache 写路径分配前 → Tail 按 append 后预计总量先执行时间淘汰及容量 / score floor，再登记全部新增字节并完整追加；FULL 判断 dirty 连续写、大文件或正常 reservation 分支。
3. Tail 自有单线程后台任务 → 每轮按最新动态周期调度；中水位时执行文件分档并替换 score map；shutdown 时随 Tail 一起关闭。
4. fsync / `writeAndFlush` 中的 force 仍是 `pendingFsyncBytes` 的权威更新点。

## 建议实现顺序

1. chunk close 时间戳 + durable 时间淘汰 + `minRetainChunks` + `maxCacheSizePerFileBytes` + 全局/entry 字节计数。
2. 低于绝对阈值时的「只看时间」快路径。
3. 中水位 score map + 分档提名 + floor / 可选时间压短。
4. 高水位作为同一算法的大 floor（+ 可选更激进启发式）。

## 实现时再定的参数

- 水位比例；文件档宽（如 10%）；档数。
- 各档 score；时间压短系数 `α`。
- `minRetainChunks` / `maxCacheSizePerFileBytes` 默认值。
