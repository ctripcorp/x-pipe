# Tail Cache 内存淘汰设计

状态：已达成一致（尚未实现）  
范围：仅 `TailCacheFileSystem` 写路径上对 `TAIL_CACHE` 的淘汰。

## 目标

1. 内存允许时，尽量按 `expectedMinRetentionMs` 保留数据。
2. 用全局 cache 上限、单文件上限约束 cache chunk 的分配内存。
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
globalUsedBytes = sum(cache entry 实际持有的 ByteBuf capacity)
ratio           = globalUsedBytes / maxCacheSizeBytes
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

上述“扣除当前尾 chunk 剩余空间”的计算用于已初始化 Tail。未初始化 Tail 需要先取得将作为 cache 起点的逻辑 `offset`，按 `[offset, offset + dataBytes)` 实际覆盖的 chunk index 数预占；不能只按 `dataBytes` 计算。大 input 截取后缀路径则按最终实际保留区间覆盖的 chunk index 数计算。

本方案中的内存用量、统计、水位以及配额，均只针对 cache 实际持有的
`ByteBuf.capacity()`：

- 每分配一个 ByteBuf 增加其实际 capacity，每释放一个 ByteBuf 减少同样的值。
- `TAIL_CACHE`、`FULL_CACHE` 都参与全局字节用量统计及配额占用。
- `FULL_CACHE` 只参与统计及配额占用，不参与淘汰；`maxCacheSizePerFileBytes` 同时约束 `TAIL_CACHE` 和 `FULL_CACHE`，`minRetainChunks` 只作用于 `TAIL_CACHE` body。
- 元数据、临时 IO buffer、`CompositeByteBuf` 以及 allocator 保留内存均不计入。
- 因此这些上限是 cache 持有的 ByteBuf capacity 上限，不是进程实际内存的硬上限。

### 配额预占与分配失败

全局和单文件上限同时也是相应新 chunk 分配的硬门禁。需要新 chunk 时：

1. 先计算本次操作实际需要新增的 ByteBuf capacity；`TAIL_CACHE` 可先按当前规则淘汰本 entry 的可淘汰 chunk，以释放已有字节配额，`FULL_CACHE` 不做淘汰。
2. 在分配任何 `ByteBuf`、复制数据或向 `chunks` 发布 cache 内容之前，一次性预占本次所需的全部配额。Tail 可以先执行只读取 size、设置起止 offset 的轻量初始化，但配额失败时必须恢复未初始化状态。
3. 任一受限维度的剩余配额不足时，配额层返回预占失败，cache helper 转为 `false`，由 `writeInternal` 统一降级到本次非 cache 写；不向上抛 cache 配额不足异常。本次不得分配部分 chunk，也不得留下部分 cache 数据或 offset 变化。
4. 配额预占成功后再分配 chunk。若实际内存分配、插入或后续初始化失败，必须释放已分配的 `ByteBuf` 并完整回滚本次预占的配额和 cache 状态。

配额预占需要提供成功或失败的原子结果，并用 reservation/token 之类的对象统一提交或回滚，避免全局、单文件字节数只更新一部分。所有上限都必须是正数，不提供 `0` 表示无限或关闭的语义，所有维度都参与预占判断。

本期所有涉及 cache 的配额不足统一按可降级处理：`ASYNC` 下本次绕过 cache 后写 backing FS，`NO_CACHE` 本身已经是直写路径，均不因 cache 配额不足中断业务。只有无法降级的 `NO_FS` 后续需要定义并抛错，但不属于本期范围。

### `writeInternal` 的 cache 写路径

Tail 初始化本质上只是读取 size 并设置 offset，没有文件数据 preload，因此无需把“未初始化 Tail”和“已初始化 Tail”拆成两个 helper。建议按 cache 语义拆成以下三个返回 boolean 的函数：

1. **Tail append**
   - entry 未初始化时先沿用现有 Tail init 逻辑取得 size 并设置 `cacheStartOffset` / `cacheEndOffset`。
   - 初始化后，根据实际 offset、当前尾 chunk 剩余空间和 input 覆盖范围计算本次需要新增的 chunk 数。
   - 执行必要的预淘汰和配额预占，成功后追加；失败时，如果是本次刚初始化的空 entry，则恢复未初始化状态。
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

- 所有参与统计的 chunk 分配、插入、移除和释放必须走统一封装，业务路径不得直接修改计数或只操作 `chunks` map。
- `FileCacheEntry` 维护自身 `cacheSizeBytes`，所有 chunk 变更时按实际 capacity 增减；单文件配额和文件排序直接读取该字段，不遍历 chunks。
- `SegmentFileCacheEntry` 另外维护聚合 size，覆盖 segment body 及其持有的 FULL index entry，index 分配/释放时同步更新，避免每次统计都遍历 `indexFiles`。需要仅针对可淘汰 body 做决策时，仍使用 body 自身 size。
- 统一封装负责区分 `TAIL_CACHE`、`FULL_CACHE` 的统计与淘汰属性，并同步维护全局、单文件字节配额以及上述 entry size。
- append、preload、truncate、delete、reset、close、初始化失败及异常回滚等路径都必须复用同一套 reserve / commit / rollback / release 操作。
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

1. **单文件最少保留 chunk 数**（`minRetainChunks`，可配，如 1 / 3 / 4）：尾部最后 `minRetainChunks` 个 chunk 是保护区，不作为淘汰候选；因此淘汰后也不得低于该数量（空/未初始化 entry 除外）。
2. **单文件最大 cache 字节数**（`maxCacheSizePerFileBytes`）：`TAIL_CACHE` / `FULL_CACHE` 分配前都必须保证操作后的 `cacheSizeBytes` 不超过该硬上限。Tail 可先在 durable 和 `minRetainChunks` 约束内预淘汰；FULL 无法完整容纳时按大文件降级规则处理。
3. **仅 durable**：不得超过 `durableFsOffset`。
4. **时间淘汰**使用每个 chunk 的 close 时间戳（见下）；可以突破「尽量保留满 retention」的软目标，但仍遵守 `minRetainChunks` 与 durable。
5. 从 **头部**（最老 chunk）淘汰，推进 `cacheStartOffset`，`release` ByteBuf，并删除对应时间戳。

### 大 input 与单文件硬上限

追加前在 entry 锁内，根据预计新增的 ByteBuf capacity 执行时间淘汰和容量淘汰，再进行配额预占及数据写入：

- 普通情况先从头部淘汰，直到 `remainingCacheBytes + inputAllocationBytes <= maxCacheSizePerFileBytes`，然后再追加；单文件字节上限不允许先超过再收敛。
- 若受 `minRetainChunks` 保护的 ByteBuf capacity 加上本次 input 所需 capacity 后超过单文件上限，旧 cache 的最小保护区也无法与本次 input 同时保留，进入整段替换路径：清空旧 cache，仅缓存本次 input 的尾部。
- 若 input 自身所需 capacity 超过单文件上限，只截取 input 最新、且实际分配 capacity 不超过 `maxCacheSizePerFileBytes` 的后缀放入 cache；未缓存的前缀仍按正常写路径写入 backing FS。
- 整段替换是 `minRetainChunks` 保护规则的特例：单文件硬上限优先，不能为了保留旧的最小窗口而拒绝一个本可通过截取后缀处理的 write。
- 清空、截取、配额变更、`cacheStartOffset` / `cacheEndOffset` 更新以及新 chunks 发布必须在同一个 entry 锁内按事务式流程完成。配额失败必须发生在清空旧 cache 之前；实现不得向 `chunks` map 逐块发布一个未完成的替换结果。

## `FULL_CACHE` 与 atomic

- atomic 是一种特殊的 `FULL_CACHE`；Index 也按普通 `FULL_CACHE` 处理，不单独定义规则。
- `FULL_CACHE` 与 `TAIL_CACHE` 一样同时受全局 `maxCacheSizeBytes` 和单文件 `maxCacheSizePerFileBytes` 硬上限约束，但 `FULL_CACHE` 不参与淘汰。
- atomic FULL 不使用固定大小 chunk：完整内容始终存放在 key 为 `0` 的单个 ByteBuf 中，capacity 等于实际文件大小；读、写、flush 和 preload 都使用该单块布局。系统默认同一 key 的读写实例类型一致，不为 atomic 与非 atomic 混用提供兼容或保护。
- `loadFullFileCache` 必须先读取文件 size，计算完整 preload 实际需要申请的 ByteBuf capacity，检查单文件上限并预占全局字节配额；只有预占成功后才读取文件数据。不得先把完整文件读入内存，再发现配额不足而丢弃，避免配额持续不足时反复产生无效文件 IO。
- preload 读文件或构建 chunks 失败时释放临时数据并回滚预占配额；若读取期间文件 size 发生变化，必须按最终实际申请的 capacity 重新校验，不能超配额发布 cache。
- 拆分后的 cache 初始化 / 追加 helper 返回 boolean。返回 `false` 表示本次无法建立完整 cache，调用方不得保留部分 cache 结果。
- `TAIL_CACHE` 初始化或追加因配额失败时返回 `false`。未初始化 entry 保持未初始化；已初始化 entry 的 pending flush 和 cache 释放交给上述 `writeInternal` 现有分支。本次 write 走非 cache 路径，下次允许重新初始化 Tail cache。连续失败可能导致频繁初始化，当前阶段接受该开销。
- `FULL_CACHE` 因全局配额暂时不足时返回 `false`；已初始化 entry 同样由 `writeInternal` 先下刷再释放。仅本次 write 绕过 cache，下次仍可重新尝试。
- `FULL_CACHE` 因 `maxCacheSizePerFileBytes` 无法容纳完整文件时，不修改 `file.cacheMode`，而是在共享 entry 上设置“大文件”标识并返回 `false`；已初始化 cache 仍由 `writeInternal` 的统一降级分支安全释放。
- `useCache` 除检查 mode 外还要检查共享 entry 的“大文件”标识。标识存在期间，该 key 的所有实例都绕过 cache，避免只修改单个 file 实例后其它实例仍读取旧 FULL cache。
- truncate 成功后清除“大文件”标识，使后续操作可以按截断后的实际大小重新尝试建立 FULL cache；若仍超过 `maxCacheSizePerFileBytes`，初始化时会再次设置标识。

atomic replace 使用旧 cache 配额转移的方式，避免新旧完整 cache 同时占用配额：

```text
oldBytes = 当前 atomic chunk 0 的 capacity
newBytes = 新内容的实际大小
delta    = newBytes - oldBytes
```

1. 在 entry 锁内先校验 `newBytes <= maxCacheSizePerFileBytes`；若不满足，按 FULL 大文件规则设置标识并让本次 atomic write 绕过 cache。
2. 若 `delta > 0`，先向全局配额申请 `delta`；申请失败时返回 `false`，由 `writeInternal` 统一处理旧 cache 的下刷与释放，仅本次 atomic write 绕过 cache。
3. 申请成功后释放旧 chunk 0；原有 `oldBytes` 配额直接转移给新的 chunk 0，再按 `newBytes` 分配并发布完整内容。
4. 若 `delta < 0`，新 cache 完整建立后归还 `-delta` 配额。
5. 任一时刻都不发布部分 atomic FULL cache。这里“保证分配成功”指字节配额已足够；实际 allocator 异常仍必须释放本次已分配内容、结清预占配额，并把 entry 留在一致的未初始化状态。

atomic read / transfer / flush 始终从 chunk 0 按真实 byte offset 切片，不使用 `offset / chunkSize` 查找；这些分支可以在公共读写 helper 内按 `file.atomicReplace` 特殊化，不要求新增独立 API。

atomic truncate 也按完整重写处理：先把截断后仍有效的内容读入临时 buffer，按新旧 capacity 差额完成配额转移并释放旧 chunk 0，再分配、写入和发布新的单块 chunk 0，最后归还多余配额。直接只改 `cacheEndOffset` 不会释放实际内存，因此不可作为 atomic truncate 的最终实现。

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

## 全局用量与水位

维护 `globalUsedBytes`（所有参与统计的 cache ByteBuf 分配/释放时按实际 capacity 增减），包含 `TAIL_CACHE` 和 `FULL_CACHE`。

### tenant 接口预留

- 当前应用中每个 tenant 本质上只有少量大文件，本期直接按文件维度控制和打分，不实现 tenant 维度的统计、注册、配额、上限或分档代码。
- 现有 open API 的 tenant 参数保留，作为后续扩展接口；本期内存控制逻辑不读取、不存储该参数。
- 现有 `maxCacheSizePerTenantBytes` 配置及相关 getter/setter、运行时代码可以删除。

水位为简单除法，需要时现算（不必缓存「当前处于哪一档」）：

```text
ratio = globalUsedBytes / maxCacheSizeBytes
```

`maxCacheSizeBytes` 必须为正数，全局字节上限始终有效。

### 快路径：低于绝对用量时只看时间

若 `globalUsedBytes < timeOnlyBelowBytes`：

- **不算**水位分档 / 中水位 score floor。
- 仅对本 write 的 entry 做 **时间淘汰**（若超过单文件上限则一并压回上限）。

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
minKeep      = minRetainChunks
maxEvictable = max(0, currentChunks - minKeep)
newChunks    = 本次 write 实际新建的 chunk 数

score = scores.get(fileKey)   // 无条目 => ABSENT（未入选）

floor = 0
若 score 存在:
  floor = max(newChunks, round(maxEvictable * score))   // score ∈ (0, 1]
  floor = min(floor, maxEvictable)
若处于高水位:
  floor = max(floor, largeFloor(maxEvictable))  // 例如 ≈ maxEvictable

effectiveRetention = expectedMinRetentionMs
若 score 存在:
  effectiveRetention *= (1 - α * score)   // 可选：时间轴压短

1) 时间淘汰头部 chunk，条件：
   - currentChunks > minKeep
   - chunk 有 close ts 且 age >= effectiveRetention
   - chunk 落在 durable 水位内
2) 若本轮已淘汰数 < floor：
   继续淘汰已 durable 的头部 chunk，直到达到 floor 或无法再删（durable / minKeep）
3) 若 currentCacheBytes > maxCacheSizePerFileBytes：
   继续淘汰已 durable 头部，直到 <= maxCacheSizePerFileBytes（且仍保留 minKeep）
```

score floor **有意在每次 write 重复计算和执行**，不是每个评分周期只执行一次：

- 容量上升时，入选文件的首要目标是不再继续增长；本次每新建一个 chunk，floor 至少增加一个 chunk。
- `round(maxEvictable * score)` 可以让本轮淘汰数大于本次新增数，使被提名的大文件主动缩小；这是预期行为。
- 只有被提名的文件承担 score floor，其它文件仍可能增长，因此被提名文件单次净缩小超过一个 chunk 是允许的。
- `minRetainChunks` 是维持 tail read 能力的最小保留量，也是所有 floor 淘汰的最终硬下界；只要不低于该值即可。
- 上述目标仍受 durable 约束。为满足硬配额而进行的 write 前预淘汰也计入本轮淘汰数，避免预淘汰后再次重复执行同一份 floor。

### 未入选文件（重要）

异步打分 **未提名** 的文件：

- **没有**默认/最低 score，也 **没有** 基于 score 的 floor。
- 仍会做：
  - **时间淘汰**（遵守 `minRetainChunks` + durable）；
  - **单文件上限** `maxCacheSizePerFileBytes`。
- **不会**获得中水位分档带来的 floor，也不会因分档做时间压短。

## 中水位异步打分（文件分档）

在中水位周期性执行（或其它合适时机）。构建新的 `Map<fileKey, score>` 并 **整体原子替换**。写路径只按自己的 key 读取当前 map（不向各个 file 推送刷分）。

### 步骤 A — 文件按用量降序

仅收集可淘汰的 `TAIL_CACHE` body 文件，按各自的 `cacheSizeBytes` 从大到小排列。

### 步骤 B — 按占全局用量的累计比例给文件分档

示例：档宽 10%（可配；档数 ≥2 可配）：

- 使用加入当前文件 **之前**的累计占比 `sumBeforeBytes / globalUsedBytes` 决定当前文件所在档；分母仍是包含 `TAIL_CACHE` 和 `FULL_CACHE` 的全局字节用量。
- `sumBeforeBytes / globalUsedBytes` 落入 `[0, 10%)` → **第 1 档**。
- `sumBeforeBytes / globalUsedBytes` 落入 `[10%, 20%)` → **第 2 档**。
- `sumBeforeBytes / globalUsedBytes` 落入 `[20%, 30%)` → **第 3 档**，以此类推。
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

- `maxCacheSizeBytes`：全局 cache ByteBuf capacity 硬上限，必须 `> 0`
- **`maxCacheSizePerFileBytes`**（新增）：单 cache entry 字节硬上限，同时作用于 `TAIL_CACHE` 和 `FULL_CACHE`，必须 `> 0`
- **`minRetainChunks`**（新增）：淘汰时单 entry 硬下限，必须 `>= 1`
- 全局、单文件上下界始终有效，任何配置都不使用 `0` 表示无限或关闭，并且必须满足 `minRetainChunks * chunkSize <= maxCacheSizePerFileBytes`
- 现有 `TailCacheFileSystemConfig` 中全局上限的 `0` 默认值需要在实现时改为有效的正数配置；初始化或动态更新得到非正数上限时直接拒绝
- `expectedMinRetentionMs`：主要时间目标，必须 `>= 0`；`0` 表示 chunk 封口后立刻过期，不承载“关闭时间淘汰”等特殊语义
- 生产配置不提供关闭时间淘汰的语义；是否可淘汰仍同时受 durable 和 `minRetainChunks` 约束
- `timeOnlyBelowBytes`：全局字节用量低于此值时，跳过水位/score floor

## 挂点

1. 所有 cache ByteBuf 分配/释放 → 按实际 capacity 更新 `globalUsedBytes`、entry size 和 segment 聚合 size；Tail chunk **封口**时写入 close ts。
2. Tail 写路径分配前 → 按预计新增量执行必要的预淘汰和配额预占；追加后执行本轮剩余的时间淘汰 / score floor。
3. 后台任务 → 文件分档提名并替换 score map（中水位时；其它时机刷新也安全）。
4. fsync / `writeAndFlush` 中的 force 仍是 `pendingFsyncBytes` 的权威更新点。

## 建议实现顺序

1. chunk close 时间戳 + durable 时间淘汰 + `minRetainChunks` + `maxCacheSizePerFileBytes` + 全局/entry 字节计数。
2. 低于绝对阈值时的「只看时间」快路径。
3. 中水位 score map + 分档提名 + floor / 可选时间压短。
4. 高水位作为同一算法的大 floor（+ 可选更激进启发式）。

## 实现时再定的参数

- 水位比例；文件档宽（如 10%）；档数。
- 各档 score；时间压短系数 `α`。
- 扫描间隔；`minRetainChunks` / `maxCacheSizePerFileBytes` 默认值。
