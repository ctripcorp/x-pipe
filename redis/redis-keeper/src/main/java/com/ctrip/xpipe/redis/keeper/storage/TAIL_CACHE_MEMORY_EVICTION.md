# Tail Cache 内存淘汰设计

状态：已达成一致（尚未实现）  
范围：仅 `TailCacheFileSystem` 写路径上对 `TAIL_CACHE` 的淘汰。

## 目标

1. 内存允许时，尽量按 `expectedMinRetentionMs` 保留数据。
2. 用全局 cache 上限、单文件 / 单 tenant 上限约束内存。
3. 只淘汰已安全落盘的数据（已 write 且已 fsync）。
4. 以 chunk 为淘汰单位；元数据尽量少；写路径尽量轻。
5. 中水位用异步分档打分，让大 tenant / 大文件更积极淘汰；分数整表替换，file 自行读取（写路径不刷分）。

## 非目标（当前阶段）

- 淘汰 `FULL_CACHE`（含当前使用 full cache 的 index）。
- 把异步淘汰当作写停滞后的唯一手段（异步可后续再加；先用单文件上限兜底）。

## 计量单位

大小一律用 **chunk 个数**（`int`），不用字节做热路径计算：

```text
chunks = floor(bytes / chunkSize)
```

配置里的 MB/字节在初始化或更新时，用 `chunkSize` 整除转换成 chunk 数。

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
| Index（当前为 full cache） | 否 |

## 硬规则（始终生效）

1. **单文件最少保留 chunk 数**（`minRetainChunks`，可配，如 1 / 3 / 4）：淘汰后不得低于该数量（空/未初始化 entry 除外）。
2. **单文件最多 chunk 数**（`maxChunksPerFile`）：超过时写路径可向该上限收敛（仍受 durable 约束）。用于写停滞后暂无异步淘汰时的兜底。
3. **仅 durable**：不得超过 `durableFsOffset`。
4. **时间淘汰**使用每个 chunk 的 close 时间戳（见下）；可以突破「尽量保留满 retention」的软目标，但仍遵守 `minRetainChunks` 与 durable。
5. 从 **头部**（最老 chunk）淘汰，推进 `cacheStartOffset`，`release` ByteBuf，并删除对应时间戳。

## 每个 chunk 的 close 时间戳

- 在 chunk **关闭（封口）**时记录时间：写满并切到下一 chunk 时。
- 当前仍在写的尾 chunk 尚无 close ts → 不做时间淘汰（若只剩它，也会被 `minRetainChunks` 保护）。
- 与 chunks 并列存储，例如 `ConcurrentHashMap<Long /*chunkIdx*/, Integer /*epoch 秒*/>`（或 `long` 毫秒）；删 chunk 时同步删 ts。
- 默认约 1MiB/chunk 时，每 chunk 一个 int/long 相对数据量可忽略。

## 全局用量与水位

维护 `globalUsedChunks`（tail cache chunk 分配/释放时 ±1）。可选维护 `tenantUsedChunks`。

水位为简单除法，需要时现算（不必缓存「当前处于哪一档」）：

```text
ratio = globalUsedChunks / maxChunks
```

其中 `maxChunks = floor(maxCacheSizeBytes / chunkSize)`。若 `maxCacheSizeBytes == 0`，视为不限制（不做基于全局 size 的淘汰）。

### 快路径：低于绝对用量时只看时间

若 `globalUsedChunks < timeOnlyBelowChunks`（由配置的 MB 阈值 ÷ chunkSize 得到）：

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

score = scores.get(fileKey)   // 无条目 => ABSENT（未入选）

floor = 0
若 score 存在:
  floor = round(maxEvictable * score)   // score ∈ (0, 1]
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
3) 若 currentChunks > maxChunksPerFile：
   继续淘汰已 durable 头部，直到 <= maxChunksPerFile（且仍 >= minKeep）
```

### 未入选文件（重要）

异步打分 **未提名** 的文件：

- **没有**默认/最低 score，也 **没有** 基于 score 的 floor。
- 仍会做：
  - **时间淘汰**（遵守 `minRetainChunks` + durable）；
  - **单文件上限** `maxChunksPerFile`。
- **不会**获得中水位分档带来的 floor，也不会因分档做时间压短。

## 中水位异步打分（tenant 分档 → 文件分数）

在中水位周期性执行（或其它合适时机）。构建新的 `Map<fileKey, score>` 并 **整体原子替换**。写路径只按自己的 key 读取当前 map（不向各个 file 推送刷分）。

Tenant 只影响「哪些文件被提名、拿到哪一档的固定分」，不在写热路径上单独维护 tenant 计数逻辑。

### 步骤 A — tenant 按用量降序

使用 `tenantUsedChunks`。

### 步骤 B — 按占全局用量的累计比例给 tenant 分档

示例：档宽 10%（可配；档数 ≥2 可配）：

- 从大到小遍历 tenant，累加 `sum / globalUsedChunks`。
- 累计落入 `(0, 10%]` → **第 1 档**。
- 落入 `(10%, 20%]` → **第 2 档**。
- （可选第 3 档等。）
- 其余 tenant → **不提名**（其下文件保持未入选）。

同一档内的 tenant 打分力度一视同仁。

### 步骤 C — 每个被提名 tenant 内选取大文件

- 该 tenant 下 `TAIL_CACHE` 文件按 `chunks` 降序。
- 累加直至达到该 tenant 用量的例如 **50%**（可配）。
- 这些文件被提名；该 tenant 内其余文件为 **未入选**（仅时间 + 单文件上限）。

### 步骤 D — 按档赋予固定分

示例（可配）：

| 提名结果 | Score |
|----------|-------|
| 第 1 档文件 | `1.0` |
| 第 2 档文件 | `0.5` |
| （第 3 档） | `0.25` |
| 未提名 | *map 中无条目* |

通过替换整张 score map 发布。

若尚未扫描或 map 中无该 key → 按未入选处理。

## 单文件 / 单 tenant 上限（部分配置已存在）

- `maxCacheSizeBytes` → `maxChunks`
- `maxCacheSizePerTenantBytes` → `maxChunksPerTenant`（可在写路径 / 扫描时强制，实现时定）
- **`maxChunksPerFile`**（新增）：单 tail entry 硬上限
- **`minRetainChunks`**（新增）：淘汰时单 entry 硬下限
- `expectedMinRetentionMs`：主要时间目标
- `timeOnlyBelowChunks`：全局用量低于此值时，跳过水位/score floor

## 挂点

1. Tail 追加路径上 chunk 分配/释放 → 更新 `globalUsedChunks` / tenant 计数；chunk **封口**时写入 close ts。
2. 写路径追加成功后 → 按当前水位规则对本 entry 执行淘汰。
3. 后台任务 → tenant/文件提名并替换 score map（中水位时；其它时机刷新也安全）。
4. fsync / `writeAndFlush` 中的 force 仍是 `pendingFsyncBytes` 的权威更新点。

## 建议实现顺序

1. chunk close 时间戳 + durable 时间淘汰 + `minRetainChunks` + `maxChunksPerFile` + 全局 chunk 计数。
2. 低于绝对阈值时的「只看时间」快路径。
3. 中水位 score map + 分档提名 + floor / 可选时间压短。
4. 高水位作为同一算法的大 floor（+ 可选更激进启发式）。

## 实现时再定的参数

- 水位比例；tenant 档宽（如 10%）；档数。
- tenant 内文件累计占比（如 50%）。
- 各档 score；时间压短系数 `α`。
- 扫描间隔；`minRetainChunks` / `maxChunksPerFile` 默认值。
- tenant 上限在写路径、扫描或两者强制。
