# Fix `/api/redis/info/global` 超时:section 过滤 + 并发拉取

- 日期: 2026-08-19
- 分支: `fix/redis-info-global-timeout`(基点: `master` @ `f83c7fd7e`)
- 作者: llj李龙姣

## 1. 背景

`GET /api/redis/info/global`(`HealthController.getGlobalAllRedisInfo`)聚合所有 DC 的 Redis `INFO` 后返回。现状:

- 每个 DC 返回的是**全量 INFO**(由 `InfoAction` 执行 `INFO` 不带 section 采集,`InfoActionContext.parse` 把每一行 `key:value` 全部塞进 Map,无过滤)。
- 聚合在 `RedisInfoService.getGlobalAllInfosRetMessage` 中用 `consoleConfig.getConsoleDomains().keySet().stream().map(consoleManager::getLocalRedisInfosByDc).reduce((acc, another) -> { acc.putAll(another); return acc; })` 完成——**串行**。
- 每个 DC 通过 `DefaultConsoleService.getAllLocalRedisInfos()` → `restTemplate.getForObject("{address}/api/redis/info/local", ...)` 拉取,restTemplate 默认 connect=1200ms、so-timeout=6000ms、retry-times=1,单 DC 最坏 ~12s。
- 该接口被 `XPipeServletInvocableHandlerMethod` 包成带 **10s 超时**(`servlet.method.timeout.milli` 默认 10000L)的 `DeferredResult`。

结果:DC 数多 / 某个 DC 慢 / payload 大时,聚合耗时突破 10s,`DeferredResult` 未在窗口内 `setResult`,抛 `AsyncRequestTimeoutException`。响应体巨大(实例数 × INFO 字段数 × DC 数)既直接拖慢传输+反序列化+合并,又把单 DC 逼近 6s 读取超时。

## 2. 目标

1. **减少返回字段**:给相关 INFO 接口加可选 `section` 参数,传了只返回该 Redis INFO section 的字段,不传保持全量(向后兼容)。
2. **并发拉取**:把跨 DC 的串行拉取改为并发,使 wall-clock ≈ 最慢的单个 DC,而非所有 DC 之和;并设总超时,杜绝单 DC 拖垮全局。
3. 保持失败语义不变:单个 DC 失败/超时被跳过,不导致整个接口失败(与现状一致),并补上日志。
4. **lz4 压缩传输**:在拉取 redis info 的接口上复用现有 lz4 机制(`LZ4CompressionResponseBodyAdvice` + `LZ4DecompressionInterceptor`),跨 DC 传输走压缩,按需触发、向后兼容。**实例级 `/api/health/redis/info/{ip}/{port}` 不加压缩逻辑**。

## 3. 非目标(YAGNI)

- 多 section / 逗号分隔(后续可扩展,本次只支持单个 section)。
- section 合法性校验 / 合法列表返回(未知 section 返回空 Map,不报错)。
- 改 `AbstractService` 的 restTemplate 超时默认值。
- 聚合改并行后的结果按 DC 维度返回(保持现有扁平 `Map<HostPort, ...>` 返回类型)。
- 区分"DC 真失败"与"该 DC 本来无实例"返回空 Map 的差异。

## 4. 行为契约

### 4.1 `section` 参数

- 4 个端点都新增 `@RequestParam(required = false) String section`:
  - `GET /api/redis/info/global`(`HealthController.getGlobalAllRedisInfo`)
  - `GET /api/redis/info/local`(`HealthController.getLocalAllRedisInfo`)
  - `GET /api/health/redis/info/all`(`CheckerHealthController.getAllRedisInfo`)
  - `GET /api/health/redis/info/{ip}/{port}`(`CheckerHealthController.getRedisInfo`)
- `section` 为 `null` 或空串 → **全量返回**(完全等同当前行为)。
- `section` 非空 → 只返回该 section(`# SectionName` 头标识)内的 `key:value`;section 名**忽略大小写**匹配。
- 传入 Redis 不存在的 section → `state=success`、`payload` 为空 Map(不报错)。
- 只支持单个 section。

### 4.2 并发拉取(global)

- global 聚合改为并发(复用 `ConsoleServiceManager` 现有 `ParallelCommandChain` + `broadcast` 模式)。
- 总等待上限 **8s**(独立于 `BROADCAST_TIMEOUT_MILLI=5000ms`,通过新增的带超时参数的 `broadcast` 重载实现,不影响其他调用方)。8s 留 2s 给序列化,低于 10s servlet 超时。
- 单个 DC 失败 / 超时 → **跳过该 DC**(`putAll` 不执行),`logger.warn` 记录 dcId + 原因;其余 DC 结果照常合并返回。
- 不传 `section` 时:并发行为同样生效(并发 + 8s 上限),且每个 DC 仍返回全量 INFO payload。

### 4.3 失败处理(现状对照)

| 情况 | 现状 | 改造后 |
|---|---|---|
|---|---|---|
| 单 DC HTTP 异常(连接拒绝/超时/4xx-5xx/反序列化失败) | `DefaultConsoleService.getAllLocalRedisInfos` 的 `catch(Throwable)` 吞掉,返回 `Collections.emptyMap()`,**静默** | 同样跳过,但补 `logger.warn`(带 dcId + cause) |
| 单 DC 卡住(>8s 未完成) | 串行下阻塞,拖垮全局,可能触发 `AsyncRequestTimeoutException` | 在 8s 处被 `broadcast` 切断,该 DC 标 timeout-skip,warn 记录 |
| 全部 DC 失败 | 返回空 Map(success) | 返回空 Map(success) |

### 4.4 lz4 压缩

- 复用现有两个组件,**不写新的 lz4 压缩/解压代码**:
  - 服务端 `LZ4CompressionResponseBodyAdvice`(`@RestControllerAdvice`,redis-console):handler 返回 `byte[]` 且请求带 `Accept-Encoding: lz4` 时压缩,设 `Content-Encoding: lz4`。
  - 客户端 `LZ4DecompressionInterceptor`:已挂在 `RestTemplateFactory` 产出的所有 restTemplate 上(`RestTemplateFactory.java:90`),见 `Content-Encoding: lz4` 自动解压。
- **加压缩的端点**(返回类型由 `Map` 改为 `byte[]`):
  - `GET /api/redis/info/local`
  - `GET /api/redis/info/global`
- **不加压缩的端点**(保持返回 `Map`):
  - `GET /api/health/redis/info/all`(checker 水平扩容、单 checker 实例数可控,payload 可控,不压缩)
  - `GET /api/health/redis/info/{ip}/{port}`(实例级,单实例 payload 小,不压缩)。
- **触发方式**:请求头驱动。客户端发 `Accept-Encoding: lz4` 才压缩;不发则服务端原样返回 JSON 字节,客户端照常反序列化。完全向后兼容,对调用方透明。
- **跨 DC 内部调用**:`DefaultConsoleService.getAllLocalRedisInfos`(调 console `/local`)主动带 `Accept-Encoding: lz4`,使跨 DC 传输走压缩。调 checker `/all` 的 `DefaultConsoleCheckerService` **不发 lz4 头、保持 `getForObject`**(checker `/all` 不压缩)。

## 5. 详细设计

### 5.1 section 感知解析

在 `InfoActionContext`(`redis-checker/.../healthcheck/actions/redisinfo/InfoActionContext.java`)增加 section 感知的解析:

- 新增 `static Map<String, String> parse(String info, String section)`:
  - `section` 为 null/空 → 走当前全量逻辑(逐行 split `:`,`length==2` 的放入 Map)。
  - `section` 非空 → 解析时跟踪当前所在 section(行以 `# ` 开头即新 section,取 `# ` 之后部分,trim、忽略大小写),只把落在匹配 section 内的 `key:value` 行放入 Map。`#` 头行本身非 `key:value`,天然不进 Map。
  - 无匹配 section → 空 Map。
- 现有 `default Map<String,String> parse(String info)` 改为委托 `parse(info, null)`,逻辑不变。

> 过滤必须发生在能拿到原始 INFO 串的阶段。现有 `parse` 结果是扁平 Map,已丢失 section 归属,无法事后按 section 过滤。原始串由 `RawInfoActionContext` 持有,可通过 `InfoActionContext.inner().getResult()` 取到。

### 5.2 section 感知序列化包装

`InfoActionContext` 已有内部类 `Result extends ActionContextRetMessage<Map<String, String>>` 和 `ResultMap extends HashMap<HostPort, Result>`。新增静态辅助:

- `static Result toRetMessage(InfoActionContext ctx, String section)`:
  - `ctx.isSuccess()` → `setState(SUCCESS)` + `setPayload(parse(ctx.inner().getResult(), section))`。
  - 否则 → `setState(FAIL)` + `setMessage(ctx.getCause().getMessage())`。等价于 `ActionContextRetMessage.from(ctx)` 的 section 感知版。
- `static ResultMap toRetMessage(Map<HostPort, InfoActionContext> contexts, String section)`:逐个套上面的逻辑,聚合为 `ResultMap`。

`ActionContextRetMessage`(泛型基类)保持不动——section 感知逻辑收敛在 `InfoActionContext` 侧,避免污染泛型类。

### 5.3 service / manager 层透传

`RedisInfoService` 接口(`redis-console/.../service/RedisInfoService.java`)+ 两个实现:

- 新增重载:`getLocalAllInfosRetMessage(String section)`、`getGlobalAllInfosRetMessage(String section)`。
- 现有无参方法委托新方法传 `null`(向后兼容)。

`DefaultRedisInfoService`:
- `getLocalAllInfosRetMessage(section)`:`InfoActionContext.toRetMessage(infoManager.getAllInfos(), section)`。
- `getGlobalAllInfosRetMessage(section)`:改为调用 `consoleManager.getAllLocalRedisInfos(consoleConfig.getConsoleDomains().keySet(), section)`(见 5.4)。

`ConsoleRedisInfoService`:
- `getLocalAllInfosRetMessage(section)`:`checkerManager.getLeaderCheckerServices().stream().map(s -> s.getAllLocalRedisInfos(section)).reduce(putAll).orElseGet(HashMap::new)`(透传 section 到 `ConsoleCheckerService`)。
- `getGlobalAllInfosRetMessage(section)`:同 `DefaultRedisInfoService`,调 `consoleManager.getAllLocalRedisInfos(dcIds, section)`。

### 5.4 ConsoleServiceManager 并发聚合(核心)

在 `ConsoleServiceManager`(`redis-console/.../console/impl/ConsoleServiceManager.java`)新增:

- `Map<HostPort, ActionContextRetMessage<Map<String,String>>> getAllLocalRedisInfos(Set<String> dcIds, String section)`:
  ```java
  Map<String, BroadcastResult<Map<HostPort, ActionContextRetMessage<Map<String,String>>>>> raw =
      broadcast(dcIds, dc -> getServiceByDc(dc).getAllLocalRedisInfos(section),
                REDIS_INFO_BROADCAST_TIMEOUT_MILLI);
  Map<HostPort, ActionContextRetMessage<Map<String,String>>> result = new HashMap<>();
  raw.forEach((dc, r) -> {
      if (r.isSuccess() && r.value != null) result.putAll(r.value);
      else logger.warn("[getAllLocalRedisInfos][{}] fail/skip: {}", dc, r.errorMsg);
  });
  return result;
  ```
- 新增常量 `private static final long REDIS_INFO_BROADCAST_TIMEOUT_MILLI = 8000L;`
- 给 `broadcast` 加带超时的重载:`broadcast(Set<String> dcIds, Function<String,T> perDcTask, long timeoutMilli)`,原 `broadcast(dcIds, perDcTask)` 委托传 `BROADCAST_TIMEOUT_MILLI`(5000),**不影响其他调用方**。
- 原有 `getLocalRedisInfosByDc(String dcId)`(单 DC,被其他逻辑用)保留,或加 `getLocalRedisInfosByDc(String dcId, String section)` 重载用于单 DC section 透传;保留无参版委托传 `null`。

### 5.5 跨服务 HTTP 透传 section

- `ConsoleService` 接口 + `DefaultConsoleService`:`getAllLocalRedisInfos(String section)` —— `section` 非空时在 `allLocalRedisInfosUrl` 拼 `?section=<enc>`(URL encode);为空时 URL 不变。无参版委托传 `null`。
- `ConsoleCheckerService` 接口 + `DefaultConsoleCheckerService`:`getAllLocalRedisInfos(String section)` —— `section` 非空时在 `allRedisInfosUrl`(`/api/health/redis/info/all`)拼 `?section=<enc>`。无参版委托传 `null`。

### 5.6 controller 层

4 个端点方法签名加 `@RequestParam(required = false) String section`,body 调对应的 `(section)` 重载。

### 5.7 lz4 压缩(复用现有组件)

样板参考 `ConsoleCheckerController.getDividedMeta`(服务端返回 `byte[]`)+ `DefaultCheckerConsoleService`(`:60-62, 75-77`,客户端 `exchange` + `Accept-Encoding: lz4`)。

**服务端(2 个 console 端点)**:把 `getLocalAllRedisInfo` / `getGlobalAllRedisInfo` 的返回类型由 `Map<HostPort, ActionContextRetMessage<Map<String,String>>>` 改为 `byte[]`。**`CheckerHealthController.getAllRedisInfo`(`/all`)与 `getRedisInfo`(`/{ip}/{port}`)保持 `Map`,不压缩。**
- 用 `ObjectMapper.writeValueAsBytes(resultMap)`(复用 `RestTemplateFactory.createObjectMapper()` 同款配置,或注入已有 `coder`)把 Map 序列化成 JSON 字节返回。
- 保留 `produces = "application/json;charset=UTF-8"`(advice 只设 `Content-Encoding`、不设 `Content-Type`,需端点自己保证)。
- 请求带 `Accept-Encoding: lz4` → 现有 advice 自动压缩;不带 → 原样 JSON 字节。两种情况调用方都能反序列化。
- `section` 参数照常生效(先按 5.1-5.6 拿到过滤后的 Map,再序列化为字节)。

**客户端(跨 DC,仅 1 处)**:
- `DefaultConsoleService.getAllLocalRedisInfos(section)`:从 `restTemplate.getForObject(url, ResultMap.class)` 改为
  ```java
  HttpHeaders headers = new HttpHeaders();
  headers.set(HttpHeaders.ACCEPT_ENCODING, "lz4");
  HttpEntity<?> entity = new HttpEntity<>(headers);
  ResponseEntity<InfoActionContext.ResultMap> resp =
      restTemplate.exchange(allLocalRedisInfosUrl, HttpMethod.GET, entity, InfoActionContext.ResultMap.class);
  return resp.getBody();
  ```
  解压由现有 `LZ4DecompressionInterceptor` 自动完成。
- `DefaultConsoleCheckerService.getAllLocalRedisInfos(section)`:**保持 `getForObject` + `?section=`,不发 `Accept-Encoding: lz4`**(checker `/all` 不压缩,返回普通 JSON Map)。

**advice 注册范围(已决策不补齐)**:`LZ4CompressionResponseBodyAdvice` 在 redis-console 模块。`/local`、`/global` 在 redis-console(covered)。checker `/all`、`/{ip}/{port}` **不做压缩**(checker 水平扩容、单 checker 实例数可控,payload 可控),因此独立 CHECKER 模式下 advice 未被 `CheckerContextConfig` 扫描的问题**不再成立**,无需移动 advice 或在 checker 侧注册。

**风险点**:`LZ4DecompressionInterceptor:44` 用 `compressed.length * 20` 作解压缓冲(启发式)。INFO JSON 高度重复,压缩比可能逼近 20x,极端情况下解压抛异常。这是现有代码既有风险;若实测出现,再考虑改 framed LZ4 带原始长度(届时才算新增 lz4 代码)。

## 6. 改动文件清单

| 文件 | 改动 |
|---|---|
| `redis-checker/.../healthcheck/actions/redisinfo/InfoActionContext.java` | `parse(info, section)`、`toRetMessage(ctx, section)`、`toRetMessage(contexts, section)` |
| `redis-checker/.../controller/CheckerHealthController.java` | `getRedisInfo` / `getAllRedisInfo` 加 `section` 参数 |
| `redis-console/.../controller/api/HealthController.java` | `getLocalAllRedisInfo` / `getGlobalAllRedisInfo` 加 `section` 参数 |
| `redis-console/.../service/RedisInfoService.java` | 接口加 `(section)` 重载 |
| `redis-console/.../service/impl/DefaultRedisInfoService.java` | 实现重载,global 改走 `consoleManager.getAllLocalRedisInfos(dcIds, section)` |
| `redis-console/.../service/impl/ConsoleRedisInfoService.java` | 实现重载,local 透传 section 到 `ConsoleCheckerService` |
| `redis-console/.../console/ConsoleServiceManager.java` | `getAllLocalRedisInfos(dcIds, section)` 并发聚合 + `broadcast` 超时重载 + `REDIS_INFO_BROADCAST_TIMEOUT_MILLI` |
| `redis-console/.../console/ConsoleService.java` | 接口加 `getAllLocalRedisInfos(section)` |
| `redis-console/.../console/impl/DefaultConsoleService.java` | URL 拼 `?section=`;`getAllLocalRedisInfos` catch 补 warn |
| `redis-console/.../checker/ConsoleCheckerService.java` | 接口加 `getAllLocalRedisInfos(section)` |
| `redis-console/.../checker/DefaultConsoleCheckerService.java` | URL 拼 `?section=`;`getAllLocalRedisInfos` 保持 `getForObject`,不发 lz4 头 |

**lz4 相关额外改动**(叠加在上述文件上):

| 文件 | lz4 改动 |
|---|---|
| `redis-console/.../controller/api/HealthController.java` | `getLocalAllRedisInfo` / `getGlobalAllRedisInfo` 返回 `byte[]`(Map→JSON 字节),复用 advice 压缩 |
| `redis-checker/.../controller/CheckerHealthController.java` | `/all`、`/{ip}/{port}` 均**保持 `Map`,不压缩** |
| `redis-console/.../console/impl/DefaultConsoleService.java` | `getAllLocalRedisInfos` 改 `exchange` + `Accept-Encoding: lz4`(调 console `/local`) |
| `redis-console/.../checker/DefaultConsoleCheckerService.java` | `getAllLocalRedisInfos` 保持 `getForObject` + `?section=`,**不发 lz4 头**(调 checker `/all`) |
| `LZ4CompressionResponseBodyAdvice` | 不动(checker 不压缩,无需移动/注册) |

## 7. 测试

- **单元** `InfoActionContext.parse(info, section)`:
  - section=null/空 → 全量字段数。
  - `replication`(小写) → 只含 `# Replication` 下字段。
  - `REPLICATION`(大写) → 同上(大小写无关)。
  - `foo`(不存在) → 空 Map。
  - 多 section 边界:某 key 出现在某 section 内、不在其它 section 内时,只在该 section 命中。
- **单元** `toRetMessage(ctx, section)`:success/fail 两种 context + section 组合,断言 payload/state/message。
- **单元** `ConsoleServiceManager.getAllLocalRedisInfos(dcIds, section)`:mock 多 DC(含一个抛异常、一个超时未完成),断言成功 DC 结果合并、失败 DC 跳过、有 warn、总耗时 < timeout。
- **集成** `CollectRedisInfoTest`:加 `?section=replication` 调 global,断言只含 replication 字段;不带 section 断言全量(回归)。
- **端点** 4 个端点各加 section 用例(local/all/单实例/global)。
- **lz4**:
  - `/local`、`/global`:带 `Accept-Encoding: lz4` 请求 → 响应有 `Content-Encoding: lz4` 且可被 `restTemplate.exchange(..., ResultMap.class)` 正确解压反序列化(与不带头的结果字段一致)。
  - `/api/health/redis/info/all`、`/api/health/redis/info/{ip}/{port}`:无论是否带头,响应均无 `Content-Encoding: lz4`,返回普通 JSON Map(确认 checker 端点不压缩)。
  - 跨 DC:global 带 section + lz4 头拉取,断言各 DC 响应经解压后合并正确。

## 8. 向后兼容 & 回滚

- 不传 `section` 时所有路径行为等同改造前(global 的并发 + 8s 上限是唯一行为变化,且是更优,不是破坏)。
- 4 个端点的 `section` 都可选,旧调用方零感知。
- 回滚:revert 本分支即可。

## 9. 待确认 / 风险

- **外部消费方**:仓库内无内部字段消费方(仅集成测试调 global)。真正的 INFO 字段消费方可能在仓库外(监控/前端)。`section` 默认全量、不传不变,因此外部消费方零影响;但需要确认是否存在"总是依赖某 section 之外字段"的调用方,若有,可建议它们显式传 section 以减负——属优化建议,非阻塞。
- **8s 上限 vs payload**:不传 section(全量大 payload)时,8s 仍可能不够(若某 DC payload 极大且传输慢)。并发 + 8s 切断已显著优于现状;若仍超,组合 `?section=` 使用即可。不为此调大 servlet 超时。
- **`globalExecutor` 容量**:并发广播占用 `GLOBAL_EXECUTOR` 线程,与现有 `broadcast` 调用方共享。各 DC 的 restTemplate 调用在其上排队;`max-per-route`/连接池默认 1000/10000,不会成为瓶颈。
