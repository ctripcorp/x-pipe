package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.core.keeper.container.CurrentDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressSnapshot;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;

/**
 * 远端机房 keepercontainer 地址快照缓存（keeper-dynamic-ip spec §8.4）。只负责远端机房，本机房由
 * {@link CurrentDcKeeperContainerAddressService} 直接提供。
 * <p>
 * <b>快照</b>：每个远端机房一份不可变快照（存活正向 / tombstone / 存活反向），拉取成功整体替换，失败保留上一份（D-J2），
 * 机房从 {@code consoleConfig.getConsoleDomains()} 中移除即丢弃。所有机房的快照作为一个不可变 map 整体发布，
 * 只由刷新线程写，读路径无锁。
 * <p>
 * <b>tombstone 本地到期</b>：远端给的是剩余保留毫秒，拉取线程拿到响应时按本地 nanoTime 换算为到期时刻，读路径按到期过滤。
 * 因此拉取持续失败也不会延长保留期，最长 = 远端保留期（D-J3）。
 * <p>
 * <b>线程模型</b>：所有 console 实例都运行，不依赖 leader。刷新直接在专属单线程调度器上执行（不用共享
 * {@code SCHEDULED_EXECUTOR}，阻塞等待不影响其他周期任务）；各远端机房的 HTTP 拉取并发投到 {@code GLOBAL_EXECUTOR}，
 * 一轮共用一个超时。{@link DynamicDelayPeriodTask} 在本轮结束后才调度下一轮，刷新之间天然不重叠。启动即触发首轮。
 * <p>
 * <b>查询规则与本地一致</b>（§5.7）：正向先查存活，未命中再查未到期 tombstone；反向只查存活。
 * <p>
 * <b>IP 重叠检测</b>：每轮刷新发布后，用本机房与各远端快照的存活反向索引检测跨机房同 IP。
 * 有重叠即 {@code logAlertEvent} 事件名常量，明细打 WARN；条件持续则每轮都报（§8.4 / §8.8）。
 */
@Component
public class RemoteDcKeeperContainerAddressCache {

    private static final Logger logger = LoggerFactory.getLogger(RemoteDcKeeperContainerAddressCache.class);

    /**
     * 一轮拉取的总超时，须小于刷新间隔。单次 HTTP 的连接/读超时与重试由 {@code DefaultConsoleService} 的 RestTemplate 决定，
     * 超出这里的等待上限即按失败处理、保留旧快照，迟到的结果直接丢弃。
     */
    private static final long DEFAULT_FETCH_TIMEOUT_MILLI = 10 * 1000L;

    static final String CAT_TYPE = "keepercontainer.address.snapshot";

    static final String CAT_NAME_FETCH_FAIL_PREFIX = "fetch.fail.";

    /** 跨机房 keepercontainer IP 重叠告警事件名。只传事件名，明细见 WARN 日志。 */
    static final String ALERT_KEEPER_CONTAINER_IP_OVERLAP = "KEEPER_CONTAINER_IP_OVERLAP";

    private static final String THREAD_NAME_REFRESH = "RemoteDcKeeperContainerAddressRefresh";

    private final ConsoleConfig consoleConfig;

    private final ConsoleServiceManager consoleServiceManager;

    private final CurrentDcKeeperContainerAddressService currentDcAddressService;

    private final ExecutorService fetchExecutor;

    private final EventMonitor eventMonitor;

    private final String currentDc;

    private final long fetchTimeoutMilli;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask refreshTask;

    /** 机房名（大写） → 快照。整体替换发布，发布后不再修改。 */
    private volatile Map<String, RemoteSnapshot> snapshots = Collections.emptyMap();

    @Autowired
    public RemoteDcKeeperContainerAddressCache(ConsoleConfig consoleConfig,
                                               ConsoleServiceManager consoleServiceManager,
                                               CurrentDcKeeperContainerAddressService currentDcAddressService,
                                               @Qualifier(GLOBAL_EXECUTOR) ExecutorService globalExecutor) {
        this(consoleConfig, consoleServiceManager, currentDcAddressService, globalExecutor, EventMonitor.DEFAULT,
                FoundationService.DEFAULT.getDataCenter(), DEFAULT_FETCH_TIMEOUT_MILLI);
    }

    @VisibleForTesting
    RemoteDcKeeperContainerAddressCache(ConsoleConfig consoleConfig,
                                        ConsoleServiceManager consoleServiceManager,
                                        CurrentDcKeeperContainerAddressService currentDcAddressService,
                                        ExecutorService fetchExecutor,
                                        EventMonitor eventMonitor,
                                        String currentDc,
                                        long fetchTimeoutMilli) {
        this.consoleConfig = consoleConfig;
        this.consoleServiceManager = consoleServiceManager;
        this.currentDcAddressService = currentDcAddressService;
        this.fetchExecutor = fetchExecutor;
        this.eventMonitor = eventMonitor;
        this.currentDc = currentDc.toUpperCase();
        this.fetchTimeoutMilli = fetchTimeoutMilli;
    }

    @PostConstruct
    public void start() {
        // 专属单线程：刷新会阻塞等待拉取结果，不能放到共享 SCHEDULED_EXECUTOR 上
        scheduled = Executors.newSingleThreadScheduledExecutor(XpipeThreadFactory.create(THREAD_NAME_REFRESH));
        // 首次延迟为 0：启动即跑首轮；下一轮在本轮结束后才调度，刷新不重叠
        refreshTask = new DynamicDelayPeriodTask(THREAD_NAME_REFRESH, this::refreshSafely,
                consoleConfig::getKeeperContainerAddressRefreshIntervalMilli, scheduled);
        try {
            refreshTask.start();
            logger.info("[start] current dc {}", currentDc);
        } catch (Exception e) {
            logger.error("[start] fail", e);
        }
    }

    @PreDestroy
    public void stop() {
        try {
            if (null != refreshTask) refreshTask.stop();
        } catch (Exception e) {
            logger.error("[stop] refresh task stop fail", e);
        }
        // 中断进行中的一轮：refresh 等待拉取时收到中断即放弃发布
        if (null != scheduled) scheduled.shutdownNow();
    }

    /**
     * 远端机房的 hostname → IP：先查存活，未命中再查未到期 tombstone。机房未缓存（首轮前、拉取从未成功、已移除）返回 null。
     */
    public String getIpByHostname(String dcId, String hostname) {
        if (isEmpty(dcId) || isEmpty(hostname)) return null;
        RemoteSnapshot snapshot = snapshots.get(dcId.toUpperCase());
        return null == snapshot ? null : snapshot.ipByHostname(hostname, nowNanos());
    }

    /** 远端机房的存活正向 ∪ 未到期 tombstone，与本地 {@code getAllHostnameToIp()} 语义一致。返回副本，未缓存时为空。 */
    public Map<String, String> getAllHostnameToIp(String dcId) {
        if (isEmpty(dcId)) return new HashMap<>();
        RemoteSnapshot snapshot = snapshots.get(dcId.toUpperCase());
        return null == snapshot ? new HashMap<>() : snapshot.allHostnameToIp(nowNanos());
    }

    /** 所有已成功拉取的远端机房完整地址快照，按机房名字典序返回副本。 */
    public Map<String, KeeperContainerAddressSnapshot> getAllAddressSnapshots() {
        Map<String, RemoteSnapshot> current = snapshots;
        long now = nowNanos();
        Map<String, KeeperContainerAddressSnapshot> result = new TreeMap<>();
        current.forEach((dc, snapshot) -> result.put(dc, snapshot.toAddressSnapshot(now)));
        return result;
    }

    /** 远端机房的 IP → hostname，只查存活反向索引，不回落 tombstone。 */
    public String getHostnameByIp(String dcId, String ip) {
        if (isEmpty(dcId) || isEmpty(ip)) return null;
        RemoteSnapshot snapshot = snapshots.get(dcId.toUpperCase());
        return null == snapshot ? null : snapshot.liveIpToHostname().get(ip);
    }

    /**
     * 不知道机房时按 hostname 查各远端机房：返回全部命中的 {@code (dc, ip)}，按机房名字典序。
     * 规则同 {@link #getIpByHostname(String, String)}，tombstone 到期判定共用同一个 now。
     * 只读一次快照 map，结果不会跨两次发布拼接（spec §8.5）。
     */
    public List<Pair<String, String>> findIpsByHostname(String hostname) {
        if (isEmpty(hostname)) return new ArrayList<>();
        Map<String, RemoteSnapshot> current = snapshots;
        long now = nowNanos();
        List<Pair<String, String>> result = new ArrayList<>();
        for (String dc : sortedDcs(current)) {
            String ip = current.get(dc).ipByHostname(hostname, now);
            if (null != ip) result.add(Pair.of(dc, ip));
        }
        return result;
    }

    /**
     * 不知道机房时按 IP 查各远端机房：只查存活反向，返回全部命中的 {@code (dc, hostname)}，按机房名字典序。
     * 各机房 pod IP 段可能重叠，由调用方按上下文消歧。只读一次快照 map（spec §8.5）。
     */
    public List<Pair<String, String>> findHostnamesByIp(String ip) {
        if (isEmpty(ip)) return new ArrayList<>();
        Map<String, RemoteSnapshot> current = snapshots;
        List<Pair<String, String>> result = new ArrayList<>();
        for (String dc : sortedDcs(current)) {
            String hostname = current.get(dc).liveIpToHostname().get(ip);
            if (null != hostname) result.add(Pair.of(dc, hostname));
        }
        return result;
    }

    private static List<String> sortedDcs(Map<String, RemoteSnapshot> current) {
        return current.keySet().stream().sorted().collect(Collectors.toList());
    }

    /** 周期任务兜住异常并打 ERROR；DynamicDelayPeriodTask 自身也会兜，但只打 INFO。 */
    private void refreshSafely() {
        try {
            refresh();
        } catch (Throwable th) {
            logger.error("[refresh][fail]", th);
        }
    }

    /**
     * 一轮刷新：各远端机房并发拉取，统一超时；成功的整体替换，失败/超时的保留旧快照，已移除机房的丢弃，再整体发布。
     * 在专属调度线程执行，会阻塞等待拉取结果。
     */
    @VisibleForTesting
    void refresh() {
        Set<String> remoteDcs = remoteDcs();
        Map<String, Future<RemoteSnapshot>> futures = new LinkedHashMap<>();
        for (String dc : remoteDcs) {
            try {
                futures.put(dc, CompletableFuture.supplyAsync(() -> fetch(dc), fetchExecutor));
            } catch (RejectedExecutionException e) {
                onFetchFail(dc, e);
            }
        }

        Map<String, RemoteSnapshot> fetched = new HashMap<>();
        // 超时用真实 nanoTime，与 tombstone 时钟（nowNanos，单测可冻结）分开
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(fetchTimeoutMilli);
        for (Map.Entry<String, Future<RemoteSnapshot>> entry : futures.entrySet()) {
            String dc = entry.getKey();
            Future<RemoteSnapshot> future = entry.getValue();
            try {
                long waitNanos = Math.max(0L, deadline - System.nanoTime());
                fetched.put(dc, future.get(waitNanos, TimeUnit.NANOSECONDS));
            } catch (TimeoutException e) {
                future.cancel(true);
                onFetchFail(dc, e);
            } catch (ExecutionException e) {
                onFetchFail(dc, null == e.getCause() ? e : e.getCause());
            } catch (InterruptedException e) {
                // 只在 stop 时发生：不发布半轮结果
                Thread.currentThread().interrupt();
                logger.warn("[refresh][interrupted] skip publish");
                return;
            }
        }

        Map<String, RemoteSnapshot> next = new HashMap<>();
        snapshots.forEach((dc, snapshot) -> {
            if (remoteDcs.contains(dc)) {
                next.put(dc, snapshot);
            } else {
                logger.info("[refresh][drop] dc {} removed from console domains", dc);
            }
        });
        next.putAll(fetched);
        Map<String, RemoteSnapshot> published = Collections.unmodifiableMap(next);
        this.snapshots = published;

        checkIpOverlap(published);
    }

    /** 跑在拉取线程：拿到响应即按本地时钟换算 tombstone 到期时刻，不把排队等待算进保留期。 */
    private RemoteSnapshot fetch(String dc) {
        KeeperContainerAddressSnapshot raw = consoleServiceManager.getKeeperContainerAddressSnapshot(dc);
        return RemoteSnapshot.from(raw, nowNanos());
    }

    private void onFetchFail(String dc, Throwable cause) {
        logger.warn("[refresh][fetch fail] dc {}, keep previous snapshot", dc, cause);
        eventMonitor.logEvent(CAT_TYPE, CAT_NAME_FETCH_FAIL_PREFIX + dc);
    }

    /** consoleDomains 中除本机房外的机房，统一大写。 */
    private Set<String> remoteDcs() {
        Map<String, String> domains = consoleConfig.getConsoleDomains();
        if (null == domains) return Collections.emptySet();
        return domains.keySet().stream()
                .filter(dc -> !isEmpty(dc))
                .map(String::toUpperCase)
                .filter(dc -> !dc.equals(currentDc))
                .collect(Collectors.toSet());
    }

    /**
     * 本机房与各远端快照的存活反向索引中，同一 IP 出现在多个机房即重叠。单个机房的反向索引 key 唯一，
     * 所以只会报跨机房重叠。只读内存，在刷新线程执行。
     */
    private void checkIpOverlap(Map<String, RemoteSnapshot> remote) {
        Map<String, List<String>> owners = new TreeMap<>();
        addOwners(owners, currentDc, currentDcAddressService.getAddressSnapshot().getLiveIpToHostname());
        remote.forEach((dc, snapshot) -> addOwners(owners, dc, snapshot.liveIpToHostname()));

        Map<String, List<String>> overlaps = new TreeMap<>();
        owners.forEach((ip, dcHostnames) -> {
            if (dcHostnames.size() > 1) {
                Collections.sort(dcHostnames);
                overlaps.put(ip, dcHostnames);
            }
        });
        if (overlaps.isEmpty()) return;

        logger.warn("[checkIpOverlap] {} ip(s) shared across dcs: {}", overlaps.size(), overlaps);
        eventMonitor.logAlertEvent(ALERT_KEEPER_CONTAINER_IP_OVERLAP);
    }

    private void addOwners(Map<String, List<String>> owners, String dc, Map<String, String> ipToHostname) {
        if (null == ipToHostname) return;
        ipToHostname.forEach((ip, hostname) ->
                owners.computeIfAbsent(ip, k -> new ArrayList<>()).add(dc + ":" + hostname));
    }

    /** 单测冻结时钟，验证 tombstone 到期而不用真等。 */
    @VisibleForTesting
    protected long nowNanos() {
        return System.nanoTime();
    }

    private static boolean isEmpty(String s) {
        return null == s || s.isEmpty();
    }

    /** 远端 tombstone，到期时刻基于本地 nanoTime。 */
    private record RemoteTombstone(String ip, long expireAtNanos) {
        boolean expired(long nowNanos) {
            return nowNanos - expireAtNanos >= 0;
        }
    }

    /**
     * 一个远端机房的快照，发布后只读。
     *
     * @param liveHostnameToIp 存活正向，hostname → 首节点 IP
     * @param tombstones       hostname → last-known IP 与本地到期时刻，与 liveHostnameToIp 的 key 不相交
     * @param liveIpToHostname 存活反向，全部存活 latch 子节点 IP → hostname
     */
    private record RemoteSnapshot(Map<String, String> liveHostnameToIp,
                                  Map<String, RemoteTombstone> tombstones,
                                  Map<String, String> liveIpToHostname,
                                  long createTimeMillis) {

        static RemoteSnapshot from(KeeperContainerAddressSnapshot raw, long receivedNanos) {
            Map<String, String> live = copyNonNull(raw.getLiveHostnameToIp());
            Map<String, RemoteTombstone> tombstones = new HashMap<>();
            raw.getTombstones().forEach((hostname, entry) -> {
                // 导出方保证 remainingMillis > 0；防御性跳过异常项与和存活冲突的项
                if (null == hostname || null == entry || isEmpty(entry.getIp())
                        || entry.getRemainingMillis() <= 0 || live.containsKey(hostname)) {
                    return;
                }
                long expireAt = receivedNanos + TimeUnit.MILLISECONDS.toNanos(entry.getRemainingMillis());
                tombstones.put(hostname, new RemoteTombstone(entry.getIp(), expireAt));
            });
            return new RemoteSnapshot(live, Collections.unmodifiableMap(tombstones),
                    copyNonNull(raw.getLiveIpToHostname()), raw.getCreateTimeMillis());
        }

        String ipByHostname(String hostname, long nowNanos) {
            String ip = liveHostnameToIp.get(hostname);
            if (null != ip) return ip;
            RemoteTombstone tombstone = tombstones.get(hostname);
            return null != tombstone && !tombstone.expired(nowNanos) ? tombstone.ip() : null;
        }

        Map<String, String> allHostnameToIp(long nowNanos) {
            Map<String, String> all = new HashMap<>(liveHostnameToIp);
            tombstones.forEach((hostname, tombstone) -> {
                if (!tombstone.expired(nowNanos)) {
                    all.putIfAbsent(hostname, tombstone.ip());
                }
            });
            return all;
        }

        KeeperContainerAddressSnapshot toAddressSnapshot(long nowNanos) {
            Map<String, KeeperContainerAddressSnapshot.TombstoneEntry> exportedTombstones = new HashMap<>();
            tombstones.forEach((hostname, tombstone) -> {
                long remainingMillis = TimeUnit.NANOSECONDS.toMillis(tombstone.expireAtNanos() - nowNanos);
                if (remainingMillis > 0) {
                    exportedTombstones.put(hostname,
                            new KeeperContainerAddressSnapshot.TombstoneEntry(tombstone.ip(), remainingMillis));
                }
            });
            return new KeeperContainerAddressSnapshot(new HashMap<>(liveHostnameToIp), exportedTombstones,
                    new HashMap<>(liveIpToHostname), createTimeMillis);
        }

        private static Map<String, String> copyNonNull(Map<String, String> source) {
            Map<String, String> copy = new HashMap<>();
            source.forEach((k, v) -> {
                if (!isEmpty(k) && !isEmpty(v)) copy.put(k, v);
            });
            return Collections.unmodifiableMap(copy);
        }
    }
}
