package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.ZkUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.apache.curator.utils.ZKPaths;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * watch 本机房 ZK 的 keepercontainer 注册路径，维护 hostname → IP 的单向映射。
 * <p>
 * 缓存刷新采用全量重建：rebuild 在执行时读取 CuratorCache 的当前快照重算映射表。
 * watch 回调与定时兜底只把重建投递到独立单线程池，不在回调线程里计算。
 * CuratorCache 的 listener 跑在 per-client 共享的 runSafe 线程上，回调里做计算会拖慢
 * 同一 CuratorFramework 上的所有 recipe，所以这里只投递。
 * <p>
 * 注册结构是两层：{@code {root}/{hostname}/_c_<uuid>-latch-<seq>}。hostname 由父节点名承载，
 * 每个 pod 通过 LeaderLatch 在其下创建一个临时顺序子节点，节点 data 是 {@link KeeperContainerAddressInfo} JSON。
 * 同一 hostname 下有多个子节点时（滚动替换期间新旧 pod 并存）按 seq 排序取首节点，
 * 即最早注册、仍存活的那个 pod（spec D-I1）。
 * <p>
 * CuratorCache 默认递归，storage 里既有注册根节点自身，也有 hostname 父节点与更深层节点；
 * rebuild 只认「根 → hostname → latch 子节点」这一层级的节点，其余一律忽略。
 * 旧的一层结构（hostname 节点自身带地址 data）不再解析（spec D-I4）。
 * <p>
 * CuratorCache 不监听 SUSPENDED/LOST，断连期间 storage 不清空，映射据此继续可用；
 * 重连后由 PersistentWatcher 重新挂 watch 并逐个复查已缓存路径，已消失的节点在那时移除。
 * <p>
 * 另有一个定时任务做兜底全量重建，覆盖 watch 事件丢失与 session 重连后事件补发不全的情况，
 * 因此不需要单独监听连接状态。这个模式与 metaserver 维护集群成员列表的做法一致。
 * <p>
 * 正反向语义不对称（spec §5.7）：
 * <ul>
 *     <li>hostname → IP：存活映射取首节点。某次 rebuild 发现 hostname 失去存活映射时，把上一份映射里的 IP
 *     记为 tombstone，保留 {@code tombstoneRetentionMilli}（默认 1h）。读路径按到期时间精确过滤，
 *     rebuild 顺带清掉到期项。hostname 重新出现存活映射即丢弃 tombstone。</li>
 *     <li>IP → hostname：只由存活 latch 子节点构建，覆盖每个 hostname 下全部子节点（不止首节点），
 *     tombstone 不进索引。同 IP 命中多个 hostname 时取字典序最小者，保证结果确定。</li>
 * </ul>
 * tombstone 纯内存，stop 或进程重启即失效。IP 复用不做即刻失效（D-I3）：保留期内 IP 被别的 pod 占用时，
 * 正向仍返回该 IP，反向则指向新的持有者。
 */
public class DefaultCurrentDcKeeperContainerAddressService extends AbstractLifecycle
        implements CurrentDcKeeperContainerAddressService, TopElement {

    private static final long DEFAULT_REFRESH_INTERVAL_MILLI = 30 * 1000L;

    /**
     * hostname 从 ZK 消失后 last-known IP 的保留时长。依据是「发现 pod 消失并完成 forceCloseDir」的时间上界，
     * 取值需实测校准（spec §5.7）。
     */
    private static final long DEFAULT_TOMBSTONE_RETENTION_MILLI = TimeUnit.HOURS.toMillis(1);

    /** 1 个在跑 + 1 个排队，其余提交由 DiscardPolicy 丢掉。 */
    private static final int REBUILD_POOL_SIZE = 1;
    private static final int REBUILD_QUEUE_CAPACITY = 1;

    /** 与 LeaderLatch 私有常量 LOCK_NAME 一致；子节点名形如 {@code _c_<uuid>-latch-0000000001}。 */
    private static final String LATCH_NODE_NAME = "latch-";

    /** 与 LeaderLatch 内部排序一致：只比较 {@link #LATCH_NODE_NAME} 之后的 seq，忽略 protection 前缀。 */
    private static final LockInternalsSorter LATCH_SORTER = StandardLockInternalsDriver::standardFixForSorting;

    private final ZkClient zkClient;
    private final long refreshIntervalMilli;
    private final long tombstoneRetentionNanos;

    private volatile String registerPath;
    private volatile CuratorCache containersCache;
    /** CuratorCache 首次加载完成。只供单测等待，业务不依赖它。 */
    private volatile boolean cacheInitialized;
    private ScheduledExecutorService scheduled;
    private ScheduledFuture<?> refreshFuture;

    /**
     * watch 与定时兜底共用的重建池：core = max = 1，队列容量 1，拒绝策略 DiscardPolicy。
     * 同一时刻最多 1 个在跑、1 个在排队，更多提交直接丢弃（n 次事件最多触发两次 rebuild）。
     * <p>
     * 合并不变量：{@link #rebuild()} 在执行时才读 cache 当前快照，因此被丢弃的提交必然被
     * 「已入队但尚未开始」的那次 rebuild 覆盖，丢弃无损。
     * 若将来改成在提交时抓快照，这个合并就不再安全。
     */
    private volatile ThreadPoolExecutor rebuildExecutor;

    /**
     * 正向、tombstone、反向三份映射作为一个不可变快照整体发布，读路径无锁且三者互相一致。
     * 只在 rebuild（及 stop、单测钩子）里被整体替换，发布后不再修改其中的 map。
     */
    private volatile AddressSnapshot snapshot = AddressSnapshot.EMPTY;

    public DefaultCurrentDcKeeperContainerAddressService(ZkClient zkClient) {
        this(zkClient, DEFAULT_REFRESH_INTERVAL_MILLI);
    }

    public DefaultCurrentDcKeeperContainerAddressService(ZkClient zkClient, long refreshIntervalMilli) {
        this(zkClient, refreshIntervalMilli, DEFAULT_TOMBSTONE_RETENTION_MILLI);
    }

    public DefaultCurrentDcKeeperContainerAddressService(ZkClient zkClient, long refreshIntervalMilli,
                                                         long tombstoneRetentionMilli) {
        this.zkClient = zkClient;
        this.refreshIntervalMilli = refreshIntervalMilli;
        this.tombstoneRetentionNanos = TimeUnit.MILLISECONDS.toNanos(tombstoneRetentionMilli);
    }

    @Override
    protected void doStart() throws Exception {
        CuratorFramework client = zkClient.get();
        String registerPath = MetaZkConfig.getKeeperContainerRegisterPath();
        this.registerPath = registerPath;
        this.cacheInitialized = false;

        rebuildExecutor = newRebuildExecutor();
        try {
            // 不传 Options 即递归：Id 之后注册结构是 {root}/{hostname}/latch-xxx 两层。
            containersCache = CuratorCache.builder(client, registerPath)
                    .withExceptionHandler(e -> logger.error("[containersCache][error]{}", registerPath, e))
                    .build();
            containersCache.listenable().addListener(new ContainersChanged());
            // start 是异步的：先挂 persistent watch，再后台加载节点。加载完成后 initialized 回调投递 rebuild，
            // doStart 不等它完成，也不因 ZK 暂时不可用而失败。
            containersCache.start();

            scheduled = Executors.newScheduledThreadPool(1,
                    XpipeThreadFactory.create("KeeperContainerAddressRefresh"));
            refreshFuture = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
                @Override
                protected void doRun() {
                    submitRebuild();
                }
            }, refreshIntervalMilli, refreshIntervalMilli, TimeUnit.MILLISECONDS);

            logger.info("[doStart][watch]{}", registerPath);
        } catch (Exception e) {
            try {
                doStop();
            } catch (Exception stopEx) {
                e.addSuppressed(stopEx);
            }
            throw e;
        }
    }

    /**
     * core = max = 1，队列容量 1，DiscardPolicy。合并不变量见 {@link #rebuildExecutor}。
     */
    private ThreadPoolExecutor newRebuildExecutor() {
        return new ThreadPoolExecutor(
                REBUILD_POOL_SIZE,
                REBUILD_POOL_SIZE,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(REBUILD_QUEUE_CAPACITY),
                XpipeThreadFactory.create("KeeperContainerAddressRebuild"),
                new ThreadPoolExecutor.DiscardPolicy());
    }

    @Override
    protected void doStop() throws Exception {
        if (refreshFuture != null) {
            refreshFuture.cancel(true);
            refreshFuture = null;
        }
        if (scheduled != null) {
            scheduled.shutdownNow();
            scheduled = null;
        }
        ThreadPoolExecutor executor = rebuildExecutor;
        rebuildExecutor = null;
        if (executor != null) {
            executor.shutdownNow();
        }
        if (containersCache != null) {
            containersCache.close();
            containersCache = null;
        }
        // tombstone 纯内存，stop 即失效
        snapshot = AddressSnapshot.EMPTY;
    }

    @Override
    public String getIpByHostname(String hostname) {
        if (null == hostname || hostname.isEmpty()) return null;
        AddressSnapshot current = snapshot;
        String ip = current.liveHostnameToIp().get(hostname);
        if (null != ip) return ip;
        Tombstone tombstone = current.tombstones().get(hostname);
        return null != tombstone && !tombstone.expired(nowNanos()) ? tombstone.ip() : null;
    }

    @Override
    public Map<String, String> getAllHostnameToIp() {
        // 与逐个 getIpByHostname 一致：含未到期的 tombstone。返回副本，避免外部修改影响内部状态
        AddressSnapshot current = snapshot;
        Map<String, String> all = new HashMap<>(current.liveHostnameToIp());
        long now = nowNanos();
        current.tombstones().forEach((hostname, tombstone) -> {
            if (!tombstone.expired(now)) {
                all.putIfAbsent(hostname, tombstone.ip());
            }
        });
        return all;
    }

    @Override
    public String getHostnameByIp(String ip) {
        if (null == ip || ip.isEmpty()) return null;
        return snapshot.liveIpToHostname().get(ip);
    }

    /**
     * 从同一份 {@link AddressSnapshot} 导出，三部分互相一致。tombstone 的 nanoTime 到期时刻换算为剩余毫秒（向下取整），
     * 已到期与不足 1ms 的不导出：接收方按剩余毫秒在本地到期，向下取整保证不会延长保留期。
     */
    @Override
    public KeeperContainerAddressSnapshot getAddressSnapshot() {
        AddressSnapshot current = snapshot;
        long now = nowNanos();
        Map<String, KeeperContainerAddressSnapshot.TombstoneEntry> tombstones = new HashMap<>();
        current.tombstones().forEach((hostname, tombstone) -> {
            long remainingMillis = tombstone.remainingMillis(now);
            if (remainingMillis > 0) {
                tombstones.put(hostname, new KeeperContainerAddressSnapshot.TombstoneEntry(tombstone.ip(), remainingMillis));
            }
        });
        return new KeeperContainerAddressSnapshot(new HashMap<>(current.liveHostnameToIp()), tombstones,
                new HashMap<>(current.liveIpToHostname()));
    }

    /**
     * 只投递，不在调用线程计算。池已关闭或尚未创建时直接返回。
     * <p>
     * 丢弃是否安全见 {@link #rebuildExecutor}：这里不能改成「提交时抓快照」。
     */
    @VisibleForTesting
    protected void submitRebuild() {
        ThreadPoolExecutor executor = this.rebuildExecutor;
        if (executor == null || executor.isShutdown()) {
            return;
        }
        executor.execute(this::rebuildSafely);
    }

    private void rebuildSafely() {
        try {
            rebuild();
        } catch (Throwable th) {
            logger.error("[rebuild][fail]", th);
        }
    }

    /**
     * 基于 ZK 当前快照全量重算映射表。幂等，可被 watch 事件与定时任务并发触发。
     * <p>
     * 必须在执行时读快照。投递侧靠丢弃合并事件，被丢掉的更新要靠这次读取补上。
     */
    @VisibleForTesting
    protected synchronized void rebuild() {
        CuratorCache cache = containersCache;
        if (null == cache) return;

        Map<String, String> freshLive = new HashMap<>();
        Map<String, String> freshIpToHostname = new HashMap<>();
        groupLatchNodesByHostname(cache.stream().toList()).forEach((hostname, latchNodes) -> {
            ChildData first = firstLatchNode(latchNodes);
            for (ChildData latchNode : latchNodes) {
                String ip = ipOf(latchNode);
                if (null == ip) continue;

                // 正向只看首节点：首节点 data 坏了也不回落到次节点。次节点是更晚注册的 pod，
                // 可能尚未就绪（与 D-I1 放弃「取末」同一理由）；解析不到时调用方走拒绝路径，失败是干净的。
                // 正向映射可信：hostname 取自 ZK 父节点名，每个 pod 只写自己的 latch 子节点，不会串。
                if (latchNode == first) {
                    freshLive.put(hostname, ip);
                }
                // 反向覆盖全部存活子节点：次节点 pod 的 IP 同样要能归属到 hostname。
                // 同 IP 多 hostname 取字典序最小者，结果与遍历顺序无关。
                freshIpToHostname.merge(ip, hostname, (a, b) -> a.compareTo(b) <= 0 ? a : b);
            }
        });

        AddressSnapshot previous = snapshot;
        AddressSnapshot fresh = new AddressSnapshot(freshLive,
                refreshTombstones(previous, freshLive, nowNanos()), freshIpToHostname);

        // 监听的是注册根路径，任一容器的增删改都会触发全量重建，因此重复内容很常见
        // （扩缩容时一批 pod 逐个上下线，以及定时兜底与 watch 事件叠加）。
        // 内容无变化时直接返回，省掉一次 volatile 写与旧 map 的提前失效。
        // 沿用下来的 tombstone 到期时间不变，所以 tombstone 无增减时 equals 成立。
        if (fresh.equals(previous)) {
            return;
        }

        if (!freshLive.equals(previous.liveHostnameToIp())) {
            logger.info("[rebuild]{} -> {}", previous.liveHostnameToIp(), freshLive);
        }
        this.snapshot = fresh;
    }

    /**
     * 上一份存活映射里、这一次失去存活映射的 hostname 新建 tombstone；已有 tombstone 未到期且 hostname 仍未恢复的沿用，
     * 到期或 hostname 恢复存活的丢弃。
     * <p>
     * 「失去存活映射」包括 latch 子节点全部消失，也包括剩余首节点 data 解析不出 IP——两种情况下正向都拿不到存活 IP，
     * last-known IP 是唯一可用的收尾线索。
     */
    private Map<String, Tombstone> refreshTombstones(AddressSnapshot previous, Map<String, String> freshLive, long now) {
        Map<String, Tombstone> tombstones = new HashMap<>();
        previous.tombstones().forEach((hostname, tombstone) -> {
            if (freshLive.containsKey(hostname)) return;
            if (tombstone.expired(now)) {
                logger.info("[rebuild][tombstone expired]{} {}", hostname, tombstone.ip());
                return;
            }
            tombstones.put(hostname, tombstone);
        });
        previous.liveHostnameToIp().forEach((hostname, ip) -> {
            if (freshLive.containsKey(hostname)) return;
            logger.info("[rebuild][tombstone]{} {}, retain {}ms", hostname, ip,
                    TimeUnit.NANOSECONDS.toMillis(tombstoneRetentionNanos));
            tombstones.put(hostname, new Tombstone(ip, now + tombstoneRetentionNanos));
        });
        return tombstones;
    }

    /** 单测替换时钟，验证 tombstone 到期而不用真等 1h。 */
    @VisibleForTesting
    protected long nowNanos() {
        return System.nanoTime();
    }

    /** 单测读取 watch 快照，确认事件已经进 cache、且发生在 rebuild 读快照之前。 */
    @VisibleForTesting
    protected List<ChildData> currentChildren() {
        CuratorCache cache = containersCache;
        if (cache == null) {
            return Collections.emptyList();
        }
        return cache.stream().collect(Collectors.toList());
    }

    /** 单测等首次加载完成，避免 initialized 回调晚到、在计数窗口里多投递一次 rebuild。 */
    @VisibleForTesting
    protected boolean cacheInitialized() {
        return cacheInitialized;
    }

    /** 单测把存活正向映射打成过期值，验证定时兜底会从 ZK 快照重建回来。tombstone 与反向索引保持不动。 */
    @VisibleForTesting
    protected synchronized void overwriteHostnameToIp(Map<String, String> hostnameToIp) {
        AddressSnapshot current = snapshot;
        this.snapshot = new AddressSnapshot(new HashMap<>(hostnameToIp), current.tombstones(), current.liveIpToHostname());
    }

    /** 单测确认到期 tombstone 已被 rebuild 清出快照，而不只是被读路径过滤。 */
    @VisibleForTesting
    protected Set<String> tombstoneHostnames() {
        return new HashSet<>(snapshot.tombstones().keySet());
    }

    /** 单测判断重建池是否已排空，避免把残留任务误认成定时兜底。 */
    @VisibleForTesting
    protected boolean rebuildExecutorIdle() {
        ThreadPoolExecutor executor = rebuildExecutor;
        if (executor == null) {
            return false;
        }
        return executor.getQueue().isEmpty()
                && executor.getActiveCount() == 0
                && executor.getTaskCount() == executor.getCompletedTaskCount();
    }

    /**
     * 按 hostname 分组 latch 子节点。只按路径层级与节点名过滤，不解 data：
     * 根节点、hostname 父节点自身的 data 都不是地址 JSON，不能进 ipOf 刷错误日志。
     * 没有 latch 子节点的 hostname 不会出现在结果里，因而不产生映射。
     */
    private Map<String, List<ChildData>> groupLatchNodesByHostname(List<ChildData> children) {
        Map<String, List<ChildData>> grouped = new HashMap<>();
        for (ChildData child : children) {
            String hostname = hostnameOfLatchNode(child);
            if (null == hostname) continue;
            grouped.computeIfAbsent(hostname, k -> new ArrayList<>()).add(child);
        }
        return grouped;
    }

    /**
     * 只认 {@code {registerPath}/{hostname}/...latch-...} 这一层级，返回 hostname；其余节点返回 null。
     * <p>
     * 节点名不含 {@link #LATCH_NODE_NAME} 的子节点必须排除：{@link LockInternals#getSortedChildren}
     * 对这类名字按全名比较，可能排到真正的 latch 节点前面被误取为首节点。
     */
    private String hostnameOfLatchNode(ChildData child) {
        String path = child.getPath();
        if (null == path) return null;
        ZKPaths.PathAndNode latch = ZKPaths.getPathAndNode(path);
        if (!latch.getNode().contains(LATCH_NODE_NAME)) return null;
        ZKPaths.PathAndNode host = ZKPaths.getPathAndNode(latch.getPath());
        if (!host.getPath().equals(registerPath)) return null;
        String hostname = host.getNode();
        return hostname.isEmpty() ? null : hostname;
    }

    /**
     * 同一 hostname 下按 seq 排序取首节点。排序规则与 LeaderLatch 一致，
     * 写法沿用 {@code DefaultApplierElectorManager.sortChildData}。latchNodes 非空。
     */
    private ChildData firstLatchNode(List<ChildData> latchNodes) {
        if (latchNodes.size() == 1) {
            return latchNodes.getFirst();
        }
        // 同一父节点下节点名唯一，可直接按名字回查
        Map<String, ChildData> byName = new HashMap<>();
        for (ChildData latchNode : latchNodes) {
            byName.put(ZKPaths.getNodeFromPath(latchNode.getPath()), latchNode);
        }
        List<String> sorted = LockInternals.getSortedChildren(LATCH_NODE_NAME, LATCH_SORTER, new ArrayList<>(byName.keySet()));
        return byName.get(sorted.getFirst());
    }

    private String ipOf(ChildData child) {
        byte[] data = child.getData();
        if (null == data || 0 == data.length) return null;
        try {
            KeeperContainerAddressInfo info =
                    Codec.DEFAULT.decode(new String(data), KeeperContainerAddressInfo.class);
            if (null == info || null == info.getIpAddress() || info.getIpAddress().isEmpty()) {
                logger.warn("[ipOf][no ip]{}", child.getPath());
                return null;
            }
            return info.getIpAddress();
        } catch (Throwable th) {
            // 单个坏节点不能影响其余容器的解析
            logger.error("[ipOf][decode fail]{}", child.getPath(), th);
            return null;
        }
    }

    @Override
    public int getOrder() {
        return 0;
    }

    /**
     * hostname 失去存活映射后保留的 last-known IP。到期判断用 nanoTime 差值，不受墙钟回拨影响。
     */
    private record Tombstone(String ip, long expireAtNanos) {
        boolean expired(long nowNanos) {
            return nowNanos - expireAtNanos >= 0;
        }

        /** 剩余保留毫秒，向下取整；已到期时 ≤ 0。 */
        long remainingMillis(long nowNanos) {
            return TimeUnit.NANOSECONDS.toMillis(expireAtNanos - nowNanos);
        }
    }

    /**
     * 一次 rebuild 的完整结果。三份 map 发布后只读。
     *
     * @param liveHostnameToIp 存活正向映射，hostname → 首节点 IP
     * @param tombstones       失去存活映射的 hostname → last-known IP，与 liveHostnameToIp 的 key 不相交
     * @param liveIpToHostname 存活反向索引，全部存活 latch 子节点 IP → hostname
     */
    private record AddressSnapshot(Map<String, String> liveHostnameToIp,
                                   Map<String, Tombstone> tombstones,
                                   Map<String, String> liveIpToHostname) {
        static final AddressSnapshot EMPTY =
                new AddressSnapshot(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * 跑在 CuratorFramework 的 runSafe 共享线程上，只投递不计算。
     */
    private class ContainersChanged implements CuratorCacheListener {

        @Override
        public void event(Type type, ChildData oldData, ChildData data) {
            logger.info("[cacheEvent]{}, {}", type, ZkUtils.toString(data != null ? data : oldData));
            submitRebuild();
        }

        @Override
        public void initialized() {
            submitRebuild();
            // 置位放在投递之后：单测看到 true 时这次 rebuild 已进池，rebuildExecutorIdle 能等到它跑完
            cacheInitialized = true;
            logger.info("[cacheInitialized]{}", registerPath);
        }
    }
}
