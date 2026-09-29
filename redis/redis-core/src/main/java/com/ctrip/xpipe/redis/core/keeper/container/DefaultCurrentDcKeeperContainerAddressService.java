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
import org.apache.curator.utils.ZKPaths;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * CuratorCache 默认递归，storage 里既有注册根节点自身，也可能有更深层节点；
 * rebuild 只认注册根的直接子节点，节点名即 hostname。
 * <p>
 * CuratorCache 不监听 SUSPENDED/LOST，断连期间 storage 不清空，映射据此继续可用；
 * 重连后由 PersistentWatcher 重新挂 watch 并逐个复查已缓存路径，已消失的节点在那时移除。
 * <p>
 * 另有一个定时任务做兜底全量重建，覆盖 watch 事件丢失与 session 重连后事件补发不全的情况，
 * 因此不需要单独监听连接状态。这个模式与 metaserver 维护集群成员列表的做法一致。
 */
public class DefaultCurrentDcKeeperContainerAddressService extends AbstractLifecycle
        implements CurrentDcKeeperContainerAddressService, TopElement {

    private static final long DEFAULT_REFRESH_INTERVAL_MILLI = 30 * 1000L;

    /** 1 个在跑 + 1 个排队，其余提交由 DiscardPolicy 丢掉。 */
    private static final int REBUILD_POOL_SIZE = 1;
    private static final int REBUILD_QUEUE_CAPACITY = 1;

    private final ZkClient zkClient;
    private final long refreshIntervalMilli;

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
     * hostname -> ip。只在 rebuild 里被整体替换，读路径无锁。
     */
    private volatile Map<String, String> hostnameToIp = new HashMap<>();

    public DefaultCurrentDcKeeperContainerAddressService(ZkClient zkClient) {
        this(zkClient, DEFAULT_REFRESH_INTERVAL_MILLI);
    }

    public DefaultCurrentDcKeeperContainerAddressService(ZkClient zkClient, long refreshIntervalMilli) {
        this.zkClient = zkClient;
        this.refreshIntervalMilli = refreshIntervalMilli;
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
        hostnameToIp = new HashMap<>();
    }

    @Override
    public String getIpByHostname(String hostname) {
        if (null == hostname || hostname.isEmpty()) return null;
        return hostnameToIp.get(hostname);
    }

    @Override
    public Map<String, String> getAllHostnameToIp() {
        // 返回快照副本，避免外部修改影响内部状态
        return new HashMap<>(hostnameToIp);
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

        List<ChildData> children = cache.stream().toList();
        Map<String, String> freshHostnameToIp = new HashMap<>();

        for (ChildData child : children) {
            // 先按路径层级过滤再解 data：根节点自身的 data 不是地址 JSON，不能进 ipOf 刷错误日志
            String hostname = hostnameOf(child);
            if (null == hostname) continue;
            String ip = ipOf(child);
            if (null == ip) continue;

            // 正向映射可信：hostname 取自 ZK 子节点名，每个容器只写自己的节点，不会串。
            freshHostnameToIp.put(hostname, ip);
        }

        // 监听的是注册根路径，任一容器的增删改都会触发全量重建，因此重复内容很常见
        // （扩缩容时一批 pod 逐个上下线，以及定时兜底与 watch 事件叠加）。
        // 内容无变化时直接返回，省掉一次 volatile 写与旧 map 的提前失效。
        if (freshHostnameToIp.equals(hostnameToIp)) {
            return;
        }

        logger.info("[rebuild]{} -> {}", hostnameToIp, freshHostnameToIp);
        this.hostnameToIp = freshHostnameToIp;
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

    /** 单测把映射打成过期值，验证定时兜底会从 ZK 快照重建回来。 */
    @VisibleForTesting
    protected void overwriteHostnameToIp(Map<String, String> hostnameToIp) {
        this.hostnameToIp = new HashMap<>(hostnameToIp);
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
     * 只认注册根的直接子节点。递归 cache 里的根节点自身、以及更深层节点一律返回 null。
     */
    private String hostnameOf(ChildData child) {
        String path = child.getPath();
        if (null == path) return null;
        ZKPaths.PathAndNode pathAndNode = ZKPaths.getPathAndNode(path);
        if (!pathAndNode.getPath().equals(registerPath)) return null;
        String node = pathAndNode.getNode();
        return node.isEmpty() ? null : node;
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
