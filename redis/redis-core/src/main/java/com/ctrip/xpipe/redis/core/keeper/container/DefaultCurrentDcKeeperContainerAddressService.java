package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.meta.MetaZkConfig;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.zk.ZkClient;
import com.ctrip.xpipe.zk.ZkUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * watch 本机房 ZK 的 keepercontainer 注册路径，维护 hostname → IP 的单向映射。
 * <p>
 * 缓存刷新采用全量重建：每次 watch 事件都基于 PathChildrenCache 的当前快照重算映射表。
 * <p>
 * 另有一个定时任务做兜底全量重建，覆盖 watch 事件丢失与 session 重连后事件补发不全的情况，
 * 因此不需要单独监听连接状态。这个模式与 metaserver 维护集群成员列表的做法一致。
 */
public class DefaultCurrentDcKeeperContainerAddressService extends AbstractLifecycle
        implements CurrentDcKeeperContainerAddressService, TopElement {

    private static final long DEFAULT_REFRESH_INTERVAL_MILLI = 30 * 1000L;

    private final ZkClient zkClient;
    private final long refreshIntervalMilli;

    private PathChildrenCache containersCache;
    private ScheduledExecutorService scheduled;
    private ScheduledFuture<?> refreshFuture;

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

        containersCache = new PathChildrenCache(client, registerPath, true,
                XpipeThreadFactory.create("KeeperContainerAddressCache"));
        containersCache.getListenable().addListener(new ContainersChanged());
        // BUILD_INITIAL_CACHE 保证 start 返回时快照已就绪，避免启动后短暂窗口内解析全部落空
        containersCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        rebuild();

        scheduled = Executors.newScheduledThreadPool(1,
                XpipeThreadFactory.create("KeeperContainerAddressRefresh"));
        refreshFuture = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                rebuild();
            }
        }, refreshIntervalMilli, refreshIntervalMilli, TimeUnit.MILLISECONDS);

        logger.info("[doStart][watch]{}", registerPath);
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
     * 基于 ZK 当前快照全量重算映射表。幂等，可被 watch 事件与定时任务并发触发。
     */
    synchronized void rebuild() {
        PathChildrenCache cache = containersCache;
        if (null == cache) return;

        List<ChildData> children = cache.getCurrentData();
        Map<String, String> freshHostnameToIp = new HashMap<>();

        for (ChildData child : children) {
            String hostname = hostnameOf(child);
            String ip = ipOf(child);
            if (null == hostname || null == ip) continue;

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

    private String hostnameOf(ChildData child) {
        String path = child.getPath();
        if (null == path) return null;
        int index = path.lastIndexOf('/');
        if (index < 0 || index == path.length() - 1) return null;
        return path.substring(index + 1);
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

    private class ContainersChanged implements PathChildrenCacheListener {

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
            logger.info("[childEvent]{}, {}", event.getType(), ZkUtils.toString(event.getData()));
            rebuild();
        }
    }
}
