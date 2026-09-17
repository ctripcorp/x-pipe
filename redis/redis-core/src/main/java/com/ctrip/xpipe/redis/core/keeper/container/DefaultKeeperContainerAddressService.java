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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * watch 本机房 ZK 的 keepercontainer 注册路径，维护 hostname 与 IP 的双向映射。
 * <p>
 * 缓存刷新采用全量重建而非增量增删：每次 watch 事件都基于 PathChildrenCache 的当前快照
 * 重算两张表。相比逐节点 put/remove，全量重建让正向与反向映射天然保持一致 —— pod 换 IP 时
 * 旧 IP 的反向映射会随重建自然消失，不需要小心处理"先删旧值再写新值"的时序。
 * <p>
 * 另有一个定时任务做兜底全量重建，覆盖 watch 事件丢失与 session 重连后事件补发不全的情况，
 * 因此不需要单独监听连接状态。这个模式与 metaserver 维护集群成员列表的做法一致。
 */
public class DefaultKeeperContainerAddressService extends AbstractLifecycle
        implements KeeperContainerAddressService, TopElement {

    private static final long DEFAULT_REFRESH_INTERVAL_MILLI = 30 * 1000L;

    private final ZkClient zkClient;
    private final long refreshIntervalMilli;

    private PathChildrenCache containersCache;
    private ScheduledExecutorService scheduled;
    private ScheduledFuture<?> refreshFuture;

    /**
     * hostname -> ip，以及它的反向表。两张表只在 rebuild 里被整体替换，读路径无锁。
     */
    private volatile Map<String, String> hostnameToIp = new HashMap<>();
    private volatile Map<String, String> ipToHostname = new HashMap<>();

    public DefaultKeeperContainerAddressService(ZkClient zkClient) {
        this(zkClient, DEFAULT_REFRESH_INTERVAL_MILLI);
    }

    public DefaultKeeperContainerAddressService(ZkClient zkClient, long refreshIntervalMilli) {
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
        ipToHostname = new HashMap<>();
    }

    @Override
    public String getIpByHostname(String hostname) {
        if (null == hostname || hostname.isEmpty()) return null;
        return hostnameToIp.get(hostname);
    }

    @Override
    public String getHostnameByIp(String ip) {
        if (null == ip || ip.isEmpty()) return null;
        return ipToHostname.get(ip);
    }

    /**
     * 基于 ZK 当前快照全量重算两张映射表。幂等，可被 watch 事件与定时任务并发触发。
     */
    synchronized void rebuild() {
        PathChildrenCache cache = containersCache;
        if (null == cache) return;

        List<ChildData> children = cache.getCurrentData();
        Map<String, String> freshHostnameToIp = new HashMap<>();
        Map<String, String> freshIpToHostname = new HashMap<>();
        // 已判定有歧义的 IP。必须单独记账：一旦从 freshIpToHostname 里删掉冲突项，
        // 后续同 IP 的 put 又会返回 null，靠返回值判断会漏掉第三个及之后的声明者。
        Set<String> ambiguousIps = new HashSet<>();

        for (ChildData child : children) {
            String hostname = hostnameOf(child);
            String ip = ipOf(child);
            if (null == hostname || null == ip) continue;

            // 正向映射可信：hostname 取自 ZK 子节点名，每个容器只写自己的节点，不会串。
            // 同 IP 的多个 hostname 都保留，误发的命令由容器端 hostname 自校验拦下。
            freshHostnameToIp.put(hostname, ip);

            if (ambiguousIps.contains(ip)) {
                logger.warn("[rebuild][ip ambiguous]{} also claimed by {}", ip, hostname);
                continue;
            }

            String previousOwner = freshIpToHostname.put(ip, hostname);
            if (null != previousOwner) {
                // Cilium 回收 IP 后立即分配给新 pod，而旧 pod 的临时节点尚未随 session 过期被清理。
                // 多个 hostname 争同一 IP 时反向解析已不可信，丢弃该 IP 的反向映射，
                // 让依赖它的入站校验走"解析不到"的拒绝路径，而不是赌一个可能错误的 hostname。
                logger.warn("[rebuild][ip conflict]{} claimed by both {} and {}", ip, previousOwner, hostname);
                ambiguousIps.add(ip);
                freshIpToHostname.remove(ip);
            }
        }

        // 监听的是注册根路径，任一容器的增删改都会触发全量重建，因此重复内容很常见
        // （扩缩容时一批 pod 逐个上下线，以及定时兜底与 watch 事件叠加）。
        // 内容无变化时直接返回，省掉两次 volatile 写与旧 map 的提前失效。
        if (freshHostnameToIp.equals(hostnameToIp) && freshIpToHostname.equals(ipToHostname)) {
            return;
        }

        logger.info("[rebuild]{} -> {}", hostnameToIp, freshHostnameToIp);
        this.hostnameToIp = freshHostnameToIp;
        this.ipToHostname = freshIpToHostname;
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
