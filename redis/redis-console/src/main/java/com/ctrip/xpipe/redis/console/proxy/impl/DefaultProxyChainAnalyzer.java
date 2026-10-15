package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SafeLoop;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainAnalyzer;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollector;
import com.ctrip.xpipe.redis.console.proxy.ProxyMonitorCollectorManager;
import com.ctrip.xpipe.redis.console.service.CrossDcKeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.XpipeMetaManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

@Component
@Lazy
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultProxyChainAnalyzer extends AbstractStartStoppable implements ProxyChainAnalyzer {

    private Logger logger = LoggerFactory.getLogger(DefaultProxyChainAnalyzer.class);

    private volatile Map<DcClusterShardPeer, ProxyChain> chains = Maps.newConcurrentMap();

    // tunnelId
    private volatile Map<String, DcClusterShardPeer> reverseMap = Maps.newConcurrentMap();

    private List<Listener> listeners = Lists.newCopyOnWriteArrayList();

    @Autowired
    private ProxyMonitorCollectorManager proxyMonitorCollectorManager;

    @Autowired
    private MetaCache metaCache;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Autowired
    private CrossDcKeeperContainerAddressService crossDcKeeperContainerAddressService;

    private EventMonitor eventMonitor = EventMonitor.DEFAULT;

    /** 链路 dst 翻译或 src 归属多命中告警事件名，每轮最多一次。只传事件名，明细见 WARN 日志。 */
    static final String ALERT_PROXY_CHAIN_KEEPER_AMBIGUOUS = "PROXY_CHAIN_KEEPER_AMBIGUOUS";

    private ScheduledFuture<?> future;

    private AtomicBoolean taskTrigger = new AtomicBoolean(false);

    public static final int ANALYZE_INTERVAL = Integer.parseInt(System.getProperty("console.proxy.chain.analyze.interval", "30000"));

    @Override
    public Map<DcClusterShardPeer, ProxyChain> getClusterShardChainMap() {
        return chains.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> ((DefaultProxyChain)e.getValue()).clone()));
    }

    @Override
    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }


    private void notifyListeners(Map<DcClusterShardPeer, ProxyChain> expired, Map<DcClusterShardPeer, ProxyChain> current) {
        new SafeLoop<Listener>(executors, listeners) {
            @Override
            protected void doRun0(Listener listener) {
                listener.onChange(expired, current);
            }
        }.run();
    }

    @VisibleForTesting
    protected void fullUpdate() {
        List<ProxyMonitorCollector> collectors = proxyMonitorCollectorManager.getProxyMonitorResults();
        List<DefaultTunnelInfo> tunnels = Lists.newArrayList();
        for(ProxyMonitorCollector collector : collectors) {
            logger.debug("[fullUpdate] {}, {}", collector.getProxyInfo(), collector.getTunnelInfos());
            tunnels.addAll(collector.getTunnelInfos());
        }

        CommandFuture<Map<SourceDest, List<DefaultTunnelInfo>>> future = new ProxyChainBuilder(tunnels).execute(executors);
        future.addListener(commandFuture -> {
            if(!commandFuture.isSuccess()) {
                logger.error("[fullUpdate]", commandFuture.cause());
                return;
            }
            new ShardTunnelsUpdater(commandFuture.getNow()).execute(executors);
        });

    }

    @VisibleForTesting
    public DefaultProxyChainAnalyzer setProxyMonitorCollectorManager(ProxyMonitorCollectorManager proxyMonitorCollectorManager) {
        this.proxyMonitorCollectorManager = proxyMonitorCollectorManager;
        return this;
    }

    @VisibleForTesting
    public DefaultProxyChainAnalyzer setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
        return this;
    }

    @VisibleForTesting
    public DefaultProxyChainAnalyzer setExecutors(ExecutorService executors) {
        this.executors = executors;
        return this;
    }

    @VisibleForTesting
    public DefaultProxyChainAnalyzer setCrossDcKeeperContainerAddressService(CrossDcKeeperContainerAddressService crossDcKeeperContainerAddressService) {
        this.crossDcKeeperContainerAddressService = crossDcKeeperContainerAddressService;
        return this;
    }

    @VisibleForTesting
    public DefaultProxyChainAnalyzer setEventMonitor(EventMonitor eventMonitor) {
        this.eventMonitor = eventMonitor;
        return this;
    }

    @Override
    public void isleader() {
        taskTrigger.set(true);
        stopAndStart();
    }

    private void stopAndStart() {
        try {
            stop();
            start();
        } catch (Exception e) {
            logger.error("[notLeader]", e);
        }
    }

    @Override
    public void notLeader() {
        taskTrigger.set(false);
        try {
            stop();
        } catch (Exception e) {
            logger.error("[notLeader]", e);
        }
    }

    @Override
    protected void doStart() throws Exception {
        future = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                if(!taskTrigger.get()) {
                    return;
                }
                fullUpdate();
            }
        }, Math.min(30000, ANALYZE_INTERVAL), ANALYZE_INTERVAL, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void doStop() throws Exception {
        if(future != null) {
            future.cancel(true);
            future = null;
        }
        reverseMap.clear();
        chains.clear();
    }

    private final class ProxyChainBuilder extends AbstractCommand<Map<SourceDest, List<DefaultTunnelInfo>>> {

        private List<DefaultTunnelInfo> tunnels;

        private Map<SourceDest, List<DefaultTunnelInfo>> result = Maps.newHashMap();

        private ProxyChainBuilder(List<DefaultTunnelInfo> tunnels) {
            this.tunnels = tunnels;
        }

        @Override
        protected void doExecute() {
            for(DefaultTunnelInfo tunnelInfo : tunnels) {
                SourceDest sourceDest = SourceDest.parse(tunnelInfo.getTunnelId());
                logger.debug("[ProxyChainBuilder] {}:{}, {}", sourceDest, tunnelInfo.getTunnelId(), tunnelInfo.getProxyModel());
                if(!result.containsKey(sourceDest)) {
                    result.put(sourceDest, Lists.newArrayListWithExpectedSize(2));
                }
                result.get(sourceDest).add(tunnelInfo);
            }
            future().setSuccess(result);
        }

        @Override
        protected void doReset() {
            result = Maps.newHashMap();
        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }
    }

    private final class ShardTunnelsUpdater extends AbstractCommand<Void> {

        private Map<SourceDest, List<DefaultTunnelInfo>> notReadyChains;

        /** 本轮因 dst 翻译或 src 归属多命中而跳过的链路数；每轮新建一个 updater，在单个执行线程内累加 */
        private int ambiguousChains = 0;

        private ShardTunnelsUpdater(Map<SourceDest, List<DefaultTunnelInfo>> notReadyChains) {
            this.notReadyChains = notReadyChains;
        }

        @Override
        protected void doExecute() {
            Map<DcClusterShardPeer, ProxyChain> results = Maps.newConcurrentMap();
            Map<String, DcClusterShardPeer> tunnelMapping = Maps.newConcurrentMap();
            for(Map.Entry<SourceDest, List<DefaultTunnelInfo>> entry : notReadyChains.entrySet()) {
                HostPort chainSrc = HostPort.fromString(entry.getKey().getKey());
                Pair<HostPort, XpipeMetaManager.MetaDesc> dstMeta = resolveDstMeta(chainSrc, getProxyChainDst(entry.getKey()));
                if (dstMeta == null) {
                    continue;
                }
                HostPort chainDst = dstMeta.getKey();
                Pair<String, String> peerClusterShard = new Pair<>(dstMeta.getValue().getClusterId(), dstMeta.getValue().getShardId());
                String peerDcId = dstMeta.getValue().getDcId();
                logger.debug("[analyzeProxyChain] get peerDcID {} from src {}, dst {}, tunnels:{}", peerDcId, chainSrc, chainDst, entry.getValue());
                if (StringUtil.isEmpty(peerDcId) ||
                        StringUtil.isEmpty(peerClusterShard.getKey()) ||
                        StringUtil.isEmpty(peerClusterShard.getValue())) {
                    continue;
                }

                if (!metaCache.isMetaChain(chainSrc, chainDst)) {
                    continue;
                }

                // 本机房 proxy 是第一跳：tunnel 所在机房即链路源端。peer 与 relay 视角都走下方 src 归属（spec §8.6）
                String backupDcId = null;
                for(DefaultTunnelInfo info : entry.getValue()) {
                    String tunnelDcId = info.getTunnelDcId();
                    if(!tunnelDcId.equalsIgnoreCase(peerDcId) && SourceDest.isFirstHop(info.getTunnelId())) {
                        backupDcId = tunnelDcId;
                        break;
                    }
                }

                if (backupDcId == null) {
                    List<Pair<String, String>> matchedKeepers = matchSrcKeepers(chainSrc, peerClusterShard, peerDcId);
                    List<String> matchedDcs = matchedKeepers.stream().map(keeper -> keeper.getKey().toUpperCase())
                            .distinct().toList();
                    if (matchedDcs.size() > 1) {
                        // 多命中不走 getDcByIpAndClusterShard 兜底：兜底同样只按 IP 查，无法消歧
                        logger.warn("[analyzeProxyChain][src ambiguous] skip chain src {}, dst {}, matched keepers {}",
                                chainSrc, chainDst, formatDcKeepers(matchedKeepers));
                        ambiguousChains++;
                        continue;
                    }
                    if (matchedDcs.size() == 1) {
                        backupDcId = matchedDcs.getFirst();
                    }
                }

                if (backupDcId == null) {
                    backupDcId = metaCache.getDcByIpAndClusterShard(chainSrc.getHost(), peerClusterShard);
                }

                if (backupDcId == null) {
                    logger.info("[analyzeProxyChain] get backupDc fail by chainSrc {}", chainSrc);
                    continue;
                }
                logger.debug("[analyzeProxyChain] get backupDc {} from src {}, dst {}", backupDcId, chainSrc, chainDst);
                DcClusterShardPeer key = new DcClusterShardPeer(backupDcId.toUpperCase(), peerClusterShard.getKey(), peerClusterShard.getValue(), peerDcId.toUpperCase());
                DefaultProxyChain chain = new DefaultProxyChain(backupDcId.toUpperCase(), peerClusterShard.getKey(), peerClusterShard.getValue(), peerDcId.toUpperCase(), entry.getValue());

                results.put(key,chain);
                entry.getValue().forEach(tunnelInfo -> tunnelMapping.put(tunnelInfo.getTunnelId(), key));
            }

            synchronized (DefaultProxyChainAnalyzer.this) {
                Map<DcClusterShardPeer, ProxyChain> expired = chains;
                chains = results;
                reverseMap = tunnelMapping;
                notifyListeners(expired, results);
            }

            // 按轮聚合：IP 重叠时 dst 多命中会成批出现，逐条告警会刷屏（spec §8.6）
            if (ambiguousChains > 0) {
                logger.warn("[analyzeProxyChain] {} ambiguous chain(s) skipped this round", ambiguousChains);
                eventMonitor.logAlertEvent(ALERT_PROXY_CHAIN_KEEPER_AMBIGUOUS);
            }

            future().setSuccess();
        }

        private HostPort getProxyChainDst(SourceDest sourceDest) {
            Endpoint endpoint = new DefaultProxyEndpoint(sourceDest.getValue());

            return new HostPort(endpoint.getHost(), endpoint.getPort());
        }

        /**
         * 链路 dst 的网络地址 → meta 地址及其 MetaDesc（spec §8.6 dst 翻译）。
         * <p>
         * 原始地址在 meta 中命中即 redis 或物理机 keeper，原样返回；否则按 IP 反查 keepercontainer，
         * 用候选 {@code hostname:port} 查 meta，恰好一个命中才采用。
         *
         * @return {@code (meta 地址, 其 MetaDesc)}，调用方直接从 MetaDesc 取分片与机房，不再按地址重复查找；
         *         零命中或多命中返回 null（多命中打 WARN 并计入本轮告警）
         */
        private Pair<HostPort, XpipeMetaManager.MetaDesc> resolveDstMeta(HostPort chainSrc, HostPort networkDst) {
            XpipeMetaManager.MetaDesc rawDesc = metaCache.findMetaDesc(networkDst);
            if (rawDesc != null) {
                return new Pair<>(networkDst, rawDesc);
            }

            Map<HostPort, XpipeMetaManager.MetaDesc> hits = new LinkedHashMap<>();
            for (Pair<String, String> container : crossDcKeeperContainerAddressService.findHostnamesByIp(networkDst.getHost())) {
                HostPort candidate = new HostPort(container.getValue(), networkDst.getPort());
                // hostname 全局唯一（spec §8.8），不同机房的同名候选对应同一条 meta
                if (!hits.containsKey(candidate)) {
                    XpipeMetaManager.MetaDesc desc = metaCache.findMetaDesc(candidate);
                    if (desc != null) {
                        hits.put(candidate, desc);
                    }
                }
            }

            if (hits.size() > 1) {
                logger.warn("[analyzeProxyChain][dst ambiguous] skip chain src {}, dst {}, matched keepers {}",
                        chainSrc, networkDst, hits.keySet());
                ambiguousChains++;
                return null;
            }
            if (hits.isEmpty()) {
                return null;
            }
            Map.Entry<HostPort, XpipeMetaManager.MetaDesc> hit = hits.entrySet().iterator().next();
            return new Pair<>(hit.getKey(), hit.getValue());
        }

        /**
         * 在分片的 keeper 中找网络 IP 等于 chainSrc 的（spec §8.6 src 归属）：容器 keeper 比 {@code (dc, hostname)}
         * 是否在反查候选中，物理机 keeper 比 IP。排除 {@code peerDcId}：backup 机房按定义不等于 peer 机房。
         * <p>
         * 只读内存：反查在循环前调用一次。
         *
         * @return 全部命中的 {@code (dc, hostname 或物理机 IP)}
         */
        private List<Pair<String, String>> matchSrcKeepers(HostPort chainSrc, Pair<String, String> clusterShard, String peerDcId) {
            String srcIp = chainSrc.getHost();
            List<Pair<String, String>> srcContainers = crossDcKeeperContainerAddressService.findHostnamesByIp(srcIp);

            List<Pair<String, String>> matched = new ArrayList<>();
            Map<KeeperMeta, String> keeper2Dc = metaCache.getKeeper2DcOfClusterShard(clusterShard.getKey(), clusterShard.getValue());
            for (Map.Entry<KeeperMeta, String> keeperAndDc : keeper2Dc.entrySet()) {
                KeeperMeta keeper = keeperAndDc.getKey();
                String dc = keeperAndDc.getValue();
                if (dc.equalsIgnoreCase(peerDcId)) {
                    continue;
                }
                if (StringUtil.isEmpty(keeper.getHostname())) {
                    if (srcIp.equals(keeper.getIp())) {
                        matched.add(new Pair<>(dc, keeper.getIp()));
                    }
                } else if (containsContainer(srcContainers, dc, keeper.getHostname())) {
                    matched.add(new Pair<>(dc, keeper.getHostname()));
                }
            }
            return matched;
        }

        private boolean containsContainer(List<Pair<String, String>> containers, String dc, String hostname) {
            for (Pair<String, String> container : containers) {
                // 门面返回的 dc 统一大写，与 meta 比较忽略大小写（spec §8.5「Jc 落地约定」）
                if (container.getKey().equalsIgnoreCase(dc) && hostname.equals(container.getValue())) {
                    return true;
                }
            }
            return false;
        }

        private List<String> formatDcKeepers(List<Pair<String, String>> dcKeepers) {
            return dcKeepers.stream().map(keeper -> keeper.getKey() + ":" + keeper.getValue()).toList();
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }
    }

    private static class SourceDest extends Pair<String, String> {

        private static final String SPLITTER = "-";

        private static final String FRONTEND_REMOTE_FORMAT = "R(%s)";

        private static SourceDest parse(String content) {
            String[] result = StringUtil.splitRemoveEmpty(SPLITTER, content);
            SourceDest sourceDest = new SourceDest();
            sourceDest.setKey(result[0]);
            sourceDest.setValue(result[result.length - 1]);
            return sourceDest;
        }

        /**
         * tunnel 所在 proxy 是否为链路第一跳。
         * <p>
         * tunnelId 为 {@code TunnelIdentity.toString()}：{@code {source}-R({frontend 对端})-L(..)->R(..)-{dest}}。
         * source 是 FORWARD_FOR 首地址，由第一跳 proxy 按 frontend channel 的 remoteAddress 记录，
         * 故第一跳时 frontend 对端与 source 字符串相等；relay 的 frontend 对端是上一跳 proxy。
         */
        private static boolean isFirstHop(String tunnelId) {
            String[] result = StringUtil.splitRemoveEmpty(SPLITTER, tunnelId);
            return result.length >= 2 && String.format(FRONTEND_REMOTE_FORMAT, result[0]).equals(result[1]);
        }
    }
}
