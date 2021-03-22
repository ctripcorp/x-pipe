package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SafeLoop;
import com.ctrip.xpipe.redis.checker.model.DcClusterShard;
import com.ctrip.xpipe.redis.console.proxy.*;
import com.ctrip.xpipe.redis.console.service.RouteService;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.GLOBAL_EXECUTOR;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;

@Component
@Lazy
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class DefaultProxyChainAnalyzer extends AbstractStartStoppable implements ProxyChainAnalyzer {

    private Logger logger = LoggerFactory.getLogger(DefaultProxyChainAnalyzer.class);

    private volatile Map<DcClusterShard, ProxyChain> chains = Maps.newConcurrentMap();

    // tunnelId
    private volatile Map<String, DcClusterShard> reverseMap = Maps.newConcurrentMap();

    private List<Listener> listeners = Lists.newCopyOnWriteArrayList();

    @Autowired
    private ProxyMonitorCollectorManager proxyMonitorCollectorManager;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private RouteService routeService;

    @Resource(name = GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Resource(name = SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    private ScheduledFuture<?> future;

    private AtomicBoolean taskTrigger = new AtomicBoolean(false);

    public static final int ANALYZE_INTERVAL = Integer.parseInt(System.getProperty("console.proxy.chain.analyze.interval", "30000"));

    @Override
    public ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId) {
        return chains.get(new DcClusterShard(backupDcId, clusterId, shardId));
    }

    @Override
    public ProxyChain getProxyChain(String tunnelId) {
        if(reverseMap.containsKey(tunnelId)) {
            return chains.get(reverseMap.get(tunnelId));
        }
        return null;
    }

    @Override
    public List<ProxyChain> getProxyChains() {
        return Lists.newArrayList(chains.values());
    }

    @Override
    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }


    private void notifyListeners(Map<DcClusterShard, ProxyChain> expired, Map<DcClusterShard, ProxyChain> current) {
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
        List<TunnelInfo> tunnels = Lists.newArrayList();
        for(ProxyMonitorCollector collector : collectors) {
            tunnels.addAll(collector.getTunnelInfos());
        }

        CommandFuture<Map<SourceDest, List<TunnelInfo>>> future = new ProxyChainBuilder(tunnels).execute(executors);
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
    public DefaultProxyChainAnalyzer setRouteService(RouteService routeService) {
        this.routeService = routeService;
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

    private final class ProxyChainBuilder extends AbstractCommand<Map<SourceDest, List<TunnelInfo>>> {

        private List<TunnelInfo> tunnels;

        private Map<SourceDest, List<TunnelInfo>> result = Maps.newHashMap();

        private ProxyChainBuilder(List<TunnelInfo> tunnels) {
            this.tunnels = tunnels;
        }

        @Override
        protected void doExecute() {
            for(TunnelInfo tunnelInfo : tunnels) {
                SourceDest sourceDest = SourceDest.parse(tunnelInfo.getTunnelId());
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

        private Map<SourceDest, List<TunnelInfo>> notReadyChains;

        private ShardTunnelsUpdater(Map<SourceDest, List<TunnelInfo>> notReadyChains) {
            this.notReadyChains = notReadyChains;
        }

        @Override
        protected void doExecute() {
            Map<DcClusterShard, ProxyChain> results = Maps.newConcurrentMap();
            Map<String, DcClusterShard> tunnelMapping = Maps.newConcurrentMap();
            for(Map.Entry<SourceDest, List<TunnelInfo>> entry : notReadyChains.entrySet()) {
                HostPort activeDcKeeper = getActiveDcKeeper(entry.getKey());
                Pair<String, String> clusterShard = metaCache.findClusterShard(activeDcKeeper);
                if(clusterShard == null || StringUtil.isEmpty(clusterShard.getKey()) || StringUtil.isEmpty(clusterShard.getValue())) {
                    continue;
                }
                String activeDcId = metaCache.getActiveDc(clusterShard.getKey(), clusterShard.getValue());
                String backupDcId = null;
                for(TunnelInfo info : entry.getValue()) {
                    String tunnelDcId = info.getTunnelDcId();
                    if(!tunnelDcId.equalsIgnoreCase(activeDcId) && routeService.existsRouteBetweenDc(activeDcId, tunnelDcId)) {
                        backupDcId = tunnelDcId;
                        break;
                    }
                }
                if(backupDcId != null) {
                    DcClusterShard key = new DcClusterShard(backupDcId, clusterShard.getKey(), clusterShard.getValue());
                    results.put(key, new DefaultProxyChain(backupDcId, clusterShard.getKey(), clusterShard.getValue(), entry.getValue()));

                    entry.getValue().forEach(tunnelInfo -> tunnelMapping.put(tunnelInfo.getTunnelId(), key));
                }
            }

            synchronized (DefaultProxyChainAnalyzer.this) {
                Map<DcClusterShard, ProxyChain> expired = chains;
                chains = results;
                reverseMap = tunnelMapping;
                notifyListeners(expired, results);
            }

            future().setSuccess();
        }

        private HostPort getActiveDcKeeper(SourceDest sourceDest) {
            Endpoint endpoint = new DefaultProxyEndpoint(sourceDest.getValue());

            return new HostPort(endpoint.getHost(), endpoint.getPort());
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

        private static SourceDest parse(String content) {
            String[] result = StringUtil.splitRemoveEmpty(SPLITTER, content);
            SourceDest sourceDest = new SourceDest();
            sourceDest.setKey(result[0]);
            sourceDest.setValue(result[result.length - 1]);
            return sourceDest;
        }
    }
}
