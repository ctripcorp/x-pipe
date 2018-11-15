package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.*;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpoint;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

@Component
public class DefaultProxyChainAnalyzer implements ProxyChainAnalyzer {

    private Logger logger = LoggerFactory.getLogger(DefaultProxyChainAnalyzer.class);

    private Map<DcClusterShard, ProxyChain> chains = Maps.newConcurrentMap();

    // tunnelId
    private Map<String, DcClusterShard> reverseMap = Maps.newConcurrentMap();

    @Autowired
    private ProxyMonitorCollectorManager proxyMonitorCollectorManager;

    @Autowired
    private MetaCache metaCache;

    @Resource(name = ConsoleContextConfig.GLOBAL_EXECUTOR)
    private ExecutorService executors;

    @Override
    public ProxyChain getProxyChain(String backupDcId, String clusterId, String shardId) {
        return chains.get(new DcClusterShard(backupDcId, clusterId, shardId));
    }

    @Override
    public ProxyChain getProxyChain(String tunnelId) {
        return chains.get(reverseMap.get(tunnelId));
    }

    @Override
    public void onGlobalEvent(ProxyMonitorCollectorManager.ProxyMonitorCollectType type) {
        fullUpdate();
    }

    @Override
    public void onLocalEvent(ProxyMonitorCollectorManager.ProxyMonitorCollectType type, ProxyModel proxyModel) {

    }

    private void notifyListeners(Map<DcClusterShard, ProxyChain> expired,
                                 Map<DcClusterShard, ProxyChain> current) {
        executors.execute(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {
                for(Map.Entry<DcClusterShard, ProxyChain> entry : expired.entrySet()) {
                    if(!entry.getValue().equals(current.get(entry.getKey()))) {
                        entry.getValue().getTunnels().forEach(tunnelInfo -> reverseMap.remove(tunnelInfo.getTunnelId()));
                    }
                }
            }
        });
    }

    private void fullUpdate() {

        List<ProxyMonitorCollector> collectors = proxyMonitorCollectorManager.getProxyMonitorResults();
        List<TunnelInfo> tunnels = Lists.newArrayList();
        for(ProxyMonitorCollector collector : collectors) {
            tunnels.addAll(collector.getTunnelInfos());
        }

        CommandFuture<Map<SourceDest, List<TunnelInfo>>> future = new ProxyChainBuilder(tunnels).execute(executors);
        future.addListener(commandFuture -> {
            if(!commandFuture.isSuccess()) {
                logger.error("[fullUpdate]", commandFuture.cause());
            }
            new ShardTunnelsUpdater(commandFuture.getNow()).execute(executors);
        });

    }

    private class ProxyChainBuilder extends AbstractCommand<Map<SourceDest, List<TunnelInfo>>> {

        private List<TunnelInfo> tunnels;

        private Map<SourceDest, List<TunnelInfo>> result = Maps.newHashMap();

        private ProxyChainBuilder(List<TunnelInfo> tunnels) {
            this.tunnels = tunnels;
        }

        @Override
        protected void doExecute() throws Exception {
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

    private class ShardTunnelsUpdater extends AbstractCommand<Void> {

        private Map<SourceDest, List<TunnelInfo>> notReadyChains;

        private ShardTunnelsUpdater(Map<SourceDest, List<TunnelInfo>> notReadyChains) {
            this.notReadyChains = notReadyChains;
        }

        @Override
        protected void doExecute() throws Exception {
            Map<DcClusterShard, ProxyChain> results = Maps.newConcurrentMap();
            for(Map.Entry<SourceDest, List<TunnelInfo>> entry : notReadyChains.entrySet()) {
                HostPort activeDcKeeper = getActiveDcKeeper(entry.getKey());
                Pair<String, String> clusterShard = metaCache.findClusterShard(activeDcKeeper);
                String activeDcId = metaCache.getActiveDc(clusterShard.getKey(), clusterShard.getValue());
                String backupDcId = null;
                for(TunnelInfo info : entry.getValue()) {
                    if(info.getTunnelDcId().equalsIgnoreCase(activeDcId))
                        continue;
                    backupDcId = info.getTunnelDcId();
                }
                if(backupDcId != null) {
                    DcClusterShard key = new DcClusterShard(backupDcId, clusterShard.getKey(), clusterShard.getValue());
                    results.put(key, new DefaultProxyChain(backupDcId, clusterShard.getKey(), clusterShard.getValue(), entry.getValue()));

                    entry.getValue().forEach(tunnelInfo -> reverseMap.put(tunnelInfo.getTunnelId(), key));
                }
            }

            synchronized (this) {
                Map<DcClusterShard, ProxyChain> expired = chains;
                chains = results;
                notifyListeners(expired, results);
            }

            future().setSuccess();
        }

        private HostPort getActiveDcKeeper(SourceDest sourceDest) {
            Endpoint endpoint = new DefaultProxyEndpoint(sourceDest.getDest());
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

    private static class DcClusterShard {

        private String dcId;

        private String clusterId;

        private String shardId;

        public DcClusterShard(String dcId, String clusterId, String shardId) {
            this.dcId = dcId;
            this.clusterId = clusterId;
            this.shardId = shardId;
        }

        public String getDcId() {
            return dcId;
        }

        public String getClusterId() {
            return clusterId;
        }

        public String getShardId() {
            return shardId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DcClusterShard that = (DcClusterShard) o;
            return Objects.equals(dcId, that.dcId) &&
                    Objects.equals(clusterId, that.clusterId) &&
                    Objects.equals(shardId, that.shardId);
        }

        @Override
        public int hashCode() {

            return Objects.hash(dcId, clusterId, shardId);
        }
    }

    private static class SourceDest {

        private static final String SPLITTER = "-";

        private String source;

        private String dest;

        public SourceDest(String source, String dest) {
            this.source = source;
            this.dest = dest;
        }

        public String getSource() {
            return source;
        }

        public String getDest() {
            return dest;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SourceDest that = (SourceDest) o;
            return Objects.equals(source, that.source) &&
                    Objects.equals(dest, that.dest);
        }

        @Override
        public int hashCode() {

            return Objects.hash(source, dest);
        }

        private static SourceDest parse(String content) {
            String[] result = StringUtil.splitRemoveEmpty(SPLITTER, content);
            return new SourceDest(result[0], result[result.length - 1]);
        }
    }
}
