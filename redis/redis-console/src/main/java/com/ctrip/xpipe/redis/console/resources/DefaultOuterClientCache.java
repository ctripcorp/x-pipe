package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.checker.OuterClientCache;
import com.ctrip.xpipe.redis.checker.cache.TimeBoundCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author lishanglin
 * date 2022/7/18
 * only cache current dc active dc one-way clusters
 */
@Component
public class DefaultOuterClientCache extends AbstractLifecycle implements OuterClientCache, TopElement {

    private OuterClientService outerClientService;

    private ConsoleConfig config;

    private TimeBoundCache<Map<String, OuterClientService.ClusterInfo>> clustersCache;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask refreshTask;

    public DefaultOuterClientCache(ConsoleConfig config) {
        this.outerClientService = OuterClientService.DEFAULT;
        this.config = config;
        this.clustersCache = new TimeBoundCache<>(() -> 10000 + config.getRedisConfCheckIntervalMilli(),
                () -> loadActiveDcClusters(FoundationService.DEFAULT.getDataCenter()));
    }

    @Override
    public OuterClientService.ClusterInfo getClusterInfo(String clusterName) throws Exception {
        OuterClientService.ClusterInfo clusterInfo = clustersCache.getData(false).get(clusterName.toLowerCase());
        if (null == clusterInfo) {
            return outerClientService.getClusterInfo(clusterName);
        }

        return null;
    }

    @Override
    public Map<String, OuterClientService.ClusterInfo> getAllActiveDcClusters(String activeDc) {
        if (FoundationService.DEFAULT.getDataCenter().equalsIgnoreCase(activeDc)) return clustersCache.getData(false);
        else return loadActiveDcClusters(activeDc);
    }

    private Map<String, OuterClientService.ClusterInfo> loadActiveDcClusters(String activeDc) {
        Map<String, OuterClientService.ClusterInfo> clusters = new HashMap<>();
        try {
            List<OuterClientService.ClusterInfo> clusterInfos = outerClientService.getActiveDcClusters(activeDc);
            for (OuterClientService.ClusterInfo cluster: clusterInfos) {
                clusters.put(cluster.getName().toLowerCase(), cluster);
            }
        } catch (RestClientException e) {
            logger.warn("[refresh] rest fail, {}", e.getMessage());
        } catch (Throwable th) {
            logger.warn("[refresh] fail", th);
        }

        return clusters;
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        this.scheduled = Executors.newScheduledThreadPool(1,
                XpipeThreadFactory.create("OuterClientCacheRefreshScheduled"));
        this.refreshTask = new DynamicDelayPeriodTask("OuterClientCacheRefresh", clustersCache::refresh,
                config::getRedisConfCheckIntervalMilli, scheduled);
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        this.refreshTask.start();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        this.refreshTask.stop();
    }

    @Override
    protected void doDispose() throws Exception {
        super.doDispose();
        this.scheduled.shutdown();
        this.scheduled = null;
        this.refreshTask = null;
    }

}
