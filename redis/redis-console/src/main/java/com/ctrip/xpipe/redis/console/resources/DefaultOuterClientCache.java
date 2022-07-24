package com.ctrip.xpipe.redis.console.resources;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.lifecycle.TopElement;
import com.ctrip.xpipe.api.migration.OuterClientService;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.checker.OuterClientCache;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.utils.job.DynamicDelayPeriodTask;
import org.springframework.stereotype.Component;

import java.util.HashMap;
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

    private static final String CURRENT_DC = FoundationService.DEFAULT.getDataCenter();

    private MetaCache metaCache;

    private OuterClientService outerClientService;

    private ConsoleConfig config;

    private Map<String, OuterClientService.ClusterInfo> clustersCache;

    private ScheduledExecutorService scheduled;

    private DynamicDelayPeriodTask refreshTask;

    public DefaultOuterClientCache(MetaCache metaCache, ConsoleConfig config) {
        this.outerClientService = OuterClientService.DEFAULT;
        this.metaCache = metaCache;
        this.config = config;
        this.clustersCache = new HashMap<>();
    }

    @Override
    public OuterClientService.ClusterInfo getClusterInfo(String clusterName) throws Exception {
        OuterClientService.ClusterInfo clusterInfo = clustersCache.get(clusterName);
        if (null == clusterInfo) {
            return outerClientService.getClusterInfo(clusterName);
        }

        return null;
    }

    @Override
    public Map<String, OuterClientService.ClusterInfo> getAllCurrentDcActiveOneWayClusters(String activeDc) throws Exception {
        return clustersCache;
    }

    @Override
    public void refresh() {
        // TODO: load cluster info from credis
        // capitalization of cluster name may be inconsistent between two system
    }

    @Override
    protected void doInitialize() throws Exception {
        super.doInitialize();
        this.scheduled = Executors.newScheduledThreadPool(1,
                XpipeThreadFactory.create("OuterClientCacheRefreshScheduled"));
        this.refreshTask = new DynamicDelayPeriodTask("OuterClientCacheRefresh", this::refresh,
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
    }

}
