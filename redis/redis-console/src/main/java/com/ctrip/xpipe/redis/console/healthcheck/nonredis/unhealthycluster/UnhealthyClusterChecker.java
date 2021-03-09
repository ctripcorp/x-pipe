package com.ctrip.xpipe.redis.console.healthcheck.nonredis.unhealthycluster;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.DelayService;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.AbstractSiteLeaderIntervalCheck;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Author lishanglin
 * Date 2020/11/10
 */
@Component
public class UnhealthyClusterChecker extends AbstractSiteLeaderIntervalCheck {

    private DelayService delayService;

    private MetaCache metaCache;

    private ConsoleConfig config;

    @Autowired
    public UnhealthyClusterChecker(DelayService delayService, MetaCache metaCache, ConsoleConfig config) {
        this.delayService = delayService;
        this.metaCache = metaCache;
        this.config = config;
    }

    private final List<ALERT_TYPE> alertType = Collections.emptyList();

    private final int checkInterval = Integer.parseInt(System.getProperty("console.unhealthy.cluster.monitor.interval", "60000"));

    private static final String METRIC_TYPE = "unhealthy_cluster";

    private static final String CURRENT_IDC = FoundationService.DEFAULT.getDataCenter();

    private MetricProxy metricProxy = ServicesUtil.getMetricProxy();

    protected void doCheck() {
        logger.info("[doCheck] begin");
        UnhealthyInfoModel unhealthyInfoModel = delayService.getDcActiveClusterUnhealthyInstance(CURRENT_IDC);
        Map<ClusterType, Integer> unhealthyClustersByType = new EnumMap<>(ClusterType.class);
        config.getOwnClusterType().forEach(type -> unhealthyClustersByType.put(ClusterType.lookup(type), 0));

        unhealthyInfoModel.getUnhealthyClusterNames().forEach(cluster -> {
            try {
                ClusterType clusterType = metaCache.getClusterType(cluster);
                if (!unhealthyClustersByType.containsKey(clusterType)) return;

                int origin = unhealthyClustersByType.get(clusterType);
                unhealthyClustersByType.put(clusterType, origin + 1);
            } catch (Exception e) {
                logger.info("[doCheck] count fail", e);
            }
        });

        unhealthyClustersByType.forEach(this::metricUnhealthyClusters);
    }

    private void metricUnhealthyClusters(ClusterType clusterType, int unhealthyClusters) {
        logger.debug("[metricUnhealthyClusters] metric {} {}", clusterType, unhealthyClusters);
        MetricData data = new MetricData(METRIC_TYPE, "-", "-", "-");
        data.setValue(unhealthyClusters);
        data.setTimestampMilli(System.currentTimeMillis());
        data.setClusterType(clusterType);

        try {
            metricProxy.writeBinMultiDataPoint(data);
        } catch (Throwable th) {
            logger.info("[metricUnhealthyClusters] fail", th);
        }
    }

    @Override
    protected long getIntervalMilli(){
        return checkInterval;
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return alertType;
    }

    @VisibleForTesting
    protected void setMetricProxy(MetricProxy metricProxy) {
        this.metricProxy = metricProxy;
    }

}
