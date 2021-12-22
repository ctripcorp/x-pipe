package com.ctrip.xpipe.redis.console.healthcheck.nonredis.unhealthycluster;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.service.DelayService;
import com.ctrip.xpipe.redis.console.healthcheck.nonredis.AbstractSiteLeaderIntervalCheck;
import com.ctrip.xpipe.redis.console.model.consoleportal.UnhealthyInfoModel;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.ctrip.xpipe.utils.StringUtil;
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

    protected static final String UNHEALTHY_CLUSTER_METRIC_TYPE = "unhealthy_cluster";

    protected static final String UNHEALTHY_INSTANCE_METRIC_TYPE = "unhealthy_instance";

    private static final String CURRENT_IDC = FoundationService.DEFAULT.getDataCenter();

    private MetricProxy metricProxy = ServicesUtil.getMetricProxy();

    protected void doCheck() {
        logger.info("[doCheck] begin");
        UnhealthyInfoModel unhealthyInfoModel = delayService.getDcActiveClusterUnhealthyInstance(CURRENT_IDC);
        Map<ClusterType, Integer> unhealthyClustersByType = new EnumMap<>(ClusterType.class);
        config.getOwnClusterType().forEach(type -> unhealthyClustersByType.put(ClusterType.lookup(type), 0));
        Map<String, ClusterType> clusterTypeCache = new HashMap<>();
        Map<String, String> activeDcCache = new HashMap<>();

        unhealthyInfoModel.getUnhealthyClusterNames().forEach(cluster -> {
            try {
                ClusterType clusterType = metaCache.getClusterType(cluster);
                if (!unhealthyClustersByType.containsKey(clusterType)) return;

                clusterTypeCache.put(cluster, clusterType);

                int origin = unhealthyClustersByType.get(clusterType);
                unhealthyClustersByType.put(clusterType, origin + 1);
            } catch (Exception e) {
                logger.info("[doCheck] count fail", e);
            }
        });

        unhealthyClustersByType.forEach(this::metricUnhealthyClusters);

        unhealthyInfoModel.accept((dc, cluster, shard, hostPort, isMaster) -> {
            ClusterType clusterType = clusterTypeCache.get(cluster);
            if (null == clusterType) return;

            String activeDc = null;
            if (clusterType.supportSingleActiveDC()) {
                activeDc = activeDcCache.get(cluster);
                if (null == activeDc) activeDcCache.put(cluster, metaCache.getActiveDc(cluster, shard));
            }

            metricUnhealthyInstance(clusterType, dc, activeDc, cluster, shard, hostPort, isMaster);
        });
    }

    private void metricUnhealthyInstance(ClusterType clusterType, String dc, String activeDc, String cluster, String shard, HostPort hostPort, boolean isMaster) {
        MetricData data = new MetricData(UNHEALTHY_INSTANCE_METRIC_TYPE, dc, cluster, shard);
        data.setHostPort(hostPort);
        data.setValue(1);
        data.setClusterType(clusterType);
        data.setTimestampMilli(System.currentTimeMillis());
        data.addTag("role", isMaster ? Server.SERVER_ROLE.MASTER.name() : Server.SERVER_ROLE.SLAVE.name());

        if (!StringUtil.isEmpty(activeDc)) {
            data.addTag("activeDc", activeDc);
            data.addTag("inActiveDc", activeDc.equalsIgnoreCase(dc) ? "1" : "0");
        } else {
            data.addTag("activeDc", "NONE");
            data.addTag("inActiveDc", "0");
        }

        try {
            metricProxy.writeBinMultiDataPoint(data);
        } catch (Throwable th) {
            logger.info("[metricUnhealthyInstance] fail", th);
        }
    }

    private void metricUnhealthyClusters(ClusterType clusterType, int unhealthyClusters) {
        logger.debug("[metricUnhealthyClusters] metric {} {}", clusterType, unhealthyClusters);
        MetricData data = new MetricData(UNHEALTHY_CLUSTER_METRIC_TYPE, "-", "-", "-");
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
