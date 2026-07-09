package com.ctrip.xpipe.redis.console.healthcheck.nonredis.clusterstatus;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.console.AbstractCrossDcIntervalAction;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.MigrationClusterTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.utils.ServicesUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Map;

@Component
public class AbnormalClusterStatusMonitor extends AbstractCrossDcIntervalAction {

    static final String CAT_TYPE = "abnormal.cluster.status";

    static final String METRIC_TYPE = "abnormal.cluster.status";

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcService dcService;

    private MetricProxy metricProxy = ServicesUtil.getMetricProxy();

    @Override
    protected void doAction() {
        Map<Long, String> dcNameMap = dcService.dcNameMap();
        List<ClusterTbl> clusters = clusterService.findMigratingClusters();
        for (ClusterTbl cluster : clusters) {
            reportCluster(cluster, dcNameMap);
        }
        logger.info("[doAction][{}] report {} abnormal clusters", CAT_TYPE, clusters.size());
    }

    private void reportCluster(ClusterTbl cluster, Map<Long, String> dcNameMap) {
        String clusterName = cluster.getClusterName();
        String clusterStatus = cluster.getStatus();
        String clusterType = cluster.getClusterType();
        String activeDc = resolveDcName(dcNameMap, cluster.getActivedcId());

        MigrationClusterTbl migrationCluster = cluster.getMigrationClusters();
        String migrationStatus = migrationCluster == null ? "" : nullToEmpty(migrationCluster.getStatus());
        String sourceDc = migrationCluster == null ? "" : resolveDcName(dcNameMap, migrationCluster.getSourceDcId());
        String destDc = migrationCluster == null ? "" : resolveDcName(dcNameMap, migrationCluster.getDestinationDcId());

        EventMonitor.DEFAULT.logEvent(CAT_TYPE, clusterName);
        sendMetricData(clusterName, clusterStatus, migrationStatus, sourceDc, destDc, activeDc, clusterType);
    }

    private MetricData getMetricData(String clusterName, String clusterStatus, String migrationStatus,
                                     String sourceDc, String destDc, String activeDc, String clusterType) {
        MetricData metricData = new MetricData(METRIC_TYPE, null, clusterName, null);
        metricData.setTimestampMilli(System.currentTimeMillis());
        metricData.setValue(1);
        metricData.addTag("clusterName", clusterName);
        metricData.addTag("clusterStatus", clusterStatus);
        metricData.addTag("migrationStatus", migrationStatus);
        metricData.addTag("sourceDc", sourceDc);
        metricData.addTag("destDc", destDc);
        metricData.addTag("activeDc", activeDc);
        metricData.addTag("clusterType", clusterType);
        return metricData;
    }

    private void sendMetricData(String clusterName, String clusterStatus, String migrationStatus,
                                String sourceDc, String destDc, String activeDc, String clusterType) {
        try {
            metricProxy.writeBinMultiDataPoint(getMetricData(clusterName, clusterStatus, migrationStatus,
                    sourceDc, destDc, activeDc, clusterType));
        } catch (MetricProxyException e) {
            logger.error("[sendMetricData][{}]", clusterName, e);
        }
    }

    private String resolveDcName(Map<Long, String> dcNameMap, long dcId) {
        if (dcId <= 0) {
            return "";
        }
        return nullToEmpty(dcNameMap.get(dcId));
    }

    private String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    @Override
    protected long getIntervalMilli() {
        return consoleConfig.getAbnormalClusterStatusMonitorIntervalMilli();
    }

    @Override
    protected long getLeastIntervalMilli() {
        return consoleConfig.getAbnormalClusterStatusMonitorIntervalMilli();
    }

    @Override
    protected List<ALERT_TYPE> alertTypes() {
        return Collections.emptyList();
    }

    @VisibleForTesting
    void setMetricProxy(MetricProxy metricProxy) {
        this.metricProxy = metricProxy;
    }
}
