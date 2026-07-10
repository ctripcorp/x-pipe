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
import com.ctrip.xpipe.utils.ServicesUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

@Component
public class AbnormalClusterStatusMonitor extends AbstractCrossDcIntervalAction {

    static final String CAT_TYPE = "abnormal.cluster.status";

    static final String METRIC_TYPE = "abnormal.cluster.status";

    @Autowired
    private ClusterService clusterService;

    private MetricProxy metricProxy = ServicesUtil.getMetricProxy();

    @Override
    protected void doAction() {
        List<ClusterTbl> clusters = clusterService.findMigratingClusters();
        for (ClusterTbl cluster : clusters) {
            reportCluster(cluster);
        }
        logger.info("[doAction][{}] report {} abnormal clusters", CAT_TYPE, clusters.size());
    }

    private void reportCluster(ClusterTbl cluster) {
        String clusterName = cluster.getClusterName();
        String clusterStatus = cluster.getStatus();
        String clusterType = cluster.getClusterType();
        MigrationClusterTbl migrationCluster = cluster.getMigrationClusters();
        String migrationStatus = migrationCluster == null ? "" : nullToEmpty(migrationCluster.getStatus());

        EventMonitor.DEFAULT.logEvent(CAT_TYPE, clusterName);
        sendMetricData(clusterName, clusterStatus, migrationStatus, clusterType);
    }

    private MetricData getMetricData(String clusterName, String clusterStatus, String migrationStatus,
                                     String clusterType) {
        MetricData metricData = new MetricData(METRIC_TYPE, null, clusterName, null);
        metricData.setTimestampMilli(System.currentTimeMillis());
        metricData.setValue(1);
        metricData.addTag("clusterName", clusterName);
        metricData.addTag("clusterStatus", clusterStatus);
        metricData.addTag("migrationStatus", migrationStatus);
        metricData.addTag("clusterType", clusterType);
        return metricData;
    }

    private void sendMetricData(String clusterName, String clusterStatus, String migrationStatus, String clusterType) {
        try {
            metricProxy.writeBinMultiDataPoint(getMetricData(clusterName, clusterStatus, migrationStatus, clusterType));
        } catch (MetricProxyException e) {
            logger.error("[sendMetricData][{}]", clusterName, e);
        }
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
