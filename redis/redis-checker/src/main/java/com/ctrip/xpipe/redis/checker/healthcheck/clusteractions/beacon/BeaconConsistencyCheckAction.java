package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.utils.ServicesUtil;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class BeaconConsistencyCheckAction extends AbstractLeaderAwareHealthCheckAction<ClusterHealthCheckInstance> {

    private BeaconManager beaconManager;

    private MetricProxy metricProxy = ServicesUtil.getMetricProxy();

    private static long lastSendTime = System.currentTimeMillis();

    public BeaconConsistencyCheckAction(ScheduledExecutorService scheduled, ClusterHealthCheckInstance instance, ExecutorService executors, BeaconManager beaconManager) {
        super(scheduled, instance, executors);
        this.beaconManager = beaconManager;
    }

    @Override
    protected void doTask() {
        try {
            tryDoTask();
        } catch (Exception e) {
            logger.error("[CheckBeaconConsistency]", e);
        }
    }

    private void tryDoTask() {
        ClusterInstanceInfo info = instance.getCheckInfo();
        String clusterId = info.getClusterId();
        int orgId = info.getOrgId();
        String lastModifyTime = info.getLastModifyTime();
        checkConsistency(clusterId, info.getClusterType(), orgId, lastModifyTime);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    @Override
    protected int getBaseCheckInterval() {
        return getActionInstance().getHealthCheckConfig().clusterCheckIntervalMilli();
    }

    private void checkConsistency(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime) {
        BeaconCheckStatus status;
        try {
            status = beaconManager.checkClusterHash(clusterId, clusterType, orgId, lastModifyTime);
        } catch (Throwable t) {
            // cluster not found in beacon
            status = BeaconCheckStatus.ERROR;
            logger.error("[checkConsistency][{}:{}:{}][fail] {}", clusterType, orgId, lastModifyTime, t.getMessage());
        }
        handleCheckResult(status, clusterId, clusterType, orgId, lastModifyTime);
    }

    private void handleCheckResult(BeaconCheckStatus status, String clusterId, ClusterType clusterType, int orgId, String lastModifyTime) {
        try {
            boolean sendMetric;
            long currentTime = System.currentTimeMillis();
            if(currentTime < lastSendTime) {
                lastSendTime = currentTime;
            }
            if(status == BeaconCheckStatus.CONSISTENCY) {
                sendMetric = currentTime - lastSendTime > getBaseCheckInterval();
            } else if (status == BeaconCheckStatus.INCONSISTENCY_IGNORE) {
                logger.info("[handleCheckResult][{}] inconsistency but ignore", clusterId);
                sendMetric = true;
            } else {
                beaconManager.registerCluster(clusterId, clusterType, orgId, lastModifyTime);
                sendMetric = true;
            }

            if (sendMetric) {
                lastSendTime = currentTime;
                sendMetricData(clusterId, status);
            }
        } catch (Throwable t) {
            logger.error("[handleCheckResult]{}:{}:{}", clusterType, orgId, t.getMessage());
        }
    }

    private MetricData getMetricData(String clusterId, BeaconCheckStatus status) {
        MetricData metricData = new MetricData("beacon.checker", null, clusterId, null);
        metricData.setTimestampMilli(System.currentTimeMillis());
        metricData.setValue(1);
        metricData.addTag("consistency", status.toString());
        return metricData;
    }

    private void sendMetricData(String trackCluster, BeaconCheckStatus status) {
        try {
            MetricData metricData = getMetricData(trackCluster, status);
            metricProxy.writeBinMultiDataPoint(metricData);
        } catch (MetricProxyException e) {
            logger.error("[sendMetricData]", e);
        }
    }
}
