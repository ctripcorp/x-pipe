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
        checkConsistency(clusterId, info.getClusterType(), orgId);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    @Override
    protected int getBaseCheckInterval() {
        return getActionInstance().getHealthCheckConfig().clusterCheckIntervalMilli();
    }

    private void checkConsistency(String clusterId, ClusterType clusterType, int orgId) {
        BeaconCheckStatus status;
        try {
            status = beaconManager.checkClusterHash(clusterId, clusterType, orgId);
        } catch (Throwable t) {
            // cluster not found in beacon
            status = BeaconCheckStatus.ERROR;
            logger.error("[checkConsistency]{}:{}:{}", clusterType, orgId, t.getMessage());
        }
        handleCheckResult(status, clusterId, clusterType, orgId);
    }

    private void handleCheckResult(BeaconCheckStatus status, String clusterId, ClusterType clusterType, int orgId) {
        try {
            if(status == BeaconCheckStatus.CONSISTENCY) {
                long currentTime = System.currentTimeMillis();
                if(currentTime < lastSendTime) {
                    lastSendTime = currentTime;
                }
                if(currentTime - lastSendTime <= getBaseCheckInterval()) {
                    // avoid send many point to hickwall
                    return;
                }
                lastSendTime = currentTime;
            } else {
                beaconManager.registerCluster(clusterId, clusterType, orgId);
            }
            sendMetricData(clusterId, status);
        } catch (Throwable t) {
            logger.error("[checkPost]{}:{}:{}", clusterType, orgId, t.getMessage());
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
