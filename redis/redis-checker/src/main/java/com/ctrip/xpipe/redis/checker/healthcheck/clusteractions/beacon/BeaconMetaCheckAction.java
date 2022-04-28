package com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.healthcheck.ClusterHealthCheckInstance;

import com.ctrip.xpipe.redis.checker.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractLeaderAwareHealthCheckAction;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public class BeaconMetaCheckAction extends AbstractLeaderAwareHealthCheckAction<ClusterHealthCheckInstance> {

    private BeaconManager beaconManager;

    public BeaconMetaCheckAction(ScheduledExecutorService scheduled, ClusterHealthCheckInstance instance, ExecutorService executors,
                                 BeaconManager beaconManager) {
        super(scheduled, instance, executors);
        this.beaconManager = beaconManager;
    }

    @Override
    protected void doTask() {
        ClusterInstanceInfo info = instance.getCheckInfo();
        String clusterId = info.getClusterId();
        int orgId = info.getOrgId();

        beaconManager.registerCluster(clusterId, info.getClusterType(), orgId);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    @Override
    protected int getBaseCheckInterval() {
        return getActionInstance().getHealthCheckConfig().clusterCheckIntervalMilli();
    }

}
