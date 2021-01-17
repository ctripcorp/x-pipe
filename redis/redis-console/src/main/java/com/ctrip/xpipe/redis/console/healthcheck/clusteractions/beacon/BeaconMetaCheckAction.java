package com.ctrip.xpipe.redis.console.healthcheck.clusteractions.beacon;

import com.ctrip.xpipe.redis.console.beacon.BeaconService;
import com.ctrip.xpipe.redis.console.beacon.BeaconServiceManager;
import com.ctrip.xpipe.redis.console.healthcheck.ClusterHealthCheckInstance;

import com.ctrip.xpipe.redis.console.healthcheck.ClusterInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.leader.AbstractLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public class BeaconMetaCheckAction extends AbstractLeaderAwareHealthCheckAction<ClusterHealthCheckInstance> {

    private BeaconMetaService beaconMetaService;

    private BeaconServiceManager beaconServiceManager;

    public BeaconMetaCheckAction(ScheduledExecutorService scheduled, ClusterHealthCheckInstance instance, ExecutorService executors,
                                 BeaconMetaService beaconMetaService, BeaconServiceManager beaconServiceManager) {
        super(scheduled, instance, executors);
        this.beaconMetaService = beaconMetaService;
        this.beaconServiceManager = beaconServiceManager;
    }

    @Override
    protected void doTask() {
        ClusterInstanceInfo info = instance.getCheckInfo();
        String clusterId = info.getClusterId();
        int orgId = info.getOrgId();

        BeaconService service = beaconServiceManager.getOrCreate(orgId);
        if (null == service) {
            logger.debug("[doTask][{}] no beacon service for org {}, skip", clusterId, orgId);
            return;
        }

        try {
            service.registerCluster(clusterId, beaconMetaService.buildBeaconGroups(clusterId));
        } catch (Throwable th) {
            logger.info("[doTask][{}] register meta fail", clusterId, th);
        }
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
