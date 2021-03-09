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

//    private BeaconMetaService beaconMetaService;
//
//    private MonitorServiceManager monitorServiceManager;

    private BeaconManager beaconManager;

    public BeaconMetaCheckAction(ScheduledExecutorService scheduled, ClusterHealthCheckInstance instance, ExecutorService executors,
                                 BeaconManager beaconManager) {
        super(scheduled, instance, executors);
//        this.beaconMetaService = beaconMetaService;
//        this.monitorServiceManager = monitorServiceManager;
        this.beaconManager = beaconManager;
    }

    @Override
    protected void doTask() {
        ClusterInstanceInfo info = instance.getCheckInfo();
        String clusterId = info.getClusterId();
        int orgId = info.getOrgId();

        beaconManager.registerCluster(clusterId, orgId);
//        MonitorService service = monitorServiceManager.getOrCreate(orgId);
//        if (null == service) {
//            logger.debug("[doTask][{}] no beacon service for org {}, skip", clusterId, orgId);
//            return;
//        }
//
//        try {
//            logger.debug("[doTask][{}] register to {}", clusterId, service.getHost());
//            service.registerCluster(clusterId, beaconMetaService.buildBeaconGroups(clusterId));
//        } catch (Throwable th) {
//            logger.info("[doTask][{}] register meta fail", clusterId, th);
//        }
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
