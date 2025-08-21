package com.ctrip.xpipe.redis.console.migration.auto;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.data.MonitorClusterMeta;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconCheckStatus;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2021/3/17
 */
@Component
public class DefaultBeaconManager implements BeaconManager {

    private MonitorManager monitorManager;

    private BeaconMetaService beaconMetaService;

    private static final Logger logger = LoggerFactory.getLogger(DefaultBeaconManager.class);

    @Autowired
    public DefaultBeaconManager(MonitorManager monitorManager, BeaconMetaService beaconMetaService) {
        this.monitorManager = monitorManager;
        this.beaconMetaService = beaconMetaService;
    }

    @Override
    public void registerCluster(String clusterId, ClusterType clusterType, int orgId) {
        registerCluster(clusterId, clusterType, orgId, false);
    }

    private void registerCluster(String clusterId, ClusterType clusterType, int orgId, boolean update) {
        MonitorService service = getMonitorService(clusterId, orgId);
        if (null == service) {
            return;
        }

        try {
            logger.debug("[registerCluster][{}] register to {}", clusterId, service.getHost());
            BeaconSystem system = BeaconSystem.findByClusterType(clusterType);
            if (null == system) {
                logger.info("[registerCluster][{}] no beacon system found", clusterId);
                return;
            }
            if(update) {
                service.updateCluster(system.getSystemName(), clusterId, beaconMetaService.buildBeaconGroups(clusterId));
            } else {
                service.registerCluster(system.getSystemName(), clusterId, beaconMetaService.buildBeaconGroups(clusterId));
            }
        } catch (Throwable th) {
            logger.info("[registerCluster][{}] register meta fail", clusterId, th);
        }
    }

    @Override
    public void updateCluster(String clusterId, ClusterType clusterType, int orgId) {
        registerCluster(clusterId, clusterType, orgId, false);
    }

    @Override
    public BeaconCheckStatus checkClusterHash(String clusterId, ClusterType clusterType, int orgId) {
        MonitorService service = getMonitorService(clusterId, orgId);
        if (null == service) {
            return BeaconCheckStatus.SERVICE_NOT_FOUND;
        }
        BeaconSystem system = BeaconSystem.findByClusterType(clusterType);
        if (null == system) {
            logger.info("[checkClusterHash][{}] no beacon system found", clusterId);
            return BeaconCheckStatus.SYSTEM_NOT_FOUND;
        }
        int hash = 0;
        try{
            hash = service.getBeaconClusterHash(system.getSystemName(), clusterId);
        } catch (XpipeRuntimeException e) {
            if(e.getMessage().contains("cluster not existed")) {
                return BeaconCheckStatus.CLUSTER_NOT_FOUND;
            }
            throw e;
        }
        MonitorClusterMeta monitorClusterMeta = new MonitorClusterMeta(beaconMetaService.buildBeaconGroups(clusterId));
        int localHash = monitorClusterMeta.generateHashCodeForBeaconCheck();
        if(localHash != hash) {
            return BeaconCheckStatus.INCONSISTENCY;
        } else {
            return BeaconCheckStatus.CONSISTENCY;
        }
    }

    private MonitorService getMonitorService(String clusterId, int orgId) {
        MonitorService service = monitorManager.get(orgId, clusterId);
        if (null == service) {
            logger.debug("[BeaconManager][{}] no beacon service for org {}, skip", clusterId, orgId);
        }
        return service;
    }

}
