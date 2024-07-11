package com.ctrip.xpipe.redis.console.migration.auto;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
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
        MonitorService service = monitorManager.get(orgId, clusterId);
        if (null == service) {
            logger.debug("[registerCluster][{}] no beacon service for org {}, skip", clusterId, orgId);
            return;
        }

        try {
            logger.debug("[registerCluster][{}] register to {}", clusterId, service.getHost());
            BeaconSystem system = BeaconSystem.findByClusterType(clusterType);
            service.registerCluster(system.getSystemName(), clusterId, beaconMetaService.buildBeaconGroups(clusterId));
        } catch (Throwable th) {
            logger.info("[registerCluster][{}] register meta fail", clusterId, th);
        }
    }

}
