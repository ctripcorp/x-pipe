package com.ctrip.xpipe.redis.console.migration.auto;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.data.MonitorClusterMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorShardMeta;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconCheckStatus;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/3/17
 */
@Component
public class DefaultBeaconManager implements BeaconManager {

    private MonitorManager monitorManager;

    private BeaconMetaService beaconMetaService;

    private CheckerConfig checkerConfig;

    private MetaCache metaCache;

    private static final Logger logger = LoggerFactory.getLogger(DefaultBeaconManager.class);
    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    @Autowired
    public DefaultBeaconManager(MonitorManager monitorManager, BeaconMetaService beaconMetaService,
                                CheckerConfig checkerConfig, MetaCache metaCache) {
        this.monitorManager = monitorManager;
        this.beaconMetaService = beaconMetaService;
        this.checkerConfig = checkerConfig;
        this.metaCache = metaCache;
    }

    @Override
    public void registerCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime, BeaconRouteType routeType) {
        registerCluster(clusterId, clusterType, orgId, lastModifyTime, routeType, Collections.emptyMap());
    }

    @Override
    public void registerCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime,
                                BeaconRouteType routeType, Map<String, HostPort> shardMasters) {
        registerCluster(clusterId, clusterType, orgId, lastModifyTime, routeType, false, shardMasters);
    }

    private void registerCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime,
                                 BeaconRouteType routeType, boolean update, Map<String, HostPort> shardMasters) {
        MonitorService service = getMonitorService(clusterId, orgId, routeType);
        if (null == service) {
            return;
        }

        try {
            logger.debug("[registerCluster][{}] register to {}", clusterId, service.getHost());
            BeaconSystem system = resolveBeaconSystem(clusterType, routeType);
            if (null == system) {
                logger.info("[registerCluster][{}] no beacon system found", clusterId);
                return;
            }
            if(update) {
                MonitorClusterMeta monitorClusterMeta = buildMonitorClusterMeta(clusterId, routeType, shardMasters);
                service.updateCluster(system.getSystemName(), clusterId, monitorClusterMeta.getNodeGroups(), monitorClusterMeta.getShards(),
                        Collections.singletonMap(EXTRA_LAST_MODIFY_TIME, lastModifyTime));
            } else {
                MonitorClusterMeta monitorClusterMeta = buildMonitorClusterMeta(clusterId, routeType, shardMasters);
                service.registerCluster(system.getSystemName(), clusterId, monitorClusterMeta.getNodeGroups(), monitorClusterMeta.getShards(),
                        Collections.singletonMap(EXTRA_LAST_MODIFY_TIME, lastModifyTime));
            }
        } catch (Throwable th) {
            logger.info("[registerCluster][{}] register meta fail", clusterId, th);
        }
    }

    @Override
    public void updateCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime, BeaconRouteType routeType) {
        registerCluster(clusterId, clusterType, orgId, lastModifyTime, routeType, false, Collections.emptyMap());
    }

    @Override
    public BeaconCheckStatus checkClusterHash(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime, BeaconRouteType routeType) {
        MonitorService service = getMonitorService(clusterId, orgId, routeType);
        if (null == service) {
            return BeaconCheckStatus.SERVICE_NOT_FOUND;
        }
        BeaconSystem system = resolveBeaconSystem(clusterType, routeType);
        if (null == system) {
            logger.info("[checkClusterHash][{}][{}] no beacon system found", clusterId, routeType);
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
        int localHash = computeLocalClusterMetaHash(clusterId, routeType, lastModifyTime);
        if (localHash == hash) {
            return BeaconCheckStatus.CONSISTENCY;
        }

        logger.info("[checkClusterHash][{}][{}] hash inconsisent lo:{} be:{}", clusterId, routeType, localHash, hash);
        BeaconCheckStatus status = BeaconCheckStatus.INCONSISTENCY;
        if (checkerConfig.checkBeaconLastModifyTime()) {
            try {
                Map<String, String> clusterExtra = service.getBeaconClusterExtra(system.getSystemName(), clusterId);
                if (clusterExtra.containsKey(EXTRA_LAST_MODIFY_TIME)) {
                    String beaconClusterLastModify = clusterExtra.get(EXTRA_LAST_MODIFY_TIME);
                    if (!StringUtil.isEmpty(lastModifyTime) && !StringUtil.isEmpty(beaconClusterLastModify)
                            && lastModifyTime.compareTo(beaconClusterLastModify) < 0) {
                        status = BeaconCheckStatus.INCONSISTENCY_IGNORE;
                    }
                }
            } catch (Throwable th) {
                logger.debug("[checkClusterHash][{}][{}][checkModifyTimeFail] {}", clusterId, routeType, th.getMessage());
            }
        }
        return status;
    }

    @Override
    public int computeClusterMetaHash(String clusterId, ClusterType clusterType, BeaconRouteType routeType) {
        return computeLocalClusterMetaHash(clusterId, routeType, resolveClusterLastModifyTime(clusterId));
    }

    private int computeLocalClusterMetaHash(String clusterId, BeaconRouteType routeType, String lastModifyTime) {
        MonitorClusterMeta monitorClusterMeta = buildMonitorClusterMeta(clusterId, routeType,
                Collections.emptyMap(), buildHashExtra(lastModifyTime));
        return monitorClusterMeta.generateHashCodeForBeaconCheck();
    }

    private String resolveClusterLastModifyTime(String clusterId) {
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (xpipeMeta == null || xpipeMeta.getDcs() == null) {
            return null;
        }
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (dcMeta.getClusters() == null) {
                continue;
            }
            ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterId);
            if (clusterMeta != null) {
                return clusterMeta.getLastModifiedTime();
            }
        }
        return null;
    }

    @Override
    public void unregisterCluster(String clusterId, ClusterType clusterType, int orgId, BeaconRouteType routeType) {
        MonitorService service = getMonitorService(clusterId, orgId, routeType);
        if (service == null) {
            return;
        }
        BeaconSystem system = resolveBeaconSystem(clusterType, routeType);
        if (system == null) {
            logger.info("[unregisterCluster][{}] no beacon system found", clusterId);
            return;
        }
        try {
            service.unregisterCluster(system.getSystemName(), clusterId);
        } catch (Throwable th) {
            logger.info("[unregisterCluster][{}] unregister fail", clusterId, th);
        }
    }

    private MonitorService getMonitorService(String clusterId, int orgId, BeaconRouteType routeType) {
        MonitorService service = monitorManager.get(orgId, clusterId, routeType);
        if (null == service) {
            logger.debug("[BeaconManager][{}] no beacon service for org {}, skip", clusterId, orgId);
        }
        return service;
    }

    private MonitorClusterMeta buildMonitorClusterMeta(String clusterId, BeaconRouteType routeType,
                                                       Map<String, HostPort> shardMasters) {
        return buildMonitorClusterMeta(clusterId, routeType, shardMasters, Collections.emptyMap());
    }

    /**
     * Extra fields used in beacon hash check. Currently only {@link #EXTRA_LAST_MODIFY_TIME} is expected;
     * beacon hash uses the same key. If new extra keys are added, update both beacon and x-pipe hash logic.
     */
    private Map<String, String> buildHashExtra(String lastModifyTime) {
        if (StringUtil.isEmpty(lastModifyTime)) {
            return Collections.emptyMap();
        }
        return Collections.singletonMap(EXTRA_LAST_MODIFY_TIME, lastModifyTime);
    }

    private MonitorClusterMeta buildMonitorClusterMeta(String clusterId, BeaconRouteType routeType,
                                                       Map<String, HostPort> shardMasters, Map<String, String> extra) {
        Map<String, String> extraForMeta = extra == null ? Collections.emptyMap() : extra;
        if (routeType == BeaconRouteType.SENTINEL) {
            Set<MonitorShardMeta> shards = beaconMetaService.buildBeaconShards(clusterId, currentDc, shardMasters);
            return new MonitorClusterMeta(null, shards, extraForMeta);
        }
        Set<MonitorGroupMeta> groups = beaconMetaService.buildBeaconGroups(clusterId);
        return new MonitorClusterMeta(groups, extraForMeta);
    }

    private BeaconSystem resolveBeaconSystem(ClusterType clusterType, BeaconRouteType routeType) {
        BeaconSystem beaconSystem = BeaconSystem.findByClusterType(clusterType);
        if (beaconSystem != null) {
            return beaconSystem;
        }
        return BeaconSystem.XPIPE_ONE_WAY;
    }

}
