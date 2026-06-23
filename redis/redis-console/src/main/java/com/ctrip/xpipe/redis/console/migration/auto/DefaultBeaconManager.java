package com.ctrip.xpipe.redis.console.migration.auto;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.data.MonitorClusterMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorGroupMeta;
import com.ctrip.xpipe.api.migration.auto.data.MonitorShardMeta;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.controller.result.RetMessage;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconCheckStatus;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.service.meta.BeaconMetaService;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
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

    private static final String CURRENT_DC = FoundationService.DEFAULT.getDataCenter();

    private MonitorManager monitorManager;

    private BeaconMetaService beaconMetaService;

    private CheckerConfig checkerConfig;

    private MetaCache metaCache;

    @Autowired(required = false)
    private ConsoleServiceManager consoleServiceManager;

    private static final Logger logger = LoggerFactory.getLogger(DefaultBeaconManager.class);

    @Autowired
    public DefaultBeaconManager(MonitorManager monitorManager, BeaconMetaService beaconMetaService,
                                CheckerConfig checkerConfig, MetaCache metaCache) {
        this.monitorManager = monitorManager;
        this.beaconMetaService = beaconMetaService;
        this.checkerConfig = checkerConfig;
        this.metaCache = metaCache;
    }

    @Override
    public void registerCluster(String clusterId, String dc, ClusterType clusterType, int orgId, String lastModifyTime,
                                BeaconRouteType routeType, Map<String, HostPort> shardMasters) {
        if (shouldForwardSentinel(routeType, dc)) {
            forwardSentinelRegister(dc, clusterId, shardMasters);
            return;
        }
        registerCluster(clusterId, dc, clusterType, orgId, lastModifyTime, routeType, false, shardMasters);
    }

    @Override
    public void updateCluster(String clusterId, String dc, ClusterType clusterType, int orgId, String lastModifyTime,
                              BeaconRouteType routeType) {
        if (shouldForwardSentinel(routeType, dc)) {
            forwardSentinelRegister(dc, clusterId, Collections.emptyMap());
            return;
        }
        registerCluster(clusterId, dc, clusterType, orgId, lastModifyTime, routeType, true, Collections.emptyMap());
    }

    @Override
    public BeaconCheckStatus checkClusterHash(String clusterId, String dc, ClusterType clusterType, int orgId,
                                              String lastModifyTime, BeaconRouteType routeType) {
        MonitorService service = getMonitorService(clusterId, orgId, dc, routeType);
        if (null == service) {
            return BeaconCheckStatus.SERVICE_NOT_FOUND;
        }
        BeaconSystem system = resolveBeaconSystem(clusterType, routeType);
        if (null == system) {
            logger.info("[checkClusterHash][{}][{}] no beacon system found", clusterId, routeType);
            return BeaconCheckStatus.SYSTEM_NOT_FOUND;
        }
        int hash;
        try {
            hash = service.getBeaconClusterHash(system.getSystemName(), clusterId);
        } catch (XpipeRuntimeException e) {
            if (e.getMessage().contains("cluster not existed")) {
                return BeaconCheckStatus.CLUSTER_NOT_FOUND;
            }
            throw e;
        }
        MonitorClusterMeta monitorClusterMeta = buildMonitorClusterMeta(clusterId, dc, routeType, Collections.emptyMap());
        int localHash = monitorClusterMeta.generateHashCodeForBeaconCheck();
        if (localHash == hash) {
            return BeaconCheckStatus.CONSISTENCY;
        }

        logger.info("[checkClusterHash][{}][{}][{}] hash inconsisent lo:{} be:{}", clusterId, dc, routeType, localHash, hash);
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
                logger.debug("[checkClusterHash][{}][{}][{}][checkModifyTimeFail] {}", clusterId, dc, routeType, th.getMessage());
            }
        }
        return status;
    }

    @Override
    public int computeClusterMetaHash(String clusterId, String dc, ClusterType clusterType, BeaconRouteType routeType) {
        return buildMonitorClusterMeta(clusterId, dc, routeType, Collections.emptyMap()).generateHashCodeForBeaconCheck();
    }

    @Override
    public void unregisterCluster(String clusterId, String dc, ClusterType clusterType, int orgId, BeaconRouteType routeType) {
        if (shouldForwardSentinel(routeType, dc)) {
            forwardSentinelUnregister(dc, clusterId);
            return;
        }
        MonitorService service = getMonitorService(clusterId, orgId, dc, routeType);
        if (service == null) {
            return;
        }
        BeaconSystem system = resolveBeaconSystem(clusterType, routeType);
        if (system == null) {
            logger.info("[unregisterCluster][{}][{}] no beacon system found", clusterId, dc);
            return;
        }
        try {
            service.unregisterCluster(system.getSystemName(), clusterId);
        } catch (Throwable th) {
            logger.info("[unregisterCluster][{}][{}] unregister fail", clusterId, dc, th);
        }
    }

    private void registerCluster(String clusterId, String dc, ClusterType clusterType, int orgId, String lastModifyTime,
                                 BeaconRouteType routeType, boolean update, Map<String, HostPort> shardMasters) {
        MonitorService service = getMonitorService(clusterId, orgId, dc, routeType);
        if (null == service) {
            return;
        }

        try {
            logger.debug("[registerCluster][{}][{}] register to {}", clusterId, dc, service.getHost());
            BeaconSystem system = resolveBeaconSystem(clusterType, routeType);
            if (null == system) {
                logger.info("[registerCluster][{}][{}] no beacon system found", clusterId, dc);
                return;
            }
            MonitorClusterMeta monitorClusterMeta = buildMonitorClusterMeta(clusterId, dc, routeType, shardMasters);
            if (update) {
                service.updateCluster(system.getSystemName(), clusterId, monitorClusterMeta.getNodeGroups(), monitorClusterMeta.getShards(),
                        Collections.singletonMap(EXTRA_LAST_MODIFY_TIME, lastModifyTime));
            } else {
                service.registerCluster(system.getSystemName(), clusterId, monitorClusterMeta.getNodeGroups(), monitorClusterMeta.getShards(),
                        Collections.singletonMap(EXTRA_LAST_MODIFY_TIME, lastModifyTime));
            }
        } catch (Throwable th) {
            logger.info("[registerCluster][{}][{}] register meta fail", clusterId, dc, th);
        }
    }

    private boolean shouldForwardSentinel(BeaconRouteType routeType, String dc) {
        return routeType == BeaconRouteType.SENTINEL && !isCurrentDc(dc);
    }

    private boolean isCurrentDc(String dc) {
        return !StringUtil.isEmpty(dc) && dc.equalsIgnoreCase(CURRENT_DC);
    }

    private void forwardSentinelRegister(String dc, String clusterId, Map<String, HostPort> shardMasters) {
        if (consoleServiceManager == null) {
            logger.info("[forwardSentinelRegister][{}][{}] ConsoleServiceManager unavailable", clusterId, dc);
            return;
        }
        try {
            RetMessage result = consoleServiceManager.postMigrateSentinelBeacon(dc, clusterId, shardMasters);
            if (result == null || result.getState() != RetMessage.SUCCESS_STATE) {
                logger.info("[forwardSentinelRegister][{}][{}] fail: {}", clusterId, dc,
                        result == null ? "null response" : result.getMessage());
            }
        } catch (Throwable th) {
            logger.info("[forwardSentinelRegister][{}][{}] fail", clusterId, dc, th);
        }
    }

    private void forwardSentinelUnregister(String dc, String clusterId) {
        if (consoleServiceManager == null) {
            logger.info("[forwardSentinelUnregister][{}][{}] ConsoleServiceManager unavailable", clusterId, dc);
            return;
        }
        try {
            RetMessage result = consoleServiceManager.preMigrateSentinelBeacon(dc, clusterId);
            if (result == null || result.getState() != RetMessage.SUCCESS_STATE) {
                logger.info("[forwardSentinelUnregister][{}][{}] fail: {}", clusterId, dc,
                        result == null ? "null response" : result.getMessage());
            }
        } catch (Throwable th) {
            logger.info("[forwardSentinelUnregister][{}][{}] fail", clusterId, dc, th);
        }
    }

    private MonitorService getMonitorService(String clusterId, int orgId, String dc, BeaconRouteType routeType) {
        String zone = resolveZone(dc);
        MonitorService service = monitorManager.get(orgId, clusterId, zone, routeType);
        if (null == service) {
            logger.debug("[BeaconManager][{}][{}] no beacon service for org {}, skip", clusterId, dc, orgId);
        }
        return service;
    }

    private String resolveZone(String dc) {
        if (StringUtil.isEmpty(dc)) {
            return null;
        }
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (xpipeMeta == null || xpipeMeta.getDcs() == null) {
            return null;
        }
        DcMeta dcMeta = xpipeMeta.getDcs().get(dc);
        return dcMeta == null ? null : dcMeta.getZone();
    }

    private MonitorClusterMeta buildMonitorClusterMeta(String clusterId, String dc, BeaconRouteType routeType,
                                                       Map<String, HostPort> shardMasters) {
        if (routeType == BeaconRouteType.SENTINEL) {
            Set<MonitorShardMeta> shards = beaconMetaService.buildSentinelBeaconShards(clusterId, dc, shardMasters);
            return new MonitorClusterMeta(null, shards, Collections.emptyMap());
        }
        Set<MonitorGroupMeta> groups = beaconMetaService.buildDrBeaconGroups(clusterId, dc);
        return new MonitorClusterMeta(groups, Collections.emptyMap());
    }

    private BeaconSystem resolveBeaconSystem(ClusterType clusterType, BeaconRouteType routeType) {
        BeaconSystem beaconSystem = BeaconSystem.findByClusterType(clusterType);
        if (beaconSystem != null) {
            return beaconSystem;
        }
        return BeaconSystem.XPIPE_ONE_WAY;
    }

}
