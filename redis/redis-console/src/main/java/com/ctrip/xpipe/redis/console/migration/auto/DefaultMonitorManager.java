package com.ctrip.xpipe.redis.console.migration.auto;

import com.ctrip.xpipe.api.codec.GenericTypeReference;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.api.migration.auto.MonitorServiceFactory;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.checker.spring.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.config.model.BeaconClusterRoute;
import com.ctrip.xpipe.redis.console.config.model.BeaconOrgRoute;
import com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.ctrip.xpipe.redis.core.config.ConsoleCommonConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.checker.config.impl.DataCenterConfigBean.KEY_BEACON_ORG_ROUTE;
import static com.ctrip.xpipe.redis.checker.config.impl.DataCenterConfigBean.KEY_BEACON_SENTINEL_ORG_ROUTE;
import static com.ctrip.xpipe.redis.console.constant.XPipeConsoleConstant.DEFAULT_ORG_ID;

/**
 * @author lishanglin
 * date 2021/1/15
 */
@Service
public class DefaultMonitorManager implements MonitorManager {

    private final MetaCache metaCache;

    private final ConsoleConfig config;

    private final ConsoleCommonConfig consoleCommonConfig;

    private Map<Long, DefaultMonitorClusterManager> orgMonitorMap;
    private Map<Long, DefaultMonitorClusterManager> sentinelOrgMonitorMap;

    private static final int CONSISTENT_HASH_VIRTUAL_NODE_NUM = 100;
    private static final String currentDc = FoundationService.DEFAULT.getDataCenter();

    private static final Logger logger = LoggerFactory.getLogger(DefaultMonitorManager.class);

    @Autowired
    public DefaultMonitorManager(MetaCache metaCache, ConsoleConfig config, ConsoleCommonConfig consoleCommonConfig) {
        this.metaCache = metaCache;
        this.config = config;
        this.consoleCommonConfig = consoleCommonConfig;
        this.init();
    }

    private void init() {
        long checkInterval = this.config.getClusterHealthCheckInterval() / 1000; // 秒
        String serverMode = this.config.getServerMode();
        MetaCache cache = ConsoleServerModeCondition.SERVER_MODE.CHECKER.name().equals(serverMode.toUpperCase()) ?
            metaCache : null;
        this.orgMonitorMap = buildOrgMonitorMap(this.config.getBeaconOrgRoutes(), cache, checkInterval);
        this.sentinelOrgMonitorMap = buildOrgMonitorMap(this.config.getBeaconSentinelOrgRoutes(), cache, checkInterval);
        registerRouteListener(KEY_BEACON_ORG_ROUTE, BeaconRouteType.DR, cache, checkInterval);
        registerRouteListener(KEY_BEACON_SENTINEL_ORG_ROUTE, BeaconRouteType.SENTINEL, cache, checkInterval);
    }

    @Override
    public MonitorService get(long orgId, String clusterName) {
        return get(orgId, clusterName, BeaconRouteType.DR);
    }

    @Override
    public MonitorService get(long orgId, String clusterName, BeaconRouteType routeType) {
        Map<Long, DefaultMonitorClusterManager> monitorMap = getRouteMonitorMap(routeType);
        DefaultMonitorClusterManager monitorClusterManager = monitorMap.get(orgId);
        if (monitorClusterManager == null) {
            monitorClusterManager = monitorMap.get(XPipeConsoleConstant.DEFAULT_ORG_ID);
            if (monitorClusterManager == null) {
                return null;
            }
        }
        return monitorClusterManager.getService(clusterName);
    }

    @Override
    public Map<Long, List<MonitorService>> getAllServices() {
        return getAllServices(BeaconRouteType.DR);
    }

    @Override
    public Map<Long, List<MonitorService>> getAllServices(BeaconRouteType routeType) {
        Map<Long, DefaultMonitorClusterManager> monitorMap = getRouteMonitorMap(routeType);
        Map<Long, List<MonitorService>> map = new HashMap<>();
        monitorMap.forEach((orgId, monitor) -> map.put(orgId, monitor.getServices()));
        return map;
    }

    @Override
    public Map<BeaconSystem, Map<Long, Set<String>>> clustersByBeaconSystemOrg() {
        return clustersByBeaconSystemOrg(BeaconRouteType.DR);
    }

    @Override
    public Map<BeaconSystem, Map<Long, Set<String>>> clustersByBeaconSystemOrg(BeaconRouteType routeType) {
        Set<Long> orgIds = this.getAllServices(routeType).keySet();
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return null;

        Map<BeaconSystem, Map<Long, Set<String>>> clusterByBeaconSystemOrg = new HashMap<>();
        for (BeaconSystem beaconSystem : BeaconSystem.values()) {
            Map<Long, Set<String>> clustersByOrg = new HashMap<>(orgIds.size());
            orgIds.forEach(orgId -> clustersByOrg.put(orgId, new HashSet<>()));
            clusterByBeaconSystemOrg.put(beaconSystem, clustersByOrg);
        }

        Set<String> supportZones = consoleCommonConfig.getBeaconSupportZones();
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (routeType == BeaconRouteType.DR) {
                if (!supportZones.isEmpty() && supportZones.stream().noneMatch(zone -> zone.equalsIgnoreCase(dcMeta.getZone()))) {
                    logger.debug("[separateClustersByOrg][zoneUnsupported] {} not in {}", dcMeta.getId(), supportZones);
                    continue;
                }
            } else if (!dcMeta.getId().equalsIgnoreCase(currentDc)) {
                // Sentinel route is managed per-site, only track clusters in current dc.
                continue;
            }

            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
                if (!StringUtil.isEmpty(clusterMeta.getAzGroupType())) {
                    clusterType = ClusterType.lookup(clusterMeta.getAzGroupType());
                }
                if (routeType == BeaconRouteType.SENTINEL && !isSentinelManagedClusterType(clusterType)) {
                    continue;
                }

                BeaconSystem beaconSystem = resolveBeaconSystemByRouteType(clusterType, routeType);
                if (null == beaconSystem) {
                    continue;
                }
                if (routeType == BeaconRouteType.DR) {
                    if (clusterType.supportSingleActiveDC() && !dcMeta.getId().equalsIgnoreCase(clusterMeta.getActiveDc())) {
                        // only register cluster whose active dc is in supported zone
                        continue;
                    }
                } else {
                    // For sentinel mode, multi-active clusters register in each dc;
                    // single-active clusters register only on current active dc.
                    if (!clusterType.supportMultiActiveDC() && !dcMeta.getId().equalsIgnoreCase(clusterMeta.getActiveDc())) {
                        continue;
                    }
                }

                Map<Long, Set<String>> clustersByOrg = clusterByBeaconSystemOrg.get(beaconSystem);
                if (orgIds.contains((long) clusterMeta.getOrgId())) {
                    clustersByOrg.get((long) clusterMeta.getOrgId()).add(clusterMeta.getId());
                } else if (orgIds.contains(DEFAULT_ORG_ID)) {
                    clustersByOrg.get(DEFAULT_ORG_ID).add(clusterMeta.getId());
                }
            }
        }

        return clusterByBeaconSystemOrg;
    }

    private BeaconSystem resolveBeaconSystemByRouteType(ClusterType clusterType, BeaconRouteType routeType) {
        BeaconSystem beaconSystem = BeaconSystem.findByClusterType(clusterType);
        if (beaconSystem != null) {
            return beaconSystem;
        }
        return BeaconSystem.XPIPE_ONE_WAY;
    }

    private boolean isSentinelManagedClusterType(ClusterType clusterType) {
        return clusterType == ClusterType.ONE_WAY
                || clusterType == ClusterType.SINGLE_DC
                || clusterType == ClusterType.LOCAL_DC;
    }

    private void registerRouteListener(String configKey, BeaconRouteType routeType, MetaCache cache, long checkInterval) {
        this.config.register(Collections.singletonList(configKey), ((key, oldValue, newValue) -> {
            logger.info("key: {}, oldValue: {}, newValue{}", key, oldValue, newValue);
            List<BeaconOrgRoute> oldOrgRoutes = JsonCodec.INSTANCE
                    .decode(oldValue, new GenericTypeReference<List<BeaconOrgRoute>>() {});
            List<BeaconOrgRoute> newOrgRoutes = JsonCodec.INSTANCE
                    .decode(newValue, new GenericTypeReference<List<BeaconOrgRoute>>() {});
            if (CollectionUtils.isEmpty(newOrgRoutes)) {
                logger.error("Monitor Service cannot be null, key {}", key);
                return;
            }
            Map<Long, DefaultMonitorClusterManager> monitorMap = getRouteMonitorMap(routeType);
            if (CollectionUtils.isEmpty(oldOrgRoutes)) {
                logger.info("first write {} config, init", key);
                setRouteMonitorMap(routeType, buildOrgMonitorMap(newOrgRoutes, cache, checkInterval));
                return;
            }

            Map<Long, Map<String, BeaconClusterRoute>> oldOrgHostRouteMap = oldOrgRoutes.stream()
                    .collect(Collectors.toMap(BeaconOrgRoute::getOrgId, or -> or.getClusterRoutes()
                            .stream().collect(Collectors.toMap(BeaconClusterRoute::getName, Function.identity()))));
            Map<Long, Map<String, BeaconClusterRoute>> newOrgHostRouteMap = newOrgRoutes.stream()
                    .collect(Collectors.toMap(BeaconOrgRoute::getOrgId, or -> or.getClusterRoutes()
                            .stream().collect(Collectors.toMap(BeaconClusterRoute::getName, Function.identity()))));

            oldOrgHostRouteMap.forEach((orgId, oldHostRouteMap) -> {
                Map<String, BeaconClusterRoute> newHostRouteMap = newOrgHostRouteMap.get(orgId);
                if (MapUtils.isEmpty(newHostRouteMap)) {
                    logger.warn("delete org id - {}, this operation is not support now, key {}", orgId, key);
                    return;
                }
                DefaultMonitorClusterManager monitorClusterManager = monitorMap.get(orgId);
                if (monitorClusterManager == null) {
                    logger.warn("org id {} not initialized for key {}, skip update", orgId, key);
                    return;
                }
                Map<String, MonitorService> nameServiceMap = monitorClusterManager.getServices().stream()
                        .collect(Collectors.toMap(MonitorService::getName, Function.identity()));

                oldHostRouteMap.forEach((name, oldClusterRoute) -> {
                    MonitorService service = nameServiceMap.get(name);
                    BeaconClusterRoute newClusterRoute = newHostRouteMap.get(name);
                    if (newClusterRoute == null) {
                        if (service != null) {
                            monitorClusterManager.removeService(service);
                        }
                    } else if (!newClusterRoute.equals(oldClusterRoute)) {
                        if (service == null) {
                            logger.warn("service {} not found for key {}, skip route update", name, key);
                            return;
                        }
                        monitorClusterManager.updateServiceWeight(service, newClusterRoute.getWeight());
                        service.updateHost(newClusterRoute.getHost());
                    }
                });
                newHostRouteMap.forEach((name, newClusterRoute) -> {
                    if (!oldHostRouteMap.containsKey(name)) {
                        monitorClusterManager.addService(this.constructMonitorService(newClusterRoute));
                    }
                });
            });

            newOrgHostRouteMap.forEach((orgId, newHostRouteMap) -> {
                if (!oldOrgHostRouteMap.containsKey(orgId)) {
                    logger.warn("add org id - {}, this operation is not support now, key {}", orgId, key);
                }
            });
        }));
    }

    private Map<Long, DefaultMonitorClusterManager> buildOrgMonitorMap(List<BeaconOrgRoute> orgRoutes,
                                                                        MetaCache cache,
                                                                        long checkInterval) {
        if (CollectionUtils.isEmpty(orgRoutes)) {
            return new HashMap<>();
        }
        return orgRoutes.stream().collect(Collectors.toMap(BeaconOrgRoute::getOrgId, orgRoute -> {
            List<MonitorService> monitorServices = orgRoute.getClusterRoutes()
                    .stream()
                    .map(this::constructMonitorService)
                    .collect(Collectors.toList());
            return new DefaultMonitorClusterManager(cache, CONSISTENT_HASH_VIRTUAL_NODE_NUM,
                    monitorServices, checkInterval);
        }));
    }

    private Map<Long, DefaultMonitorClusterManager> getRouteMonitorMap(BeaconRouteType routeType) {
        return routeType == BeaconRouteType.SENTINEL ? sentinelOrgMonitorMap : orgMonitorMap;
    }

    private void setRouteMonitorMap(BeaconRouteType routeType, Map<Long, DefaultMonitorClusterManager> monitorMap) {
        if (routeType == BeaconRouteType.SENTINEL) {
            this.sentinelOrgMonitorMap = monitorMap;
        } else {
            this.orgMonitorMap = monitorMap;
        }
    }

    private MonitorService constructMonitorService(BeaconClusterRoute route) {
        return MonitorServiceFactory.DEFAULT.build(route.getName(), route.getHost(), route.getWeight());
    }

}
