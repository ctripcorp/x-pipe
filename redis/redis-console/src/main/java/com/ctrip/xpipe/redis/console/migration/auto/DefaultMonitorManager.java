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
import com.ctrip.xpipe.redis.console.controller.api.vo.ClusterShardInfo;
import com.ctrip.xpipe.redis.console.controller.api.vo.DRBeaconUsageItem;
import com.ctrip.xpipe.redis.console.controller.api.vo.DRClusterBeaconRouteItem;
import com.ctrip.xpipe.redis.console.controller.api.vo.RegionBeaconUsage;
import com.ctrip.xpipe.redis.console.controller.api.vo.SentinelBeaconUsageItem;
import com.ctrip.xpipe.redis.console.controller.api.vo.SentinelClusterBeaconRouteItem;
import com.ctrip.xpipe.redis.core.beacon.BeaconSentinelMetaUtil;
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
    private static final String UNKNOWN_REGION = "UNKNOWN";

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
    public MonitorService get(long orgId, String clusterName, String zone, BeaconRouteType routeType) {
        Map<Long, DefaultMonitorClusterManager> monitorMap = getRouteMonitorMap(routeType);
        DefaultMonitorClusterManager monitorClusterManager = monitorMap.get(orgId);
        if (monitorClusterManager == null) {
            monitorClusterManager = monitorMap.get(XPipeConsoleConstant.DEFAULT_ORG_ID);
            if (monitorClusterManager == null) {
                return null;
            }
        }
        // Current version: global org route, zone reserved for future multi-Region Beacon routing.
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
    public Map<BeaconSystem, Map<Long, Map<MonitorService, Set<String>>>> clustersByBeaconSystemOrg() {
        return clustersByBeaconSystemOrg(BeaconRouteType.DR);
    }

    @Override
    public Map<BeaconSystem, Map<Long, Map<MonitorService, Set<String>>>> clustersByBeaconSystemOrg(BeaconRouteType routeType) {
        Set<Long> orgIds = this.getAllServices(routeType).keySet();
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (null == xpipeMeta) return null;

        Map<BeaconSystem, Map<Long, Map<MonitorService, Set<String>>>> clusterByBeaconSystemOrg = new HashMap<>();
        for (BeaconSystem beaconSystem : BeaconSystem.values()) {
            Map<Long, Map<MonitorService, Set<String>>> clustersByOrg = new HashMap<>(orgIds.size());
            orgIds.forEach(orgId -> clustersByOrg.put(orgId, new HashMap<>()));
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
                continue;
            }

            for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
                if (routeType == BeaconRouteType.DR
                        && !ClusterType.supportClusterMigration(clusterMeta.getType(), clusterMeta.getAzGroupType())) {
                    continue;
                }

                ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
                if (!StringUtil.isEmpty(clusterMeta.getAzGroupType())) {
                    clusterType = ClusterType.lookup(clusterMeta.getAzGroupType());
                }
                if (routeType == BeaconRouteType.SENTINEL && !BeaconSentinelMetaUtil.isSentinelManagedClusterType(clusterType)) {
                    continue;
                }

                BeaconSystem beaconSystem = resolveBeaconSystemByRouteType(clusterType, routeType);
                if (null == beaconSystem) {
                    continue;
                }
                if (routeType == BeaconRouteType.DR) {
                    if (clusterType.supportSingleActiveDC() && !dcMeta.getId().equalsIgnoreCase(clusterMeta.getActiveDc())) {
                        continue;
                    }
                } else {
                    if (!clusterType.supportMultiActiveDC() && !dcMeta.getId().equalsIgnoreCase(clusterMeta.getActiveDc())) {
                        continue;
                    }
                    if (!config.supportSentinelBeacon(clusterMeta.getOrgId(), clusterMeta.getId())) {
                        continue;
                    }
                }

                long orgId = orgIds.contains((long) clusterMeta.getOrgId())
                        ? clusterMeta.getOrgId()
                        : (orgIds.contains(DEFAULT_ORG_ID) ? DEFAULT_ORG_ID : -1L);
                if (orgId < 0) {
                    continue;
                }

                MonitorService monitorService = get(orgId, clusterMeta.getId(), dcMeta.getZone(), routeType);
                if (monitorService == null) {
                    continue;
                }

                Map<Long, Map<MonitorService, Set<String>>> clustersByOrg = clusterByBeaconSystemOrg.get(beaconSystem);
                clustersByOrg.computeIfAbsent(orgId, ignored -> new HashMap<>())
                        .computeIfAbsent(monitorService, ignored -> new HashSet<>())
                        .add(clusterMeta.getId());
            }
        }

        fillMonitorServicePlaceholders(clusterByBeaconSystemOrg, orgIds, routeType);
        return clusterByBeaconSystemOrg;
    }

    @Override
    public Set<String> getBeaconDcs(String clusterName, BeaconRouteType routeType) {
        Set<String> result = new LinkedHashSet<>();
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (xpipeMeta == null) {
            logger.warn("[getBeaconDcs] xpipeMeta null");
            return result;
        }

        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (BeaconSentinelMetaUtil.isBeaconCandidate(dcMeta, clusterName, routeType == BeaconRouteType.DR,
                    consoleCommonConfig.getBeaconSupportZones())) {
                result.add(dcMeta.getId().toUpperCase());
            }
        }
        logger.info("[getBeaconDcs] cluster={}, routeType={}, result={}", clusterName, routeType, result);
        return result;
    }

    @Override
    public List<SentinelClusterBeaconRouteItem> getSentinelClusterRoutes(String clusterName) {
        List<SentinelClusterBeaconRouteItem> result = new ArrayList<>();
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (xpipeMeta == null) {
            logger.warn("[getSentinelClusterRoutes] xpipeMeta is null, cluster={}", clusterName);
            return result;
        }

        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            // Each console only emits its own DC's data; other DCs come via cross-console forwarding.
            if (!dcMeta.getId().equalsIgnoreCase(currentDc)) {
                continue;
            }
            if (!BeaconSentinelMetaUtil.isBeaconCandidate(dcMeta, clusterName, false,
                    Collections.emptySet())) {
                continue;
            }

            ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterName);
            ClusterType clusterType = ClusterType.lookup(!StringUtil.isEmpty(clusterMeta.getAzGroupType())
                    ? clusterMeta.getAzGroupType() : clusterMeta.getType());
            BeaconSystem beaconSystem = resolveBeaconSystemByRouteType(clusterType, BeaconRouteType.SENTINEL);
            MonitorService monitorService = get(clusterMeta.getOrgId(), clusterMeta.getId(), dcMeta.getZone(), BeaconRouteType.SENTINEL);

            result.add(new SentinelClusterBeaconRouteItem(beaconSystem, dcMeta, clusterMeta, monitorService));
        }

        return result;
    }

    @Override
    public Map<String, DRClusterBeaconRouteItem> getDRClusterRoutes(String clusterName) {
        Map<String, DRClusterBeaconRouteItem> result = new LinkedHashMap<>();
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (xpipeMeta == null) {
            logger.warn("[getDRClusterRoutes] xpipeMeta is null, cluster={}", clusterName);
            return result;
        }

        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            if (!BeaconSentinelMetaUtil.isBeaconCandidate(dcMeta, clusterName, true,
                    consoleCommonConfig.getBeaconSupportZones())) {
                continue;
            }

            ClusterMeta clusterMeta = dcMeta.getClusters().get(clusterName);
            ClusterType clusterType = ClusterType.lookup(!StringUtil.isEmpty(clusterMeta.getAzGroupType())
                    ? clusterMeta.getAzGroupType() : clusterMeta.getType());
            BeaconSystem beaconSystem = resolveBeaconSystemByRouteType(clusterType, BeaconRouteType.DR);
            MonitorService monitorService = get(clusterMeta.getOrgId(), clusterMeta.getId(), dcMeta.getZone(), BeaconRouteType.DR);

            List<String> dcs = new ArrayList<>();
            if (!StringUtil.isEmpty(clusterMeta.getActiveDc())) {
                dcs.add(clusterMeta.getActiveDc().toUpperCase());
            }
            if (!StringUtil.isEmpty(clusterMeta.getBackupDcs())) {
                for (String dc : clusterMeta.getBackupDcs().split("\\s*,\\s*")) {
                    if (dc.isEmpty()) continue;
                    String upper = dc.toUpperCase();
                    if (!dcs.contains(upper)) dcs.add(upper);
                }
            }
            String region = dcMeta.getZone() != null ? dcMeta.getZone().toUpperCase() : UNKNOWN_REGION;

            result.put(region, new DRClusterBeaconRouteItem(beaconSystem, clusterMeta, dcs, monitorService));
        }

        return result;
    }

    @Override
    public List<SentinelBeaconUsageItem> getSentinelBeaconUsage(String system, boolean includeClusters) {
        Map<BeaconSystem, Map<Long, Map<MonitorService, Set<String>>>> beaconData = clustersByBeaconSystemOrg(BeaconRouteType.SENTINEL);
        if (beaconData == null || beaconData.isEmpty()) {
            return Collections.emptyList();
        }

        String currentDcUpper = currentDc.toUpperCase();
        Map<String, Map<String, Integer>> shardCountMap = metaCache.getClusterShardCounts();

        List<SentinelBeaconUsageItem> result = new ArrayList<>();
        beaconData.forEach((beaconSystem, orgMap) -> {
            if (!beaconSystem.getSystemName().equalsIgnoreCase(system)) return;
            orgMap.forEach((orgId, serviceMap) -> serviceMap.forEach((service, clusters) -> {
                int totalShards = clusters.stream()
                        .mapToInt(c -> shardCountMap.getOrDefault(c, Collections.emptyMap())
                                .getOrDefault(currentDcUpper, 0))
                        .sum();
                List<ClusterShardInfo> clusterList = null;
                if (includeClusters) {
                    clusterList = new ArrayList<>();
                    for (String clusterName : clusters) {
                        int shards = shardCountMap.getOrDefault(clusterName, Collections.emptyMap())
                                .getOrDefault(currentDcUpper, 0);
                        clusterList.add(new ClusterShardInfo(clusterName, shards));
                    }
                }
                result.add(new SentinelBeaconUsageItem(beaconSystem, BeaconRouteType.SENTINEL, orgId, service,
                        clusters.size(), totalShards, clusterList));
            }));
        });

        return result;
    }

    @Override
    public Map<String, RegionBeaconUsage> getDRBeaconUsage(String system, boolean includeClusters) {
        Map<String, RegionBeaconUsage> result = new LinkedHashMap<>();
        XpipeMeta xpipeMeta = metaCache.getXpipeMeta();
        if (xpipeMeta == null) {
            return result;
        }

        Set<String> supportZones = consoleCommonConfig.getBeaconSupportZones();
        Map<String, String> dcToRegion = new HashMap<>();
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            String region = dcMeta.getZone();
            if (region == null) continue;
            if (!supportZones.isEmpty() && supportZones.stream().noneMatch(z -> z.equalsIgnoreCase(region))) {
                continue;
            }
            dcToRegion.put(dcMeta.getId().toUpperCase(), region.toUpperCase());
            result.computeIfAbsent(region.toUpperCase(), r -> new RegionBeaconUsage())
                    .getDcs().add(dcMeta.getId().toUpperCase());
        }

        Map<BeaconSystem, Map<Long, Map<MonitorService, Set<String>>>> beaconData = clustersByBeaconSystemOrg(BeaconRouteType.DR);
        if (beaconData == null || beaconData.isEmpty()) {
            return result;
        }

        Map<String, Map<String, Integer>> shardCountMap = metaCache.getClusterShardCounts();
        Map<String, String> clusterToActiveDc = new HashMap<>();
        for (DcMeta dcMeta : xpipeMeta.getDcs().values()) {
            for (ClusterMeta cm : dcMeta.getClusters().values()) {
                if (cm.getActiveDc() != null) {
                    clusterToActiveDc.putIfAbsent(cm.getId(), cm.getActiveDc().toUpperCase());
                }
            }
        }

        beaconData.forEach((beaconSystem, orgMap) -> {
            if (!beaconSystem.getSystemName().equalsIgnoreCase(system)) return;
            orgMap.forEach((orgId, serviceMap) -> serviceMap.forEach((service, clusters) -> {
                String region = UNKNOWN_REGION;
                List<ClusterShardInfo> clusterInfos = includeClusters ? new ArrayList<>() : null;
                int totalShards = 0;
                for (String clusterName : clusters) {
                    String activeDc = clusterToActiveDc.get(clusterName);
                    int shards = activeDc != null
                            ? shardCountMap.getOrDefault(clusterName, Collections.emptyMap())
                                    .getOrDefault(activeDc, 0)
                            : 0;
                    if (clusterInfos != null) {
                        clusterInfos.add(new ClusterShardInfo(clusterName, shards));
                    }
                    totalShards += shards;
                    if (region.equals(UNKNOWN_REGION) && activeDc != null) {
                        String beaconRegion = dcToRegion.get(activeDc);
                        if (beaconRegion != null) {
                            region = beaconRegion;
                        }
                    }
                }
                RegionBeaconUsage regionUsage = result.computeIfAbsent(region, r -> new RegionBeaconUsage());
                regionUsage.getBeacons().add(new DRBeaconUsageItem(beaconSystem, orgId, service,
                        clusters.size(), totalShards, clusterInfos));
            }));
        });

        return result;
    }

    private void fillMonitorServicePlaceholders(
            Map<BeaconSystem, Map<Long, Map<MonitorService, Set<String>>>> clusterByBeaconSystemOrg,
            Set<Long> orgIds, BeaconRouteType routeType) {
        Map<Long, List<MonitorService>> servicesByOrg = getAllServices(routeType);
        clusterByBeaconSystemOrg.values().forEach(clustersByOrg -> orgIds.forEach(orgId -> {
            List<MonitorService> services = servicesByOrg.get(orgId);
            if (CollectionUtils.isEmpty(services)) {
                return;
            }
            Map<MonitorService, Set<String>> byService = clustersByOrg.get(orgId);
            services.forEach(service -> byService.computeIfAbsent(service, ignored -> new HashSet<>()));
        }));
    }

    private BeaconSystem resolveBeaconSystemByRouteType(ClusterType clusterType, BeaconRouteType routeType) {
        BeaconSystem beaconSystem = BeaconSystem.findByClusterType(clusterType);
        if (beaconSystem != null) {
            return beaconSystem;
        }
        return BeaconSystem.XPIPE_ONE_WAY;
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
