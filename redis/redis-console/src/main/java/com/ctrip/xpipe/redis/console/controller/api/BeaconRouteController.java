package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX + "/beacon")
public class BeaconRouteController extends AbstractConsoleController {

    @Autowired
    private MonitorManager monitorManager;

    @Autowired
    private BeaconManager beaconManager;

    @Autowired(required = false)
    private ConsoleServiceManager consoleServiceManager;

    private static final Logger logger = LoggerFactory.getLogger(BeaconRouteController.class);

    @GetMapping("/sentinel/clusters")
    public Map<String, Set<String>> getSentinelBeaconClusters(@RequestParam(name = "system", required = false) String system,
                                                               @RequestParam(name = "routeType", required = false) String routeType) {
        BeaconRouteType selectedRouteType = parseRouteType(routeType);
        String systemName = (system == null || system.isEmpty()) ? BeaconSystem.XPIPE_ONE_WAY.getSystemName() : system;
        Map<Long, List<MonitorService>> services = monitorManager.getAllServices(selectedRouteType);
        if (services.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Set<String>> result = new LinkedHashMap<>();
        services.values().stream().flatMap(List::stream).forEach(service -> {
            Set<String> clusters = service.fetchAllClusters(systemName);
            String key = service.getName() + "@" + service.getHost();
            result.put(key, clusters);
        });
        return result;
    }

    @GetMapping("/sentinel/route")
    public Map<String, String> getSentinelBeaconRoute(@RequestParam("clusterName") String clusterName,
                                                       @RequestParam("orgId") long orgId,
                                                       @RequestParam(name = "routeType", required = false) String routeType) {
        BeaconRouteType selectedRouteType = parseRouteType(routeType);
        MonitorService service = monitorManager.get(orgId, clusterName, null, selectedRouteType);
        if (service == null) {
            return Collections.emptyMap();
        }
        Map<String, String> route = new LinkedHashMap<>();
        route.put("clusterName", clusterName);
        route.put("orgId", String.valueOf(orgId));
        route.put("beaconName", service.getName());
        route.put("beaconHost", service.getHost());
        route.put("routeType", selectedRouteType.name());
        return route;
    }

    /**
     * Hash of local monitor cluster meta, same computation as {@link com.ctrip.xpipe.redis.console.migration.auto.DefaultBeaconManager#checkClusterHash}
     * local side ({@code MonitorClusterMeta#generateHashCodeForBeaconCheck(boolean)}).
     */
    @GetMapping("/sentinel/cluster/hash")
    public Map<String, Object> getClusterMetaHash(@RequestParam("clusterName") String clusterName,
                                                  @RequestParam("clusterType") String clusterType,
                                                  @RequestParam(name = "dc", required = false) String dc,
                                                  @RequestParam(name = "routeType", required = false) String routeType) {
        BeaconRouteType selectedRouteType = parseRouteType(routeType);
        ClusterType type = ClusterType.lookup(clusterType);
        String anchorDc = (dc == null || dc.isEmpty()) ? FoundationService.DEFAULT.getDataCenter() : dc;
        int metaHash = beaconManager.computeClusterMetaHash(clusterName, anchorDc, type, selectedRouteType);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("clusterName", clusterName);
        result.put("clusterType", type.name());
        result.put("dc", anchorDc);
        result.put("routeType", selectedRouteType.name());
        result.put("metaHash", metaHash);
        return result;
    }

    @GetMapping("/usage")
    public Object getBeaconUsage(@RequestParam String beaconMode,
                                 @RequestParam(required = false, defaultValue = "xpipe") String system,
                                 @RequestParam(required = false, defaultValue = "false") boolean includeClusters,
                                 @RequestParam(required = false, defaultValue = "false") boolean inner) {
        BeaconRouteType selectedRouteType = BeaconRouteType.valueOf(beaconMode.toUpperCase());
        List<Map<String, Object>> localData = buildUsage(selectedRouteType, system, includeClusters);

        if (inner) {
            return localData;
        }

        if (selectedRouteType == BeaconRouteType.DR) {
            return ConsoleServiceManager.wrapResponse(0, "success", localData);
        }

        // SENTINEL user-facing: forward to all consoles and group by DC
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(FoundationService.DEFAULT.getDataCenter().toUpperCase(),
                ConsoleServiceManager.wrapResponse(0, "success", localData));
        try {
            if (consoleServiceManager != null) {
                result.putAll(consoleServiceManager.getAllConsoleBeaconUsage(system, includeClusters));
            }
        } catch (Exception e) {
            logger.error("[getBeaconUsage] forward fail", e);
        }
        return result;
    }

    @GetMapping("/cluster/{clusterName}")
    public Object getClusterBeaconRoute(@PathVariable String clusterName,
                                        @RequestParam String beaconMode,
                                        @RequestParam(required = false, defaultValue = "false") boolean inner) {
        BeaconRouteType selectedRouteType = BeaconRouteType.valueOf(beaconMode.toUpperCase());
        List<Map<String, Object>> localData = monitorManager.getClusterRoutes(clusterName, selectedRouteType);

        if (inner) {
            return localData;
        }

        // Group by DC and wrap each DC's result
        Map<String, List<Map<String, Object>>> grouped = new LinkedHashMap<>();
        for (Map<String, Object> item : localData) {
            grouped.computeIfAbsent((String) item.get("dcName"), k -> new ArrayList<>()).add(item);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        grouped.forEach((dc, data) -> result.put(dc, ConsoleServiceManager.wrapResponse(0, "success", data)));

        if (selectedRouteType == BeaconRouteType.SENTINEL) {
            try {
                if (consoleServiceManager != null) {
                    Set<String> beaconDcs = monitorManager.getBeaconDcs(clusterName, selectedRouteType);
                    logger.info("[getClusterBeaconRoute] cluster={}, allBeaconDcs={}", clusterName, beaconDcs);
                    beaconDcs.remove(FoundationService.DEFAULT.getDataCenter().toUpperCase());
                    logger.info("[getClusterBeaconRoute] cluster={}, forwardTargets={}", clusterName, beaconDcs);
                    if (!beaconDcs.isEmpty()) {
                        result.putAll(consoleServiceManager.getAllConsoleClusterBeaconRoute(clusterName, beaconDcs));
                    }
                }
            } catch (Exception e) {
                logger.error("[getClusterBeaconRoute] forward fail", e);
            }
        }
        return result;
    }

    private List<Map<String, Object>> buildUsage(BeaconRouteType routeType, String system, boolean includeClusters) {
        Map<BeaconSystem, Map<Long, Map<MonitorService, Set<String>>>> beaconData =
                monitorManager.clustersByBeaconSystemOrg(routeType);

        if (beaconData == null || beaconData.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, String> clusterTypeMap = includeClusters ? monitorManager.getClusterTypeMap(routeType) : null;
        Map<String, Integer> shardCountMap = monitorManager.getClusterShardCounts();

        List<Map<String, Object>> result = new ArrayList<>();
        beaconData.forEach((beaconSystem, orgMap) -> {
            if (!beaconSystem.getSystemName().equalsIgnoreCase(system)) {
                return;
            }
            orgMap.forEach((orgId, serviceMap) -> {
                serviceMap.forEach((service, clusters) -> {
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("system", beaconSystem.getSystemName());
                    item.put("beaconMode", routeType.name());
                    item.put("orgId", orgId);
                    item.put("beaconName", service.getName());
                    item.put("beaconHost", service.getHost());
                    item.put("clusterCount", clusters.size());
                    int totalShards = clusters.stream()
                            .mapToInt(c -> shardCountMap.getOrDefault(c, 0)).sum();
                    item.put("shardCount", totalShards);
                    if (includeClusters) {
                        List<Map<String, Object>> clusterList = new ArrayList<>();
                        for (String clusterName : clusters) {
                            Map<String, Object> clusterInfo = new LinkedHashMap<>();
                            clusterInfo.put("name", clusterName);
                            clusterInfo.put("type", clusterTypeMap.getOrDefault(clusterName, "UNKNOWN"));
                            clusterInfo.put("shardCount", shardCountMap.getOrDefault(clusterName, 0));
                            clusterList.add(clusterInfo);
                        }
                        item.put("clusters", clusterList);
                    }
                    result.add(item);
                });
            });
        });

        return result;
    }

    private BeaconRouteType parseRouteType(String routeType) {
        if (routeType == null || routeType.isEmpty()) {
            return BeaconRouteType.SENTINEL;
        }
        return BeaconRouteType.valueOf(routeType.toUpperCase());
    }
}
