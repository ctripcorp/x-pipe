package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.dto.ApiResponse;
import com.ctrip.xpipe.redis.console.controller.api.dto.BeaconUsageItem;
import com.ctrip.xpipe.redis.console.controller.api.dto.ClusterBeaconRouteItem;
import com.ctrip.xpipe.redis.console.controller.api.dto.ClusterShardInfo;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
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
import java.util.function.Supplier;

@RestController
@RequestMapping(AbstractConsoleController.API_PREFIX + "/beacon")
public class BeaconRouteController extends AbstractConsoleController {

    @Autowired
    private MonitorManager monitorManager;

    @Autowired
    private BeaconManager beaconManager;

    @Autowired(required = false)
    private ConsoleServiceManager consoleServiceManager;

    @Autowired
    private MetaCache metaCache;

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
                                 @RequestParam(required = false, defaultValue = "false") boolean includeClusters) {
        BeaconRouteType selectedRouteType = BeaconRouteType.valueOf(beaconMode.toUpperCase());

        if (selectedRouteType == BeaconRouteType.DR) {
            return ApiResponse.success(buildUsage(selectedRouteType, system, includeClusters));
        }

        // SENTINEL user-facing: local + forward to other consoles, aggregated by Manager
        if (consoleServiceManager == null) {
            String currentDc = FoundationService.DEFAULT.getDataCenter().toUpperCase();
            Map<String, ApiResponse<List<BeaconUsageItem>>> result = new LinkedHashMap<>();
            result.put(currentDc, ApiResponse.success(buildUsage(selectedRouteType, system, includeClusters)));
            return result;
        }
        return consoleServiceManager.getAllConsoleBeaconUsage(
                system, includeClusters,
                () -> buildUsage(selectedRouteType, system, includeClusters));
    }

    @GetMapping("/usage/local")
    public List<BeaconUsageItem> getBeaconUsageLocal(@RequestParam String beaconMode,
                                                     @RequestParam(required = false, defaultValue = "xpipe") String system,
                                                     @RequestParam(required = false, defaultValue = "false") boolean includeClusters) {
        return buildUsage(BeaconRouteType.valueOf(beaconMode.toUpperCase()), system, includeClusters);
    }

    @GetMapping("/cluster/{clusterName}")
    public Object getClusterBeaconRoute(@PathVariable String clusterName,
                                        @RequestParam String beaconMode) {
        BeaconRouteType selectedRouteType = BeaconRouteType.valueOf(beaconMode.toUpperCase());
        Supplier<List<ClusterBeaconRouteItem>> localSupplier =
                () -> monitorManager.getClusterRoutes(clusterName, selectedRouteType);

        if (selectedRouteType != BeaconRouteType.SENTINEL || consoleServiceManager == null) {
            return groupLocalByDc(localSupplier.get());
        }

        Set<String> beaconDcs = monitorManager.getBeaconDcs(clusterName, selectedRouteType);
        return consoleServiceManager.getAllConsoleClusterBeaconRoute(clusterName, beaconDcs, localSupplier);
    }

    @GetMapping("/cluster/{clusterName}/local")
    public List<ClusterBeaconRouteItem> getClusterBeaconRouteLocal(@PathVariable String clusterName,
                                                                    @RequestParam String beaconMode) {
        return monitorManager.getClusterRoutes(clusterName, BeaconRouteType.valueOf(beaconMode.toUpperCase()));
    }

    private Map<String, ApiResponse<List<ClusterBeaconRouteItem>>> groupLocalByDc(List<ClusterBeaconRouteItem> localData) {
        Map<String, List<ClusterBeaconRouteItem>> grouped = new LinkedHashMap<>();
        for (ClusterBeaconRouteItem item : localData) {
            grouped.computeIfAbsent(item.getDcName(), k -> new ArrayList<>()).add(item);
        }
        Map<String, ApiResponse<List<ClusterBeaconRouteItem>>> result = new LinkedHashMap<>();
        grouped.forEach((dc, data) -> result.put(dc, ApiResponse.success(data)));
        return result;
    }

    private List<BeaconUsageItem> buildUsage(BeaconRouteType routeType, String system, boolean includeClusters) {
        Map<BeaconSystem, Map<Long, Map<MonitorService, Set<String>>>> beaconData =
                monitorManager.clustersByBeaconSystemOrg(routeType);

        if (beaconData == null || beaconData.isEmpty()) {
            return Collections.emptyList();
        }

        String currentDc = FoundationService.DEFAULT.getDataCenter().toUpperCase();
        Map<String, Map<String, Integer>> shardCountMap = metaCache.getClusterShardCounts();

        List<BeaconUsageItem> result = new ArrayList<>();
        beaconData.forEach((beaconSystem, orgMap) -> {
            if (!beaconSystem.getSystemName().equalsIgnoreCase(system)) {
                return;
            }
            orgMap.forEach((orgId, serviceMap) -> serviceMap.forEach((service, clusters) -> {
                BeaconUsageItem item = new BeaconUsageItem();
                item.setSystem(beaconSystem.getSystemName());
                item.setBeaconMode(routeType.name());
                item.setOrgId(orgId);
                item.setBeaconName(service.getName());
                item.setBeaconHost(service.getHost());
                item.setClusterCount(clusters.size());
                item.setShardCount(clusters.stream()
                        .mapToInt(c -> shardCountMap.getOrDefault(c, Collections.emptyMap())
                                .getOrDefault(currentDc, 0))
                        .sum());
                if (includeClusters) {
                    List<ClusterShardInfo> clusterList = new ArrayList<>();
                    for (String clusterName : clusters) {
                        int shards = shardCountMap.getOrDefault(clusterName, Collections.emptyMap())
                                .getOrDefault(currentDc, 0);
                        clusterList.add(new ClusterShardInfo(clusterName, shards));
                    }
                    item.setClusters(clusterList);
                }
                result.add(item);
            }));
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
