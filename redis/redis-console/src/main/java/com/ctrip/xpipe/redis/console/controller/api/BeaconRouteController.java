package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.console.console.impl.ConsoleServiceManager;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.controller.api.vo.DRClusterBeaconRouteItem;
import com.ctrip.xpipe.redis.console.controller.api.vo.RegionBeaconUsage;
import com.ctrip.xpipe.redis.console.controller.api.vo.RestResponse;
import com.ctrip.xpipe.redis.console.controller.api.vo.SentinelBeaconUsageItem;
import com.ctrip.xpipe.redis.console.controller.api.vo.SentinelClusterBeaconRouteItem;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
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

    @GetMapping("/sentinel/usage")
    public Map<String, RestResponse<List<SentinelBeaconUsageItem>>> getSentinelBeaconUsage(
            @RequestParam(required = false, defaultValue = "xpipe") String system,
            @RequestParam(required = false, defaultValue = "false") boolean includeClusters) {
        if (consoleServiceManager == null) {
            String currentDc = FoundationService.DEFAULT.getDataCenter().toUpperCase();
            Map<String, RestResponse<List<SentinelBeaconUsageItem>>> result = new LinkedHashMap<>();
            result.put(currentDc, RestResponse.success(monitorManager.getSentinelBeaconUsage(system, includeClusters)));
            return result;
        }
        return consoleServiceManager.getAllConsoleSentinelBeaconUsage(system, includeClusters);
    }

    @GetMapping("/sentinel/usage/local")
    public List<SentinelBeaconUsageItem> getSentinelBeaconUsageLocal(
            @RequestParam(required = false, defaultValue = "xpipe") String system,
            @RequestParam(required = false, defaultValue = "false") boolean includeClusters) {
        return monitorManager.getSentinelBeaconUsage(system, includeClusters);
    }

    @GetMapping("/dr/usage")
    public RestResponse<Map<String, RegionBeaconUsage>> getDRBeaconUsage(
            @RequestParam(required = false, defaultValue = "xpipe") String system,
            @RequestParam(required = false, defaultValue = "false") boolean includeClusters) {
        return RestResponse.success(monitorManager.getDRBeaconUsage(system, includeClusters));
    }

    @GetMapping("/sentinel/cluster/{clusterName}")
    public Map<String, RestResponse<List<SentinelClusterBeaconRouteItem>>> getSentinelClusterBeaconRoute(
            @PathVariable String clusterName) {
        if (consoleServiceManager == null) {
            return groupLocalByDc(monitorManager.getSentinelClusterRoutes(clusterName));
        }
        return consoleServiceManager.getAllConsoleSentinelClusterBeaconRoute(clusterName);
    }

    @GetMapping("/sentinel/cluster/{clusterName}/local")
    public List<SentinelClusterBeaconRouteItem> getSentinelClusterBeaconRouteLocal(@PathVariable String clusterName) {
        return monitorManager.getSentinelClusterRoutes(clusterName);
    }

    @GetMapping("/dr/cluster/{clusterName}")
    public RestResponse<Map<String, DRClusterBeaconRouteItem>> getDRClusterBeaconRoute(@PathVariable String clusterName) {
        return RestResponse.success(monitorManager.getDRClusterRoutes(clusterName));
    }

    private Map<String, RestResponse<List<SentinelClusterBeaconRouteItem>>> groupLocalByDc(List<SentinelClusterBeaconRouteItem> localData) {
        Map<String, List<SentinelClusterBeaconRouteItem>> grouped = new LinkedHashMap<>();
        for (SentinelClusterBeaconRouteItem item : localData) {
            grouped.computeIfAbsent(item.getDcName(), k -> new ArrayList<>()).add(item);
        }
        Map<String, RestResponse<List<SentinelClusterBeaconRouteItem>>> result = new LinkedHashMap<>();
        grouped.forEach((dc, data) -> result.put(dc, RestResponse.success(data)));
        return result;
    }

    private BeaconRouteType parseRouteType(String routeType) {
        if (routeType == null || routeType.isEmpty()) {
            return BeaconRouteType.SENTINEL;
        }
        return BeaconRouteType.valueOf(routeType.toUpperCase());
    }
}
