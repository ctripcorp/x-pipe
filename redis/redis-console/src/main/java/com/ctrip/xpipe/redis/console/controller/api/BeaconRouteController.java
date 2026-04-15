package com.ctrip.xpipe.redis.console.controller.api;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.BeaconManager;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.migration.auto.MonitorManager;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
        MonitorService service = monitorManager.get(orgId, clusterName, selectedRouteType);
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
     * local side ({@code MonitorClusterMeta#generateHashCodeForBeaconCheck}).
     */
    @GetMapping("/sentinel/cluster/hash")
    public Map<String, Object> getClusterMetaHash(@RequestParam("clusterName") String clusterName,
                                                  @RequestParam("clusterType") String clusterType,
                                                  @RequestParam(name = "routeType", required = false) String routeType) {
        BeaconRouteType selectedRouteType = parseRouteType(routeType);
        ClusterType type = ClusterType.lookup(clusterType);
        int metaHash = beaconManager.computeClusterMetaHash(clusterName, type, selectedRouteType);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("clusterName", clusterName);
        result.put("clusterType", type.name());
        result.put("routeType", selectedRouteType.name());
        result.put("metaHash", metaHash);
        return result;
    }

    private BeaconRouteType parseRouteType(String routeType) {
        if (routeType == null || routeType.isEmpty()) {
            return BeaconRouteType.SENTINEL;
        }
        return BeaconRouteType.valueOf(routeType.toUpperCase());
    }
}
