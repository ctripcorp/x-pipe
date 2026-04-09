package com.ctrip.xpipe.redis.console.migration.auto;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.checker.BeaconRouteType;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public interface MonitorManager {

    MonitorService get(long orgId, String clusterName);

    default MonitorService get(long orgId, String clusterName, BeaconRouteType routeType) {
        return get(orgId, clusterName);
    }

    Map<Long, List<MonitorService>> getAllServices();

    default Map<Long, List<MonitorService>> getAllServices(BeaconRouteType routeType) {
        return getAllServices();
    }

    Map<BeaconSystem, Map<Long, Set<String>>> clustersByBeaconSystemOrg();

    default Map<BeaconSystem, Map<Long, Set<String>>> clustersByBeaconSystemOrg(BeaconRouteType routeType) {
        return clustersByBeaconSystemOrg();
    }
}
