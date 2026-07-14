package com.ctrip.xpipe.redis.console.migration.auto;

import com.ctrip.xpipe.api.migration.auto.MonitorService;
import com.ctrip.xpipe.redis.core.beacon.BeaconRouteType;
import com.ctrip.xpipe.redis.console.controller.api.vo.DRClusterBeaconRouteItem;
import com.ctrip.xpipe.redis.console.controller.api.vo.RegionBeaconUsage;
import com.ctrip.xpipe.redis.console.controller.api.vo.SentinelBeaconUsageItem;
import com.ctrip.xpipe.redis.console.controller.api.vo.SentinelClusterBeaconRouteItem;
import com.ctrip.xpipe.redis.core.beacon.BeaconSystem;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public interface MonitorManager {

    MonitorService get(long orgId, String clusterName, String zone, BeaconRouteType routeType);

    Map<Long, List<MonitorService>> getAllServices();

    Map<Long, List<MonitorService>> getAllServices(BeaconRouteType routeType);

    Map<BeaconSystem, Map<Long, Map<MonitorService, Set<String>>>> clustersByBeaconSystemOrg();

    Map<BeaconSystem, Map<Long, Map<MonitorService, Set<String>>>> clustersByBeaconSystemOrg(BeaconRouteType routeType);

    List<SentinelClusterBeaconRouteItem> getSentinelClusterRoutes(String clusterName);

    Map<String, DRClusterBeaconRouteItem> getDRClusterRoutes(String clusterName);

    Set<String> getBeaconDcs(String clusterName, BeaconRouteType routeType);

    List<SentinelBeaconUsageItem> getSentinelBeaconUsage(String system, boolean includeClusters);

    Map<String, RegionBeaconUsage> getDRBeaconUsage(String system, boolean includeClusters);
}
