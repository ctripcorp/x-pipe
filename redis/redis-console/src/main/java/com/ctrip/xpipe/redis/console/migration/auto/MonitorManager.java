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

    MonitorService get(long orgId, String clusterName, String zone, BeaconRouteType routeType);

    Map<Long, List<MonitorService>> getAllServices();

    Map<Long, List<MonitorService>> getAllServices(BeaconRouteType routeType);

    Map<BeaconSystem, Map<Long, Map<MonitorService, Set<String>>>> clustersByBeaconSystemOrg();

    Map<BeaconSystem, Map<Long, Map<MonitorService, Set<String>>>> clustersByBeaconSystemOrg(BeaconRouteType routeType);

    Map<String, String> getClusterTypeMap(BeaconRouteType routeType);

    List<Map<String, Object>> getClusterRoutes(String clusterName, BeaconRouteType routeType);

    Map<String, String> getClusterDcTypes(String clusterName);

    /**
     * 返回该集群在指定路由类型下需要注册 beacon 的 DC 集合
     */
    Set<String> getBeaconDcs(String clusterName, BeaconRouteType routeType);

    /**
     * 返回所有集群的分片数量，key 为集群名，value 为分片数
     */
    Map<String, Integer> getClusterShardCounts();
}
