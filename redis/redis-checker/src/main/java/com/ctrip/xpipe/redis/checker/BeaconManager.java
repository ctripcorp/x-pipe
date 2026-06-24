package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconCheckStatus;

import java.util.Collections;
import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public interface BeaconManager {

    String EXTRA_LAST_MODIFY_TIME = "lastModifyTime";

    default void registerCluster(String clusterId, String dc, ClusterType clusterType, int orgId, String lastModifyTime,
                                 BeaconRouteType routeType) {
        registerCluster(clusterId, dc, clusterType, orgId, lastModifyTime, routeType, Collections.emptyMap());
    }

    void registerCluster(String clusterId, String dc, ClusterType clusterType, int orgId, String lastModifyTime,
                         BeaconRouteType routeType, Map<String, HostPort> shardMasters);

    void updateCluster(String clusterId, String dc, ClusterType clusterType, int orgId, String lastModifyTime,
                       BeaconRouteType routeType);

    BeaconCheckStatus checkClusterHash(String clusterId, String dc, ClusterType clusterType, int orgId,
                                       String lastModifyTime, BeaconRouteType routeType);

    /**
     * Local cluster meta hash used in {@link #checkClusterHash}; same as
     * {@code buildMonitorClusterMeta(clusterId, routeType).generateHashCodeForBeaconCheck()} in console implementation.
     */
    int computeClusterMetaHash(String clusterId, String dc, ClusterType clusterType, BeaconRouteType routeType);

    void unregisterCluster(String clusterId, String dc, ClusterType clusterType, int orgId, BeaconRouteType routeType);

}
