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

    default void registerCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime) {
        registerCluster(clusterId, clusterType, orgId, lastModifyTime, BeaconRouteType.DR);
    }

    default void registerCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime, BeaconRouteType routeType) {
        registerCluster(clusterId, clusterType, orgId, lastModifyTime, routeType, Collections.emptyMap());
    }

    void registerCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime,
                         BeaconRouteType routeType, Map<String, HostPort> shardMasters);

    default void updateCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime) {
        updateCluster(clusterId, clusterType, orgId, lastModifyTime, BeaconRouteType.DR);
    }

    void updateCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime, BeaconRouteType routeType);

    default BeaconCheckStatus checkClusterHash(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime) {
        return checkClusterHash(clusterId, clusterType, orgId, lastModifyTime, BeaconRouteType.DR);
    }

    BeaconCheckStatus checkClusterHash(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime, BeaconRouteType routeType);

    /**
     * Local cluster meta hash used in {@link #checkClusterHash}; same as
     * {@code buildMonitorClusterMeta(...).generateHashCodeForBeaconCheck(includeExtra)} in console implementation,
     * where {@code includeExtra} follows {@code beacon.should.compute.extra.in.hash}.
     */
    int computeClusterMetaHash(String clusterId, ClusterType clusterType, BeaconRouteType routeType);

    default void unregisterCluster(String clusterId, ClusterType clusterType, int orgId) {
        unregisterCluster(clusterId, clusterType, orgId, BeaconRouteType.DR);
    }

    default void unregisterCluster(String clusterId, ClusterType clusterType, int orgId, BeaconRouteType routeType) {
        // no-op for compatibility in test implementations
    }

}
