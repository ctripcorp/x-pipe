package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconCheckStatus;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public interface BeaconManager {

    String EXTRA_LAST_MODIFY_TIME = "lastModifyTime";

    default void registerCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime) {
        registerCluster(clusterId, clusterType, orgId, lastModifyTime, BeaconRouteType.DR);
    }

    void registerCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime, BeaconRouteType routeType);

    default void updateCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime) {
        updateCluster(clusterId, clusterType, orgId, lastModifyTime, BeaconRouteType.DR);
    }

    void updateCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime, BeaconRouteType routeType);

    default BeaconCheckStatus checkClusterHash(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime) {
        return checkClusterHash(clusterId, clusterType, orgId, lastModifyTime, BeaconRouteType.DR);
    }

    BeaconCheckStatus checkClusterHash(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime, BeaconRouteType routeType);

}
