package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconCheckStatus;

/**
 * @author lishanglin
 * date 2021/3/17
 */
public class TestBeaconManager implements BeaconManager {

    @Override
    public void registerCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime) {

    }

    @Override
    public void registerCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime,
                                BeaconRouteType routeType) {
        registerCluster(clusterId, clusterType, orgId, lastModifyTime);
    }

    @Override
    public void updateCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime) {
        
    }

    @Override
    public void updateCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime,
                              BeaconRouteType routeType) {
        updateCluster(clusterId, clusterType, orgId, lastModifyTime);
    }

    @Override
    public BeaconCheckStatus checkClusterHash(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime) {
        return BeaconCheckStatus.UNKNOWN;
    }

    @Override
    public BeaconCheckStatus checkClusterHash(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime,
                                              BeaconRouteType routeType) {
        return checkClusterHash(clusterId, clusterType, orgId, lastModifyTime);
    }

    @Override
    public int computeClusterMetaHash(String clusterId, ClusterType clusterType, BeaconRouteType routeType) {
        return 0;
    }
}
