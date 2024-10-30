package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconCheckStatus;

/**
 * @author lishanglin
 * date 2021/3/17
 */
public class TestBeaconManager implements BeaconManager {

    @Override
    public void registerCluster(String clusterId, ClusterType clusterType, int orgId) {

    }

    @Override
    public void updateCluster(String clusterId, ClusterType clusterType, int orgId) {
        
    }

    @Override
    public BeaconCheckStatus checkClusterHash(String clusterId, ClusterType clusterType, int orgId) {
        return BeaconCheckStatus.UNKNOWN;
    }
}
