package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconCheckStatus;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public interface BeaconManager {

    void registerCluster(String clusterId, ClusterType clusterType, int orgId);

    void updateCluster(String clusterId, ClusterType clusterType, int orgId);

    BeaconCheckStatus checkClusterHash(String clusterId, ClusterType clusterType, int orgId);

}
