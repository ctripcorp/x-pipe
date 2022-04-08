package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.cluster.ClusterType;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public interface BeaconManager {

    void registerCluster(String clusterId, ClusterType clusterType, int orgId);

}
