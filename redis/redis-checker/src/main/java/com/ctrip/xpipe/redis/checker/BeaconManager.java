package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconCheckStatus;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public interface BeaconManager {

    String EXTRA_LAST_MODIFY_TIME = "lastModifyTime";

    void registerCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime);

    void updateCluster(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime);

    BeaconCheckStatus checkClusterHash(String clusterId, ClusterType clusterType, int orgId, String lastModifyTime);

}
