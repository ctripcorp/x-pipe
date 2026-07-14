package com.ctrip.xpipe.redis.checker;

import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.healthcheck.clusteractions.beacon.BeaconCheckStatus;
import com.ctrip.xpipe.redis.core.beacon.BeaconRouteType;

import java.util.Map;

/**
 * @author lishanglin
 * date 2021/3/17
 */
public class TestBeaconManager implements BeaconManager {

    @Override
    public void registerCluster(String clusterId, String dc, ClusterType clusterType, int orgId, String lastModifyTime,
                                BeaconRouteType routeType, Map<String, HostPort> shardMasters) {

    }

    @Override
    public void updateCluster(String clusterId, String dc, ClusterType clusterType, int orgId, String lastModifyTime,
                              BeaconRouteType routeType) {

    }

    @Override
    public BeaconCheckStatus checkClusterHash(String clusterId, String dc, ClusterType clusterType, int orgId,
                                              String lastModifyTime, BeaconRouteType routeType) {
        return BeaconCheckStatus.UNKNOWN;
    }

    @Override
    public int computeClusterMetaHash(String clusterId, String dc, ClusterType clusterType, BeaconRouteType routeType) {
        return 0;
    }

    @Override
    public void unregisterCluster(String clusterId, String dc, ClusterType clusterType, int orgId, BeaconRouteType routeType) {

    }
}
