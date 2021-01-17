package com.ctrip.xpipe.redis.console.beacon;

import com.ctrip.xpipe.redis.console.beacon.data.BeaconGroupMeta;

import java.util.Set;

/**
 * @author lishanglin
 * date 2021/1/10
 */
public interface BeaconService {

    Set<String> fetchAllClusters();

    void registerCluster(String clusterName, Set<BeaconGroupMeta> groups);

    void unregisterCluster(String clusterName);

}
