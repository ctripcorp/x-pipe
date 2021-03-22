package com.ctrip.xpipe.redis.checker;

/**
 * @author lishanglin
 * date 2021/3/12
 */
public interface BeaconManager {

    void registerCluster(String clusterId, int orgId);

}
