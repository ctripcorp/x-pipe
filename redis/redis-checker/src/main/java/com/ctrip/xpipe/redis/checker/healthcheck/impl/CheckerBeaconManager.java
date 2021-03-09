package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.redis.checker.BeaconManager;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2021/3/12
 */
@Component
public class CheckerBeaconManager implements BeaconManager {

    public void registerCluster(String clusterId, int orgId) {

    }

}
