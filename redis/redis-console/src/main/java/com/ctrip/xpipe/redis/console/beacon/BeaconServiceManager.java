package com.ctrip.xpipe.redis.console.beacon;

import java.util.Map;

/**
 * @author lishanglin
 * date 2021/1/15
 */
public interface BeaconServiceManager {

    BeaconService getOrCreate(long orgId);

    Map<Long, BeaconService> getAllServices();

}
