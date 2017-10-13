package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.endpoint.HostPort;

/**
 * @author chen.zhu
 * <p>
 * Oct 13, 2017
 */
public interface RedisAlertManager {
    RedisAlert findOrCreateRedisAlert(ALERT_TYPE alertType, String clusterId,
                                      String shardId, HostPort hostPort, String message);

}
