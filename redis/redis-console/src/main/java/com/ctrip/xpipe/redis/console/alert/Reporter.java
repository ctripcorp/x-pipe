package com.ctrip.xpipe.redis.console.alert;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.Collection;

/**
 * @author chen.zhu
 * <p>
 * Oct 13, 2017
 */
public interface Reporter {
    void report(RedisAlert redisAlert);
    void report(Collection<RedisAlert> redisAlerts);
}
