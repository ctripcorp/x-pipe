package com.ctrip.xpipe.redis.console.healthcheck.delay;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Aug 29, 2018
 */
public interface DelayCollector {

    void collect(RedisHealthCheckInstance instance);

}
