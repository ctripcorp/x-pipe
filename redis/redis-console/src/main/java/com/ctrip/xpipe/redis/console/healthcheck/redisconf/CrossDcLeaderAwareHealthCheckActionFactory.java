package com.ctrip.xpipe.redis.console.healthcheck.redisconf;

import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 30, 2018
 */
public interface CrossDcLeaderAwareHealthCheckActionFactory {

    CrossDcLeaderAwareHealthCheckAction create(RedisHealthCheckInstance instance);

    Class<? extends CrossDcLeaderAwareHealthCheckAction> support();
}
