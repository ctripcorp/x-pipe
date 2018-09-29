package com.ctrip.xpipe.redis.console.healthcheck.redisconf;

import com.ctrip.xpipe.api.cluster.CrossDcLeaderAware;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;

/**
 * @author chen.zhu
 * <p>
 * Sep 30, 2018
 */
public interface CrossDcLeaderAwareHealthCheckManager extends CrossDcLeaderAware {

    void registerTo(RedisHealthCheckInstance instance);

    void removeFrom(RedisHealthCheckInstance instance);

}
