package com.ctrip.xpipe.redis.console.healthcheck;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSession;

/**
 * @author chen.zhu
 * <p>
 * Aug 23, 2018
 */
public interface RedisHealthCheckInstance extends HealthCheckInstance<RedisInstanceInfo> {

    Endpoint getEndpoint();

    RedisSession getRedisSession();

}
