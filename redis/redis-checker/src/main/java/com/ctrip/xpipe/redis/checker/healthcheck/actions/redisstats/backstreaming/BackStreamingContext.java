package com.ctrip.xpipe.redis.checker.healthcheck.actions.redisstats.backstreaming;

import com.ctrip.xpipe.redis.checker.healthcheck.AbstractActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;

/**
 * @author lishanglin
 * date 2021/1/26
 */
public class BackStreamingContext extends AbstractActionContext<Boolean, RedisHealthCheckInstance>  {

    public BackStreamingContext(RedisHealthCheckInstance instance, Boolean result) {
        super(instance, result);
    }

}