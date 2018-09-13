package com.ctrip.xpipe.redis.console.healthcheck;

/**
 * @author chen.zhu
 * <p>
 * Sep 06, 2018
 */
public interface ActionContext<C> {

    RedisHealthCheckInstance instance();

    C getResult();

    long getRecvTimeMilli();
}
