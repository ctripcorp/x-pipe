package com.ctrip.xpipe.redis.console.health.redisconf;

/**
 * @author chen.zhu
 * <p>
 * Sep 21, 2017
 */
public interface Callbackable<V extends Object> {

    void success(V message);

    void fail(Throwable throwable);
}
