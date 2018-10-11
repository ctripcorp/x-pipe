package com.ctrip.xpipe.redis.console.healthcheck.session;

/**
 * @author chen.zhu
 * <p>
 * Oct 08, 2018
 */
public interface Callbackable<V> {

    void success(V message);

    void fail(Throwable throwable);
}
