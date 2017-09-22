package com.ctrip.xpipe.redis.console.health.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Sep 21, 2017
 */
public interface Callbackable<V extends Object> {

    void success(V message);

    void fail(Throwable throwable);
}
