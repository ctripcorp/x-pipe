package com.ctrip.xpipe.redis.console.health.migration;

/**
 * @author chen.zhu
 * <p>
 * Sep 21, 2017
 */
public interface MigrationHealthCheck {
    void check();

    void fail(Throwable throwable);
}
