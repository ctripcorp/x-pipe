package com.ctrip.xpipe.redis.console.healthcheck;

/**
 * @author chen.zhu
 * <p>
 * Aug 29, 2018
 */
public interface HealthStatusManager {

    void markDown(RedisHealthCheckInstance instance, MarkDownReason reason);

    void markUp(RedisHealthCheckInstance instance, MarkUpReason reason);

    enum MarkDownReason {
        PING_FAIL, LAG
    }

    enum MarkUpReason {
        PING_OK, DELAY_HEALTHY
    }

    interface MarkDownWorker {
        void doMarkDown(RedisHealthCheckInstance instance);
    }

    interface MarkUpWorker {
        void doMarkUp(RedisHealthCheckInstance instance);
    }
}
