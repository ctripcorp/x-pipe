package com.ctrip.xpipe.redis.core.store.ratelimit;

/**
 * @author lishanglin
 * date 2024/7/25
 */
public interface SyncRateLimiter {

    void acquire(int syncByte);

}
