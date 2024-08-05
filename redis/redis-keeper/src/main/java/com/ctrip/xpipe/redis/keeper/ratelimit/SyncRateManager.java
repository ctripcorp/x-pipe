package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.redis.core.store.ratelimit.SyncRateLimiter;

/**
 * @author lishanglin
 * date 2024/7/25
 */
public interface SyncRateManager {

    void setTotalIOLimit(int limit);

    int getTotalIOLimit();

    SyncRateLimiter generateFsyncRateLimiter();

    SyncRateLimiter generatePsyncRateLimiter();

}
