package com.ctrip.xpipe.redis.keeper.ratelimit.impl;

import com.ctrip.xpipe.redis.core.store.ratelimit.SyncRateLimiter;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;

/**
 * @author lishanglin
 * date 2024/7/29
 */
public class UnlimitedSyncRateManager implements SyncRateManager {

    @Override
    public void setTotalIOLimit(int limit) {

    }

    @Override
    public int getTotalIOLimit() {
        return 0;
    }

    @Override
    public SyncRateLimiter generateFsyncRateLimiter() {
        return new UnlimitedRateLimiter();
    }

    @Override
    public SyncRateLimiter generatePsyncRateLimiter() {
        return new UnlimitedRateLimiter();
    }

    class UnlimitedRateLimiter implements SyncRateLimiter {
        @Override
        public void acquire(int syncByte) {

        }
    }

}
