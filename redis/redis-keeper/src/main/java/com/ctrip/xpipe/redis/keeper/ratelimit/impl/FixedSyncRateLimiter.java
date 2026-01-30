package com.ctrip.xpipe.redis.keeper.ratelimit.impl;

import com.ctrip.xpipe.redis.core.store.ratelimit.SyncRateLimiter;
import com.google.common.util.concurrent.RateLimiter;

public class FixedSyncRateLimiter implements SyncRateLimiter {

    private FixedSyncRateLimiterConfig config;

    private RateLimiter rateLimiter;

    public FixedSyncRateLimiter(FixedSyncRateLimiterConfig config) {
        this.config = config;
        this.rateLimiter = RateLimiter.create(Math.max(1, config.getBytesPerSecond()));
    }

    @Override
    public void acquire(int syncByte) {
        if (syncByte <= 0 || config.getBytesPerSecond() <= 0) return;
        if (getRate() != config.getBytesPerSecond()) rateLimiter.setRate(Math.max(1, config.getBytesPerSecond()));
        rateLimiter.acquire(syncByte);
    }

    @Override
    public int getRate() {
        return (int) rateLimiter.getRate();
    }

    public interface FixedSyncRateLimiterConfig {
        int getBytesPerSecond();
    }

}
