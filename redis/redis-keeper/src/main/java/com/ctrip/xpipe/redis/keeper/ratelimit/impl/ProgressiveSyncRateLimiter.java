package com.ctrip.xpipe.redis.keeper.ratelimit.impl;

import com.ctrip.xpipe.redis.core.store.ratelimit.SyncRateLimiter;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class ProgressiveSyncRateLimiter implements SyncRateLimiter {

    private ProgressiveSyncRateLimiterConfig config;

    private RateLimiter rateLimiter;

    private SystemSecondsProvider systemSecondsProvider;

    private int record;

    private int[] records;

    private int recordPos;

    private long lastRecordSeconds;

    private long lastCheckSeconds;

    private Object identify;

    private Logger logger = LoggerFactory.getLogger(ProgressiveSyncRateLimiter.class);

    public ProgressiveSyncRateLimiter(Object identify, ProgressiveSyncRateLimiterConfig config) {
        this(identify, config, SystemSecondsProvider.DEFAULT);
    }

    public ProgressiveSyncRateLimiter(Object identify, ProgressiveSyncRateLimiterConfig config, SystemSecondsProvider systemSecondsProvider) {
        this.systemSecondsProvider = systemSecondsProvider;
        this.identify = identify;
        this.config = config;
        this.rateLimiter = RateLimiter.create(baseBytesLimits());
        this.records = new int[config.getCheckInterval()];
        reset(systemSecondsProvider.getSystemSeconds());
    }

    private int baseBytesLimits() {
        return Math.max(config.getMaxBytesLimit(), 1);
    }

    @Override
    public void acquire(int syncByte) {
        if (syncByte <= 0) return;
        doAcquire(syncByte);

        long currentSeconds = systemSecondsProvider.getSystemSeconds();
        long recordInterval = currentSeconds - lastRecordSeconds;
        if (currentSeconds < lastRecordSeconds || recordInterval >= records.length) {
            reset(currentSeconds);
            record += syncByte;
        } else {
            if (recordInterval > 0) {
                insertRecord(currentSeconds, (int) recordInterval - 1);
                long checkInterval = currentSeconds - lastCheckSeconds;
                if (checkInterval >= records.length) {
                    updateLimit(currentSeconds);
                }
            }
            record += syncByte;
        }
    }

    private void reset(long currentSeconds) {
        record = 0;
        recordPos = 0;
        Arrays.fill(records, 0);
        lastRecordSeconds = currentSeconds;
        lastCheckSeconds = currentSeconds;
        setRate(baseBytesLimits());
        logger.info("[reset][{}] {}", identify, getRate());
    }

    private void insertRecord(long currentSeconds, int skips) {
        records[recordPos] = record;
        recordPos++;
        if (recordPos >= records.length) recordPos = 0;
        this.lastRecordSeconds = currentSeconds;
        this.record = 0;

        int end = recordPos + skips;
        if (end >= records.length) {
            Arrays.fill(records, recordPos, records.length - 1, (byte) 0);
            recordPos = 0;
            end = end - records.length;
        }
        if (end > recordPos) {
            Arrays.fill(records, recordPos, end, (byte) 0);
            recordPos = end;
        }
    }

    private void updateLimit(long currentSeconds) {
        long total = Arrays.stream(records).sum();
        long average = total / records.length;
        long currentLimit = getRate();

        if (average > 0.75 * currentLimit) {
            setRate(Math.max(1, Math.min(config.getMaxBytesLimit(), 2 * average)));
        } else if (average < 0.25 * currentLimit) {
            setRate(Math.max(1, Math.max(config.getMinBytesLimit(), 2 * average)));
        } else if (currentLimit > config.getMaxBytesLimit()) {
            setRate(Math.max(1, config.getMaxBytesLimit()));
        } else if (currentLimit < config.getMinBytesLimit()) {
            setRate(Math.max(1, config.getMinBytesLimit()));
        }
        this.lastCheckSeconds = currentSeconds;
        if (currentLimit != getRate()) {
            logger.info("[updateLimit][{}] {}", identify, getRate());
        }
    }

    @Override
    public int getRate() {
        return (int)this.rateLimiter.getRate();
    }

    @VisibleForTesting
    protected void doAcquire(int permits) {
        rateLimiter.acquire(permits);
    }

    @VisibleForTesting
    protected void setRate(double permitsPerSecond) {
        rateLimiter.setRate(permitsPerSecond);
    }

    public interface ProgressiveSyncRateLimiterConfig {

        int getMinBytesLimit();

        int getMaxBytesLimit();

        int getCheckInterval();

    }

    public interface SystemSecondsProvider {
        long getSystemSeconds();

        SystemSecondsProvider DEFAULT = new SystemSecondsProvider() {
            @Override
            public long getSystemSeconds() {
                return System.currentTimeMillis()/1000;
            }
        };

    }

}
