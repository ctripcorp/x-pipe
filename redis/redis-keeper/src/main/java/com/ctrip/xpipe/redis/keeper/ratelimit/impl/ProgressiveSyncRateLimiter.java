package com.ctrip.xpipe.redis.keeper.ratelimit.impl;

import com.ctrip.xpipe.redis.core.store.ratelimit.SyncRateLimiter;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;

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

    private static final Logger logger = LoggerFactory.getLogger(ProgressiveSyncRateLimiter.class);

    private final Deque<CheckDecision> checkDecisions = new ArrayDeque<>();

    private boolean lastRateLimitEnabled;

    public ProgressiveSyncRateLimiter(Object identify, ProgressiveSyncRateLimiterConfig config) {
        this(identify, config, SystemSecondsProvider.DEFAULT);
    }

    public ProgressiveSyncRateLimiter(Object identify, ProgressiveSyncRateLimiterConfig config, SystemSecondsProvider systemSecondsProvider) {
        this.systemSecondsProvider = systemSecondsProvider;
        this.identify = identify;
        this.config = config;
        this.rateLimiter = RateLimiter.create(baseBytesLimits());
        this.records = new int[Math.max(1, config.getCheckInterval())];
        this.lastRateLimitEnabled = config.isRateLimitEnabled();
        reset(systemSecondsProvider.getSystemSeconds());
    }

    private int baseBytesLimits() {
        return Math.max(config.getMaxBytesLimit(), 1);
    }

    @Override
    public void acquire(int syncByte) {
        if (syncByte <= 0) return;
        boolean rateLimitEnabled = config.isRateLimitEnabled();
        if (!rateLimitEnabled) {
            lastRateLimitEnabled = false;
            return;
        }

        long currentSeconds = systemSecondsProvider.getSystemSeconds();
        if (!lastRateLimitEnabled) {
            reset(currentSeconds);
            lastRateLimitEnabled = true;
        }

        doAcquire(syncByte);

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
        checkDecisions.clear();
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

        Direction direction = decideDirection(average, currentLimit);
        recordDecision(direction, average);
        if (direction == Direction.INCREASE) {
            int rounds = Math.max(1, config.getIncreaseCheckRounds());
            if (checkDecisions.size() >= rounds) {
                long maxAverage = maxAverage(rounds);
                setRate(Math.max(1, Math.min(config.getMaxBytesLimit(), 2 * maxAverage)));
            }
        } else if (direction == Direction.DECREASE) {
            int rounds = Math.max(1, config.getDecreaseCheckRounds());
            if (checkDecisions.size() >= rounds) {
                long maxAverage = maxAverage(rounds);
                setRate(Math.max(1, Math.max(config.getMinBytesLimit(), 2 * maxAverage)));
            }
        }
        if (getRate() > config.getMaxBytesLimit()) {
            setRate(Math.max(1, config.getMaxBytesLimit()));
        } else if (getRate() < config.getMinBytesLimit()) {
            setRate(Math.max(1, config.getMinBytesLimit()));
        }
        if (currentLimit != getRate()) {
            logger.info("[updateLimit][{}] {}", identify, getRate());
        }

        ensureRecordsSize();
        this.lastCheckSeconds = currentSeconds;
    }

    private void ensureRecordsSize() {
        int checkInterval = Math.max(1, config.getCheckInterval());
        if (checkInterval == records.length) return;

        logger.info("[updateRecordsSize][{}] {} -> {}", identify, records.length, checkInterval);
        records = new int[checkInterval];
        recordPos = 0;
    }

    private Direction decideDirection(long average, long currentLimit) {
        if (average > 0.75 * currentLimit) return Direction.INCREASE;
        if (average < 0.25 * currentLimit) return Direction.DECREASE;
        return Direction.FLAT;
    }

    private void recordDecision(Direction direction, long average) {
        if (direction == Direction.FLAT) {
            checkDecisions.clear();
            return;
        }
        if (!checkDecisions.isEmpty()) {
            CheckDecision last = checkDecisions.peekLast();
            if (last.direction != direction) {
                checkDecisions.clear();
            }
        }
        checkDecisions.addLast(new CheckDecision(direction, average));
    }

    private long maxAverage(int rounds) {
        long max = 0;
        int count = 0;
        for (Iterator<CheckDecision> it = checkDecisions.descendingIterator(); it.hasNext() && count < rounds; ) {
            max = Math.max(max, it.next().average);
            count++;
        }
        return max;
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

        int getIncreaseCheckRounds();

        int getDecreaseCheckRounds();

        boolean isRateLimitEnabled();

    }

    private enum Direction {
        INCREASE,
        DECREASE,
        FLAT
    }

    private static class CheckDecision {
        private final Direction direction;
        private final long average;

        private CheckDecision(Direction direction, long average) {
            this.direction = direction;
            this.average = average;
        }
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
