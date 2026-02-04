package com.ctrip.xpipe.redis.keeper.ratelimit.impl;

import com.ctrip.xpipe.AbstractTest;
import com.google.common.util.concurrent.AtomicDouble;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@RunWith(MockitoJUnitRunner.class)
public class ProgressiveSyncRateLimiterTest extends AbstractTest {

    private ProgressiveSyncRateLimiter.ProgressiveSyncRateLimiterConfig config;

    private AtomicInteger minBytes = new AtomicInteger(100);

    private AtomicInteger maxBytes = new AtomicInteger(1000);

    private AtomicInteger checkInterval = new AtomicInteger(2);

    private AtomicInteger increaseCheckRounds = new AtomicInteger(2);

    private AtomicInteger decreaseCheckRounds = new AtomicInteger(3);

    private AtomicBoolean rateLimitEnabled = new AtomicBoolean(true);

    private AtomicDouble limit = new AtomicDouble(0);

    private ProgressiveSyncRateLimiter.SystemSecondsProvider systemSecondsProvider;

    private AtomicLong systemSeconds = new AtomicLong(0);

    private ProgressiveSyncRateLimiter syncRateLimiter;

    @Before
    public void beforeProgressiveSyncRateLimiterTest() throws Exception {
        config = new ProgressiveSyncRateLimiter.ProgressiveSyncRateLimiterConfig() {
            @Override
            public int getMinBytesLimit() {
                return minBytes.get();
            }

            @Override
            public int getMaxBytesLimit() {
                return maxBytes.get();
            }

            @Override
            public int getCheckInterval() {
                return checkInterval.get();
            }

            @Override
            public int getIncreaseCheckRounds() {
                return increaseCheckRounds.get();
            }

            @Override
            public int getDecreaseCheckRounds() {
                return decreaseCheckRounds.get();
            }

            @Override
            public boolean isRateLimitEnabled() {
                return rateLimitEnabled.get();
            }
        };

        systemSecondsProvider = new ProgressiveSyncRateLimiter.SystemSecondsProvider() {
            @Override
            public long getSystemSeconds() {
                return systemSeconds.get();
            }
        };

        syncRateLimiter = new ProgressiveSyncRateLimiter(new Object(), config, systemSecondsProvider) {
            @Override
            protected void doAcquire(int permits) {
            }

            @Override
            protected void setRate(double permitsPerSecond) {
                limit.set(permitsPerSecond);
            }

            @Override
            public int getRate() {
                return (int)limit.get();
            }
        };
    }

    @Test
    public void testRateIncreaseWithConsecutiveRounds() {
        syncRateLimiter.setRate(minBytes.get());
        Assert.assertEquals(minBytes.get(), syncRateLimiter.getRate());

        runSeconds(3, minBytes.get());
        Assert.assertEquals(minBytes.get(), syncRateLimiter.getRate());

        runSeconds(2, minBytes.get());
        Assert.assertEquals(minBytes.get() * 2, syncRateLimiter.getRate());
    }

    @Test
    public void testRateDecreaseWithConsecutiveRounds() {
        syncRateLimiter.setRate(maxBytes.get());
        Assert.assertEquals(maxBytes.get(), syncRateLimiter.getRate());

        runSeconds(3, minBytes.get());
        Assert.assertEquals(maxBytes.get(), syncRateLimiter.getRate());

        runSeconds(2, minBytes.get());
        Assert.assertEquals(maxBytes.get(), syncRateLimiter.getRate());

        runSeconds(2, minBytes.get());
        Assert.assertEquals(minBytes.get() * 2, syncRateLimiter.getRate());
    }

    @Test
    public void testRateNoDecreaseWithFluctuation() {
        decreaseCheckRounds.set(3);
        syncRateLimiter.setRate(maxBytes.get());
        Assert.assertEquals(maxBytes.get(), syncRateLimiter.getRate());

        runSeconds(2, minBytes.get());
        runSeconds(2, maxBytes.get());
        runSeconds(2, minBytes.get());
        runSeconds(2, maxBytes.get());

        Assert.assertEquals(maxBytes.get(), syncRateLimiter.getRate());
    }

    @Test
    public void testRateClampToMax() {
        increaseCheckRounds.set(1);
        checkInterval.set(2);
        maxBytes.set(300);

        syncRateLimiter.setRate(minBytes.get());
        Assert.assertEquals(minBytes.get(), syncRateLimiter.getRate());

        runSeconds(3, maxBytes.get());
        Assert.assertEquals(maxBytes.get(), syncRateLimiter.getRate());
    }

    @Test
    public void testRateLimitDisabledThenEnabledResets() throws Exception {
        syncRateLimiter.setRate(minBytes.get());
        Assert.assertEquals(minBytes.get(), syncRateLimiter.getRate());

        rateLimitEnabled.set(false);
        runSeconds(5, maxBytes.get());
        Assert.assertEquals(minBytes.get(), syncRateLimiter.getRate());
        Assert.assertEquals(0, getRecordsSum());

        rateLimitEnabled.set(true);
        syncRateLimiter.acquire(minBytes.get());
        Assert.assertEquals(maxBytes.get(), syncRateLimiter.getRate());
        Assert.assertEquals(0, getRecordsSum());
    }

    @Test
    public void testConfigChangeUpdatesCheckInterval() throws Exception {
        Assert.assertEquals(2, getRecordsLength());

        runSeconds(3, minBytes.get());
        Assert.assertEquals(2, getRecordsLength());

        checkInterval.set(4);
        runSeconds(2, minBytes.get());
        Assert.assertEquals(4, getRecordsLength());
    }

    @Test
    public void testConfigChangeUpdatesIncreaseRounds() {
        increaseCheckRounds.set(3);
        syncRateLimiter.setRate(minBytes.get());
        Assert.assertEquals(minBytes.get(), syncRateLimiter.getRate());

        runSeconds(5, minBytes.get());
        Assert.assertEquals(minBytes.get(), syncRateLimiter.getRate());

        increaseCheckRounds.set(1);
        runSeconds(2, minBytes.get());
        Assert.assertEquals(minBytes.get() * 2, syncRateLimiter.getRate());
    }

    @Test
    public void testConfigChangeUpdatesDecreaseRounds() {
        decreaseCheckRounds.set(4);
        syncRateLimiter.setRate(maxBytes.get());
        Assert.assertEquals(maxBytes.get(), syncRateLimiter.getRate());

        runSeconds(5, minBytes.get());
        Assert.assertEquals(maxBytes.get(), syncRateLimiter.getRate());

        decreaseCheckRounds.set(2);
        runSeconds(2, minBytes.get());
        Assert.assertEquals(minBytes.get() * 2, syncRateLimiter.getRate());
    }

    private void runSeconds(int seconds, int bytesPerSecond) {
        for (int i = 0; i < seconds; i++) {
            syncRateLimiter.acquire(bytesPerSecond);
            systemSeconds.incrementAndGet();
        }
    }

    private int getRecordsLength() throws Exception {
        Field recordsField = ProgressiveSyncRateLimiter.class.getDeclaredField("records");
        recordsField.setAccessible(true);
        int[] records = (int[]) recordsField.get(syncRateLimiter);
        return records.length;
    }

    private int getRecordsSum() throws Exception {
        Field recordsField = ProgressiveSyncRateLimiter.class.getDeclaredField("records");
        recordsField.setAccessible(true);
        int[] records = (int[]) recordsField.get(syncRateLimiter);
        int sum = 0;
        for (int record : records) {
            sum += record;
        }
        return sum;
    }
}
