package com.ctrip.xpipe.redis.keeper.ratelimit.impl;

import com.ctrip.xpipe.AbstractTest;
import com.google.common.util.concurrent.AtomicDouble;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

@RunWith(MockitoJUnitRunner.class)
public class ProgressiveSyncRateLimiterTest extends AbstractTest {

    private ProgressiveSyncRateLimiter.ProgressiveSyncRateLimiterConfig config;

    private AtomicInteger minBytes = new AtomicInteger(100);

    private AtomicInteger maxBytes = new AtomicInteger(1000);

    private AtomicInteger checkInterval = new AtomicInteger(2);

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
    public void testRateIncrease() {
        syncRateLimiter.setRate(minBytes.get());
        Assert.assertEquals(minBytes.get(), syncRateLimiter.getRate());
        syncRateLimiter.acquire(syncRateLimiter.getRate()/2);
        syncRateLimiter.acquire(syncRateLimiter.getRate()/2);
        systemSeconds.incrementAndGet();
        syncRateLimiter.acquire(syncRateLimiter.getRate()/2);
        syncRateLimiter.acquire(syncRateLimiter.getRate()/2);
        systemSeconds.incrementAndGet();
        syncRateLimiter.acquire(syncRateLimiter.getRate()/2);
        syncRateLimiter.acquire(syncRateLimiter.getRate()/2);
        Assert.assertEquals(minBytes.get() * 2, syncRateLimiter.getRate());
        IntStream.range(0, 100).forEach(i -> {
            systemSeconds.incrementAndGet();
            syncRateLimiter.acquire(syncRateLimiter.getRate()/2);
            syncRateLimiter.acquire(syncRateLimiter.getRate()/2);
        });
        Assert.assertEquals(maxBytes.get(), syncRateLimiter.getRate());
    }

    @Test
    public void testRateDecrease() {
        syncRateLimiter.setRate(maxBytes.get());
        Assert.assertEquals(maxBytes.get(), syncRateLimiter.getRate());
        syncRateLimiter.acquire(syncRateLimiter.getRate()/10);
        systemSeconds.incrementAndGet();
        syncRateLimiter.acquire(syncRateLimiter.getRate()/10);
        systemSeconds.incrementAndGet();
        syncRateLimiter.acquire(syncRateLimiter.getRate()/10);
        Assert.assertEquals(maxBytes.get()/5, syncRateLimiter.getRate());
        IntStream.range(0, 100).forEach(i -> {
            systemSeconds.incrementAndGet();
            syncRateLimiter.acquire(syncRateLimiter.getRate()/10);
        });
        Assert.assertEquals(minBytes.get(), syncRateLimiter.getRate());
    }

    @Test
    public void testRateFlat() {
        int rate = (int)(maxBytes.get() * 0.4);
        IntStream.range(0, 1000).forEach(i -> {
            systemSeconds.incrementAndGet();
            syncRateLimiter.acquire(rate);
        });
        Assert.assertEquals(2 * rate, syncRateLimiter.getRate());
    }

    @Test
    public void testTimeRollback() {
        int rate = (int)(maxBytes.get() * 0.5);
        IntStream.range(0, 1000).forEach(i -> {
            systemSeconds.incrementAndGet();
            syncRateLimiter.acquire(rate);
        });
        Assert.assertEquals(2 * rate, syncRateLimiter.getRate());
        systemSeconds.set(0);
        int newRate = (int)(maxBytes.get() * 0.1);
        IntStream.range(0, 1000).forEach(i -> {
            systemSeconds.incrementAndGet();
            syncRateLimiter.acquire(newRate);
            logger.info("[testTimeRollback]{}", syncRateLimiter.getRate());
        });
        Assert.assertEquals(2 * newRate, syncRateLimiter.getRate());
    }

    @Test
    public void testNoDataForAWhile() {
        syncRateLimiter.setRate(maxBytes.get()/2);
        Assert.assertEquals(maxBytes.get()/2, syncRateLimiter.getRate());
        syncRateLimiter.acquire(syncRateLimiter.getRate());
        systemSeconds.addAndGet(100);
        syncRateLimiter.acquire(syncRateLimiter.getRate());
        Assert.assertEquals(maxBytes.get(), syncRateLimiter.getRate());
        int newRate = (int)(maxBytes.get() * 0.1);
        IntStream.range(0, 10).forEach(i -> {
            systemSeconds.incrementAndGet();
            syncRateLimiter.acquire(newRate);
        });
        Assert.assertEquals(2 * newRate, syncRateLimiter.getRate());
    }

    @Test
    public void testConfigChange() {
        IntStream.range(0, 10).forEach(i -> {
            systemSeconds.incrementAndGet();
            syncRateLimiter.acquire(maxBytes.get());
        });
        Assert.assertEquals(maxBytes.get(), syncRateLimiter.getRate());
        maxBytes.set(500);
        IntStream.range(0, 10).forEach(i -> {
            systemSeconds.incrementAndGet();
            syncRateLimiter.acquire(maxBytes.get());
        });
        Assert.assertEquals(maxBytes.get(), syncRateLimiter.getRate());
    }

}
