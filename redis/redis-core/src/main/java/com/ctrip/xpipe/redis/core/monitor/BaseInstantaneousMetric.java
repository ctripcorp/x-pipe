package com.ctrip.xpipe.redis.core.monitor;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author chen.zhu
 * <p>
 * Oct 22, 2018
 */
public class BaseInstantaneousMetric implements InstantaneousMetric {

    private static final int STATS_METRIC_SAMPLES = 16;

    private long statsCount = 1L;

    private volatile long lastSampleTime = System.currentTimeMillis();

    private volatile long lastSampleCount = 0L;

    private long[] samples = new long[STATS_METRIC_SAMPLES];

    private AtomicInteger index = new AtomicInteger(0);

    @Override
    public long getInstantaneousMetric() {
        long result = 0L;
        for(long sample : samples) {
            result += sample;
        }
        int base = statsCount > STATS_METRIC_SAMPLES ? STATS_METRIC_SAMPLES : (int) statsCount;
        return result / base;
    }

    @Override
    public void trackInstantaneousMetric(long currentStats) {
        long currentTime = System.currentTimeMillis();
        long interval = currentTime - lastSampleTime;
        long delta = currentStats - lastSampleCount;
        lastSampleTime = currentTime;
        lastSampleCount = currentStats;
        samples[index.getAndIncrement()] = interval > 0 ? (delta * 1000)/interval : 0;
        index.set(index.get() % (STATS_METRIC_SAMPLES - 1));
        statsCount ++;
    }

}
