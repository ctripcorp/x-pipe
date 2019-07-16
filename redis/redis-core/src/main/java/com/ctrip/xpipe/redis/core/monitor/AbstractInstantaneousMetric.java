package com.ctrip.xpipe.redis.core.monitor;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractInstantaneousMetric implements InstantaneousMetric {

    private static final int STATS_METRIC_SAMPLES = 16;

    private final int statsMetricSamples;

    private long statsCount = 1L;

    private final long[] samples;

    private AtomicInteger index = new AtomicInteger(0);

    public AbstractInstantaneousMetric() {
        this(STATS_METRIC_SAMPLES);
    }

    public AbstractInstantaneousMetric(int statsMetricSamples) {
        this.statsMetricSamples = statsMetricSamples;
        samples = new long[statsMetricSamples];
    }

    @Override
    public long getInstantaneousMetric() {
        long result = 0L;
        for(long sample : samples) {
            result += sample;
        }
        int base = statsCount > statsMetricSamples ? statsMetricSamples : (int) statsCount;
        return result / base;
    }

    @Override
    public void trackInstantaneousMetric(long currentStats) {
        samples[index.getAndIncrement()] = getSample(currentStats);
        index.set(index.get() % (statsMetricSamples - 1));
        statsCount ++;
    }

    protected abstract long getSample(long currentStats);
}
