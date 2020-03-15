package com.ctrip.xpipe.redis.core.monitor;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractInstantaneousMetric implements InstantaneousMetric {

    private static final int STATS_METRIC_SAMPLES = 16;

    private final int statsMetricSamples;

    private final static long INIT_STATE = -1L;
    private long statsCount = INIT_STATE;

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
        if (statsCount < 1) {
            return 0;
        }
        long result = 0L;
        for(long sample : samples) {
            result += sample;
        }
        long base = statsCount > statsMetricSamples ? statsMetricSamples : statsCount;
        return result / base;
    }

    @Override
    public void trackInstantaneousMetric(long currentStats) {
        int idx = index.getAndIncrement() % statsMetricSamples;
        samples[idx] = getSample(currentStats);
        if (statsCount == INIT_STATE) {
            index.set(0);
        } else {
            index.set(index.get() % statsMetricSamples);
        }
        statsCount ++;
    }

    protected abstract long getSample(long currentStats);
}
