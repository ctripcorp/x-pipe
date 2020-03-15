package com.ctrip.xpipe.redis.core.monitor;

/**
 * @author chen.zhu
 * <p>
 * Oct 22, 2018
 */
public class BaseInstantaneousMetric extends AbstractInstantaneousMetric {

    private final long INIT_VALUE = -1;
    private volatile long lastSampleTime = INIT_VALUE;

    private volatile long lastSampleCount = 0L;

    @Override
    protected long getSample(long currentStats) {
        long currentTime = System.currentTimeMillis();
        long interval = 0, delta = 0;
        if (lastSampleTime != INIT_VALUE) {
            interval = currentTime - lastSampleTime;
            delta = currentStats - lastSampleCount;
        }
        lastSampleTime = currentTime;
        lastSampleCount = currentStats;
        return interval > 0 ? (delta * 1000) /interval : 0;
    }

}
