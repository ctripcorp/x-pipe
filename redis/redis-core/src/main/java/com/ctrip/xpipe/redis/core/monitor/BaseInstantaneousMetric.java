package com.ctrip.xpipe.redis.core.monitor;

/**
 * @author chen.zhu
 * <p>
 * Oct 22, 2018
 */
public class BaseInstantaneousMetric extends AbstractInstantaneousMetric {

    private volatile long lastSampleTime = System.currentTimeMillis();

    private volatile long lastSampleCount = 0L;

    @Override
    protected long getSample(long currentStats) {
        long currentTime = System.currentTimeMillis();
        long interval = currentTime - lastSampleTime;
        long delta = currentStats - lastSampleCount;
        lastSampleTime = currentTime;
        lastSampleCount = currentStats;
        return interval > 0 ? (delta * 1000)/interval : 0;
    }

}
