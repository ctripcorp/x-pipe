package com.ctrip.xpipe.redis.core.monitor;

public class InstantaneousCounterMetric extends AbstractInstantaneousMetric {

    @Override
    protected long getSample(long currentStats) {
        return currentStats;
    }
}
