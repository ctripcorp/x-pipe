package com.ctrip.xpipe.redis.keeper.monitor;

/**
 * @author chen.zhu
 * <p>
 * Oct 22, 2018
 */
public interface InstantaneousMetric {

    long getInstantaneousMetric();

    void trackInstantaneousMetric(long currentStats);
}
