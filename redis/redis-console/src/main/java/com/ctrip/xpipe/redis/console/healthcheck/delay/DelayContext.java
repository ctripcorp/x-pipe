package com.ctrip.xpipe.redis.console.healthcheck.delay;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.redis.console.healthcheck.action.HEALTH_STATE;

/**
 * @author chen.zhu
 * <p>
 * Aug 24, 2018
 */
public interface DelayContext extends Lifecycle {

    public static final long SAMPLE_LOST_AND_NO_PONG = -99999L * 1000 * 1000;

    public static final long SAMPLE_LOST_BUT_PONG = 99999L * 1000 * 1000;

    long lastTimeDelayMilli();

    long lastDelayNano();

    long lastDelayPubTimeNano();

    boolean isHealthy();

    HEALTH_STATE getHealthState();

}
