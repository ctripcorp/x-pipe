package com.ctrip.xpipe.redis.console.healthcheck.actions.interaction;

import com.ctrip.xpipe.redis.console.healthcheck.actions.delay.DelayActionListener;
import com.ctrip.xpipe.redis.console.healthcheck.actions.ping.PingActionListener;

/**
 * @author chen.zhu
 * <p>
 * Oct 11, 2018
 */
public interface DelayPingActionCollector {

    PingActionListener createPingActionListener();

    DelayActionListener createDelayActionListener();

}
