package com.ctrip.xpipe.redis.proxy.monitor.session;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
public interface SessionStats extends Startable, Stoppable {

    void increaseInputBytes(long bytes);

    void increaseOutputBytes(long bytes);

    long lastUpdateTime();

    long getInputBytes();

    long getOutputBytes();

    long getInputInstantaneousBPS();

    long getOutputInstantaneousBPS();
}
