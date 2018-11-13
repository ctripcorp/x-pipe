package com.ctrip.xpipe.redis.proxy.monitor.stats;


import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.redis.core.proxy.monitor.SocketStatsResult;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public interface SocketStats extends Startable, Stoppable {

    SocketStatsResult getSocketStatsResult();

}
