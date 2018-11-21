package com.ctrip.xpipe.redis.proxy.monitor.stats;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.redis.core.proxy.monitor.PingStatsResult;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public interface PingStats extends Startable, Stoppable {

    Endpoint getEndpoint();

    PingStatsResult getPingStatsResult();
}
