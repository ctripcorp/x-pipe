package com.ctrip.xpipe.redis.proxy.monitor;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.redis.proxy.monitor.session.OutboundBufferMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.session.SessionStats;
import com.ctrip.xpipe.redis.proxy.monitor.stats.SocketStats;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
public interface SessionMonitor extends Startable, Stoppable {

    SessionStats getSessionStats();

    OutboundBufferMonitor getOutboundBufferMonitor();

    SocketStats getSocketStats();
}
