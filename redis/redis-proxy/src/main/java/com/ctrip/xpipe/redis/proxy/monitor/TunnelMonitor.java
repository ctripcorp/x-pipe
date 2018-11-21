package com.ctrip.xpipe.redis.proxy.monitor;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.monitor.stats.TunnelStats;

/**
 * @author chen.zhu
 * <p>
 * Jun 07, 2018
 */
public interface TunnelMonitor extends Startable, Stoppable {

    SessionMonitor getFrontendSessionMonitor();

    SessionMonitor getBackendSessionMonitor();

    TunnelStats getTunnelStats();

    void record(Tunnel tunnel);
}
