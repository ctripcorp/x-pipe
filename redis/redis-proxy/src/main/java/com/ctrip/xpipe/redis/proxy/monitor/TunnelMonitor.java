package com.ctrip.xpipe.redis.proxy.monitor;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @author chen.zhu
 * <p>
 * Jun 07, 2018
 */
public interface TunnelMonitor extends Startable, Stoppable {

    SessionMonitor getFrontendSessionMonitor();

    SessionMonitor getBackendSessionMonitor();

}
