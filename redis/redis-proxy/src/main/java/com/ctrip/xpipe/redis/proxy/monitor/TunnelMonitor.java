package com.ctrip.xpipe.redis.proxy.monitor;

import com.ctrip.xpipe.redis.proxy.monitor.stats.SocketStats;
import com.ctrip.xpipe.redis.proxy.monitor.stats.TunnelStats;

/**
 * @author chen.zhu
 * <p>
 * Jun 07, 2018
 */
public interface TunnelMonitor {

    TunnelStats getTunnelStats();

    SocketStats getSocketStats();

}
