package com.ctrip.xpipe.redis.proxy.monitor;

import com.ctrip.xpipe.redis.proxy.Tunnel;

/**
 * @author chen.zhu
 * <p>
 * Jun 07, 2018
 */
public interface TunnelMonitorManager {

    TunnelMonitor getOrCreate(Tunnel tunnel);

    void remove(Tunnel tunnel);
}
