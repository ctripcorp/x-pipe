package com.ctrip.xpipe.redis.proxy.monitor;

import com.ctrip.xpipe.redis.proxy.Tunnel;

import java.util.Set;

/**
 * @author chen.zhu
 * <p>
 * Jun 07, 2018
 */
public interface TunnelMonitorManager {

    TunnelMonitor getOrCreate(Tunnel tunnel);

    Set<Tunnel> getAllTunnels();

    void remove(Tunnel tunnel);
}
