package com.ctrip.xpipe.redis.proxy.monitor.impl;

import com.ctrip.xpipe.api.factory.ObjectFactory;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelMonitorManager;
import com.ctrip.xpipe.utils.MapUtils;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Jun 07, 2018
 */
public abstract class AbstractTunnelMonitorManager implements TunnelMonitorManager {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    private Map<Tunnel, TunnelMonitor> tunnelMonitors = Maps.newConcurrentMap();

    @Override
    public TunnelMonitor getOrCreate(Tunnel tunnel) {
        return MapUtils.getOrCreate(tunnelMonitors, tunnel, new ObjectFactory<TunnelMonitor>() {
            @Override
            public TunnelMonitor create() {
                return createTunnelMonitor(tunnel);
            }
        });
    }

    protected abstract TunnelMonitor createTunnelMonitor(Tunnel tunnel);

    @Override
    public void remove(Tunnel tunnel) {
        tunnelMonitors.remove(tunnel);
    }
}
