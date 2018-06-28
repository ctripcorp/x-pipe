package com.ctrip.xpipe.redis.proxy.monitor.impl;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.monitor.ByteBufRecorder;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelMonitor;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelMonitorManager;

/**
 * @author chen.zhu
 * <p>
 * Jun 07, 2018
 */
public class NoneTunnelMonitorManager extends AbstractTunnelMonitorManager implements TunnelMonitorManager {

    @Override
    protected TunnelMonitor createTunnelMonitor(Tunnel tunnel) {
        return new NoneTunnelMonitor();
    }

    public class NoneTunnelMonitor implements TunnelMonitor {

        @Override
        public ByteBufRecorder getByteBufRecorder() {
            return new NoneByteBufRecorder();
        }
    }
}
