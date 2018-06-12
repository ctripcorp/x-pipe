package com.ctrip.xpipe.redis.proxy.monitor.impl;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.monitor.ByteBufRecorder;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelMonitor;

/**
 * @author chen.zhu
 * <p>
 * Jun 07, 2018
 */
public class DebugTunnelMonitor implements TunnelMonitor {

    private Tunnel tunnel;

    public DebugTunnelMonitor(Tunnel tunnel) {
        this.tunnel = tunnel;
    }

    @Override
    public ByteBufRecorder getByteBufRecorder() {
        return new DebugByteBufRecorder(tunnel);
    }
}
