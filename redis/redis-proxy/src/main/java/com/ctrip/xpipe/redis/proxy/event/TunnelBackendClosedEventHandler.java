package com.ctrip.xpipe.redis.proxy.event;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelStateChangeEvent;
import com.ctrip.xpipe.redis.proxy.tunnel.state.TunnelClosing;

/**
 * @author chen.zhu
 * <p>
 * May 29, 2018
 */
public class TunnelBackendClosedEventHandler extends AbstractTunnelEventHandler {

    public TunnelBackendClosedEventHandler(Tunnel tunnel, TunnelStateChangeEvent event) {
        super(tunnel, event);
    }

    @Override
    protected void doHandle() {
        tunnel.frontend().release();
        tunnel.setState(new TunnelClosing((DefaultTunnel) tunnel));
    }
}
