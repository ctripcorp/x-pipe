package com.ctrip.xpipe.redis.proxy.tunnel.state;

import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelState;

/**
 * @author chen.zhu
 * <p>
 * May 12, 2018
 */
public class FrontendClosed extends AbstractTunnelState {

    public FrontendClosed(DefaultTunnel tunnel) {
        super(tunnel);
    }

    @Override
    public String name() {
        return "Frontend-Closed";
    }

    @Override
    protected TunnelState doNextAfterSuccess() {
        return new TunnelClosing(tunnel);
    }

    @Override
    protected TunnelState doNextAfterFail() {
        return new TunnelClosing(tunnel);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
