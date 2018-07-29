package com.ctrip.xpipe.redis.proxy.tunnel.state;

import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelState;

/**
 * @author chen.zhu
 * <p>
 * May 12, 2018
 */
public class TunnelClosing extends AbstractTunnelState {

    public TunnelClosing(DefaultTunnel tunnel) {
        super(tunnel);
    }

    @Override
    public String name() {
        return "Tunnel-Closing";
    }

    @Override
    protected TunnelState doNextAfterSuccess() {
        return new TunnelClosed(tunnel);
    }

    @Override
    protected TunnelState doNextAfterFail() {
        return new TunnelClosing(tunnel);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }
}
