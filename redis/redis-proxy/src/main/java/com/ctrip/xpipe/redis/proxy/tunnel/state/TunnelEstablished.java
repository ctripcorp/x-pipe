package com.ctrip.xpipe.redis.proxy.tunnel.state;

import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelState;

/**
 * @author chen.zhu
 * <p>
 * May 12, 2018
 */
public class TunnelEstablished extends AbstractTunnelState {

    public TunnelEstablished(DefaultTunnel tunnel) {
        super(tunnel);
    }

    @Override
    public String name() {
        return "Tunnel-Established";
    }

    @Override
    protected TunnelState doNextAfterSuccess() {
        return new TunnelEstablished(tunnel);
    }

    @Override
    protected TunnelState doNextAfterFail() {
        return new TunnelClosing(tunnel);
    }

    @Override
    public boolean isValidNext(TunnelState tunnelState) {
        return tunnelState.equals(new FrontendClosed(null))
                || tunnelState.equals(new BackendClosed(null))
                || tunnelState.equals(new TunnelEstablished(null))
                || tunnelState.equals(new TunnelClosing(null));
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
