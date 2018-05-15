package com.ctrip.xpipe.redis.proxy.tunnel.state;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.tunnel.DefaultTunnel;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelState;
import io.netty.buffer.ByteBuf;

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
        return "Established";
    }

    @Override
    public void forward(ByteBuf message, Session src) {
        tunnel.doForward(message, src);
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
}
