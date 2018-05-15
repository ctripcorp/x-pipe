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
public class TunnelHalfEstablished extends AbstractTunnelState {

    public TunnelHalfEstablished(DefaultTunnel tunnel) {
        super(tunnel);
    }

    @Override
    public String name() {
        return "Half-Established";
    }

    @Override
    public void forward(ByteBuf message, Session src) {
        throw new UnsupportedOperationException("Front channel auto-read should be false");
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
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}
