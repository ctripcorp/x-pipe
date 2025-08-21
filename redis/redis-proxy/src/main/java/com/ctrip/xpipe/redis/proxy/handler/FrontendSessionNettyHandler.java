package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import io.netty.buffer.ByteBuf;


/**
 * @author chen.zhu
 * <p>
 * May 22, 2018
 */
public class FrontendSessionNettyHandler extends AbstractSessionNettyHandler {

    public FrontendSessionNettyHandler(Tunnel tunnel) {
        this.tunnel = tunnel;
        this.setSession(tunnel.frontend());
    }

    @Override
    protected void doMsgTransfer(ByteBuf msg) {
        tunnel.forwardToBackend(msg);
    }

}
