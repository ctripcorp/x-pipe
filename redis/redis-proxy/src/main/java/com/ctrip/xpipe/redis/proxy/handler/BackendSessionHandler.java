package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * May 23, 2018
 */
public class BackendSessionHandler extends AbstractSessionNettyHandler {

    public BackendSessionHandler(Tunnel tunnel) {
        super.tunnel = tunnel;
        super.session = tunnel.backend();
    }

    @Override
    protected void doMsgTransfer(ByteBuf msg) {
        tunnel.forwardToFrontend(msg);
    }

}
