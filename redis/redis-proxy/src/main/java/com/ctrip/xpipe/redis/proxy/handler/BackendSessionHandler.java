package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.exception.ResourceIncorrectException;
import com.ctrip.xpipe.redis.proxy.exception.WriteToClosedSessionException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;

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
