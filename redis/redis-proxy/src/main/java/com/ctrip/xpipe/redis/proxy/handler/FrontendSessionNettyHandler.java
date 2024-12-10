package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.exception.ResourceIncorrectException;
import com.ctrip.xpipe.redis.proxy.exception.WriteToClosedSessionException;
import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;


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
