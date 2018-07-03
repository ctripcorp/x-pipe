package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.exception.ResourceIncorrectException;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

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
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if(!(msg instanceof ByteBuf)) {
            logger.error("[channelRead] InCorrect Type: {}", msg.getClass().getName());
            throw new ResourceIncorrectException("Unexpected type for read: {}" + msg.getClass().getName());
        }
        tunnel.forwardToFrontend((ByteBuf) msg);

        ctx.fireChannelRead(msg);
    }

}
