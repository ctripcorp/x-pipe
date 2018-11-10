package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.exception.ResourceIncorrectException;
import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;


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
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        if(msg instanceof ByteBuf) {
            if(tunnel != null) {
                tunnel.forwardToBackend((ByteBuf) msg);
            } else {
                logger.error("[doChannelRead] send non-proxy-protocol from channel {}: {} from channel: {}",
                        ChannelUtil.getDesc(ctx.channel()), formatByteBuf("RECEIVE", (ByteBuf) msg));
                ctx.channel().close();
            }
        } else {
            throw new ResourceIncorrectException("Unexpected type for read: " + msg.getClass().getName());
        }
        ctx.fireChannelRead(msg);
    }

}
