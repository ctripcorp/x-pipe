package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.proxy.exception.ResourceIncorrectException;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author chen.zhu
 * <p>
 * May 22, 2018
 */
public class FrontendSessionNettyHandler extends AbstractSessionNettyHandler {

    private TunnelManager tunnelManager;

    public FrontendSessionNettyHandler(TunnelManager tunnelManager) {
        this.tunnelManager = tunnelManager;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if(msg instanceof ProxyProtocol) {
            logger.info("[channelRead][ProxyProtocol-Received] {}", msg.toString());
            handleProxyProtocol(ctx, msg);
            return;
        }

        if(msg instanceof ByteBuf) {
            if(tunnel != null) {
                tunnel.forwardToBackend((ByteBuf) msg);
            } else {
                logger.error("[doChannelRead] send non-proxy-protocol from channel {}: {} from channel: {}",
                        ChannelUtil.getDesc(ctx.channel()), formatByteBuf("RECEIVE", (ByteBuf) msg));
                ctx.channel().close();
            }
        } else {
            throw new ResourceIncorrectException("Unexpected type for read: {}" + msg.getClass().getName());
        }
        ctx.fireChannelRead(msg);
    }

    private void handleProxyProtocol(ChannelHandlerContext ctx, Object msg) {
        logger.debug("[doChannelRead][ProxyProtocol] {}", msg);
        try {
            tunnel = tunnelManager.create(ctx.channel(), (ProxyProtocol) msg);
            session = tunnel.frontend();
        } catch (Exception e) {
            logger.error("[channelRead] Error when create tunnel: ", e);
            throw e;
        }
    }

}
