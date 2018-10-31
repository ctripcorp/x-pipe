package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.api.proxy.ProxyRequestResponseProtocol;
import com.ctrip.xpipe.netty.AbstractNettyHandler;
import com.ctrip.xpipe.redis.proxy.listener.ProxyReqResProtocolListener;
import io.netty.channel.ChannelHandlerContext;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public class ProxyReqResProtocolHandler extends AbstractNettyHandler {

    private ProxyReqResProtocolListener listener;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if(!(msg instanceof ProxyRequestResponseProtocol)) {
            ctx.pipeline().remove(this);
            super.channelRead(ctx, msg);
            return;
        }
        ProxyRequestResponseProtocol protocol = (ProxyRequestResponseProtocol) msg;
        listener.onCommand(ctx.channel(), protocol);
    }
}
