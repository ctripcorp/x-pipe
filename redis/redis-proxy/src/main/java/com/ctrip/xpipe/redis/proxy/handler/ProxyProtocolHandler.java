package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.redis.core.proxy.DefaultProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public class ProxyProtocolHandler extends ChannelDuplexHandler {

    private static final Logger logger = LoggerFactory.getLogger(ProxyProtocolHandler.class);

    private TunnelManager tunnelManager;

    public ProxyProtocolHandler(TunnelManager tunnelManager) {
        super();
        this.tunnelManager = tunnelManager;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {

        logger.info("[channelRead] msg: {}", ((ByteBuf)msg).toString(Charset.defaultCharset()));
        ProxyProtocol protocol = new DefaultProxyProtocol().read((ByteBuf)msg);
        tunnelManager.getOrCreate(ctx.channel(), protocol);

        // remove this handler, no need for further data processing
        // (proxy protocol is sent and only sent first time connection established)
        ctx.pipeline().remove(this);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if(msg instanceof ProxyProtocol) {
            ProxyProtocol protocol = (ProxyProtocol) msg;
            ctx.write(protocol.output(), promise);
        }
    }
}
