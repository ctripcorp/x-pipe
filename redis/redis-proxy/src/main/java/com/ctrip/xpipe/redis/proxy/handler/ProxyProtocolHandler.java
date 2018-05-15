package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.netty.AbstractNettyHandler;
import com.ctrip.xpipe.redis.core.exception.ProxyProtocolException;
import com.ctrip.xpipe.redis.core.proxy.DefaultProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public class ProxyProtocolHandler extends AbstractNettyHandler {

    private static final Logger logger = LoggerFactory.getLogger(ProxyProtocolHandler.class);

    private AtomicBoolean protocolProcessed = new AtomicBoolean(false);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        ByteBuf byteBuf = (ByteBuf) msg;
        logger.debug("[channelRead] msg: {}", byteBuf.toString(Charset.defaultCharset()));
        if(isProxyProtocol(byteBuf)) {
            if(!protocolProcessed.get()) {
                logger.info("[channelRead] msg: {}", byteBuf.toString(Charset.defaultCharset()));
                ProxyProtocol protocol = null;
                try {
                    protocol = new DefaultProxyProtocol().read(byteBuf);
                    protocol.recordPath(ctx.channel());
                    protocolProcessed.set(true);
                } catch (Exception e) {
                    logger.error("[channelRead]", e);
                    throw new ProxyProtocolException(e.getMessage());
                }
                byteBuf.release();
                ctx.fireChannelRead(protocol);
            }
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if(msg instanceof ProxyProtocol) {
            ProxyProtocol protocol = (ProxyProtocol) msg;
            ctx.write(protocol.output(), promise);
        } else {
            super.write(ctx, msg, promise);
        }
    }

    @VisibleForTesting
    protected boolean isProxyProtocol(ByteBuf byteBuf) {
        try {
            return byteBuf.toString(Charset.defaultCharset()).toLowerCase().startsWith("+proxy");
        } catch (Exception e) {
            logger.error("[isProxyProtocol]", e);
            return false;
        }
    }
}
