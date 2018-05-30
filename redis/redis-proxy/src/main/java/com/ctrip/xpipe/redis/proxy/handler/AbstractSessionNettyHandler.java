package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.netty.AbstractNettyHandler;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosing;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;

import static io.netty.util.internal.StringUtil.NEWLINE;

/**
 * @author chen.zhu
 * <p>
 * May 23, 2018
 */
public abstract class AbstractSessionNettyHandler extends AbstractNettyHandler {

    protected Tunnel tunnel;

    protected Session session;

    protected static final int HIGH_WATER_MARK = 1024 * 1024;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ctx.channel().config().setWriteBufferHighWaterMark(HIGH_WATER_MARK);
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.info("[channelInactive]");
        if(tunnel != null && session != null) {
            try {
                session.setSessionState(new SessionClosed(session));
                setTunnelStateWhenSessionClosed();
            } catch (Exception e) {
                logger.error("[channelInactive]", e);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        logger.error("[exceptionCaught] ", cause);
        session.setSessionState(new SessionClosing(session));
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        logger.debug("[channelWritabilityChanged] writable: {}", ctx.channel().isWritable());

        if(ctx.channel().isWritable()) {
            session.onChannelWritable();
        } else {
            session.onChannelNotWritable();
        }
        super.channelWritabilityChanged(ctx);
    }

    protected abstract void setTunnelStateWhenSessionClosed();

    @VisibleForTesting
    protected String formatByteBuf(String eventName, ByteBuf msg) {
        int length = msg.readableBytes();
        if (length == 0) {
            StringBuilder buf = new StringBuilder(eventName.length() + 4);
            buf.append(eventName).append(": 0B");
            return buf.toString();
        } else {
            int rows = length / 16 + (length % 15 == 0? 0 : 1) + 4;
            StringBuilder buf = new StringBuilder(eventName.length() + 2 + 10 + 1 + 2 + rows * 80);

            buf.append(eventName).append(": ").append(length).append('B').append(NEWLINE);
            ByteBufUtil.appendPrettyHexDump(buf, msg);

            return buf.toString();
        }
    }

    protected void setTunnel(Tunnel tunnel) {
        this.tunnel = tunnel;
    }

    protected void setSession(Session session) {
        this.session = session;
    }
}
