package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.netty.AbstractNettyHandler;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.exception.ResourceIncorrectException;
import com.ctrip.xpipe.redis.proxy.exception.WriteToClosedSessionException;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosed;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.NotSslRecordException;
import io.netty.util.ReferenceCountUtil;

import static io.netty.util.internal.StringUtil.NEWLINE;

/**
 * @author chen.zhu
 * <p>
 * May 23, 2018
 */
public abstract class AbstractSessionNettyHandler extends AbstractNettyHandler {

    protected Tunnel tunnel;

    protected Session session;

    protected abstract void doMsgTransfer(ByteBuf msg);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if(!(msg instanceof ByteBuf)) {
            logger.error("[channelRead] InCorrect Type: {}", msg.getClass().getName());
            ReferenceCountUtil.release(msg);
            throw new ResourceIncorrectException("Unexpected type for read: {}" + msg.getClass().getName());
        }

        boolean sessionBroken = false;
        try {
            doMsgTransfer((ByteBuf) msg);
        } catch (WriteToClosedSessionException e) {
            logger.info("[forward][forward closed, close] {}", e.getMessage());
            sessionBroken = true;
        } catch (Throwable th) {
            logger.info("[forward][fail, close]", th);
            sessionBroken = true;
        }

        if (sessionBroken) {
            ReferenceCountUtil.release(msg);
            session.release();
            return;
        }

        ctx.fireChannelRead(msg);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        logger.info("[channelInactive]");
        if(session != null && session.getSessionState() != null &&
                !session.getSessionState().equals(new SessionClosed(session))) {
            try {
                session.release();
            } catch (Exception e) {
                logger.error("[channelInactive]", e);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        session.release();
        if(cause instanceof NotSslRecordException) {
            logger.warn("[NotSslRecordException]", cause);
            return;
        }
        super.exceptionCaught(ctx, cause);
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
        logger.debug("[channelWritabilityChanged] writable: {}", ctx.channel().isWritable());

        logger.info("[channelWritabilityChanged] buffer size: {}", ctx.channel().unsafe().outboundBuffer().totalPendingWriteBytes());
        if(ctx.channel().isWritable()) {
            session.setWritableState(Session.SessionWritableState.WRITABLE);
        } else {
            session.setWritableState(Session.SessionWritableState.UNWRITABLE);
        }
        super.channelWritabilityChanged(ctx);
    }

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
