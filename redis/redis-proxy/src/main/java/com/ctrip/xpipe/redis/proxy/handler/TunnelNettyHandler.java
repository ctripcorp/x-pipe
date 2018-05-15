package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.netty.AbstractNettyHandler;
import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.exception.ResourceNotFoundException;
import com.ctrip.xpipe.redis.proxy.session.DefaultSession;
import com.ctrip.xpipe.redis.proxy.session.state.SessionClosing;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;

import java.nio.charset.Charset;

/**
 * @author chen.zhu
 * <p>
 * May 11, 2018
 */
public class TunnelNettyHandler extends AbstractNettyHandler {

    private TunnelManager tunnelManager;

    private Tunnel tunnel;

    public TunnelNettyHandler(TunnelManager tunnelManager) {
        this.tunnelManager = tunnelManager;
    }

    public TunnelNettyHandler(Tunnel tunnel) {
        this.tunnel = tunnel;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if(msg instanceof ProxyProtocol) {
            logger.info("[doChannelRead][ProxyProtocol] {}", msg);
            // block until backend established, or exception when create tunnel
            ctx.channel().config().setAutoRead(false);
            try {
                tunnel = tunnelManager.getOrCreate(ctx.channel(), (ProxyProtocol) msg);
            } catch (Exception e) {
                logger.error("[channelRead] Erro when create tunnel: ", e);
                ctx.channel().config().setAutoRead(true);
            }
            ReferenceCountUtil.release(msg);
            return;
        }
        if(msg instanceof ByteBuf) {

            if(tunnel != null) {
                tunnel.session(ctx.channel()).forward((ByteBuf) msg);
            } else {
                logger.error("[doChannelRead] channel closing: try to send non-proxy-protocol: {} from channel: {}",
                        ((ByteBuf) msg).toString(Charset.defaultCharset()), ChannelUtil.getDesc(ctx.channel()));
                ctx.channel().close();
            }
        } else {
            logger.error("[doChannelRead] Not expect type for read: {}", msg.getClass().getName());
            EventMonitor.DEFAULT.logAlertEvent(String.format("Not expect type for read: %s", msg.getClass().getName()));
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if(tunnel != null) {
            try {
                Session session = tunnel.session(ctx.channel());
                session.setSessionState(new SessionClosing((DefaultSession) session));
            } catch (ResourceNotFoundException e) {
                logger.error("[channelInactive]", e);
            }
        }
        ctx.channel().close();
    }
}
