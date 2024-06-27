package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.api.proxy.ProxyRequestResponseProtocol;
import com.ctrip.xpipe.redis.core.proxy.parser.AbstractProxyOptionParser;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.handler.response.ProxyReqResProtocolHandlerManager;
import com.ctrip.xpipe.redis.proxy.monitor.stats.PingStatsManager;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.tunnel.TunnelManager;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

public class ProxyProtocolHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(ProxyProtocolHandler.class);

    private TunnelManager tunnelManager;

    private Tunnel tunnel;

    private ProxyReqResProtocolHandlerManager protocolHandlerManager;

    public ProxyProtocolHandler(TunnelManager tunnelManager, ResourceManager resourceManager,
                                PingStatsManager pingStatsManager) {
        this.tunnelManager = tunnelManager;
        this.protocolHandlerManager = new ProxyReqResProtocolHandlerManager(resourceManager,
                tunnelManager, pingStatsManager);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if(!(msg instanceof ProxyProtocol)) {
            logger.error("[channelRead] not proxy protocol, class: {}", msg.getClass());
        }
        handleProxyProtocol(ctx, msg);
    }

    private void handleProxyProtocol(ChannelHandlerContext ctx, Object msg) {
        logger.debug("[doChannelRead][ProxyProtocol][{}] {}", ChannelUtil.getDesc(ctx.channel()), msg);
        if(msg instanceof ProxyConnectProtocol) {
            ProxyConnectProtocol protocol = (ProxyConnectProtocol) msg;
            logger.info("[channelRead][ProxyProtocol-Received][{}] {}", ChannelUtil.getDesc(ctx.channel()), protocol.toString());
            if (ctx.channel().remoteAddress() instanceof InetSocketAddress) {
                protocol.recordForwardFor((InetSocketAddress) ctx.channel().remoteAddress());
            }
            tunnel = tunnelManager.create(ctx.channel(), protocol);
            uninstallSelf(ctx);
        } else if(msg instanceof ProxyRequestResponseProtocol) {
            ProxyRequestResponseProtocol protocol = (ProxyRequestResponseProtocol) msg;
            logger.debug("[ProxyRequestResponseProtocol][{}] {}", ChannelUtil.getDesc(ctx.channel()), protocol.getContent());
            long start = System.currentTimeMillis();
            protocolHandlerManager.handle(ctx.channel(),
                    StringUtil.splitRemoveEmpty(AbstractProxyOptionParser.ELEMENT_SPLITTER, protocol.getContent()));
            logger.debug("[ProxyRequestResponseProtocol][{}] {}; duration: {}", ChannelUtil.getDesc(ctx.channel()),
                    protocol.getContent(), System.currentTimeMillis() - start);
        }
    }

    private void uninstallSelf(ChannelHandlerContext ctx) {
        if(tunnel == null) {
            logger.error("[initChannel] tunnel should not be null");
            return;
        }
        ChannelPipeline pipeline = ctx.channel().pipeline();
        pipeline.remove(this);
    }
}
