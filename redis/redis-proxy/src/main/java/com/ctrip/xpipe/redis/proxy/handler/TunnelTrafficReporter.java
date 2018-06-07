package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.netty.ChannelTrafficStatisticsHandler;
import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.controller.ComponentRegistryHolder;
import com.ctrip.xpipe.redis.proxy.monitor.TunnelMonitorManager;
import com.ctrip.xpipe.redis.proxy.spring.Production;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public class TunnelTrafficReporter extends ChannelTrafficStatisticsHandler {

    private Session session;

    private String CAT_TYPE;

    private String CAT_NAME_IN, CAT_NAME_OUT;

    public TunnelTrafficReporter(long reportIntervalMillis, Session session) {
        super(reportIntervalMillis);
        this.session = session;
        this.CAT_TYPE = String.format("Tunnel.%s", session.tunnel().identity());
        this.CAT_NAME_IN = String.format("%s.In", session.getSessionType());
        this.CAT_NAME_OUT = String.format("%s.Out", session.getSessionType());
    }

    @Override
    protected void doChannelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if(msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            ProxyConfig config = ComponentRegistryHolder.getComponentRegistry().getComponent(ProxyConfig.class);
            if (config.debugTunnel()) {
                TunnelMonitorManager manager = (TunnelMonitorManager) ComponentRegistryHolder.getComponentRegistry()
                        .getComponent(Production.TUNNEL_MONITOR_MANAGER);
                manager.getOrCreate(session.tunnel()).getByteBufRecorder().recordInbound(buf);
            }
        }
    }

    @Override
    protected void doWrite(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if(msg instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) msg;
            ProxyConfig config = ComponentRegistryHolder.getComponentRegistry().getComponent(ProxyConfig.class);
            if (config.debugTunnel()) {
                TunnelMonitorManager manager = (TunnelMonitorManager) ComponentRegistryHolder.getComponentRegistry()
                        .getComponent(Production.TUNNEL_MONITOR_MANAGER);
                manager.getOrCreate(session.tunnel()).getByteBufRecorder().recordOutbound(buf);
            }
        }
    }

    @Override
    protected void doReportTraffic(long readBytes, long writtenBytes, String remoteIp, int remotePort) {
        EventMonitor.DEFAULT.logEvent(CAT_TYPE, String.format("%s->%s:%d", session.getSessionType(), remoteIp, remotePort));
        if(readBytes > 0) {
            logger.debug("[doReportTraffic][tunnel-{}][{}] read bytes: {}", session.tunnel().identity(),
                    session.getSessionType(), readBytes);
            EventMonitor.DEFAULT.logEvent(CAT_TYPE, CAT_NAME_IN, readBytes);
        }
        if(writtenBytes > 0) {
            logger.debug("[doReportTraffic][tunnel-{}][{}] write bytes: {}", session.tunnel().identity(),
                    session.getSessionType(), writtenBytes);
            EventMonitor.DEFAULT.logEvent(CAT_TYPE, CAT_NAME_OUT, writtenBytes);
        }
        if(readBytes != writtenBytes) {
            logger.info("[doReportTraffic] read bytes: {}, write bytes: {}", readBytes, writtenBytes);
        }
    }
}
