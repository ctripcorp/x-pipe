package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.netty.ChannelTrafficStatisticsHandler;
import com.ctrip.xpipe.redis.proxy.Session;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public class TunnelTrafficReporter extends ChannelTrafficStatisticsHandler {

    private Session session;

    public TunnelTrafficReporter(long reportIntervalMillis, Session session) {
        super(reportIntervalMillis);
        this.session = session;
    }

    @Override
    protected void doChannelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    }

    @Override
    protected void doWrite(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
    }

    @Override
    protected void doReportTraffic(long readBytes, long writtenBytes, String remoteIp, int remotePort) {
        if(readBytes > 0) {
            logger.debug("[doReportTraffic] read bytes: {}", readBytes);
            String type = String.format("Tunnel.%s.In.%s", session.getSessionType().name(), session.tunnel().identity());
            String name = String.format("%s:%s", remoteIp, remotePort);
            EventMonitor.DEFAULT.logEvent(type, name, readBytes);
        }
        if(writtenBytes > 0) {
            logger.debug("[doReportTraffic] write bytes: {}", writtenBytes);
            String type = String.format("Tunnel.%s.Out.%s", session.getSessionType().name(), session.tunnel().identity());
            String name = String.format("%s:%s", remoteIp, remotePort);
            EventMonitor.DEFAULT.logEvent(type, name, writtenBytes);
        }
    }
}
