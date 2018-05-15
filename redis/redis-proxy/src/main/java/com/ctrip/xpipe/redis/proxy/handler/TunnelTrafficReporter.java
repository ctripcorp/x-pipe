package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.netty.ChannelTrafficStatisticsHandler;
import com.ctrip.xpipe.redis.proxy.Tunnel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
public class TunnelTrafficReporter extends ChannelTrafficStatisticsHandler {

    private Tunnel tunnel;

    public TunnelTrafficReporter(long reportIntervalMillis, Tunnel tunnel) {
        super(reportIntervalMillis);
        this.tunnel = tunnel;
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
            logger.info("[doReportTraffic] read bytes: {}", readBytes);
            String type = String.format("Tunnel.In.%s", tunnel.identity());
            String name = String.format("%s:%s", remoteIp, remotePort);
            EventMonitor.DEFAULT.logEvent(type, name, readBytes);
        }
        if(writtenBytes > 0) {
            logger.info("[doReportTraffic] write bytes: {}", writtenBytes);
            String type = String.format("Tunnel.Out.%s", tunnel.identity());
            String name = String.format("%s:%s", remoteIp, remotePort);
            EventMonitor.DEFAULT.logEvent(type, name, writtenBytes);
        }
    }
}
