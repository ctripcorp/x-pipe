package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.netty.ChannelTrafficStatisticsHandler;
import com.ctrip.xpipe.redis.proxy.Session;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public class DataStreamHandler extends ChannelTrafficStatisticsHandler {

    private Session session;

    public DataStreamHandler(long reportIntervalMillis, Session session) {
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

    }
}
