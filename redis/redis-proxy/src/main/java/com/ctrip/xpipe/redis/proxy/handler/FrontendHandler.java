package com.ctrip.xpipe.redis.proxy.handler;

import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.netty.ChannelTrafficStatisticsHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public class FrontendHandler extends ChannelTrafficStatisticsHandler implements Observer {

    public FrontendHandler(long reportIntervalMillis) {
        super(reportIntervalMillis);
    }

    @Override
    public void update(Object args, Observable observable) {

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
