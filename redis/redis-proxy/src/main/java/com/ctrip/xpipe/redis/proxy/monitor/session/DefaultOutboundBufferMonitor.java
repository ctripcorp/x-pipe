package com.ctrip.xpipe.redis.proxy.monitor.session;

import com.ctrip.xpipe.redis.proxy.Session;
import io.netty.channel.Channel;


/**
 * @author chen.zhu
 * <p>
 * Oct 30, 2018
 */
public class DefaultOutboundBufferMonitor implements OutboundBufferMonitor {

    private Session session;

    public DefaultOutboundBufferMonitor(Session session) {
        this.session = session;
    }

    private long trackOutboundBuffer(Channel channel) {
        return channel.unsafe().outboundBuffer().totalPendingWriteBytes();
    }

    @Override
    public long getOutboundBufferCumulation() {
        return trackOutboundBuffer(session.getChannel());
    }


}
