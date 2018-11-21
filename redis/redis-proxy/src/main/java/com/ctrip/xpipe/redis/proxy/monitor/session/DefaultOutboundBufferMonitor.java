package com.ctrip.xpipe.redis.proxy.monitor.session;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.utils.ChannelUtil;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author chen.zhu
 * <p>
 * Oct 30, 2018
 */
public class DefaultOutboundBufferMonitor implements OutboundBufferMonitor {

    private static final Logger logger = LoggerFactory.getLogger(DefaultOutboundBufferMonitor.class);

    public final static long UNKNOWN_OUTBOUND_BUFFER = -1L;

    private Session session;

    public DefaultOutboundBufferMonitor(Session session) {
        this.session = session;
    }

    private long trackOutboundBuffer(Channel channel) {
        if(!channel.isActive()) {
            logger.warn("[trackOutboundBuffer] channel not active: {}", ChannelUtil.getDesc(channel));
            return UNKNOWN_OUTBOUND_BUFFER;
        }
        return channel.unsafe().outboundBuffer().totalPendingWriteBytes();
    }

    @Override
    public long getOutboundBufferCumulation() {
        return trackOutboundBuffer(session.getChannel());
    }


}
