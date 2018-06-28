package com.ctrip.xpipe.redis.proxy.monitor.impl;

import com.ctrip.xpipe.redis.proxy.Tunnel;
import com.ctrip.xpipe.redis.proxy.monitor.ByteBufRecorder;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author chen.zhu
 * <p>
 * Jun 07, 2018
 */
public class DebugByteBufRecorder implements ByteBufRecorder {

    private static final String LOGGER_NAME = "com.ctrip.xpipe.redis.proxy.monitor.recorder";

    private static final Logger logger = LoggerFactory.getLogger(LOGGER_NAME);

    private Tunnel tunnel;

    public DebugByteBufRecorder(Tunnel tunnel) {
        this.tunnel = tunnel;
    }

    @Override
    public void recordInbound(ByteBuf msg) {
        logger.info("[{}][in]{}", tunnel.identity(), copyString(msg));
    }

    @Override
    public void recordOutbound(ByteBuf msg) {
        logger.info("[{}][out]{}", tunnel.identity(), copyString(msg));
    }


    private String copyString(ByteBuf buf) {
        byte[] bytes = new byte[buf.readableBytes()];
        int readerIndex = buf.readerIndex();
        buf.getBytes(readerIndex, bytes);
        return new String(bytes);
    }
}
