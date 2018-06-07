package com.ctrip.xpipe.redis.proxy.monitor.impl;

import com.ctrip.xpipe.redis.proxy.monitor.ByteBufRecorder;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * Jun 07, 2018
 */
public class NoneByteBufRecorder implements ByteBufRecorder {
    @Override
    public void recordInbound(ByteBuf msg) {
        //do nothing
    }

    @Override
    public void recordOutbound(ByteBuf msg) {
        //do nothing
    }
}
