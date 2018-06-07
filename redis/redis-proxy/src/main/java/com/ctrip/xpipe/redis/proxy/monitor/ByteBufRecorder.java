package com.ctrip.xpipe.redis.proxy.monitor;

import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * Jun 07, 2018
 */
public interface ByteBufRecorder {

    void recordInbound(ByteBuf msg);

    void recordOutbound(ByteBuf msg);
}
