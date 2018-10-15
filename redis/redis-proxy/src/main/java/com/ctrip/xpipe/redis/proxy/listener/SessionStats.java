package com.ctrip.xpipe.redis.proxy.listener;

import io.netty.buffer.ByteBuf;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Sep 25, 2018
 */
public interface SessionStats {

    void sessionRead(ByteBuf msg);

    void sessionWrite(ByteBuf msg);

    long getSessionCurrentQPS();

    long getSessionPeakQPS();

    List<AutoReadEvent> getAutoReadEvents();

    class AutoReadEvent {
        long startTime;
        long endTime;
    }
}
