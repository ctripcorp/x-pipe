package com.ctrip.xpipe.redis.proxy.buffer;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * May 13, 2018
 */
public interface BufferStore extends Releasable {

    void offer(ByteBuf byteBuf);

    ByteBuf poll();

    boolean isEmpty();

    void clearAndSend();
}
