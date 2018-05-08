package com.ctrip.xpipe.redis.proxy.session;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public interface SessionState {

    ChannelFuture tryWrite(ByteBuf byteBuf);

    ChannelFuture tryConnect();

    void disconnect();

}
