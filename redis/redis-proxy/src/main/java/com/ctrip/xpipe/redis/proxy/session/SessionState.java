package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.redis.proxy.State;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public interface SessionState extends State<SessionState> {

    ChannelFuture tryWrite(ByteBuf byteBuf);

    void disconnect();

}
