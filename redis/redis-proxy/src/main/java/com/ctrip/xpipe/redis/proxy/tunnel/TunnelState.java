package com.ctrip.xpipe.redis.proxy.tunnel;

import com.ctrip.xpipe.redis.proxy.Session;
import com.ctrip.xpipe.redis.proxy.State;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public interface TunnelState extends State<TunnelState> {

    void forward(ByteBuf message, Session src);

}
