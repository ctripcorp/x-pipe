package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.redis.proxy.State;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */
public interface SessionState extends State<SessionState> {

    void tryWrite(ByteBuf byteBuf);

}
