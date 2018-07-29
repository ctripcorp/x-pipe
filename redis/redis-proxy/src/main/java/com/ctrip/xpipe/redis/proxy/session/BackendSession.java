package com.ctrip.xpipe.redis.proxy.session;

import com.ctrip.xpipe.redis.proxy.Session;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * May 24, 2018
 */
public interface BackendSession extends Session {

    void sendAfterProtocol(ByteBuf byteBuf) throws Exception;

}
