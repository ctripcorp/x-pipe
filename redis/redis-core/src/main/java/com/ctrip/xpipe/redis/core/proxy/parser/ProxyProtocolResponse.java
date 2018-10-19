package com.ctrip.xpipe.redis.core.proxy.parser;

import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * Oct 18, 2018
 */
public interface ProxyProtocolResponse<T> {

    ByteBuf format();

    T getPayload();

    /**
     * return true if read complete
     * otherwise, null
    * */
    boolean tryRead(ByteBuf buffer);
}
