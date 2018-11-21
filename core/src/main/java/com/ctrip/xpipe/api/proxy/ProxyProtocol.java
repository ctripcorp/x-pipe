package com.ctrip.xpipe.api.proxy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * @author chen.zhu
 * <p>
 * Oct 23, 2018
 */
public interface ProxyProtocol {
    String KEY_WORD = "PROXY";

    ByteBuf output();

}
