package com.ctrip.xpipe.redis.core.proxy;

import com.ctrip.xpipe.api.proxy.ProxyRequestResponseProtocol;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
public interface ProxyReqResProtocolParser extends ProxyProtocolParser {

    @Override
    ProxyRequestResponseProtocol read(String protocol);

    @Override
    ProxyRequestResponseProtocol read(ByteBuf byteBuf);

    String getContent();
}
