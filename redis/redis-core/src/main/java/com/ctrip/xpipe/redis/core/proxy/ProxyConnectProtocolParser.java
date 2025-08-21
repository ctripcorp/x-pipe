package com.ctrip.xpipe.redis.core.proxy;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyOptionParser;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * Oct 24, 2018
 */
@SuppressWarnings("unchecked")
public interface ProxyConnectProtocolParser extends ProxyProtocolParser {

    @Override
    ProxyConnectProtocol read(String protocol);

    @Override
    ProxyConnectProtocol read(ByteBuf byteBuf);

    void addProxyOptionParser(ProxyOptionParser parser);

    void removeProxyOptionParser(ProxyOptionParser parser);
}
