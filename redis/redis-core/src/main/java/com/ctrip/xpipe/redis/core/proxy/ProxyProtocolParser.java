package com.ctrip.xpipe.redis.core.proxy;

import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.redis.core.proxy.parser.ProxyOptionParser;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface ProxyProtocolParser {


    ByteBuf format();

    ProxyConnectProtocol read(String protocol);

    ProxyConnectProtocol read(ByteBuf byteBuf);

    ProxyOptionParser getProxyOptionParser(PROXY_OPTION option);

}
