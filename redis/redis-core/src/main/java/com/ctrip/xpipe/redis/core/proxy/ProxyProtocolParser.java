package com.ctrip.xpipe.redis.core.proxy;

import com.ctrip.xpipe.redis.core.proxy.parser.ProxyOptionParser;
import io.netty.buffer.ByteBuf;

/**
 * @author chen.zhu
 * <p>
 * May 09, 2018
 */
public interface ProxyProtocolParser {


    ByteBuf format();

    ProxyProtocol read(String protocol);

    ProxyProtocol read(ByteBuf byteBuf);

    ProxyOptionParser getProxyOptionParser(PROXY_OPTION option);

}
