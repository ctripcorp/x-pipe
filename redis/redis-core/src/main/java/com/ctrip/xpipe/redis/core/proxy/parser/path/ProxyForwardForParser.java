package com.ctrip.xpipe.redis.core.proxy.parser.path;

import com.ctrip.xpipe.redis.core.proxy.parser.ProxyOptionParser;

import java.net.InetSocketAddress;

/**
 * @author chen.zhu
 * <p>
 * May 04, 2018
 */
public interface ProxyForwardForParser extends ProxyOptionParser {

    void append(InetSocketAddress address);
}
