package com.ctrip.framework.xpipe.redis.proxy;

import java.net.InetSocketAddress;

public interface ProxyResourceManager {

    byte[] getProxyConnectProtocol();

    InetSocketAddress nextHop();
}
