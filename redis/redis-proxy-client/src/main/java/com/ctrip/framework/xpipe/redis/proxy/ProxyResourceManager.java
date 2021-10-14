package com.ctrip.framework.xpipe.redis.proxy;

import java.net.InetSocketAddress;
import java.util.List;

public interface ProxyResourceManager {

    byte[] getProxyConnectProtocol();

    InetSocketAddress nextHop();
    
    List<ProxyInetSocketAddress> nextEndpoints();
}
