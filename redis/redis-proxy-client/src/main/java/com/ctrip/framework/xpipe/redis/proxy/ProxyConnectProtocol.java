package com.ctrip.framework.xpipe.redis.proxy;

import java.net.InetSocketAddress;
import java.util.List;

public interface ProxyConnectProtocol {

    List<InetSocketAddress> nextEndpoints();

    byte[] output();

}
