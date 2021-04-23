package com.ctrip.framework.xpipe.redis.proxy;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.exception.NoResourceException;
import io.netty.buffer.ByteBuf;

public interface ProxyResourceManager extends Startable, Stoppable {

    ByteBuf getProxyConnectProtocol();

    ProxyEndpoint nextHop() throws NoResourceException;
}
