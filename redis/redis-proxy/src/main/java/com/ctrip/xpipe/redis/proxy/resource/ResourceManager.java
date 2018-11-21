package com.ctrip.xpipe.redis.proxy.resource;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Jul 30, 2018
 */
public interface ResourceManager extends ProxyResourceManager {

    NettySslHandlerFactory getClientSslHandlerFactory();

    NettySslHandlerFactory getServerSslHandlerFactory();

    ScheduledExecutorService getGlobalSharedScheduled();

    ProxyConfig getProxyConfig();

    SimpleKeyedObjectPool<Endpoint, NettyClient> getKeyedObjectPool();
}
