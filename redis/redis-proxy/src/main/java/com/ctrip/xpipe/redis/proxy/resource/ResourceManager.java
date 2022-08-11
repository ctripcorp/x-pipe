package com.ctrip.xpipe.redis.proxy.resource;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.monitor.stats.SocketStatsManager;

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

    SocketStatsManager getSocketStatsManager();

    SimpleKeyedObjectPool<Endpoint, NettyClient> getKeyedObjectPool();
}
