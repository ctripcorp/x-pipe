package com.ctrip.xpipe.redis.integratedtest.metaserver.proxy;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.proxy.endpoint.*;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyServerSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.resource.ResourceManager;
import com.ctrip.xpipe.redis.proxy.resource.SslEnabledNettyClientFactory;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class LocalResourceManager implements ResourceManager {
    private static final Logger logger = LoggerFactory.getLogger(LocalResourceManager.class);

    private NettySslHandlerFactory clientSslHandlerFactory ;

    private NettySslHandlerFactory serverSslHandlerFactory ;

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(4, XpipeThreadFactory.create("scheduled"));

    private ProxyEndpointManager endpointManager = new DefaultProxyEndpointManager(()->10000);

    private ProxyConfig config ;

    private NextHopAlgorithm algorithm = new NaiveNextHopAlgorithm();

    private volatile SimpleKeyedObjectPool<Endpoint, NettyClient> keyedObjectPool;
    public LocalResourceManager(ProxyConfig config) {
        this.config = config;
        clientSslHandlerFactory = new NettyClientSslHandlerFactory(config);
        serverSslHandlerFactory = new NettyServerSslHandlerFactory(config);
    }

    @Override
    public NettySslHandlerFactory getClientSslHandlerFactory() {
        return clientSslHandlerFactory;
    }

    @Override
    public NettySslHandlerFactory getServerSslHandlerFactory() {
        return serverSslHandlerFactory;
    }

    @Override
    public ScheduledExecutorService getGlobalSharedScheduled() {
        return scheduled;
    }

    @Override
    public ProxyConfig getProxyConfig() {
        return config;
    }

    @Override
    public SimpleKeyedObjectPool<Endpoint, NettyClient> getKeyedObjectPool() {
        if(keyedObjectPool == null) {
            synchronized (this) {
                if(keyedObjectPool == null) {
                    keyedObjectPool = new XpipeNettyClientKeyedObjectPool(new SslEnabledNettyClientFactory(this));
                    try {
                        LifecycleHelper.initializeIfPossible(keyedObjectPool);
                        LifecycleHelper.startIfPossible(keyedObjectPool);
                    } catch (Exception e) {
                        logger.error("[createKeyedObjectPool]", e);
                    }
                }
            }
        }

        return keyedObjectPool;
    }

    public LocalResourceManager setConfig(ProxyConfig config) {
        this.config = config;
        return this;
    }

    public LocalResourceManager setEndpointManager(ProxyEndpointManager endpointManager) {
        this.endpointManager = endpointManager;
        return this;
    }

    public LocalResourceManager setClientSslHandlerFactory(NettySslHandlerFactory clientSslHandlerFactory) {
        this.clientSslHandlerFactory = clientSslHandlerFactory;
        return this;
    }

    @Override
    public ProxyEndpointSelector createProxyEndpointSelector(ProxyConnectProtocol protocol) {
        ProxyEndpointSelector selector = new DefaultProxyEndpointSelector(protocol.nextEndpoints(), endpointManager);
        selector.setNextHopAlgorithm(algorithm);
        selector.setSelectStrategy(new SelectOneCycle(selector));
        return selector;
    }
}
