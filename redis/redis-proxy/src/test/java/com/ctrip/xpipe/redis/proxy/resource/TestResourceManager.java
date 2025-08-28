package com.ctrip.xpipe.redis.proxy.resource;

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
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.monitor.stats.SocketStatsManager;
import com.ctrip.xpipe.redis.proxy.monitor.stats.impl.DefaultSocketStatsManager;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
public class TestResourceManager implements ResourceManager {

    private static final Logger logger = LoggerFactory.getLogger(TestResourceManager.class);

    private NettySslHandlerFactory clientSslHandlerFactory = new NettyClientSslHandlerFactory(new TestProxyConfig());

    private NettySslHandlerFactory serverSslHandlerFactory = new NettyServerSslHandlerFactory(new TestProxyConfig());

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(4, XpipeThreadFactory.create("scheduled"));

    private ProxyEndpointManager endpointManager = new DefaultProxyEndpointManager(()->10000);

    private ProxyConfig config = new TestProxyConfig();

    private SocketStatsManager socketStatsManager = new DefaultSocketStatsManager();

    private NextHopAlgorithm algorithm = new NaiveNextHopAlgorithm();

    private GlobalTrafficControlManager trafficControlManager;

    private volatile SimpleKeyedObjectPool<Endpoint, NettyClient> keyedObjectPool;

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
    public SocketStatsManager getSocketStatsManager() {
        return socketStatsManager;
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

    public TestResourceManager setConfig(ProxyConfig config) {
        this.config = config;
        return this;
    }

    public TestResourceManager setEndpointManager(ProxyEndpointManager endpointManager) {
        this.endpointManager = endpointManager;
        return this;
    }

    public TestResourceManager setClientSslHandlerFactory(NettySslHandlerFactory clientSslHandlerFactory) {
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

    public void setGlobalTrafficControlManager(GlobalTrafficControlManager trafficControlManager) {
        this.trafficControlManager = trafficControlManager;
    }

    @Override
    public GlobalTrafficControlManager getGlobalTrafficControlManager() {
        // For testing, return null since we don't need traffic control in tests
        return trafficControlManager;
    }
}