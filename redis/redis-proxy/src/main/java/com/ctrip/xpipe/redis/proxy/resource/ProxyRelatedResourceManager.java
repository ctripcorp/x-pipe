package com.ctrip.xpipe.redis.proxy.resource;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.proxy.ProxyConnectProtocol;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.netty.commands.NettyKeyedPoolClientFactory;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.proxy.endpoint.*;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.redis.proxy.monitor.stats.SocketStatsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.proxy.spring.Production.*;

/**
 * @author chen.zhu
 * <p>
 * Jul 30, 2018
 */
@Component
public class ProxyRelatedResourceManager implements ResourceManager {

    private static final Logger logger = LoggerFactory.getLogger(ProxyRelatedResourceManager.class);

    @Resource(name = CLIENT_SSL_HANDLER_FACTORY)
    private NettySslHandlerFactory clientSslHandlerFactory;

    @Resource(name = SERVER_SSL_HANDLER_FACTORY)
    private NettySslHandlerFactory serverSslHandlerFactory;

    @Resource(name = GLOBAL_SCHEDULED)
    private ScheduledExecutorService scheduled;

    @Resource(name = GLOBAL_ENDPOINT_MANAGER)
    private ProxyEndpointManager endpointManager;

    @Autowired
    private ProxyConfig config;

    @Autowired
    private SocketStatsManager socketStatsManager;

    private NextHopAlgorithm algorithm = new NaiveNextHopAlgorithm();

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
                    createKeyedObjectPool();
                }
            }
        }
        return keyedObjectPool;
    }

    @Override
    public ProxyEndpointSelector createProxyEndpointSelector(ProxyConnectProtocol protocol) {
        ProxyEndpointSelector selector = new DefaultProxyEndpointSelector(protocol.nextEndpoints(), endpointManager);
        selector.setNextHopAlgorithm(algorithm);
        selector.setSelectStrategy(new SelectOneCycle(selector));
        return selector;
    }

    private void createKeyedObjectPool() {
        keyedObjectPool = new XpipeNettyClientKeyedObjectPool(getKeyedPoolClientFactory());
        try {
            LifecycleHelper.initializeIfPossible(keyedObjectPool);
            LifecycleHelper.startIfPossible(keyedObjectPool);
        } catch (Exception e) {
            logger.error("[createKeyedObjectPool]", e);
        }
    }

    private NettyKeyedPoolClientFactory getKeyedPoolClientFactory() {
        return new SslEnabledNettyClientFactory(this);
    }

}
