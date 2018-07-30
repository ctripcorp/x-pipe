package com.ctrip.xpipe.redis.proxy.resource;

import com.ctrip.xpipe.redis.core.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.*;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.resource.ProxyProxyResourceManager;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
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

    private NextHopAlgorithm algorithm = new NaiveNextHopAlgorithm();

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
    public ProxyEndpointSelector createProxyEndpointSelector(ProxyProtocol protocol) {
        ProxyEndpointSelector selector = new DefaultProxyEndpointSelector(protocol.nextEndpoints(), endpointManager);
        selector.setNextHopAlgorithm(algorithm);
        selector.setSelectStrategy(new SelectOneCycle(selector));
        return selector;
    }

    public ProxyRelatedResourceManager setConfig(ProxyConfig config) {
        this.config = config;
        return this;
    }

    public ProxyRelatedResourceManager setEndpointManager(ProxyEndpointManager endpointManager) {
        this.endpointManager = endpointManager;
        return this;
    }

    public ProxyRelatedResourceManager setClientSslHandlerFactory(NettySslHandlerFactory clientSslHandlerFactory) {
        this.clientSslHandlerFactory = clientSslHandlerFactory;
        return this;
    }
}
