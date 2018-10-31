package com.ctrip.xpipe.redis.proxy.resource;

import com.ctrip.xpipe.api.proxy.ProxyProtocol;
import com.ctrip.xpipe.redis.core.proxy.endpoint.*;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyServerSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.Resource;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.proxy.spring.Production.*;
import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Oct 29, 2018
 */
public class TestResourceManager implements ResourceManager {

    private NettySslHandlerFactory clientSslHandlerFactory = new NettyClientSslHandlerFactory(new TestProxyConfig());

    private NettySslHandlerFactory serverSslHandlerFactory = new NettyServerSslHandlerFactory(new TestProxyConfig());

    private ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(4, XpipeThreadFactory.create("scheduled"));

    private ProxyEndpointManager endpointManager = new DefaultProxyEndpointManager(()->10000);

    private ProxyConfig config = new TestProxyConfig();

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
}