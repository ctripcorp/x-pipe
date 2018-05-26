package com.ctrip.xpipe.redis.proxy.spring;

import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyServerSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.AbstractRedisProxyServerTest;
import com.ctrip.xpipe.redis.proxy.TestProxyConfig;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.spring.AbstractProfile;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import static com.ctrip.xpipe.redis.proxy.spring.Production.CLIENT_SSL_HANDLER_FACTORY;
import static com.ctrip.xpipe.redis.proxy.spring.Production.SERVER_SSL_HANDLER_FACTORY;

/**
 * @author chen.zhu
 * <p>
 * May 15, 2018
 */
@Configuration
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class ProductionTest extends AbstractRedisProxyServerTest {

    private ProxyConfig config = new TestProxyConfig();

    @Bean
    public ProxyEndpointManager getProxyEndpointManager() {
        return new DefaultProxyEndpointManager(() -> config.endpointHealthCheckIntervalSec());
    }

    @Bean(name = CLIENT_SSL_HANDLER_FACTORY)
    public NettySslHandlerFactory clientSslHandlerFactory() {
        return new NettyClientSslHandlerFactory(config);
    }

    @Bean(name = SERVER_SSL_HANDLER_FACTORY)
    public NettySslHandlerFactory serverSslHandlerFactory() {
        return new NettyServerSslHandlerFactory(config);
    }
}