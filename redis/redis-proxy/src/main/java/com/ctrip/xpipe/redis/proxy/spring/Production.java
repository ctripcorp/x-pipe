package com.ctrip.xpipe.redis.proxy.spring;

import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyServerSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */

@Configuration
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class Production extends AbstractProfile {

    public static final String CLIENT_SSL_HANDLER_FACTORY = "clientSslHandlerFactory";

    public static final String SERVER_SSL_HANDLER_FACTORY = "clientSslHandlerFactory";

    public static final String BACKEND_EVENTLOOP_GROUP = "backendEventLoopGroup";

    @Autowired
    private ProxyConfig config;

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

    @Bean(name = BACKEND_EVENTLOOP_GROUP)
    public EventLoopGroup backendEventLoopGroup() {
        return new NioEventLoopGroup(config.backendEventLoopNum(), XpipeThreadFactory.create("backend"));
    }
}
