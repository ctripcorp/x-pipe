package com.ctrip.xpipe.redis.proxy.spring;

import com.ctrip.xpipe.redis.core.proxy.endpoint.DefaultProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.endpoint.ProxyEndpointManager;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyClientSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettyServerSslHandlerFactory;
import com.ctrip.xpipe.redis.core.proxy.handler.NettySslHandlerFactory;
import com.ctrip.xpipe.redis.proxy.config.DefaultProxyConfig;
import com.ctrip.xpipe.redis.proxy.config.ProxyConfig;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.util.concurrent.MoreExecutors;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * May 10, 2018
 */

@Configuration
@Profile(AbstractProfile.PROFILE_NAME_PRODUCTION)
public class Production extends AbstractProfile {

    public static final String GLOBAL_ENDPOINT_MANAGER = "globalProxyEndpointManager";

    public static final String CLIENT_SSL_HANDLER_FACTORY = "clientSslHandlerFactory";

    public static final String SERVER_SSL_HANDLER_FACTORY = "serverSslHandlerFactory";

    public static final String GLOBAL_SCHEDULED = "globalScheduled";

    private ProxyConfig config = new DefaultProxyConfig();

    @Bean
    public ProxyConfig getProxyConfig() {
        return config;
    }

    @Bean(name = CLIENT_SSL_HANDLER_FACTORY)
    public NettySslHandlerFactory clientSslHandlerFactory() {
        return new NettyClientSslHandlerFactory(config);
    }

    @Bean(name = SERVER_SSL_HANDLER_FACTORY)
    public NettySslHandlerFactory serverSslHandlerFactory() {
        return new NettyServerSslHandlerFactory(config);
    }

    @Bean(name = GLOBAL_SCHEDULED)
    public ScheduledExecutorService getScheduled() {
        int corePoolSize = Math.min(OsUtils.getCpuCount(), 4);
        return MoreExecutors.getExitingScheduledExecutorService(
                new ScheduledThreadPoolExecutor(corePoolSize, XpipeThreadFactory.create(GLOBAL_SCHEDULED)),
                1, TimeUnit.SECONDS
        );
    }

    @Bean(name = GLOBAL_ENDPOINT_MANAGER)
    public ProxyEndpointManager getProxyResourceManager() {
        return new DefaultProxyEndpointManager(() -> config.endpointHealthCheckIntervalSec());
    }
}
