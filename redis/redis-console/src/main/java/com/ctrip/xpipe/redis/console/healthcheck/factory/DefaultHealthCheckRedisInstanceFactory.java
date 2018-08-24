package com.ctrip.xpipe.redis.console.healthcheck.factory;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckContext;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.config.DefaultHealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.config.ProxyEnabledHealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.redis.DefaultRedisInstanceInfo;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
@Component
public class DefaultHealthCheckRedisInstanceFactory implements HealthCheckRedisInstanceFactory {

    private static final Logger logger = LoggerFactory.getLogger(DefaultHealthCheckRedisInstanceFactory.class);

    @Autowired
    private HealthCheckContextFactory healthCheckContextFactory;

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private HealthCheckEndpointFactory endpointFactory;

    private DefaultHealthCheckConfig defaultHealthCheckConfig;

    private ProxyEnabledHealthCheckConfig proxyEnabledHealthCheckConfig;

    @PostConstruct
    public void init() {
        defaultHealthCheckConfig = new DefaultHealthCheckConfig(consoleConfig);
        proxyEnabledHealthCheckConfig = new ProxyEnabledHealthCheckConfig(consoleConfig);
    }

    @Override
    public RedisHealthCheckInstance create(RedisMeta redisMeta) {

        RedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();

        RedisInstanceInfo info = createRedisInstanceInfo(redisMeta);
        HealthCheckContext context = createHealthCheckContext(instance, redisMeta);
        Endpoint endpoint = endpointFactory.getOrCreateEndpoint(redisMeta);
        HealthCheckConfig config = isEndpointProxyEnabled(endpoint) ? proxyEnabledHealthCheckConfig : defaultHealthCheckConfig;

        ((DefaultRedisHealthCheckInstance) instance).setEndpoint(endpoint)
                .setHealthCheckConfig(config)
                .setRedisInstanceInfo(info)
                .setHealthCheckContext(context);

        try {
            LifecycleHelper.initializeIfPossible(instance);
        } catch (Exception e) {
            logger.error("[create]", e);
        }
        return instance;
    }

    private HealthCheckContext createHealthCheckContext(RedisHealthCheckInstance instance, RedisMeta redisMeta) {
        return healthCheckContextFactory.create(instance, redisMeta);
    }

    private RedisInstanceInfo createRedisInstanceInfo(RedisMeta redisMeta) {
        return new DefaultRedisInstanceInfo(
                redisMeta.parent().parent().parent().getId(),
                redisMeta.parent().parent().getId(),
                redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()));
    }

    private boolean isEndpointProxyEnabled(Endpoint endpoint) {
        return endpoint instanceof ProxyEnabled;
    }
}
