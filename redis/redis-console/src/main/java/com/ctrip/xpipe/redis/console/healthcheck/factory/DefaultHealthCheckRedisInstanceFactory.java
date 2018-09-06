package com.ctrip.xpipe.redis.console.healthcheck.factory;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.proxy.ProxyEnabled;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.health.RedisSessionManager;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionListener;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.config.DefaultHealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.config.ProxyEnabledHealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.delay.DelayAction;
import com.ctrip.xpipe.redis.console.healthcheck.impl.DefaultRedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.ping.PingAction;
import com.ctrip.xpipe.redis.console.healthcheck.ping.PingService;
import com.ctrip.xpipe.redis.console.spring.ConsoleContextConfig;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
@Component
public class DefaultHealthCheckRedisInstanceFactory implements HealthCheckRedisInstanceFactory {

    private static final Logger logger = LoggerFactory.getLogger(DefaultHealthCheckRedisInstanceFactory.class);

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private HealthCheckEndpointFactory endpointFactory;

    @Autowired
    private RedisSessionManager redisSessionManager;

    @Resource(name = ConsoleContextConfig.SCHEDULED_EXECUTOR)
    private ScheduledExecutorService scheduled;

    @Autowired
    private PingService pingService;

    @Autowired
    private List<HealthCheckActionListener> listeners;

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
        Endpoint endpoint = endpointFactory.getOrCreateEndpoint(redisMeta);
        HealthCheckConfig config = isEndpointProxyEnabled(endpoint) ? proxyEnabledHealthCheckConfig : defaultHealthCheckConfig;

        ((DefaultRedisHealthCheckInstance) instance).setEndpoint(endpoint)
                .setHealthCheckConfig(config)
                .setRedisInstanceInfo(info)
                .setSession(redisSessionManager.findOrCreateSession(endpoint));
        initActions(instance);

        try {
            LifecycleHelper.initializeIfPossible(instance);
        } catch (Exception e) {
            logger.error("[create]", e);
        }
        return instance;
    }

    private RedisInstanceInfo createRedisInstanceInfo(RedisMeta redisMeta) {
        RedisInstanceInfo info =  new DefaultRedisInstanceInfo(
                redisMeta.parent().parent().parent().getId(),
                redisMeta.parent().parent().getId(),
                redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()));
        info.isMaster(redisMeta.isMaster());
        return info;
    }

    private boolean isEndpointProxyEnabled(Endpoint endpoint) {
        return endpoint instanceof ProxyEnabled;
    }

    private void initActions(RedisHealthCheckInstance instance) {
        new PingAction(scheduled, instance).addListeners(listeners);
        new DelayAction(scheduled, instance, pingService).addListeners(listeners);
    }
}
