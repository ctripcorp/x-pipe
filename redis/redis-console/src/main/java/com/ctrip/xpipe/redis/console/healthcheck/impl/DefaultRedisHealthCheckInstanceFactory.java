package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.api.cluster.CrossDcClusterServer;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.HealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.console.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.console.healthcheck.config.CompositeHealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.crossdc.CrossDcLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSessionManager;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
@Component
public class DefaultRedisHealthCheckInstanceFactory implements RedisHealthCheckInstanceFactory {

    private static final Logger logger = LoggerFactory.getLogger(DefaultRedisHealthCheckInstanceFactory.class);

    @Autowired
    private ConsoleConfig consoleConfig;

    @Autowired
    private HealthCheckEndpointFactory endpointFactory;

    @Autowired
    private RedisSessionManager redisSessionManager;

    @Autowired
    private List<HealthCheckActionFactory> factories;

    @Autowired(required = false)
    private CrossDcClusterServer clusterServer;


    @Override
    public RedisHealthCheckInstance create(RedisMeta redisMeta) {

        DefaultRedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();

        RedisInstanceInfo info = createRedisInstanceInfo(redisMeta);
        Endpoint endpoint = endpointFactory.getOrCreateEndpoint(redisMeta);
        HealthCheckConfig config = new CompositeHealthCheckConfig(endpoint, consoleConfig);

        instance.setEndpoint(endpoint)
                .setHealthCheckConfig(config)
                .setRedisInstanceInfo(info)
                .setSession(redisSessionManager.findOrCreateSession(endpoint));
        initActions(instance);

        try {
            LifecycleHelper.initializeIfPossible(instance);
            LifecycleHelper.startIfPossible(instance);
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
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                redisMeta.parent().getActiveDc());
        info.isMaster(redisMeta.isMaster());
        return info;
    }

    @SuppressWarnings("unchecked")
    private void initActions(DefaultRedisHealthCheckInstance instance) {
        for(HealthCheckActionFactory factory : factories) {
            if(factory instanceof CrossDcLeaderAwareHealthCheckActionFactory) {
                installActionIfNeeded((CrossDcLeaderAwareHealthCheckActionFactory) factory, instance);
            } else {
                instance.register(factory.create(instance));
            }
        }

    }

    private void installActionIfNeeded(CrossDcLeaderAwareHealthCheckActionFactory factory,
                                       DefaultRedisHealthCheckInstance instance) {
        logger.debug("[try install action] {}", factory.support());
        if(clusterServer != null && clusterServer.amILeader()) {
            logger.debug("[cluster server not null][installed]");
            instance.register(factory.create(instance));
        }
    }

    @VisibleForTesting
    protected DefaultRedisHealthCheckInstanceFactory setClusterServer(CrossDcClusterServer clusterServer) {
        this.clusterServer = clusterServer;
        return this;
    }
}
