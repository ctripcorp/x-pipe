package com.ctrip.xpipe.redis.console.healthcheck.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.console.cluster.ConsoleLeaderElector;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.healthcheck.*;
import com.ctrip.xpipe.redis.console.healthcheck.config.CompositeHealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.console.healthcheck.leader.SiteLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.console.healthcheck.session.RedisSessionManager;
import com.ctrip.xpipe.redis.console.healthcheck.util.ClusterTypeSupporterSeparator;
import com.ctrip.xpipe.redis.console.resources.MetaCache;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.utils.VisibleForTesting;
import jline.internal.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
@Component
public class DefaultRedisHealthCheckInstanceFactory implements RedisHealthCheckInstanceFactory {

    private static final Logger logger = LoggerFactory.getLogger(DefaultRedisHealthCheckInstanceFactory.class);

    private ConsoleConfig consoleConfig;

    private HealthCheckEndpointFactory endpointFactory;

    private RedisSessionManager redisSessionManager;

    private Map<ClusterType, List<HealthCheckActionFactory<?>>> factoriesByClusterType;

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Nullable
    private ConsoleLeaderElector clusterServer;

    private MetaCache metaCache;

    @Autowired(required = false)
    public DefaultRedisHealthCheckInstanceFactory(ConsoleConfig consoleConfig, HealthCheckEndpointFactory endpointFactory,
                                                  RedisSessionManager redisSessionManager, List<HealthCheckActionFactory<?>> factories,
                                                  ConsoleLeaderElector clusterServer, MetaCache metaCache) {
        this.consoleConfig = consoleConfig;
        this.endpointFactory = endpointFactory;
        this.redisSessionManager = redisSessionManager;
        this.clusterServer = clusterServer;
        this.metaCache = metaCache;
        this.factoriesByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(factories);
    }

    @Autowired(required = false)
    public DefaultRedisHealthCheckInstanceFactory(ConsoleConfig consoleConfig, HealthCheckEndpointFactory endpointFactory,
                                                  RedisSessionManager redisSessionManager, List<HealthCheckActionFactory<?>> factories,
                                                  MetaCache metaCache) {
        this(consoleConfig, endpointFactory, redisSessionManager, factories, null, metaCache);
    }

    @Override
    public RedisHealthCheckInstance create(RedisMeta redisMeta) {

        DefaultRedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();

        RedisInstanceInfo info = createRedisInstanceInfo(redisMeta);
        Endpoint endpoint = endpointFactory.getOrCreateEndpoint(redisMeta);
        HealthCheckConfig config = new CompositeHealthCheckConfig(info, consoleConfig);

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
        ClusterType clusterType = ClusterType.lookup(redisMeta.parent().parent().getType());
        DefaultRedisInstanceInfo info =  new DefaultRedisInstanceInfo(
                redisMeta.parent().parent().parent().getId(),
                redisMeta.parent().parent().getId(),
                redisMeta.parent().getId(),
                new HostPort(redisMeta.getIp(), redisMeta.getPort()),
                redisMeta.parent().getActiveDc(), clusterType);
        info.isMaster(redisMeta.isMaster());
        if (clusterType.supportSingleActiveDC()) {
            info.setCrossRegion(metaCache.isCrossRegion(info.getActiveDc(), info.getDcId()));
        } else if (clusterType.supportMultiActiveDC()) {
            info.setCrossRegion(metaCache.isCrossRegion(currentDcId, info.getDcId()));
        }

        return info;
    }

    @SuppressWarnings("unchecked")
    private void initActions(DefaultRedisHealthCheckInstance instance) {
        for(HealthCheckActionFactory<?> factory : factoriesByClusterType.get(instance.getRedisInstanceInfo().getClusterType())) {
            if(factory instanceof SiteLeaderAwareHealthCheckActionFactory) {
                installActionIfNeeded((SiteLeaderAwareHealthCheckActionFactory) factory, instance);
            } else {
                instance.register(factory.create(instance));
            }
        }

    }

    private void installActionIfNeeded(SiteLeaderAwareHealthCheckActionFactory factory,
                                       DefaultRedisHealthCheckInstance instance) {
        logger.debug("[try install action] {}", factory.support());
        if(clusterServer != null && clusterServer.amILeader()) {
            logger.debug("[cluster server not null][installed]");
            instance.register(factory.create(instance));
        }
    }

    @VisibleForTesting
    protected DefaultRedisHealthCheckInstanceFactory setClusterServer(ConsoleLeaderElector clusterServer) {
        this.clusterServer = clusterServer;
        return this;
    }
}
