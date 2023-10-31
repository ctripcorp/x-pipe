package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.checker.DcRelationsService;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisCheckRule;
import com.ctrip.xpipe.redis.checker.healthcheck.config.CompositeHealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.config.DefaultHealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.session.KeeperSessionManager;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSessionManager;
import com.ctrip.xpipe.redis.checker.healthcheck.util.ClusterTypeSupporterSeparator;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisCheckRuleMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
@Component
public class DefaultHealthCheckInstanceFactory implements HealthCheckInstanceFactory {

    private static final Logger logger = LoggerFactory.getLogger(DefaultHealthCheckInstanceFactory.class);

    private CheckerConfig checkerConfig;

    private DcRelationsService dcRelationsService;

    private HealthCheckEndpointFactory endpointFactory;

    private RedisSessionManager redisSessionManager;

    private KeeperSessionManager keeperSessionManager;

    private List<KeeperHealthCheckActionFactory<?>> keeperHealthCheckActionFactories;

    private Map<ClusterType, List<RedisHealthCheckActionFactory<?>>> factoriesByClusterType;

    private Map<ClusterType, List<ClusterHealthCheckActionFactory<?>>> clusterHealthCheckFactoriesByClusterType;

    private static final String currentDcId = FoundationService.DEFAULT.getDataCenter();

    @Nullable
    private GroupCheckerLeaderElector clusterServer;

    private MetaCache metaCache;

    @Autowired(required = false)
    public DefaultHealthCheckInstanceFactory(CheckerConfig checkerConfig, HealthCheckEndpointFactory endpointFactory,
                                             RedisSessionManager redisSessionManager, KeeperSessionManager keeperSessionManager,
                                             List<RedisHealthCheckActionFactory<?>> factories,
                                             List<KeeperHealthCheckActionFactory<?>> keeperHealthCheckActionFactories,
                                             List<ClusterHealthCheckActionFactory<?>> clusterHealthCheckFactories,
                                             GroupCheckerLeaderElector clusterServer, MetaCache metaCache, DcRelationsService dcRelationsService) {
        this.checkerConfig = checkerConfig;
        this.dcRelationsService = dcRelationsService;
        this.endpointFactory = endpointFactory;
        this.redisSessionManager = redisSessionManager;
        this.keeperSessionManager = keeperSessionManager;
        this.clusterServer = clusterServer;
        this.metaCache = metaCache;
        this.factoriesByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(factories);
        this.clusterHealthCheckFactoriesByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(clusterHealthCheckFactories);
        this.keeperHealthCheckActionFactories = keeperHealthCheckActionFactories;
    }

    @Autowired(required = false)
    public DefaultHealthCheckInstanceFactory(CheckerConfig checkerConfig, HealthCheckEndpointFactory endpointFactory,
                                             RedisSessionManager redisSessionManager,  KeeperSessionManager keeperSessionManager,
                                             List<RedisHealthCheckActionFactory<?>> factories,
                                             List<KeeperHealthCheckActionFactory<?>> keeperHealthCheckActionFactories,
                                             List<ClusterHealthCheckActionFactory<?>> clusterHealthCheckFactories,
                                             MetaCache metaCache, DcRelationsService dcRelationsService) {
        this(checkerConfig, endpointFactory, redisSessionManager, keeperSessionManager, factories,
                keeperHealthCheckActionFactories, clusterHealthCheckFactories, null, metaCache, dcRelationsService);
    }

    @Override
    public void remove(RedisHealthCheckInstance instance) {
        Endpoint endpoint = instance.getEndpoint();
        endpointFactory.remove(new HostPort(endpoint.getHost(), endpoint.getPort()));
        stopCheck(instance);
    }

    @Override
    public void remove(KeeperHealthCheckInstance instance) {
        Endpoint endpoint = instance.getEndpoint();
        endpointFactory.remove(new HostPort(endpoint.getHost(), endpoint.getPort()));
        stopCheck(instance);
    }

    @Override
    public void remove(ClusterHealthCheckInstance instance) {
        stopCheck(instance);
    }

    @Override
    public RedisHealthCheckInstance create(RedisMeta redisMeta) {

        DefaultRedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();

        RedisInstanceInfo info = createRedisInstanceInfo(redisMeta);
        Endpoint endpoint = endpointFactory.getOrCreateEndpoint(redisMeta);
        HealthCheckConfig config = new CompositeHealthCheckConfig(info, checkerConfig, dcRelationsService);

        instance.setEndpoint(endpoint)
                .setSession(redisSessionManager.findOrCreateSession(endpoint))
                .setInstanceInfo(info)
                .setHealthCheckConfig(config);
        initActions(instance);
        startCheck(instance);

        return instance;
    }

    private RedisInstanceInfo createRedisInstanceInfo(RedisMeta redisMeta) {
        ClusterMeta clusterMeta = redisMeta.parent().parent();
        ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());

        List<RedisCheckRule> redisCheckRules = new LinkedList<>();
        if (!StringUtil.isEmpty(clusterMeta.getActiveRedisCheckRules())) {
            for (String ruleId : clusterMeta.getActiveRedisCheckRules().split(",")) {
                RedisCheckRuleMeta redisCheckRuleMeta = metaCache.getXpipeMeta().getRedisCheckRules().get(Long.parseLong(ruleId));
                if (redisCheckRuleMeta != null) {
                    redisCheckRules.add(new RedisCheckRule(redisCheckRuleMeta.getCheckType(), Codec.DEFAULT.decode(redisCheckRuleMeta.getParam(), Map.class)));
                    logger.info("[createRedisInstanceInfo] add redis check rule {} {} to redis {}:{}",
                            redisCheckRuleMeta.getCheckType(), redisCheckRuleMeta.getParam(), redisMeta.getIp(), redisMeta.getPort());
                }
            }
        }

        DefaultRedisInstanceInfo info =  new DefaultRedisInstanceInfo(clusterMeta.parent().getId(), clusterMeta.getId(),
            redisMeta.parent().getId(), new HostPort(redisMeta.getIp(), redisMeta.getPort()),
            redisMeta.parent().getActiveDc(), clusterType, redisCheckRules);
        info.isMaster(redisMeta.isMaster());
        info.setAzGroupType(clusterMeta.getAzGroupType());
        info.setAsymmetricCluster(metaCache.isAsymmetricCluster(info.getClusterId()));
        if (clusterType.supportSingleActiveDC()) {
            info.setCrossRegion(metaCache.isCrossRegion(info.getActiveDc(), info.getDcId()));
            info.setShardDbId(redisMeta.parent().getDbId());
            info.setActiveDcShardIds(metaCache.dcShardIds(info.getClusterId(), info.getActiveDc()));
        } else if (clusterType.supportMultiActiveDC()) {
            info.setCrossRegion(metaCache.isCrossRegion(currentDcId, info.getDcId()));
        }

        return info;
    }

    @Override
    public ClusterHealthCheckInstance create(ClusterMeta clusterMeta) {
        DefaultClusterHealthCheckInstance instance = new DefaultClusterHealthCheckInstance();

        ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
        ClusterInstanceInfo info = new DefaultClusterInstanceInfo(clusterMeta.getId(), clusterMeta.getActiveDc(),
            clusterType, clusterMeta.getOrgId());
        info.setAzGroupType(clusterMeta.getAzGroupType());
        info.setAsymmetricCluster(metaCache.isAsymmetricCluster(clusterMeta.getId()));
        HealthCheckConfig config = new DefaultHealthCheckConfig(checkerConfig, dcRelationsService);

        instance.setInstanceInfo(info).setHealthCheckConfig(config);
        initActions(instance);
        startCheck(instance);

        return instance;
    }

    @Override
    public KeeperHealthCheckInstance create(KeeperMeta keeperMeta) {
        DefaultKeeperHealthCheckInstance instance = new DefaultKeeperHealthCheckInstance();

        KeeperInstanceInfo keeper = createKeeperInstanceInfo(keeperMeta);
        HealthCheckConfig config = new CompositeHealthCheckConfig(keeper, checkerConfig, dcRelationsService);
        Endpoint endpoint = endpointFactory.getOrCreateEndpoint(keeperMeta);

        instance.setEndpoint(endpoint)
                .setSession(keeperSessionManager.findOrCreateSession(endpoint))
                .setInstanceInfo(keeper)
                .setHealthCheckConfig(config);
        initKeeperActions(instance);
        startCheck(instance);

        return instance;
    }

    private KeeperInstanceInfo createKeeperInstanceInfo(KeeperMeta keeperMeta) {
        ClusterType clusterType = ClusterType.lookup(((ClusterMeta)keeperMeta.parent().parent()).getType());

        DefaultKeeperInstanceInfo info = new DefaultKeeperInstanceInfo(
                ((ClusterMeta)keeperMeta.parent().parent()).parent().getId(),
                ((ClusterMeta)keeperMeta.parent().parent()).getId(),
                keeperMeta.parent().getId(),
                new HostPort(keeperMeta.getIp(), keeperMeta.getPort()),
                keeperMeta.parent().getActiveDc(), clusterType);
        info.setActive(keeperMeta.isActive());

        return info;
    }

    private void initKeeperActions(DefaultKeeperHealthCheckInstance instance) {
        keeperHealthCheckActionFactories.forEach(keeperHealthCheckActionFactory -> {
            initActions(instance, keeperHealthCheckActionFactory);
        });
    }

    @Override
    public RedisHealthCheckInstance createRedisInstanceForAssignedAction(RedisMeta redis) {
        DefaultRedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();

        RedisInstanceInfo info = createRedisInstanceInfo(redis);
        HealthCheckConfig config = new CompositeHealthCheckConfig(info, checkerConfig, dcRelationsService);
        Endpoint endpoint = endpointFactory.getOrCreateEndpoint(redis);

        instance.setEndpoint(endpoint)
                .setSession(redisSessionManager.findOrCreateSession(endpoint))
                .setInstanceInfo(info)
                .setHealthCheckConfig(config);

        initActionsForRedisForAssignedAction(instance);
        startCheck(instance);

        return instance;
    }

    private void initActionsForRedisForAssignedAction(DefaultRedisHealthCheckInstance instance) {
        for(RedisHealthCheckActionFactory<?> factory : factoriesByClusterType.get(instance.getCheckInfo().getClusterType())) {
            if (factory instanceof KeeperSupport)
                initActions(instance, factory);
        }
    }

    @SuppressWarnings("unchecked")
    private void initActions(DefaultRedisHealthCheckInstance instance) {
        for(RedisHealthCheckActionFactory<?> factory : factoriesByClusterType.get(instance.getCheckInfo().getClusterType())) {
            initActions(instance, factory);
        }
    }

    private void initActions(HealthCheckInstance instance, HealthCheckActionFactory factory) {
        if (factory.supportInstnace(instance)) {
            if (factory instanceof SiteLeaderAwareHealthCheckActionFactory) {
                installActionIfNeeded((SiteLeaderAwareHealthCheckActionFactory) factory, instance);
            } else {
                instance.register(factory.create(instance));
            }
        }
    }

    private void startCheck(HealthCheckInstance instance) {
        try {
            LifecycleHelper.initializeIfPossible(instance);
            LifecycleHelper.startIfPossible(instance);
        } catch (Exception e) {
            logger.error("[startCheck]", e);
        }
    }


    private void stopCheck(HealthCheckInstance instance) {
        try {
            LifecycleHelper.stopIfPossible(instance);
        } catch (Exception e) {
            logger.error("[stopCheck]", e);
        }
    }

    private void initActions(DefaultClusterHealthCheckInstance instance) {
        for(ClusterHealthCheckActionFactory<?> factory : clusterHealthCheckFactoriesByClusterType.get(instance.getCheckInfo().getClusterType())) {
            if (factory instanceof SiteLeaderAwareHealthCheckActionFactory) {
                installActionIfNeeded((SiteLeaderAwareHealthCheckActionFactory) factory, instance);
            } else {
                instance.register(factory.create(instance));
            }
        }
    }

    private void installActionIfNeeded(SiteLeaderAwareHealthCheckActionFactory factory, HealthCheckInstance instance) {
        logger.debug("[try install action] {}", factory.support());
        if(clusterServer != null && clusterServer.amILeader()) {
            logger.debug("[cluster server not null][installed]");
            instance.register(factory.create(instance));
        }
    }

    @VisibleForTesting
    protected DefaultHealthCheckInstanceFactory setClusterServer(GroupCheckerLeaderElector clusterServer) {
        this.clusterServer = clusterServer;
        return this;
    }
}
