package com.ctrip.xpipe.redis.checker.healthcheck.impl;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.checker.RelationsService;
import com.ctrip.xpipe.redis.checker.cluster.GroupCheckerLeaderElector;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.inforeplid.InfoReplIdActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.redisconf.RedisCheckRule;
import com.ctrip.xpipe.redis.checker.healthcheck.config.CompositeHealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.config.DefaultHealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.config.HealthCheckConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.SiteLeaderAwareHealthCheckActionFactory;
import com.ctrip.xpipe.redis.checker.healthcheck.session.KeeperSessionManager;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSessionManager;
import com.ctrip.xpipe.redis.checker.healthcheck.util.ClusterTypeSupporterSeparator;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RedisCheckRuleMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.keeper.KeeperDiskTypeUtils;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
@Component
public class DefaultHealthCheckInstanceFactory implements HealthCheckInstanceFactory {

    private static final Logger logger = LoggerFactory.getLogger(DefaultHealthCheckInstanceFactory.class);

    private CheckerConfig checkerConfig;

    private RelationsService relationsService;

    private HealthCheckEndpointFactory endpointFactory;

    private RedisSessionManager redisSessionManager;

    private KeeperSessionManager keeperSessionManager;

    private List<KeeperHealthCheckActionFactory<?>> keeperHealthCheckActionFactories = Collections.emptyList();

    private final Map<HealthCheckAction, KeeperHealthCheckActionFactory<?>> keeperFactoriesByAction = new ConcurrentHashMap<>();

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
                                             List<ClusterHealthCheckActionFactory<?>> clusterHealthCheckFactories,
                                             GroupCheckerLeaderElector clusterServer, MetaCache metaCache, RelationsService relationsService) {
        this.checkerConfig = checkerConfig;
        this.relationsService = relationsService;
        this.endpointFactory = endpointFactory;
        this.redisSessionManager = redisSessionManager;
        this.keeperSessionManager = keeperSessionManager;
        this.clusterServer = clusterServer;
        this.metaCache = metaCache;
        this.factoriesByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(factories);
        this.clusterHealthCheckFactoriesByClusterType = ClusterTypeSupporterSeparator.divideByClusterType(clusterHealthCheckFactories);
    }

    @Autowired(required = false)
    public DefaultHealthCheckInstanceFactory(CheckerConfig checkerConfig, HealthCheckEndpointFactory endpointFactory,
                                             RedisSessionManager redisSessionManager, KeeperSessionManager keeperSessionManager,
                                             List<RedisHealthCheckActionFactory<?>> factories,
                                             List<ClusterHealthCheckActionFactory<?>> clusterHealthCheckFactories,
                                             MetaCache metaCache, RelationsService relationsService) {
        this(checkerConfig, endpointFactory, redisSessionManager, keeperSessionManager, factories,
                clusterHealthCheckFactories, null, metaCache, relationsService);
    }

    @Autowired(required = false)
    @VisibleForTesting
    public void setKeeperHealthCheckActionFactories(List<KeeperHealthCheckActionFactory<?>> factories) {
        this.keeperHealthCheckActionFactories = new ArrayList<>(factories);
    }

    @Override
    public void remove(RedisHealthCheckInstance instance) {
        Endpoint endpoint = instance.getEndpoint();
        endpointFactory.remove(new HostPort(endpoint.getHost(), endpoint.getPort()));
        stopCheck(instance);
    }

    @Override
    public void remove(KeeperHealthCheckInstance instance) {
        cleanupKeeperInstance(instance, false);
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
        HealthCheckConfig config = new CompositeHealthCheckConfig(info, checkerConfig, relationsService, metaCache.isCrossRegion(currentDcId, info.getDcId()));

        instance.setEndpoint(endpoint)
                .setSession(redisSessionManager.findOrCreateSession(endpoint))
                .setInstanceInfo(info)
                .setHealthCheckConfig(config);
        initActions(instance);
        startCheck(instance);

        return instance;
    }

    @Override
    public KeeperHealthCheckInstance create(KeeperMeta keeperMeta) {
        DefaultKeeperHealthCheckInstance instance = new DefaultKeeperHealthCheckInstance();
        try {
            KeeperInstanceInfo info = createKeeperInstanceInfo(keeperMeta);
            Endpoint endpoint = new DefaultEndPoint(info.getHostPort().getHost(), info.getHostPort().getPort());

            instance.setEndpoint(endpoint)
                    .setSession(keeperSessionManager.findOrCreateSession(endpoint))
                    .setTfs(isTfsKeeper(keeperMeta));
            instance.setInstanceInfo(info).setHealthCheckConfig(new DefaultHealthCheckConfig(checkerConfig, dcRelationsService));
            initActionsForKeeper(instance);
            LifecycleHelper.initializeIfPossible(instance);
            LifecycleHelper.startIfPossible(instance);
            return instance;
        } catch (Exception e) {
            try {
                cleanupKeeperInstance(instance, true);
            } catch (RuntimeException cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            throw new IllegalStateException("failed to create Keeper health-check instance for "
                    + keeperMeta.getIp() + ":" + keeperMeta.getPort(), e);
        }
    }

    private void initActionsForKeeper(DefaultKeeperHealthCheckInstance instance) {
        List<KeeperHealthCheckActionFactory<?>> supportedFactories = keeperHealthCheckActionFactories.stream()
                .filter(factory -> factory.supportInstnace(instance))
                .collect(java.util.stream.Collectors.toList());
        if (supportedFactories.size() != 1) {
            throw new IllegalStateException("Keeper instance must have exactly one delay action factory, actual: "
                    + supportedFactories.size());
        }

        KeeperHealthCheckActionFactory<?> factory = supportedFactories.get(0);
        HealthCheckAction action = factory.create(instance);
        if (action == null) {
            throw new IllegalStateException("Keeper delay action factory returned null");
        }
        instance.register(action);
        keeperFactoriesByAction.put(action, factory);
    }

    private void cleanupKeeperInstance(KeeperHealthCheckInstance instance, boolean rollback) {
        List<HealthCheckAction> actions = new ArrayList<>(instance.getHealthCheckActions());
        try {
            for (HealthCheckAction action : actions) {
                destroyKeeperAction(action);
            }
            LifecycleHelper.stopIfPossible(instance);
            if (rollback) {
                LifecycleHelper.disposeIfPossible(instance);
            }
        } catch (Exception e) {
            throw new IllegalStateException("failed to cleanup Keeper health-check instance " + instance, e);
        }

        for (HealthCheckAction action : actions) {
            keeperFactoriesByAction.remove(action);
            instance.unregister(action);
        }
        if (instance instanceof DefaultKeeperHealthCheckInstance) {
            ((DefaultKeeperHealthCheckInstance) instance).setEndpoint(null).setSession(null);
        }
    }

    @SuppressWarnings("unchecked")
    private void destroyKeeperAction(HealthCheckAction action) throws Exception {
        KeeperHealthCheckActionFactory factory = keeperFactoriesByAction.get(action);
        if (factory == null) {
            throw new IllegalStateException("missing Keeper action factory for " + action);
        }
        factory.destroy(action);
    }

    private boolean isTfsKeeper(KeeperMeta keeperMeta) {
        Long containerId = keeperMeta.getKeeperContainerId();
        if (containerId == null) {
            return false;
        }
        ShardMeta shardMeta = keeperMeta.parent();
        ClusterMeta clusterMeta = shardMeta.parent();
        DcMeta dcMeta = clusterMeta.parent();
        for (KeeperContainerMeta containerMeta : dcMeta.getKeeperContainers()) {
            if (Objects.equals(containerId, containerMeta.getId())) {
                return KeeperDiskTypeUtils.isTfs(containerMeta.getDiskType());
            }
        }
        return false;
    }

    private KeeperInstanceInfo createKeeperInstanceInfo(KeeperMeta keeperMeta) {
        ShardMeta shardMeta = keeperMeta.parent();
        ClusterMeta clusterMeta = shardMeta.parent();
        DcMeta dcMeta = clusterMeta.parent();
        DefaultKeeperInstanceInfo info = new DefaultKeeperInstanceInfo(dcMeta.getId(), clusterMeta.getId(),
                shardMeta.getId(), shardMeta.getDbId(), new HostPort(keeperMeta.getIp(), keeperMeta.getPort()),
                clusterMeta.getActiveDc(), ClusterType.lookup(clusterMeta.getType()));
        Integer orgId = clusterMeta.getOrgId();
        info.setClusterOrgId(orgId == null ? -1 : orgId);
        info.setStatus(clusterMeta.getStatus());
        return info;
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
        Integer orgId = clusterMeta.getOrgId();
        info.setClusterOrgId(orgId == null ? -1 : orgId);
        info.isMaster(redisMeta.isMaster());
        if (redisMeta.getCreateTime() != null) {
            info.setCreateTime(new Date(redisMeta.getCreateTime()));
        }
        if (clusterType.supportSingleActiveDC()) {
            info.setCrossRegion(metaCache.isCrossRegion(info.getActiveDc(), info.getDcId()));
            info.setShardDbId(redisMeta.parent().getDbId());
        } else if (clusterType.supportMultiActiveDC()) {
            info.setCrossRegion(metaCache.isCrossRegion(currentDcId, info.getDcId()));
        }
        info.setStatus(clusterMeta.getStatus());

        return info;
    }

    @Override
    public ClusterHealthCheckInstance create(ClusterMeta clusterMeta) {
        DefaultClusterHealthCheckInstance instance = new DefaultClusterHealthCheckInstance();

        ClusterType clusterType = ClusterType.lookup(clusterMeta.getType());
        ClusterInstanceInfo info = getClusterInstanceInfo(clusterMeta, clusterType);
        HealthCheckConfig config = new DefaultHealthCheckConfig(checkerConfig, relationsService);

        instance.setInstanceInfo(info).setHealthCheckConfig(config);
        initActions(instance);
        startCheck(instance);

        return instance;
    }

    private ClusterInstanceInfo getClusterInstanceInfo(ClusterMeta clusterMeta, ClusterType clusterType) {
        Set<String> backupDcs = new HashSet<>();
        if (clusterMeta.getBackupDcs() != null && !clusterMeta.getBackupDcs().isEmpty()) {
            backupDcs.addAll(Arrays.asList(clusterMeta.getBackupDcs().toLowerCase().split("\\s*,\\s*")));
        }
        DefaultClusterInstanceInfo info = new DefaultClusterInstanceInfo(clusterMeta.getId(), clusterMeta.getActiveDc(),
                clusterType, clusterMeta.getOrgId(), clusterMeta.getLastModifiedTime());
        info.setBackupDcs(Lists.newArrayList(backupDcs));
        info.setStatus(clusterMeta.getStatus());
        return info;
    }

    @Override
    public RedisHealthCheckInstance getOrCreateRedisInstanceForInfoReplIdAction(RedisMeta redis) {
        DefaultRedisHealthCheckInstance instance = new DefaultRedisHealthCheckInstance();

        RedisInstanceInfo info = createRedisInstanceInfo(redis);
        HealthCheckConfig config = new CompositeHealthCheckConfig(info, checkerConfig, relationsService, metaCache.isCrossRegion(currentDcId, info.getDcId()));
        Endpoint endpoint = endpointFactory.getOrCreateEndpoint(redis);

        instance.setEndpoint(endpoint)
                .setSession(redisSessionManager.findOrCreateSession(endpoint))
                .setInstanceInfo(info)
                .setHealthCheckConfig(config);

        initActionsForRedisForInfoReplIdAction(instance);
        startCheck(instance);

        return instance;
    }

    private void initActionsForRedisForInfoReplIdAction(DefaultRedisHealthCheckInstance instance) {
        List<RedisHealthCheckActionFactory<?>> redisHealthCheckActionFactories = factoriesByClusterType.get(instance.getCheckInfo().getClusterType());
        if (redisHealthCheckActionFactories == null) {
            return;
        }
        for(RedisHealthCheckActionFactory<?> factory : redisHealthCheckActionFactories) {
            if (factory instanceof PingActionFactory || factory instanceof InfoReplIdActionFactory) {
                initActions(instance, factory);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void initActions(DefaultRedisHealthCheckInstance instance) {
        List<RedisHealthCheckActionFactory<?>> redisHealthCheckActionFactories = factoriesByClusterType.get(instance.getCheckInfo().getClusterType());
        if (redisHealthCheckActionFactories == null) return;
        for(RedisHealthCheckActionFactory<?> factory : redisHealthCheckActionFactories) {
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
        List<ClusterHealthCheckActionFactory<?>> clusterHealthCheckActionFactories = clusterHealthCheckFactoriesByClusterType.get(instance.getCheckInfo().getClusterType());
        if (clusterHealthCheckActionFactories == null) return;
        ClusterInstanceInfo info = instance.getCheckInfo();
        boolean isBackupDcAndCrossRegion = ClusterType.ONE_WAY == info.getClusterType() && metaCache.isBackupDcAndCrossRegion(currentDcId, info.getActiveDc(), info.getBackupDcs());
        for (ClusterHealthCheckActionFactory<?> factory : clusterHealthCheckActionFactories) {
            if (factory instanceof SiteLeaderAwareHealthCheckActionFactory) {
                if (!isBackupDcAndCrossRegion || factory instanceof CrossRegionSupport) {
                    installActionIfNeeded((SiteLeaderAwareHealthCheckActionFactory) factory, instance);
                }
            } else if (!isBackupDcAndCrossRegion || factory instanceof CrossRegionSupport) {
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
