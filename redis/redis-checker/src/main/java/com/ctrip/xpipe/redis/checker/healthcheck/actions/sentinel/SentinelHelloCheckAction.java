package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.exception.ExceptionUtils;
import com.ctrip.xpipe.redis.checker.Persistence;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
public class SentinelHelloCheckAction extends AbstractLeaderAwareHealthCheckAction<ClusterHealthCheckInstance> {

    private static final Logger logger = LoggerFactory.getLogger(SentinelHelloCheckAction.class);

    protected static int SENTINEL_COLLECT_INFO_INTERVAL = 5000;

    public static final String HELLO_CHANNEL = "__sentinel__:hello";

    private Map<RedisHealthCheckInstance, Set<SentinelHello>> hellos = Maps.newConcurrentMap();

    private Map<RedisHealthCheckInstance, Throwable> errors = Maps.newConcurrentMap();

    private CheckerDbConfig checkerDbConfig;

    private Persistence persistence;

    private static final int SENTINEL_CHECK_BASE_INTERVAL = 10000;

    protected long lastStartTime = System.currentTimeMillis();

    private MetaCache metaCache;

    private HealthCheckInstanceManager instanceManager;

    public SentinelHelloCheckAction(ScheduledExecutorService scheduled, ClusterHealthCheckInstance instance,
                                    ExecutorService executors, CheckerDbConfig checkerDbConfig, Persistence persistence, MetaCache metaCache, HealthCheckInstanceManager instanceManager) {
        super(scheduled, instance, executors);
        this.checkerDbConfig = checkerDbConfig;
        this.persistence = persistence;
        this.metaCache = metaCache;
        this.instanceManager= instanceManager;
    }

    @Override
    protected void doTask() {
        lastStartTime = System.currentTimeMillis();

        Set<RedisHealthCheckInstance> redisInstancesToCheck = redisInstancesToCheck();

        subAllRedisInstances(redisInstancesToCheck);

        scheduled.schedule(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() throws Exception {
                processSentinelHellos();
            }
        }, SENTINEL_COLLECT_INFO_INTERVAL, TimeUnit.MILLISECONDS);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    Set<RedisHealthCheckInstance> redisInstancesToCheck() {
        Set<RedisHealthCheckInstance> redisHealthCheckInstances = new HashSet<>();
        metaCache.getXpipeMeta().getDcs().forEach((dc, dcMeta) -> {
            ClusterMeta clusterMeta = dcMeta.getClusters().get(getActionInstance().getCheckInfo().getClusterId());
            clusterMeta.getShards().forEach((shardId, shardMeta) -> {
                shardMeta.getRedises().forEach((redisMeta) -> {
                    RedisHealthCheckInstance redisInstance = instanceManager.getOrCreate(redisMeta);
                    if (super.shouldCheck(redisInstance))
                        redisHealthCheckInstances.add(redisInstance);
                });
            });
        });
        return redisHealthCheckInstances;
    }

    void subAllRedisInstances(Set<RedisHealthCheckInstance> redisInstancesToCheck) {
        redisInstancesToCheck.forEach(redisInstanceToCheck -> {
            RedisInstanceInfo info = redisInstanceToCheck.getCheckInfo();
            if (redisInstanceToCheck.getCheckInfo().isInActiveDc()) {
                logger.info("[doTask][{}-{}] in active dc, redis {}", info.getClusterId(), info.getShardId(), redisInstanceToCheck.getEndpoint());
            }
            redisInstanceToCheck.getRedisSession().subscribeIfAbsent(HELLO_CHANNEL, new RedisSession.SubscribeCallback() {
                @Override
                public void message(String channel, String message) {
                    executors.execute(new Runnable() {
                        @Override
                        public void run() {
                            SentinelHello hello = SentinelHello.fromString(message);
                            Set<SentinelHello> currentInstanceHellos = hellos.get(redisInstanceToCheck);
                            if (currentInstanceHellos == null) {
                                hellos.put(redisInstanceToCheck, Sets.newHashSet(hello));
                            } else {
                                currentInstanceHellos.add(hello);
                            }
                        }
                    });
                }

                @Override
                public void fail(Throwable e) {
                    if (ExceptionUtils.isStackTraceUnnecessary(e)) {
                        logger.error("[sub-failed][{}] {}", redisInstanceToCheck.getCheckInfo().getHostPort(), e.getMessage());
                    } else {
                        logger.error("[sub-failed][{}]", redisInstanceToCheck.getCheckInfo().getHostPort(), e);
                    }
                    errors.put(redisInstanceToCheck, e);
                }
            });
        });
    }

    @VisibleForTesting
    protected void processSentinelHellos() {

        List<SentinelActionContext> contexts = new ArrayList<>();

        for (RedisHealthCheckInstance instance : hellos.keySet()) {
            instance.getRedisSession().closeSubscribedChannel(HELLO_CHANNEL);
            contexts.add(new SentinelActionContext(instance, hellos.get(instance)));
        }

        for (RedisHealthCheckInstance instance : errors.keySet()) {
            contexts.add(new SentinelActionContext(instance, errors.get(instance)));
        }

        contexts.forEach(this::notifyListeners);

        hellos = Maps.newConcurrentMap();
        errors = Maps.newConcurrentMap();
    }

    protected boolean shouldCheck(HealthCheckInstance checkInstance) {
        long current = System.currentTimeMillis();
        if( current - lastStartTime < getIntervalMilli()){
            logger.debug("[generatePlan][too quick {}, quit]", current - lastStartTime);
            return false;
        }

        String cluster = checkInstance.getCheckInfo().getClusterId();
        if (!checkerDbConfig.shouldSentinelCheck(cluster)) {
            logger.warn("[doTask][BackupDc] cluster is in sentinel check whitelist, quit");

            return false;
        }

        if (persistence.isClusterOnMigration(cluster)) {
            logger.warn("[shouldStart][{}] in migration, stop check", cluster);
            return false;
        }

        return checkerDbConfig.isSentinelAutoProcess();
    }

    @Override
    protected int getBaseCheckInterval() {
        return SENTINEL_CHECK_BASE_INTERVAL;
    }

    protected int getIntervalMilli() {
        return instance.getHealthCheckConfig().getSentinelCheckIntervalMilli();
    }

}
