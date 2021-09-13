package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.Persistence;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.*;
import com.ctrip.xpipe.redis.checker.healthcheck.leader.AbstractLeaderAwareHealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.google.common.collect.Maps;
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

    private Map<RedisHealthCheckInstance, SentinelHellos> hellos = Maps.newConcurrentMap();

    private Map<RedisHealthCheckInstance, Throwable> errors = Maps.newConcurrentMap();

    private CheckerDbConfig checkerDbConfig;

    private Persistence persistence;

    public static final String LOG_TITLE = "SentinelHelloCollect";

    private MetaCache metaCache;

    private HealthCheckInstanceManager instanceManager;

    private Set<RedisHealthCheckInstance> redisInstancesToCheck = new HashSet<>();

    private volatile boolean collecting = false;

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
        TransactionMonitor transaction = TransactionMonitor.DEFAULT;

        transaction.logTransactionSwallowException("sentinel.health.check", instance.getCheckInfo().getClusterId(), new Task() {
            @Override
            public void go() throws Exception {
                redisInstancesToCheck = redisInstancesToCheck();

                collecting = true;

                subAllRedisInstances(redisInstancesToCheck);

                scheduled.schedule(new AbstractExceptionLogTask() {
                    @Override
                    protected void doRun() throws Exception {
                        processSentinelHellos();
                    }
                }, SENTINEL_COLLECT_INFO_INTERVAL, TimeUnit.MILLISECONDS);
            }

            @Override
            public Map<String, Object> getData() {
                Map<String, Object> transactionData = new HashMap<>();
                transactionData.put("checkInterval", getBaseCheckInterval());
                transactionData.put("checkRedisInstances", redisInstancesToCheck);
                return transactionData;
            }
        });
    }

    @Override
    protected int getBaseCheckInterval() {
        return instance.getHealthCheckConfig().getSentinelCheckIntervalMilli();
    }

    @Override
    protected int getCheckTimeInterval(int baseInterval) {
        return Math.abs(random.nextInt(baseInterval) % baseInterval);
    }

    @Override
    protected Logger getHealthCheckLogger() {
        return logger;
    }

    Set<RedisHealthCheckInstance> redisInstancesToCheck() {
        Set<RedisHealthCheckInstance> redisHealthCheckInstances = new HashSet<>();
        try {
            metaCache.getXpipeMeta().getDcs().forEach((dc, dcMeta) -> {
                ClusterMeta clusterMeta = dcMeta.getClusters().get(getActionInstance().getCheckInfo().getClusterId());
                logger.debug("[{}-{}][{}]found in MetaCache", LOG_TITLE, instance.getCheckInfo().getClusterId(), dc);
                if (clusterMeta != null) {
                    Map<String, ShardMeta> clusterShards = clusterMeta.getShards();
                    logger.debug("[{}-{}][{}]shards num:{}, detail info:{}", LOG_TITLE, instance.getCheckInfo().getClusterId(), dc, clusterShards.size(), clusterShards);
                    clusterShards.forEach((shardId, shardMeta) -> {
                        try {
                            List<RedisMeta> redisMetas = shardMeta.getRedises();
                            logger.debug("[{}-{}+{}][{}]redis num:{}, detail info:{}", LOG_TITLE, instance.getCheckInfo().getClusterId(), shardId, dc, redisMetas.size(), redisMetas);
                            redisMetas.forEach((redisMeta) -> {
                                try {
                                    RedisHealthCheckInstance redisInstance = instanceManager.findRedisHealthCheckInstance(new HostPort(redisMeta.getIp(), redisMeta.getPort()));
                                    if (super.shouldCheck(redisInstance)) {
                                        redisHealthCheckInstances.add(redisInstance);
                                        hellos.put(redisInstance, new SentinelHellos());
                                    } else {
                                        redisInstance.getRedisSession().closeSubscribedChannel(HELLO_CHANNEL);
                                    }
                                } catch (Exception e) {
                                    logger.warn("[{}-{}+{}]get redis health check instance {}:{} failed", LOG_TITLE, instance.getCheckInfo().getClusterId(), shardId, redisMeta.getIp(), redisMeta.getPort(), e);
                                }
                            });
                        } catch (Exception e) {
                            logger.warn("[{}-{}+{}]get redis health check instance from shard {} failed", LOG_TITLE, instance.getCheckInfo().getClusterId(), shardId, shardId, e);
                        }
                    });
                }
            });
        } catch (Exception e) {
            logger.warn("[{}-{}]get redis health check instances from cluster {} failed", LOG_TITLE, instance.getCheckInfo().getClusterId(), instance.getCheckInfo().getClusterId(), e);
        }
        return redisHealthCheckInstances;
    }

    void subAllRedisInstances(Set<RedisHealthCheckInstance> redisInstancesToCheck) {
        redisInstancesToCheck.forEach(redisInstanceToCheck -> {
            try {
                RedisInstanceInfo info = redisInstanceToCheck.getCheckInfo();
                if (redisInstanceToCheck.getCheckInfo().isInActiveDc()) {
                    logger.info("[{}-{}+{}]{} instance {} in active dc", LOG_TITLE, info.getClusterShardHostport().getClusterName(),
                            info.getShardId(), info.getDcId(), redisInstanceToCheck.getCheckInfo().getHostPort());
                }
                redisInstanceToCheck.getRedisSession().subscribeIfAbsent(HELLO_CHANNEL, new RedisSession.SubscribeCallback() {
                    @Override
                    public void message(String channel, String message) {
                        if (!collecting)
                            return;

                        SentinelHello hello = SentinelHello.fromString(message);
                        hellos.get(redisInstanceToCheck).addSentinelHello(hello);

                    }

                    @Override
                    public void fail(Throwable e) {
                        if (!collecting)
                            return;

                        logger.warn("[{}-{}+{}]{} instance {} sub-failed, reason:{}", LOG_TITLE, info.getClusterShardHostport().getClusterName(), info.getShardId(), info.getDcId(), info.getHostPort(), e.getMessage());
                        errors.put(redisInstanceToCheck, e);
                    }
                });
            } catch (Exception e) {
                logger.warn("[{}-{}]subscribe redis instance {}:{} failed", LOG_TITLE,instance.getCheckInfo().getClusterId(), redisInstanceToCheck.getEndpoint().getHost(), redisInstanceToCheck.getEndpoint().getPort(), e);
            }
        });
    }

    @VisibleForTesting
    protected void processSentinelHellos() {

        collecting = false;

        if (hellos.size() + errors.size() == 0) {
            logger.warn("[{}-{}]sub result empty", LOG_TITLE, instance.getCheckInfo().getClusterId());
            resetResults();
            return;
        }

        List<SentinelActionContext> contexts = new ArrayList<>();

        for (RedisHealthCheckInstance instance : errors.keySet())
            hellos.remove(instance);

        for (RedisHealthCheckInstance instance : hellos.keySet()) {
            contexts.add(new SentinelActionContext(instance, hellos.get(instance).getSentinelHellos()));
        }

        for (RedisHealthCheckInstance instance : errors.keySet()) {
            contexts.add(new SentinelActionContext(instance, errors.get(instance)));
        }

        resetResults();

        logger.debug("[{}-{}]sub result: {}", LOG_TITLE, instance.getCheckInfo().getClusterId(), contexts);
        contexts.forEach(this::notifyListeners);
    }

    @Override
    protected boolean shouldCheck(HealthCheckInstance checkInstance) {
        String cluster = checkInstance.getCheckInfo().getClusterId();

        if (!checkerDbConfig.shouldSentinelCheck(cluster)) {
            logger.warn("[{}][BackupDc] cluster {} is in sentinel check whitelist, quit", LOG_TITLE, cluster);
            return false;
        }

        if (persistence.isClusterOnMigration(cluster)) {
            logger.warn("[{}][{}] in migration, stop check", LOG_TITLE, cluster);
            return false;
        }

        return checkerDbConfig.isSentinelAutoProcess();
    }

    @Override
    public void doStop() {
        redisInstancesToCheck.forEach(redisInstance -> {
            redisInstance.getRedisSession().closeSubscribedChannel(HELLO_CHANNEL);
        });
        resetResults();
        super.doStop();
    }

    void resetResults() {
        hellos.clear();
        errors.clear();
        collecting = false;
    }

    @VisibleForTesting
    Map<RedisHealthCheckInstance, SentinelHellos> getHellos() {
        return hellos;
    }

    @VisibleForTesting
    void setHellos(Map<RedisHealthCheckInstance, SentinelHellos> hellos) {
        this.hellos = hellos;
    }

    @VisibleForTesting
    Map<RedisHealthCheckInstance, Throwable> getErrors() {
        return errors;
    }

    @VisibleForTesting
    boolean isCollecting() {
        return collecting;
    }

    @VisibleForTesting
    void setCollecting(boolean collecting) {
        this.collecting = collecting;
    }

    @VisibleForTesting
    void setErrors(Map<RedisHealthCheckInstance, Throwable> errors) {
        this.errors = errors;
    }

    class SentinelHellos {
        private Set<SentinelHello> sentinelHellos = new HashSet<>();

        public void addSentinelHello(SentinelHello sentinelHello) {
            sentinelHellos.add(sentinelHello);
        }

        public Set<SentinelHello> getSentinelHellos() {
            return sentinelHellos;
        }

        public SentinelHellos addSentinelHellos(Set<SentinelHello> sentinelHellos) {
            sentinelHellos.addAll(sentinelHellos);
            return this;
        }
    }
}
