package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.SequenceCommandChain;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.PersistenceCache;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.config.CheckerDbConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.ActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.HealthCheckAction;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisInstanceInfo;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelActionContext;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCollector;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelLeakyBucket;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command.*;
import com.ctrip.xpipe.redis.core.exception.MasterNotFoundException;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;
import static com.ctrip.xpipe.redis.checker.resource.Resource.SENTINEL_KEYED_NETTY_CLIENT_POOL;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.THREAD_POOL_TIME_OUT;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
@Component("defaultSentinelHelloCollector")
public class DefaultSentinelHelloCollector implements SentinelHelloCollector {

    protected static final Logger logger = LoggerFactory.getLogger(DefaultSentinelHelloCollector.class);

    @Autowired
    protected MetaCache metaCache;

    @Autowired
    private CheckerConfig checkerConfig;

    @Autowired
    private CheckerDbConfig checkerDbConfig;

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private SentinelManager sentinelManager;

    @Autowired
    private PersistenceCache persistenceCache;

    @Resource(name = SENTINEL_KEYED_NETTY_CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    private SentinelLeakyBucket leakyBucket;

    private ExecutorService collectExecutor;

    private ExecutorService resetExecutor;

    private ScheduledExecutorService scheduled;

    private Map<ClusterType, String[]> clusterTypeSentinelConfig = new HashMap<>();

    public static final String NO_SUCH_MASTER = "ERR No such master with that name";

    @PostConstruct
    public void postConstruct() {
        try {
            collectExecutor = new DefaultExecutorFactory(getClass().getSimpleName() + "-collect", 2 * OsUtils.getCpuCount(), 2 * OsUtils.getCpuCount(),
                    new ThreadPoolExecutor.AbortPolicy()).createExecutorService();
            resetExecutor = new DefaultExecutorFactory(getClass().getSimpleName() + "-reset", 2 * OsUtils.getCpuCount(), 2 * OsUtils.getCpuCount(),
                    new ThreadPoolExecutor.AbortPolicy()).createExecutorService();
            scheduled = MoreExecutors.getExitingScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(Math.min(8, OsUtils.getCpuCount()), XpipeThreadFactory.create(getClass().getSimpleName() + "-scheduled")),
                    THREAD_POOL_TIME_OUT, TimeUnit.SECONDS
            );
            leakyBucket = new SentinelLeakyBucket(checkerConfig, scheduled);
            leakyBucket.start();
            Map<String, String> configMap = checkerConfig.sentinelMasterConfig();
            configMap.forEach((k, v) -> {
                String[] sentinelConfigs = v.split("\\s*,\\s*");
                clusterTypeSentinelConfig.put(ClusterType.lookup(k), sentinelConfigs);
            });
        } catch (Exception e) {
            logger.error("[postConstruct]", e);
        }
    }

    @PreDestroy
    public void preDestroy() {
        if (collectExecutor != null) {
            try {
                collectExecutor.shutdownNow();
            } catch (Exception e) {
                logger.error("[preDestroy-collectExecutor]", e);
            }
        }
        if (resetExecutor != null) {
            try {
                resetExecutor.shutdownNow();
            } catch (Exception e) {
                logger.error("[preDestroy-resetExecutor]", e);
            }
        }
        if (scheduled != null) {
            try {
                scheduled.shutdownNow();
            } catch (Exception e) {
                logger.error("[preDestroy-scheduled]", e);
            }
        }
        if (leakyBucket != null) {
            try {
                leakyBucket.stop();
            } catch (Exception e) {
                logger.error("[preDestroy-leakyBucket]", e);
            }
        }
    }

    @Override
    public void onAction(SentinelActionContext context) {
        try {
            CommandFuture<Void> future = new SentinelHelloCollectorCommand(context).execute(collectExecutor);
            ScheduledFuture<?> timeoutFuture = scheduled.schedule(new Runnable() {
                @Override
                public void run() {
                    future.cancel(true);
                    CatEventMonitor.DEFAULT.logEvent("Sentinel.Hello.Collector.Cancel",
                            String.format("%s+%s", context.instance().getCheckInfo().getShardId(), context.instance().getCheckInfo().getDcId()));
                }
            }, context.instance().getHealthCheckConfig().getSentinelCheckIntervalMilli(), TimeUnit.MILLISECONDS);
            future.addListener(commandFuture -> {
                if (!commandFuture.isCancelled())
                    timeoutFuture.cancel(true);
            });
        } catch (Throwable e) {
            logger.error("[DefaultSentinelHelloCollector]onAction", e);
        }
    }

    @Override
    public void stopWatch(HealthCheckAction action) {

    }

    @Override
    public boolean worksfor(ActionContext t) {
        return false;
    }

    protected String getSentinelMonitorName(RedisInstanceInfo info) {
        return metaCache.getSentinelMonitorName(info.getClusterId(), info.getShardId());
    }

    protected Set<HostPort> getSentinels(RedisInstanceInfo info) {
        return metaCache.getActiveDcSentinels(info.getClusterId(), info.getShardId());
    }

    protected HostPort getMaster(RedisInstanceInfo info) throws MasterNotFoundException {
        return metaCache.findMaster(info.getClusterId(), info.getShardId());
    }

    protected List<HostPort> getShardInstances(RedisInstanceInfo info) {
        return metaCache.getRedisOfDcClusterShard(info.getActiveDc(), info.getClusterId(), info.getShardId()).stream().map(redisMeta -> new HostPort(redisMeta.getIp(),redisMeta.getPort())).collect(Collectors.toList());
    }

    public class SentinelHelloCollectorCommand extends AbstractCommand<Void> {

        private RedisInstanceInfo info;
        private Set<SentinelHello> hellos;
        private String sentinelMonitorName;
        private Set<HostPort> sentinels;
        private HostPort metaMaster;
        private List<HostPort> shardInstances;

        public SentinelHelloCollectorCommand(SentinelActionContext context) {
            this.hellos = Sets.newHashSet(context.getResult());
            this.info = context.instance().getCheckInfo();
            this.sentinelMonitorName = getSentinelMonitorName(info);
            this.sentinels = getSentinels(info);
            this.metaMaster = getMetaMaster();
            this.shardInstances = getShardInstances(info);
        }

        @Override
        protected void doExecute() throws Throwable {
            if (!future().isDone()) {
                String cluster = info.getClusterId();
                if (!checkerDbConfig.shouldSentinelCheck(info.getClusterId())) {
                    logger.info("[{}-{}+{}] {} in white list, skip", LOG_TITLE, info.getClusterId(), info.getShardId(), info.getClusterId());
                    future().setSuccess();
                } else if (persistenceCache.isClusterOnMigration(cluster)) {
                    logger.info("[{}-{}+{}] {} in migration, skip", LOG_TITLE, cluster, info.getShardId(), cluster);
                    future().setSuccess();
                } else {
                    TransactionMonitor transaction = TransactionMonitor.DEFAULT;
                    transaction.logTransactionSwallowException("sentinel.hello.collect", info.getClusterId(), new Task() {

                        @Override
                        public void go() throws Exception {
                            SequenceCommandChain chain = new SequenceCommandChain(false, false);
                            SentinelHelloCollectContext context = new SentinelHelloCollectContext(info, hellos, sentinelMonitorName, sentinels, metaMaster, shardInstances, clusterTypeSentinelConfig);
                            chain.add(new DeleteWrongSentinels(context, sentinelManager));
                            chain.add(new CheckMissingOrMasterSwitchedSentinels(context, alertManager, checkerConfig, sentinelManager));
                            chain.add(new CheckFailoverInProgress(context, sentinelManager));
                            chain.add(new CheckTrueMaster(context, alertManager, keyedObjectPool, scheduled));
                            chain.add(new AnalyseHellos(context, checkerConfig));
                            chain.add(new AcquireLeakyBucket(context, leakyBucket));
                            chain.add(new DeleteSentinels(context, sentinelManager));
                            chain.add(new ResetSentinels(context, metaCache, keyedObjectPool, scheduled, resetExecutor, sentinelManager, checkerConfig));
                            chain.add(new AddSentinels(context, sentinelManager, checkerConfig));
                            chain.add(new SetSentinels(context, sentinelManager));
                            chain.execute().addListener(commandFuture -> {
                                future().setSuccess();
                            });
                        }

                        @Override
                        public Map<String, Object> getData() {
                            Map<String, Object> transactionData = new HashMap<>();
                            transactionData.put("monitorName", sentinelMonitorName);
                            transactionData.put("sentinels", sentinels);
                            transactionData.put("hellos", hellos);
                            return transactionData;
                        }
                    });
                }
            }
        }

        HostPort getMetaMaster() {
            HostPort metaMaster = null;
            try {
                metaMaster = getMaster(info);
            } catch (MasterNotFoundException e) {
                logger.warn("[{}-{}+{}] {} master not found", LOG_TITLE, info.getClusterId(), info.getShardId(), info.getDcId(), e);
            }

            return metaMaster;
        }

        @Override
        protected void doReset() {
        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }

    }

    @VisibleForTesting
    public void setAlertManager(AlertManager alertManager) {
        this.alertManager = alertManager;
    }

    @VisibleForTesting
    protected DefaultSentinelHelloCollector setMetaCache(MetaCache metaCache) {
        this.metaCache = metaCache;
        return this;
    }

    @VisibleForTesting
    protected DefaultSentinelHelloCollector setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
        return this;
    }

    @VisibleForTesting
    protected DefaultSentinelHelloCollector setKeyedObjectPool(XpipeNettyClientKeyedObjectPool keyedObjectPool) {
        this.keyedObjectPool = keyedObjectPool;
        return this;
    }


    public void setCollectExecutor(ExecutorService collectExecutor) {
        this.collectExecutor = collectExecutor;
    }

    public void setResetExecutor(ExecutorService resetExecutor) {
        this.resetExecutor = resetExecutor;
    }

    @VisibleForTesting
    void setClusterTypeSentinelConfig(Map<ClusterType, String[]> clusterTypeSentinelConfig) {
        this.clusterTypeSentinelConfig = clusterTypeSentinelConfig;
    }
}
