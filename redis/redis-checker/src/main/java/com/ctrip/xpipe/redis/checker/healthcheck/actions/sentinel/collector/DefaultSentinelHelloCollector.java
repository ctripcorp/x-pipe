package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.concurrent.DefaultExecutorFactory;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
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
import com.ctrip.xpipe.redis.checker.healthcheck.session.DefaultRedisSessionManager;
import com.ctrip.xpipe.redis.checker.healthcheck.session.RedisSession;
import com.ctrip.xpipe.redis.core.exception.MasterNotFoundException;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.meta.QuorumConfig;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterRole;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.*;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;
import static com.ctrip.xpipe.redis.checker.resource.Resource.KEYED_NETTY_CLIENT_POOL;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.THREAD_POOL_TIME_OUT;

/**
 * @author chen.zhu
 * <p>
 * Oct 09, 2018
 */
@Component("defaultSentinelHelloCollector")
public class DefaultSentinelHelloCollector implements SentinelHelloCollector {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSentinelHelloCollector.class);

    private static final String SENTINEL_TYPE = "sentinel";

    @Autowired
    protected MetaCache metaCache;

    @Autowired
    private CheckerConfig checkerConfig;

    @Autowired
    private CheckerDbConfig checkerDbConfig;

    @Autowired
    private DefaultRedisSessionManager sessionManager;

    @Autowired
    private AlertManager alertManager;

    @Autowired
    private SentinelManager sentinelManager;

    @Resource(name = KEYED_NETTY_CLIENT_POOL)
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;

    private SentinelLeakyBucket leakyBucket;

    private ExecutorService collectExecutor;

    private ExecutorService resetExecutor;

    private ScheduledExecutorService scheduled;

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
                    EventMonitor.DEFAULT.logEvent(SENTINEL_TYPE,
                            String.format("[%s]%s+%s", "cancel", context.instance().getCheckInfo().getShardId(), context.instance().getCheckInfo().getDcId()));
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

    private void collect(SentinelActionContext context) {

        RedisInstanceInfo info = context.instance().getCheckInfo();
        String cluster = info.getClusterId();

        if ((System.currentTimeMillis() - context.getRecvTimeMilli()) > context.instance().getHealthCheckConfig().getSentinelCheckIntervalMilli()) {
            logger.warn("[{}-{}+{}] {} expired, skip", LOG_TITLE, cluster, info.getShardId(), cluster);
            return;
        }

        if (!checkerDbConfig.shouldSentinelCheck(cluster)) {
            logger.info("[{}-{}+{}] {} in white list, skip", LOG_TITLE, cluster, info.getShardId(), cluster);
            return;
        }

        Set<SentinelHello> originalHellos = context.getResult();
        Set<SentinelHello> hellos = Sets.newHashSet(originalHellos);
        String clusterId = info.getClusterId();
        String shardId = info.getShardId();
        String sentinelMonitorName = getSentinelMonitorName(info);
        Set<HostPort> sentinels = getSentinels(info);
        QuorumConfig quorumConfig = checkerConfig.getDefaultSentinelQuorumConfig();

        TransactionMonitor transaction = TransactionMonitor.DEFAULT;
        transaction.logTransactionSwallowException("sentinel.hello.collect", clusterId, new Task() {

            Set<SentinelHello> toDelete = new HashSet<>();
            Set<HostPort> trueMasters = new HashSet<>();
            Set<SentinelHello> toAdd = new HashSet<>();
            @Override
            public void go() throws Exception {
                HostPort trueMaster = null;
                try{
                    trueMaster = getMaster(info);
                } catch (MasterNotFoundException e) {
                    logger.error("[{}-{}+{}] {} master not found", LOG_TITLE, clusterId, shardId, info.getDcId(), e);
                }

                logger.debug("[{}-{}+{}] {} collected hellos: {}", LOG_TITLE, clusterId, shardId, info.getDcId(), hellos);

                // check stale hellos
                toDelete.addAll(checkStaleHellos(sentinelMonitorName, sentinels, hellos));

                // check true master
                trueMasters.addAll(checkTrueMasters(trueMaster, hellos));
                if (!currentMasterConsistent(trueMasters)) {
                    logger.warn("[{}-{}+{}] {} currentMasterConsistent: {}", LOG_TITLE, clusterId,shardId, sentinelMonitorName, trueMasters);
                    String message = String.format("master inconsistent, monitorName:%s, masters:%s",sentinelMonitorName, trueMasters);
                    alertManager.alert(clusterId, shardId, info.getHostPort(), ALERT_TYPE.SENTINEL_MONITOR_INCONSIS, message);
                    return;
                }
                trueMaster = trueMasters.iterator().next();

                // check wrong master hellos
                toDelete.addAll(checkWrongMasterHellos(hellos, trueMaster));

                // checkReset
                checkReset(clusterId, shardId, sentinelMonitorName, hellos);

                // check add
                toAdd.addAll(checkToAdd(clusterId, shardId, sentinelMonitorName, sentinels, hellos, trueMaster, quorumConfig));

                doAction(sentinelMonitorName, trueMaster, toDelete, toAdd, quorumConfig);
            }

            @Override
            public Map<String, Object> getData() {
                Map<String, Object> transactionData = new HashMap<>();
                transactionData.put("monitorName", sentinelMonitorName);
                transactionData.put("sentinels", sentinels);
                transactionData.put("hellos", originalHellos);
                transactionData.put("trueMasters", trueMasters);
                transactionData.put("toDelete", toDelete);
                transactionData.put("toAdd", toAdd);
                return transactionData;
            }
        });
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

    @VisibleForTesting
    protected Set<SentinelHello> checkWrongMasterHellos(Set<SentinelHello> hellos, HostPort trueMaster) {
        Set<SentinelHello> wrongMasters = new HashSet<>();
        for (SentinelHello sentinelHello : hellos) {
            if (!sentinelHello.getMasterAddr().equals(trueMaster)) {
                wrongMasters.add(sentinelHello);
            }
        }
        hellos.removeAll(wrongMasters);
        return wrongMasters;
    }

    @VisibleForTesting
    protected Set<HostPort> checkTrueMasters(HostPort metaMaster, Set<SentinelHello> hellos) {
        Set<HostPort> currentCollectedMasters = collectMetaMasterAndHelloMasters(metaMaster, hellos);
        if (currentMasterConsistent(currentCollectedMasters))
            return currentCollectedMasters;

        if (currentCollectedMasters.isEmpty())
            return Sets.newHashSet();

        Set<HostPort> trueMasters = new HashSet<>();
        currentCollectedMasters.forEach(currentCollectedMaster -> {
            RoleCommand roleCommand = new RoleCommand(keyedObjectPool.getKeyPool(new DefaultEndPoint(currentCollectedMaster.getHost(), currentCollectedMaster.getPort())), scheduled);
            try {
                Role role = roleCommand.execute().get(1000, TimeUnit.MILLISECONDS);
                if (role instanceof MasterRole) {
                    trueMasters.add(currentCollectedMaster);
                }
            } catch (Throwable e) {
                logger.warn("[{}][checkTrueMasters] check redis {} role err", LOG_TITLE, currentCollectedMaster, e);
            }
        });

        return trueMasters;
    }

    @VisibleForTesting
    protected Set<HostPort> collectMetaMasterAndHelloMasters(HostPort metaMaster, Set<SentinelHello> hellos) {
        Set<HostPort> masters = new HashSet<>();
        hellos.forEach(sentinelHello -> {
            masters.add(sentinelHello.getMasterAddr());
        });

        if (metaMaster != null)
            masters.add(metaMaster);
        return masters;
    }

    @VisibleForTesting
    protected Set<SentinelHello> checkStaleHellos(String sentinelMonitorName, Set<HostPort> sentinels,
                                        Set<SentinelHello> hellos) {

        Set<SentinelHello> toDelete = new HashSet<>();

        hellos.forEach((hello) -> {

            if (!hello.getMonitorName().equals(sentinelMonitorName)) {
                toDelete.add(hello);
            }
        });

        hellos.forEach((hello) -> {
            HostPort sentinel = hello.getSentinelAddr();
            if (!sentinels.contains(sentinel)) {
                toDelete.add(hello);
            }

        });

        hellos.forEach((hello) -> {
            if (isHelloMasterInWrongDc(hello)) {
                toDelete.add(hello);
            }
        });

        toDelete.forEach(hellos::remove);

        return toDelete;
    }

    @VisibleForTesting
    protected boolean currentMasterConsistent(Set<HostPort> currentMasters) {
        return currentMasters != null && currentMasters.size() == 1;
    }

    @VisibleForTesting
    protected void checkReset(String clusterId, String shardId, String sentinelMonitorName, Set<SentinelHello> hellos) {

        resetExecutor.execute(new Runnable() {
            @Override
            public void run() {
                Set<HostPort> allKeepers = metaCache.getAllKeepers();
                hellos.forEach((hello) -> {
                    HostPort sentinelAddr = hello.getSentinelAddr();
                    Sentinel sentinel = new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort());
                    try {
                        List<HostPort> slaves = sentinelManager.slaves(sentinel, sentinelMonitorName);
                        boolean shoudReset = false;
                        String reason = null;
                        Set<HostPort> keepers = new HashSet<>();

                        for (HostPort currentSlave : slaves) {

                            if (allKeepers.contains(currentSlave)) {
                                keepers.add(currentSlave);
                            }

                            Pair<String, String> clusterShard = metaCache.findClusterShard(currentSlave);
                            if (clusterShard == null) {
                                if (isKeeperOrDead(currentSlave)) {
                                    shoudReset = true;
                                    reason = String.format("[%s]keeper or dead, current:%s,%s, with no cluster shard", currentSlave, clusterId, shardId);
                                } else {
                                    String message = String.format("sentinel monitors redis %s not in xpipe", currentSlave.toString());
                                    alertManager.alert(clusterId, shardId, currentSlave, ALERT_TYPE.SENTINEL_MONITOR_REDUNDANT_REDIS, message);
                                }
                                continue;
                            }
                            if (!ObjectUtils.equals(clusterId, clusterShard.getKey()) || !ObjectUtils.equals(shardId, clusterShard.getValue())) {
                                shoudReset = true;
                                reason = String.format("[%s], current:%s,%s, but meta:%s:%s", currentSlave, clusterId, shardId, clusterShard.getKey(), clusterShard.getValue());
                                break;
                            }
                        }

                        if (!shoudReset && keepers.size() >= 2) {
                            shoudReset = true;
                            reason = String.format("%s,%s, has %d keepers:%s", clusterId, shardId, keepers.size(), keepers);
                        }

                        if (shoudReset) {
                            logger.info("[{}-{}+{}][reset]{}, {}, {}", LOG_TITLE, clusterId, shardId, sentinelMonitorName, sentinelAddr, reason);
                            EventMonitor.DEFAULT.logEvent(SENTINEL_TYPE,
                                    String.format("[%s]%s,%s", ALERT_TYPE.SENTINEL_RESET, sentinelAddr, reason));
                            sentinelManager.reset(sentinel, sentinelMonitorName);
                        }
                    } catch (Exception e) {
                        logger.error("[{}-{}+{}][checkReset]{}", LOG_TITLE, clusterId, shardId, hello, e);
                    }
                });
            }
        });

    }

    @VisibleForTesting
    protected boolean isKeeperOrDead(HostPort hostPort) {

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Object> role = new AtomicReference<>();

        try {
            RedisSession redisSession = sessionManager.findOrCreateSession(hostPort);
            redisSession.role(new RedisSession.RollCallback() {
                @Override
                public void role(String roleDesc) {
                    role.set(roleDesc);
                    latch.countDown();
                }

                @Override
                public void fail(Throwable e) {
                    logger.error("[isKeeperOrDead][fail]" + hostPort, e);
                    role.set(e);
                    latch.countDown();
                }
            });
        } catch (Exception e) {
            role.set(e);
            logger.error("[isKeeperOrDead]" + hostPort, e);
        }

        try {
            latch.await(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("[isKeeperOrDead]latch await error: {}", e);
        }

        if (role.get() instanceof String && Server.SERVER_ROLE.KEEPER.sameRole((String) role.get())) {
            return true;
        }
        if (role.get() instanceof CommandExecutionException || role.get() instanceof CommandTimeoutException
                || role.get() instanceof SocketException) {
            return true;
        }
        logger.info("[isKeeperOrDead] role: {}", role.get());
        return false;
    }

    @VisibleForTesting
    protected void doAction(String sentinelMonitorName, HostPort masterAddr, Set<SentinelHello> toDelete, Set<SentinelHello> toAdd,
                            QuorumConfig quorumConfig) {
        if ((toDelete == null || toDelete.size() == 0) && (toAdd == null || toAdd.size() == 0)) {
            return;
        }

        if (toAdd != null && toAdd.size() > 0) {
            logger.info("[{}-{}][toAdd]master: {}, stl: {}", LOG_TITLE, sentinelMonitorName, masterAddr,
                    toAdd.stream().map(SentinelHello::getSentinelAddr).collect(Collectors.toSet()));
        }

        if (toDelete != null && toDelete.size() > 0) {
            logger.info("[{}-{}][toDelete]{}", LOG_TITLE, sentinelMonitorName, toDelete);
        }

        // add rate limit logic to reduce frequently sentinel operations
        if (!leakyBucket.tryAcquire()) {
            logger.warn("[{}-{}][acquire failed]", LOG_TITLE, sentinelMonitorName);
            return;
        } else {
            // I got the lock, remember to release it
            leakyBucket.delayRelease(1000, TimeUnit.MILLISECONDS);
        }

        if (toDelete != null) {
            toDelete.forEach((hello -> {
                HostPort sentinelAddr = hello.getSentinelAddr();
                try {
                    CatEventMonitor.DEFAULT.logEvent(SENTINEL_TYPE, "[del]" + hello);
                    sentinelManager.removeSentinelMonitor(new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort()), hello.getMonitorName());
                    logger.info("[{}-{}][deleted]{}", LOG_TITLE, sentinelMonitorName, hello);
                } catch (Exception e) {
                    logger.error("[{}-{}][deleted]{}", LOG_TITLE, sentinelMonitorName, hello, e);
                }
            }));
        }

        if (toAdd != null) {
            toAdd.forEach((hello) -> {
                HostPort sentinelAddr = hello.getSentinelAddr();
                try {
                    Sentinel sentinel = new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort());
                    boolean doAdd = true;
                    try {
                        HostPort masterHostPort = sentinelManager.getMasterOfMonitor(sentinel, hello.getMonitorName());
                        if (masterHostPort != null) {
                            if (hello.getMasterAddr().equals(masterHostPort)) {
                                doAdd = false;
                                logger.info("[{}-{}][already exist]{}, {}", LOG_TITLE, sentinelMonitorName, masterHostPort, hello.getSentinelAddr());
                            } else {
                                sentinelManager.removeSentinelMonitor(sentinel, hello.getMonitorName());
                                logger.info("[{}-{}][removed wrong master]{}, {}", LOG_TITLE, sentinelMonitorName, masterHostPort, hello.getSentinelAddr());
                            }
                        }
                    } catch (Exception e) {
                        logger.warn("[{}-{}][check master exist error]{}", LOG_TITLE, sentinelMonitorName, hello.getSentinelAddr());
                    }
                    if (doAdd) {
                        CatEventMonitor.DEFAULT.logEvent(SENTINEL_TYPE, "[add]" + hello);
                        sentinelManager.monitorMaster(sentinel, hello.getMonitorName(), hello.getMasterAddr(), quorumConfig.getQuorum());
                        logger.info("[{}-{}][added]{}", LOG_TITLE, sentinelMonitorName, hello);
                    }
                } catch (Exception e) {
                    logger.error("[{}-{}][added]{}", LOG_TITLE, sentinelMonitorName, hello, e);
                }
            });
        }
    }


    @VisibleForTesting
    protected boolean isHelloMasterInWrongDc(SentinelHello hello) {
        // delete check for master not in primary dc
        HostPort hostPort = hello.getMasterAddr();
        return metaCache.inBackupDc(hostPort);
    }

    @VisibleForTesting
    protected Set<SentinelHello> checkToAdd(String clusterId, String shardId, String sentinelMonitorName, Set<HostPort> sentinels, Set<SentinelHello> hellos, HostPort masterAddr, QuorumConfig quorumConfig) {

        if(masterAddr == null){
            logger.warn("[{}-{}][checkToAdd][no master]", LOG_TITLE, sentinelMonitorName);
            return Sets.newHashSet();
        }

        if (StringUtil.isEmpty(sentinelMonitorName)) {
            logger.warn("[{}-{}+{}][checkToAdd][no monitor name]", clusterId, shardId);
            return Sets.newHashSet();
        }

        if (hellos.size() >= quorumConfig.getTotal()) {
            return Sets.newHashSet();
        }

        Set<HostPort> currentSentinels = new HashSet<>();
        hellos.forEach((hello -> currentSentinels.add(hello.getSentinelAddr())));

        Set<SentinelHello> toAdd = new HashSet<>();
        int toAddSize = quorumConfig.getTotal() - hellos.size();

        int i = 0;
        for (HostPort hostPort : sentinels) {
            if (!currentSentinels.contains(hostPort)) {
                i++;
                if(i > toAddSize){
                    break;
                }
                toAdd.add(new SentinelHello(hostPort, masterAddr, sentinelMonitorName));
            }
        }
        return toAdd;
    }

    public class SentinelHelloCollectorCommand extends AbstractCommand<Void>{

        private SentinelActionContext context;

        public SentinelHelloCollectorCommand(SentinelActionContext context) {
            this.context = context;
        }

        @Override
        protected void doExecute() throws Throwable {
            if (!future().isDone()) {
                collect(context);
                if (!future().isDone()) {
                    future().setSuccess();
                }
            }
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
    public void setSessionManager(DefaultRedisSessionManager sessionManager) {
        this.sessionManager = sessionManager;
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
    protected DefaultSentinelHelloCollector setCheckerConfig(CheckerConfig checkerConfig) {
        this.checkerConfig = checkerConfig;
        return this;
    }

    @VisibleForTesting
    protected DefaultSentinelHelloCollector setSentinelManager(SentinelManager sentinelManager) {
        this.sentinelManager = sentinelManager;
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

    @VisibleForTesting
    protected DefaultSentinelHelloCollector setCheckerDbConfig(CheckerDbConfig checkerDbConfig) {
        this.checkerDbConfig = checkerDbConfig;
        return this;
    }

    public void setCollectExecutor(ExecutorService collectExecutor) {
        this.collectExecutor = collectExecutor;
    }

    public void setResetExecutor(ExecutorService resetExecutor) {
        this.resetExecutor = resetExecutor;
    }
}
