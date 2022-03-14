package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.monitor.Task;
import com.ctrip.xpipe.api.monitor.TransactionMonitor;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.command.*;
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
import com.ctrip.xpipe.redis.core.exception.NoResourceException;
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

    protected static final Logger logger = LoggerFactory.getLogger(DefaultSentinelHelloCollector.class);

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

    private Map<ClusterType, String[]> clusterTypeSentinelConfig = new HashMap<>();

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
    protected Set<HostPort> metaMasterAndHelloMasters(HostPort metaMaster, Set<SentinelHello> hellos) {
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
                doCheckReset(clusterId,shardId,sentinelMonitorName,hellos);
            }
        });

    }

    void doCheckReset(String clusterId, String shardId, String sentinelMonitorName, Set<SentinelHello> hellos){
        Set<HostPort> allKeepers = metaCache.getAllKeepers();
        hellos.forEach((hello) -> {
            HostPort sentinelAddr = hello.getSentinelAddr();
            Sentinel sentinel = new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort());
            try {
                List<HostPort> slaves = sentinelManager.slaves(sentinel, sentinelMonitorName).execute().getOrHandle(2050, TimeUnit.MILLISECONDS, throwable -> {
                    logger.error("[{}-{}][checkReset-slaves]{}", LOG_TITLE, sentinelMonitorName, sentinel, throwable);
                    return new ArrayList<>();
                });

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
                    CatEventMonitor.DEFAULT.logEvent("Sentinel.Hello.Collector.Reset", String.format("%s,%s", sentinelAddr, reason));
                    sentinelManager.reset(sentinel, sentinelMonitorName).execute().getOrHandle(1000, TimeUnit.MILLISECONDS, throwable -> {
                        logger.error("[{}-{}+{}][reset]{}, {}", LOG_TITLE, clusterId, shardId, sentinelMonitorName, sentinelAddr, throwable);
                        return null;
                    });
                }
            } catch (Exception e) {
                logger.error("[{}-{}+{}][checkReset]{}", LOG_TITLE, clusterId, shardId, hello, e);
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
                public void role(String roleDesc, Role detail) {
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
            logger.error("[isKeeperOrDead]latch await error", e);
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
            logger.warn("[{}-{}+{}][checkToAdd][no monitor name]", LOG_TITLE, clusterId, shardId);
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

        private final RedisInstanceInfo info;
        private final Set<SentinelHello> hellos ;
        private final String sentinelMonitorName;
        private final Set<HostPort> sentinels ;
        private HostPort trueMaster = null;
        private final Set<SentinelHello> toDelete = new HashSet<>();
        private final Set<SentinelHello> toAdd = new HashSet<>();

        public SentinelHelloCollectorCommand(SentinelActionContext context) {
            this.hellos = Sets.newHashSet(context.getResult());
            this.info = context.instance().getCheckInfo();
            this.sentinelMonitorName = getSentinelMonitorName(info);
            this.sentinels = getSentinels(info);
        }

        @Override
        protected void doExecute() throws Throwable {
            if (!future().isDone()) {
                if (!checkerDbConfig.shouldSentinelCheck(info.getClusterId())) {
                    logger.info("[{}-{}+{}] {} in white list, skip", LOG_TITLE, info.getClusterId(), info.getShardId(), info.getClusterId());
                    future().setSuccess();
                } else {
                    TransactionMonitor transaction = TransactionMonitor.DEFAULT;
                    transaction.logTransactionSwallowException("sentinel.hello.collect", info.getClusterId(), new Task() {

                        @Override
                        public void go() throws Exception {
                            SequenceCommandChain chain = new SequenceCommandChain(false);
                            chain.add(new CheckTrueMaster());
                            chain.add(new AnalyseHellos());
                            chain.add(new AcquireLeakyBucket());
                            chain.add(new SentinelReset());
                            chain.add(new SentinelDelete());
                            chain.add(new SentinelAdd());
                            chain.add(new SentinelSet());
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

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return getClass().getSimpleName();
        }

        class CheckTrueMaster extends AbstractCommand<Void> {

            @Override
            public String getName() {
                return "CheckTrueMaster";
            }

            @Override
            protected void doExecute() throws Throwable {
                HostPort metaMaster = null;
                try {
                    metaMaster = getMaster(info);
                } catch (MasterNotFoundException e) {
                    logger.warn("[{}-{}+{}] {} master not found", LOG_TITLE, info.getClusterId(), info.getShardId(), info.getDcId(), e);
                }

                Set<HostPort> currentCollectedMasters = metaMasterAndHelloMasters(metaMaster, hellos);

                if (currentMasterConsistent(currentCollectedMasters)) {
                    trueMaster = currentCollectedMasters.iterator().next();
                    logger.debug("[{}-{}+{}] {} true master: {}", LOG_TITLE, info.getClusterId(), info.getShardId(), sentinelMonitorName, trueMaster);
                    future().setSuccess();
                }  else {
                    logger.warn("[{}-{}+{}]collected masters not unique: {}", LOG_TITLE, info.getClusterId(), info.getShardId(), currentCollectedMasters);
                    Map<HostPort, HostPort> trueMastersMap = new ConcurrentHashMap<>();
                    ParallelCommandChain chain = new ParallelCommandChain(MoreExecutors.directExecutor(),false);
                    currentCollectedMasters.forEach(currentCollectedMaster -> {
                        RoleCommand roleCommand = new RoleCommand(keyedObjectPool.getKeyPool(new DefaultEndPoint(currentCollectedMaster.getHost(), currentCollectedMaster.getPort())), scheduled);
                        roleCommand.future().addListener(innerFuture -> {
                            if (innerFuture.isSuccess()) {
                                logger.info("[{}-{}+{}]instance {} role {}", LOG_TITLE, info.getClusterId(), info.getShardId(), currentCollectedMaster, innerFuture.get());
                                if (innerFuture.get() instanceof MasterRole) {
                                    trueMastersMap.put(currentCollectedMaster, currentCollectedMaster);
                                }
                            } else {
                                logger.warn("[{}-{}+{}]instance {} role failed", LOG_TITLE, info.getClusterId(), info.getShardId(), currentCollectedMaster, innerFuture.cause());
                            }
                        });
                        chain.add(roleCommand);
                    });

                    HostPort finalMetaMaster = metaMaster;
                    chain.execute().addListener(outerFuture -> {
                        try {
                            if (currentMasterConsistent(trueMastersMap.keySet())) {
                                trueMaster = trueMastersMap.keySet().iterator().next();
                                logger.info("[{}-{}+{}] {} true master: {}", LOG_TITLE, info.getClusterId(), info.getShardId(), sentinelMonitorName, trueMaster);
                                future().setSuccess();
                            } else if (metaMasterIsTrueMaster(finalMetaMaster, trueMastersMap.keySet())) {
                                trueMaster = finalMetaMaster;
                                logger.info("[{}-{}+{}] {} true master: {}", LOG_TITLE, info.getClusterId(), info.getShardId(), sentinelMonitorName, trueMaster);
                                future().setSuccess();
                            } else {
                                logger.warn("[{}-{}+{}] {} currentMasterConsistent: {}", LOG_TITLE, info.getClusterId(), info.getShardId(), sentinelMonitorName, trueMastersMap.keySet());
                                String message = String.format("master inconsistent, monitorName:%s, masters:%s", sentinelMonitorName, trueMastersMap.keySet());
                                alertManager.alert(info.getClusterId(), info.getShardId(), info.getHostPort(), ALERT_TYPE.SENTINEL_MONITOR_INCONSIS, message);
                                future().setFailure(new MasterNotFoundException(info.getClusterId(), info.getShardId()));
                            }
                        } catch (Throwable e) {
                            logger.error("[{}-{}+{}]CheckTrueMaster failed,", LOG_TITLE, info.getClusterId(), info.getShardId(), e);
                            future().setFailure(e);
                        }
                    });
                }
            }

            boolean metaMasterIsTrueMaster(HostPort metaMaster, Set<HostPort> trueMasters) {
                return noTrueMastersFound(metaMaster, trueMasters) || trueMastersContainsMetaMaster(metaMaster, trueMasters);
            }

            boolean trueMastersContainsMetaMaster(HostPort metaMaster, Set<HostPort> trueMasters) {
                return metaMaster != null && trueMasters.contains(metaMaster);
            }

            boolean noTrueMastersFound(HostPort metaMaster, Set<HostPort> trueMasters) {
                return metaMaster != null && trueMasters.isEmpty();
            }

            @Override
            protected void doReset() {

            }
        }

        class AnalyseHellos extends AbstractCommand<Void> {

            @Override
            public String getName() {
                return "AnalyseHellos";
            }

            @Override
            protected void doExecute() throws Throwable {
                // check stale hellos
                toDelete.addAll(checkStaleHellos(sentinelMonitorName, sentinels, hellos));
                // check wrong master hellos
                checkWrongMasterHellos(hellos, trueMaster);
                // check add
                toAdd.addAll(checkToAdd(info.getClusterId(), info.getShardId(), sentinelMonitorName, sentinels, hellos, trueMaster, checkerConfig.getDefaultSentinelQuorumConfig()));

                future().setSuccess();
            }

            @Override
            protected void doReset() {

            }
        }

        class AcquireLeakyBucket extends AbstractCommand<Void> {
            @Override
            public String getName() {
                return null;
            }

            @Override
            protected void doExecute() throws Throwable {
                // add rate limit logic to reduce frequently sentinel operations
                if (!leakyBucket.tryAcquire()) {
                    logger.warn("[{}-{}][acquire failed]", LOG_TITLE, sentinelMonitorName);
                    future().setFailure(new NoResourceException("leakyBucket.tryAcquire failed"));
                } else {
                    // I got the lock, remember to release it
                    leakyBucket.delayRelease(1000, TimeUnit.MILLISECONDS);
                    future().setSuccess();
                }
            }

            @Override
            protected void doReset() {

            }
        }

        class SentinelReset extends AbstractCommand<Void> {

            @Override
            public String getName() {
                return "SentinelReset";
            }

            @Override
            protected void doExecute() throws Throwable {
                checkReset(info.getClusterId(), info.getShardId(), sentinelMonitorName, hellos);
                future().setSuccess();
            }

            @Override
            protected void doReset() {

            }
        }

        class SentinelDelete extends AbstractCommand<Void> {
            @Override
            public String getName() {
                return "SentinelDelete";
            }

            @Override
            protected void doExecute() throws Throwable {

                if (toDelete.size() == 0) {
                    future().setSuccess();
                } else {
                    logger.info("[{}-{}][toDelete]{}", LOG_TITLE, sentinelMonitorName, toDelete);

                    ParallelCommandChain deleteChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);

                    toDelete.forEach((hello -> {
                        CatEventMonitor.DEFAULT.logEvent("Sentinel.Hello.Collector.Remove", hello.toString());
                        HostPort sentinelAddr = hello.getSentinelAddr();
                        deleteChain.add(sentinelManager.removeSentinelMonitor(new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort()), hello.getMonitorName()));
                    }));

                    deleteChain.execute().addListener(deleteFuture -> {
                        if (deleteFuture.isSuccess()) {
                            logger.info("[{}-{}][deleted]{}", LOG_TITLE, sentinelMonitorName, toDelete);
                        } else {
                            logger.error("[{}-{}][deleted]{}", LOG_TITLE, sentinelMonitorName, toDelete, deleteFuture.cause());
                        }
                        future().setSuccess();
                    });
                }

            }

            @Override
            protected void doReset() {

            }
        }

        class SentinelAdd extends AbstractCommand<Void> {

            @Override
            public String getName() {
                return "SentinelAdd";
            }

            @Override
            protected void doExecute() throws Throwable {
                if (toAdd.size() == 0) {
                    future().setSuccess();
                } else {
                    logger.info("[{}-{}][toAdd]master: {}, stl: {}", LOG_TITLE, sentinelMonitorName, trueMaster,
                            toAdd.stream().map(SentinelHello::getSentinelAddr).collect(Collectors.toSet()));

                    SequenceCommandChain removeAndAdd = new SequenceCommandChain(true);
                    ParallelCommandChain deleteChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
                    ParallelCommandChain addChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);

                    toAdd.forEach((hello) -> {
                        CatEventMonitor.DEFAULT.logEvent("Sentinel.Hello.Collector.RemoveAndAdd", hello.toString());
                        HostPort sentinelAddr = hello.getSentinelAddr();
                        Sentinel sentinel = new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort());
                        deleteChain.add(sentinelManager.removeSentinelMonitor(sentinel, sentinelMonitorName));
                        addChain.add(sentinelManager.monitorMaster(sentinel, hello.getMonitorName(), hello.getMasterAddr(), checkerConfig.getDefaultSentinelQuorumConfig().getQuorum()));
                    });

                    removeAndAdd.add(deleteChain);
                    removeAndAdd.add(addChain);
                    removeAndAdd.execute().addListener(innerFuture -> {
                        if (innerFuture.isSuccess()) {
                            logger.info("[{}-{}][added]{}", LOG_TITLE, sentinelMonitorName, toAdd);
                        } else {
                            logger.error("[{}-{}][added]{}", LOG_TITLE, sentinelMonitorName, toAdd, innerFuture.cause());
                        }
                        future().setSuccess();
                    });
                }
            }

            @Override
            protected void doReset() {

            }
        }

        class SentinelSet extends AbstractCommand<Void> {

            @Override
            public String getName() {
                return "SentinelSet";
            }

            @Override
            protected void doExecute() throws Throwable {
                if (toAdd.size() == 0) {
                    future().setSuccess();
                } else {
                    String[] sentinelConfigs = clusterTypeSentinelConfig.get(info.getClusterType());

                    if (sentinelConfigs == null || sentinelConfigs.length == 0) {
                        future().setSuccess();
                    } else {
                        ParallelCommandChain setChain = new ParallelCommandChain(MoreExecutors.directExecutor(), false);
                        toAdd.forEach((hello) -> {
                            CatEventMonitor.DEFAULT.logEvent("Sentinel.Hello.Collector.SentinelSet", hello.toString());
                            HostPort sentinelAddr = hello.getSentinelAddr();
                            Sentinel sentinel = new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort());
                            setChain.add(sentinelManager.sentinelSet(sentinel, sentinelMonitorName, sentinelConfigs));
                        });
                        setChain.execute().addListener(innerFuture -> {
                            if (innerFuture.isSuccess()) {
                                logger.info("[{}-{}][sentinelSet]{},{}", LOG_TITLE, sentinelMonitorName, toAdd, sentinelConfigs);
                            } else {
                                logger.error("[{}-{}][sentinelSet]{},{}", LOG_TITLE, sentinelMonitorName, toAdd, sentinelConfigs, innerFuture.cause());
                            }
                            future().setSuccess();
                        });
                    }
                }
            }

            @Override
            protected void doReset() {

            }
        }

        public HostPort getTrueMaster() {
            return trueMaster;
        }

        public Set<SentinelHello> getToDelete() {
            return toDelete;
        }

        public Set<SentinelHello> getToAdd() {
            return toAdd;
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
