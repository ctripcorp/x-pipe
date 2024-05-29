package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.config.CheckerConfig;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.MasterRole;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.google.common.collect.Sets;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class ResetSentinels extends AbstractSentinelHelloCollectCommand {

    private SentinelManager sentinelManager;
    private MetaCache metaCache;
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;
    private ScheduledExecutorService scheduled;
    private ExecutorService resetExecutor;
    private CheckerConfig checkerConfig;
    private AtomicInteger resetSentinelCounts = new AtomicInteger();

    public ResetSentinels(SentinelHelloCollectContext context, MetaCache metaCache,
                          XpipeNettyClientKeyedObjectPool keyedObjectPool,
                          ScheduledExecutorService scheduled, ExecutorService resetExecutor, SentinelManager sentinelManager,
                          CheckerConfig checkerConfig) {
        super(context);
        this.metaCache = metaCache;
        this.keyedObjectPool = keyedObjectPool;
        this.scheduled = scheduled;
        this.resetExecutor = resetExecutor;
        this.sentinelManager = sentinelManager;
        this.checkerConfig = checkerConfig;
    }

    @Override
    protected void doExecute() throws Throwable {
        resetExecutor.execute(() -> checkReset(context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getSentinelMonitorName(), context.getToCheckReset()));
        future().setSuccess();
    }

    protected void checkReset(String clusterId, String shardId, String sentinelMonitorName, Set<SentinelHello> hellos) {
        if (hellos.isEmpty())
            return;

        if (hellos.size() < checkerConfig.getDefaultSentinelQuorumConfig().getTotal()) {
            logger.warn("[{}-{}+{}]sentinel missing, ignore reset, hellos:{}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), hellos);
            return;
        }

        Map<HostPort, List<HostPort>> sentinelsSlaves = sentinelsSlaves(sentinelMonitorName, hellos);

        if (overHalfSentinelsLostSlaves(sentinelsSlaves)) {
            logger.warn("[{}-{}+{}]over half sentinels lost slaves: {}, ignore reset", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), sentinelsSlaves);
            return;
        }

        ParallelCommandChain resetChain = new ParallelCommandChain(resetExecutor, false);
        for (SentinelHello hello : hellos) {
            HostPort sentinelAddr = hello.getSentinelAddr();
            List<HostPort> slaves = sentinelsSlaves.get(sentinelAddr);
            resetChain.add(new ResetSentinel(clusterId, shardId, hello, sentinelMonitorName, slaves));
        }
        CommandFuture resetFuture = resetChain.execute();
        ScheduledFuture<?> resetTimeoutFuture = scheduled.schedule(new Runnable() {
            @Override
            public void run() {
                resetFuture.cancel(true);
            }
        }, 3000, TimeUnit.MILLISECONDS);
        resetFuture.addListener(commandFuture -> {
            if (!commandFuture.isCancelled())
                resetTimeoutFuture.cancel(true);
        });
    }

    boolean overHalfSentinelsLostSlaves(Map<HostPort, List<HostPort>> sentinelsSlaves) {
        Map<HostPort, List<HostPort>> sentinels2AllSlaves = new HashMap<>();
        sentinelsSlaves.forEach((sentinel, slaves) -> {
            if (sentinelHasAllSlaves(slaves)) {
                sentinels2AllSlaves.put(sentinel, slaves);
            }
        });
        return sentinels2AllSlaves.size() < checkerConfig.getDefaultSentinelQuorumConfig().getQuorum();
    }

    private Map<HostPort, List<HostPort>> sentinelsSlaves(String sentinelMonitorName, Set<SentinelHello> hellos) {
        Map<HostPort, List<HostPort>> sentinel2Slaves = new ConcurrentHashMap<>();
        ParallelCommandChain sentinelSlaves = new ParallelCommandChain(resetExecutor, false);
        for (SentinelHello hello : hellos) {
            HostPort sentinelAddr = hello.getSentinelAddr();
            Sentinel sentinel = new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort());
            Command<List<HostPort>> slavesCommand = sentinelManager.slaves(sentinel, sentinelMonitorName);
            slavesCommand.future().addListener(future -> {
                if (future.isSuccess())
                    sentinel2Slaves.put(sentinelAddr, future.get());
                else {
                    logger.warn("[{}-{}][checkReset-slaves]{}", LOG_TITLE, sentinelMonitorName, sentinel, future.cause());
                }
            });
            sentinelSlaves.add(slavesCommand);
        }

        try {
            sentinelSlaves.execute().get(2050, TimeUnit.MILLISECONDS);
        } catch (Throwable th) {
            logger.warn("[{}-{}+{}]get sentinel slaves error", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId());
        }

        return sentinel2Slaves;
    }

    private Set<HostPort> tooManyKeepers(List<HostPort> slaves) {
        if (!context.getInfo().getClusterType().supportKeeper())
            return new HashSet<>();

        Set<HostPort> allKeepers = metaCache.getAllKeepers();
        Set<HostPort> keepers = new HashSet<>();
        for (HostPort currentSlave : slaves) {
            if (allKeepers.contains(currentSlave)) {
                keepers.add(currentSlave);
            }
        }
        slaves.removeAll(keepers);
        if (keepers.size() > EXPECTED_KEEPER_COUNT) return keepers;
        return new HashSet<>();
    }


    private Set<HostPort> unknownInstances(List<HostPort> slaves) {
        Set<HostPort> unknownInstances = Sets.newHashSet(slaves);
        unknownInstances.removeAll(context.getShardInstances());
        slaves.removeAll(unknownInstances);
        return unknownInstances;
    }

    private static final int EXPECTED_KEEPER_COUNT = 1;
    private static final int EXPECTED_MASTER_COUNT = 1;
    boolean shouldReset(List<HostPort> slaves, String clusterId, String shardId, String sentinelMonitorName, HostPort sentinelAddr) {
        Set<HostPort> toManyKeepers = tooManyKeepers(slaves);
        Set<HostPort> unknownSlaves = unknownInstances(slaves);

        if (toManyKeepers.isEmpty() && unknownSlaves.isEmpty())
            return false;

        List<HostPort> masterSlaves = masterSlaves();
        if (!masterHasAllSlaves(masterSlaves,clusterId, shardId, sentinelMonitorName, sentinelAddr))
            return false;

        if (shouldResetTooManyKeepers(masterSlaves, toManyKeepers)) {
            logger.info("[{}-{}+{}][reset]{}, {}, too many keepers: {}", LOG_TITLE, clusterId, shardId, sentinelMonitorName, sentinelAddr, toManyKeepers);
            return true;
        }

        if (shouldResetUnknownInstances(masterSlaves, unknownSlaves)) {
            logger.info("[{}-{}+{}][reset]{}, {}, unknown slaves: {}", LOG_TITLE, clusterId, shardId, sentinelMonitorName, sentinelAddr, unknownSlaves);
            return true;
        }

        return false;
    }

    private boolean sentinelHasAllSlaves(List<HostPort> sentinelSlaves) {
        if (sentinelSlaves.isEmpty())
            return false;

        Set<HostPort> shardInstances = Sets.newHashSet(context.getShardInstances());
        shardInstances.removeAll(sentinelSlaves);
        return shardInstances.size() == EXPECTED_MASTER_COUNT;
    }

    private boolean shouldResetTooManyKeepers(List<HostPort> masterSlaves, Set<HostPort> toManyKeepers) {
        if(toManyKeepers.isEmpty())
            return false;

        Set<HostPort> slaves = Sets.newHashSet(masterSlaves);
        slaves.retainAll(toManyKeepers);

        return slaves.size() <= EXPECTED_KEEPER_COUNT;
    }

    private boolean shouldResetUnknownInstances(List<HostPort> masterSlaves, Set<HostPort> unknownSlaves) {
        if(unknownSlaves.isEmpty())
            return false;

        Set<HostPort> slaves = Sets.newHashSet(masterSlaves);
        slaves.retainAll(unknownSlaves);

        return slaves.isEmpty();
    }

    private List<HostPort> masterSlaves() {
        HostPort master = context.getTrueMasterInfo().getKey();
        RoleCommand roleCommand = new RoleCommand(keyedObjectPool.getKeyPool(new DefaultEndPoint(master.getHost(), master.getPort())), scheduled);

        try {
            Role role = roleCommand.execute().get(660, TimeUnit.MILLISECONDS);
            if (role instanceof MasterRole) {
                MasterRole masterRole = (MasterRole) role;
                return masterRole.getSlaveHostPorts();
            }
        } catch (Throwable th) {
            logger.warn("[{}-{}+{}]get slaves from master failed", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), th);
        }

        return new ArrayList<>();
    }

    private boolean masterHasAllSlaves(List<HostPort> masterSlaves, String clusterId, String shardId, String sentinelMonitorName, HostPort sentinelAddr) {
        HostPort master = context.getTrueMasterInfo().getKey();
        Set<HostPort> masterAndSlaves = Sets.newHashSet(masterSlaves);
        masterAndSlaves.add(master);

        boolean masterHasAllSlaves = masterAndSlaves.containsAll(context.getShardInstances());
        if (!masterHasAllSlaves) {
            logger.info("[{}-{}+{}][reset]{}, {}, master:{} lost connection with some slaves, stop reset, current slaves:{}, expected slaves:{}", LOG_TITLE, clusterId, shardId, sentinelMonitorName, sentinelAddr, master, masterSlaves, context.getShardInstances());
        }
        return masterHasAllSlaves;
    }

    class ResetSentinel extends AbstractCommand<Void> {

        private String clusterId;
        private String shardId;
        private SentinelHello hello;
        private String sentinelMonitorName;
        private List<HostPort> slaves;

        public ResetSentinel(String clusterId, String shardId, SentinelHello sentinelHello, String sentinelMonitorName, List<HostPort> slaves) {
            this.clusterId = clusterId;
            this.shardId = shardId;
            this.hello = sentinelHello;
            this.sentinelMonitorName = sentinelMonitorName;
            this.slaves = slaves;
        }

        @Override
        public String getName() {
            return "ResetSentinel";
        }

        @Override
        protected void doExecute() throws Throwable {
            HostPort sentinelAddr = hello.getSentinelAddr();
            Sentinel sentinel = new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort());

            if (slaves.isEmpty())
                return;

            if (shouldReset(slaves, clusterId, shardId, sentinelMonitorName, sentinelAddr)) {
                if (resetSentinelCounts.incrementAndGet() < checkerConfig.getDefaultSentinelQuorumConfig().getQuorum()) {
                    CatEventMonitor.DEFAULT.logEvent("Sentinel.Hello.Collector.Reset", sentinelMonitorName);
                    sentinelManager.reset(sentinel, sentinelMonitorName).execute().getOrHandle(1000, TimeUnit.MILLISECONDS, throwable -> {
                        logger.error("[{}-{}+{}][reset]{}, {}", LOG_TITLE, clusterId, shardId, sentinelMonitorName, sentinelAddr, throwable);
                        return null;
                    });
                } else {
                    logger.warn("[{}-{}][reset]try to reset sentinel {} failed, rate limit", LOG_TITLE, sentinelMonitorName, sentinel);
                }
            }
        }

        @Override
        protected void doReset() {

        }
    }

    ResetSentinels setKeyedObjectPool(XpipeNettyClientKeyedObjectPool keyedObjectPool) {
        this.keyedObjectPool = keyedObjectPool;
        return this;
    }

    ResetSentinels setScheduled(ScheduledExecutorService scheduled) {
        this.scheduled = scheduled;
        return this;
    }
}
