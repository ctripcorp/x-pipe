package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.api.command.Command;
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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class ResetSentinels extends AbstractSentinelHelloCollectCommand {

    private SentinelManager sentinelManager;
    private MetaCache metaCache;
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;
    private ScheduledExecutorService scheduled;
    private ExecutorService resetExecutor;
    private CheckerConfig checkerConfig;

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
        resetExecutor.execute(() -> checkSentinels(context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getSentinelMonitorName(), context.getToCheckReset()));
        future().setSuccess();
    }

    protected void checkSentinels(String clusterId, String shardId, String sentinelMonitorName, Set<SentinelHello> hellos) {
        if (hellos.isEmpty())
            return;

        if (hellos.size() < checkerConfig.getDefaultSentinelQuorumConfig().getQuorum()) {
            logger.warn("[{}-{}+{}]sentinels less then quorum, ignore reset, hellos:{}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), hellos);
            return;
        }

        Map<HostPort, List<HostPort>> allSentinels = sentinelsSlaves(sentinelMonitorName, hellos);
        Map<HostPort, List<HostPort>> availableSentinels = sentinels2AllSlaves(allSentinels);
        if (overHalfSentinelsLostSlaves(availableSentinels)) {
            logger.warn("[{}-{}+{}]over half sentinels lost slaves: {}, ignore reset", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), allSentinels);
            return;
        }

        List<HostPort> shouldResetSentinels = shouldResetSentinels(clusterId, shardId, sentinelMonitorName, hellos, allSentinels);

        List<HostPort> resetSentinels = sentinelsToReset(availableSentinels, Lists.newArrayList(shouldResetSentinels));
        if (resetSentinels.isEmpty())
            return;

        logger.info("[{}-{}+{}]{} to reset sentinels:{}", LOG_TITLE, clusterId, shardId, sentinelMonitorName, resetSentinels);
        resetSentinels.forEach(sentinel -> {
            CatEventMonitor.DEFAULT.logEvent("Sentinel.Hello.Collector.Reset", sentinelMonitorName);
            sentinelManager.reset(new Sentinel(sentinel.toString(), sentinel.getHost(), sentinel.getPort()), sentinelMonitorName).execute().getOrHandle(1000, TimeUnit.MILLISECONDS, throwable -> {
                logger.error("[{}-{}+{}][reset]{}, {}", LOG_TITLE, clusterId, shardId, sentinelMonitorName, sentinel, throwable);
                return null;
            });
        });

    }

    List<HostPort> shouldResetSentinels(String clusterId, String shardId, String sentinelMonitorName, Set<SentinelHello> hellos, Map<HostPort, List<HostPort>> allSentinels) {
        Map<HostPort, HostPort> shouldResetSentinelsMap = new ConcurrentHashMap<>();
        ParallelCommandChain checkCommand = new ParallelCommandChain(resetExecutor, false);
        for (SentinelHello hello : hellos) {
            HostPort sentinelAddr = hello.getSentinelAddr();
            List<HostPort> slaves = allSentinels.get(sentinelAddr);
            CheckSentinel resetSentinel = new CheckSentinel(clusterId, shardId, hello, sentinelMonitorName, slaves);
            resetSentinel.future().addListener(future -> {
                if (future.isSuccess() && future.get())
                    shouldResetSentinelsMap.put(sentinelAddr, sentinelAddr);
            });
            checkCommand.add(resetSentinel);
        }

        try {
            checkCommand.execute().get(2050, TimeUnit.MILLISECONDS);
        } catch (Throwable th) {
            logger.warn("[{}-{}+{}]check all sentinels error, hellos:{} ", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), hellos, th);
        }

        return Lists.newArrayList(shouldResetSentinelsMap.keySet());
    }


    List<HostPort> sentinelsToReset(Map<HostPort, List<HostPort>> availableSentinels, List<HostPort> shouldResetSentinels) {
        List<HostPort> sentinelsToReset = new ArrayList<>();
        if (shouldResetSentinels.isEmpty())
            return sentinelsToReset;

        //reset unavailable sentinels first
        sentinelsToReset.addAll(shouldResetSentinels.stream().filter(sentinel -> !availableSentinels.containsKey(sentinel)).collect(Collectors.toList()));
        shouldResetSentinels.removeAll(sentinelsToReset);

        //all shouldResetSentinels unavailable
        if (shouldResetSentinels.isEmpty())
            return sentinelsToReset;

        //leave quorum availableSentinels
        int canResetAvailableSentinelSize = availableSentinels.size() - checkerConfig.getDefaultSentinelQuorumConfig().getQuorum();
        if (canResetAvailableSentinelSize >= shouldResetSentinels.size()) {
            sentinelsToReset.addAll(shouldResetSentinels);
            return sentinelsToReset;
        }

        for (int i = 0; i < canResetAvailableSentinelSize; i++) {
            sentinelsToReset.add(shouldResetSentinels.get(i));
        }

        return sentinelsToReset;
    }

    Map<HostPort, List<HostPort>> sentinels2AllSlaves(Map<HostPort, List<HostPort>> sentinelsSlaves) {
        Map<HostPort, List<HostPort>> sentinels2AllSlaves = new HashMap<>();
        sentinelsSlaves.forEach((sentinel, slaves) -> {
            if (sentinelHasAllSlaves(slaves)) {
                sentinels2AllSlaves.put(sentinel, slaves);
            }
        });
        return sentinels2AllSlaves;
    }

    boolean overHalfSentinelsLostSlaves(Map<HostPort, List<HostPort>> availableSentinels) {
        return availableSentinels.size() < checkerConfig.getDefaultSentinelQuorumConfig().getQuorum();
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
            logger.warn("[{}-{}+{}]get sentinel slaves error", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), th);
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
        if (slaves.isEmpty())
            return false;

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

    class CheckSentinel extends AbstractCommand<Boolean> {

        private String clusterId;
        private String shardId;
        private SentinelHello hello;
        private String sentinelMonitorName;
        private List<HostPort> slaves;

        public CheckSentinel(String clusterId, String shardId, SentinelHello sentinelHello, String sentinelMonitorName, List<HostPort> slaves) {
            this.clusterId = clusterId;
            this.shardId = shardId;
            this.hello = sentinelHello;
            this.sentinelMonitorName = sentinelMonitorName;
            this.slaves = slaves;
        }

        @Override
        public String getName() {
            return "CheckSentinel";
        }

        @Override
        protected void doExecute() throws Throwable {
            HostPort sentinelAddr = hello.getSentinelAddr();
            future().setSuccess(shouldReset(slaves, clusterId, shardId, sentinelMonitorName, sentinelAddr));
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
