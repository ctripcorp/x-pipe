package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.redis.core.protocal.pojo.SlaveRole;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;

import java.util.*;
import java.util.concurrent.*;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class ResetSentinels extends AbstractSentinelHelloCollectCommand {

    private SentinelManager sentinelManager;
    private MetaCache metaCache;
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;
    private ScheduledExecutorService scheduled;
    private ExecutorService resetExecutor;

    public ResetSentinels(SentinelHelloCollectContext context, MetaCache metaCache,
                          XpipeNettyClientKeyedObjectPool keyedObjectPool,
                          ScheduledExecutorService scheduled, ExecutorService resetExecutor,SentinelManager sentinelManager) {
        super(context);
        this.metaCache = metaCache;
        this.keyedObjectPool = keyedObjectPool;
        this.scheduled = scheduled;
        this.resetExecutor = resetExecutor;
        this.sentinelManager = sentinelManager;
    }

    @Override
    protected void doExecute() throws Throwable {
        checkReset(context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getSentinelMonitorName(), context.getToCheckReset());
        future().setSuccess();
    }

    protected void checkReset(String clusterId, String shardId, String sentinelMonitorName, Set<SentinelHello> hellos) {
        if (hellos.isEmpty())
            return;
        ParallelCommandChain resetChain = new ParallelCommandChain(resetExecutor, false);
        for (SentinelHello hello : hellos) {
            resetChain.add(new ResetSentinel(clusterId, shardId, hello, sentinelMonitorName));
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


    private Set<HostPort> unknownInstances(List<HostPort> slaves, String clusterId, String shardId){
        Set<HostPort> unknownInstances = new HashSet<>();
        for (HostPort currentSlave : slaves) {
            Pair<String, String> clusterShard = metaCache.findClusterShard(currentSlave);
            if (clusterShard == null || !ObjectUtils.equals(clusterId, clusterShard.getKey()) || !ObjectUtils.equals(shardId, clusterShard.getValue()))
                unknownInstances.add(currentSlave);
        }
        return unknownInstances;
    }

    private Set<HostPort> connectedToTrueMaster(Set<HostPort> invalidSlaves) {
        Map<HostPort, SlaveRole> connectedSlaves = new ConcurrentHashMap<>();

        ParallelCommandChain slaveRoleChain = new ParallelCommandChain();
        for (HostPort hostPort : invalidSlaves) {
            RoleCommand roleCommand = new RoleCommand(keyedObjectPool.getKeyPool(new DefaultEndPoint(hostPort.getHost(), hostPort.getPort())), scheduled);
            roleCommand.future().addListener(future -> {
                if (future.isSuccess()) {
                    Role role = future.get();
                    if (role instanceof SlaveRole) {
                        SlaveRole slaveRole = (SlaveRole) role;
                        HostPort trueMaster = context.getTrueMasterInfo().getKey();
                        if (slaveRole.getMasterHost().equals(trueMaster.getHost()) && slaveRole.getMasterPort() == trueMaster.getPort()) {
                            connectedSlaves.put(hostPort, slaveRole);
                        }
                    }
                }
            });
            slaveRoleChain.add(roleCommand);
        }

        try {
            slaveRoleChain.execute().get(1500, TimeUnit.MILLISECONDS);
        } catch (Throwable th) {
            logger.warn("[{}-{}+{}]parallel role command to slaves error", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), th);
        }


        return connectedSlaves.keySet();
    }

    private static final int EXPECTED_KEEPER_COUNT = 1;
    boolean shouldReset(List<HostPort> slaves, String clusterId, String shardId, String sentinelMonitorName, HostPort sentinelAddr) {
        Set<HostPort> toManyKeepers = tooManyKeepers(slaves);
        if (shouldResetTooManyKeepers(toManyKeepers)) {
            logger.info("[{}-{}+{}][reset]{}, {}, too many keepers: {}", LOG_TITLE, clusterId, shardId, sentinelMonitorName, sentinelAddr, toManyKeepers);
            return true;
        }

        Set<HostPort> unknownSlaves = unknownInstances(slaves, clusterId, shardId);
        if (shouldResetUnknownInstances(unknownSlaves)) {
            logger.info("[{}-{}+{}][reset]{}, {}, unknown slaves: {}", LOG_TITLE, clusterId, shardId, sentinelMonitorName, sentinelAddr, unknownSlaves);
            return true;
        }

        return false;
    }

    private boolean shouldResetTooManyKeepers(Set<HostPort> toManyKeepers) {
        return !toManyKeepers.isEmpty() && connectedToTrueMaster(toManyKeepers).size() <= EXPECTED_KEEPER_COUNT;
    }

    private boolean shouldResetUnknownInstances(Set<HostPort> unknownSlaves) {
        return !unknownSlaves.isEmpty() && connectedToTrueMaster(unknownSlaves).isEmpty();
    }

    class ResetSentinel extends AbstractCommand<Void> {

        private String clusterId;
        private String shardId;
        private SentinelHello hello;
        private String sentinelMonitorName;

        public ResetSentinel(String clusterId, String shardId, SentinelHello sentinelHello, String sentinelMonitorName) {
            this.clusterId = clusterId;
            this.shardId = shardId;
            this.hello = sentinelHello;
            this.sentinelMonitorName = sentinelMonitorName;
        }

        @Override
        public String getName() {
            return "ResetSentinel";
        }

        @Override
        protected void doExecute() throws Throwable {
            HostPort sentinelAddr = hello.getSentinelAddr();
            Sentinel sentinel = new Sentinel(sentinelAddr.toString(), sentinelAddr.getHost(), sentinelAddr.getPort());

            List<HostPort> slaves = sentinelManager.slaves(sentinel, sentinelMonitorName).execute().getOrHandle(2050, TimeUnit.MILLISECONDS, throwable -> {
                logger.warn("[{}-{}][checkReset-slaves]{}", LOG_TITLE, sentinelMonitorName, sentinel, throwable);
                return new ArrayList<>();
            });

            if (slaves.isEmpty())
                return;

            if (shouldReset(slaves, clusterId, shardId, sentinelMonitorName, sentinelAddr)) {
                CatEventMonitor.DEFAULT.logEvent("Sentinel.Hello.Collector.Reset", sentinelMonitorName);
                sentinelManager.reset(sentinel, sentinelMonitorName).execute().getOrHandle(1000, TimeUnit.MILLISECONDS, throwable -> {
                    logger.error("[{}-{}+{}][reset]{}, {}", LOG_TITLE, clusterId, shardId, sentinelMonitorName, sentinelAddr, throwable);
                    return null;
                });
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
