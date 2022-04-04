package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.server.Server;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.command.CommandTimeoutException;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.alert.ALERT_TYPE;
import com.ctrip.xpipe.redis.checker.alert.AlertManager;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import com.ctrip.xpipe.redis.core.protocal.cmd.RoleCommand;
import com.ctrip.xpipe.redis.core.protocal.pojo.Role;
import com.ctrip.xpipe.redis.core.protocal.pojo.Sentinel;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.ObjectUtils;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class ResetSentinels extends AbstractSentinelHelloCollectCommand {

    private SentinelManager sentinelManager;
    private MetaCache metaCache;
    private AlertManager alertManager;
    private XpipeNettyClientKeyedObjectPool keyedObjectPool;
    private ScheduledExecutorService scheduled;
    private ExecutorService resetExecutor;

    public ResetSentinels(SentinelHelloCollectContext context, MetaCache metaCache,
                          AlertManager alertManager, XpipeNettyClientKeyedObjectPool keyedObjectPool,
                          ScheduledExecutorService scheduled, ExecutorService resetExecutor,SentinelManager sentinelManager) {
        super(context);
        this.metaCache = metaCache;
        this.alertManager = alertManager;
        this.keyedObjectPool = keyedObjectPool;
        this.scheduled = scheduled;
        this.resetExecutor = resetExecutor;
        this.sentinelManager = sentinelManager;
    }

    @Override
    protected void doExecute() throws Throwable {
        checkReset(context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getSentinelMonitorName(), context.getHellos());
        future().setSuccess();
    }

    protected void checkReset(String clusterId, String shardId, String sentinelMonitorName, Set<SentinelHello> hellos) {
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

    Pair<Boolean, String> shouldReset(List<HostPort> slaves, String clusterId, String shardId) {
        if (context.getInfo().getClusterType().supportKeeper()) {
            Pair<Boolean, String> tooManyKeepers = tooManyKeepers(slaves, clusterId, shardId);
            if (tooManyKeepers.getKey()) return tooManyKeepers;
        }

        Pair<Boolean, String> inOtherClusterShard = inOtherClusterShard(slaves, clusterId, shardId);
        if (inOtherClusterShard.getKey()) return inOtherClusterShard;

        Pair<Boolean, String> unknownInstanceAndIsKeeperOrDead = redundantInstances(slaves, clusterId, shardId);
        if (unknownInstanceAndIsKeeperOrDead.getKey()) return unknownInstanceAndIsKeeperOrDead;

        return new Pair<>(false, null);
    }

    Pair<Boolean, String> tooManyKeepers(List<HostPort> slaves, String clusterId, String shardId) {
        Set<HostPort> allKeepers = metaCache.getAllKeepers();
        Set<HostPort> keepers = new HashSet<>();
        for (HostPort currentSlave : slaves) {

            if (allKeepers.contains(currentSlave)) {
                keepers.add(currentSlave);
            }
        }
        slaves.removeAll(keepers);
        if (keepers.size() > 1)
            return new Pair<>(true, String.format("%s,%s, has %d keepers:%s", clusterId, shardId, keepers.size(), keepers));
        else
            return new Pair<>(false, null);
    }

    Pair<Boolean, String> inOtherClusterShard(List<HostPort> slaves, String clusterId, String shardId) {
        for (HostPort currentSlave : slaves) {
            Pair<String, String> clusterShard = metaCache.findClusterShard(currentSlave);
            if (clusterShard != null) {
                if (!ObjectUtils.equals(clusterId, clusterShard.getKey()) || !ObjectUtils.equals(shardId, clusterShard.getValue()))
                    return new Pair<>(true, String.format("[%s], current:%s,%s, but meta:%s:%s", currentSlave, clusterId, shardId, clusterShard.getKey(), clusterShard.getValue()));
            }
        }
        return new Pair<>(false, null);
    }


    Pair<Boolean, String> redundantInstances(List<HostPort> slaves, String clusterId, String shardId) {
        for (HostPort currentSlave : slaves) {
            Pair<String, String> clusterShard = metaCache.findClusterShard(currentSlave);
            if (clusterShard == null) {
                if (redundantInstance(currentSlave))
                    return new Pair<>(true, String.format("[%s]keeper or dead, current:%s,%s, with no cluster shard", currentSlave, clusterId, shardId));
                else {
                    String message = String.format("sentinel monitors redis %s not in xpipe", currentSlave.toString());
                    alertManager.alert(clusterId, shardId, currentSlave, ALERT_TYPE.SENTINEL_MONITOR_REDUNDANT_REDIS, message);
                }
            }
        }
        return new Pair<>(false, null);
    }

    boolean redundantInstance(HostPort hostPort) {
        AtomicBoolean redundant = new AtomicBoolean(false);
        RoleCommand roleCommand = new RoleCommand(keyedObjectPool.getKeyPool(new DefaultEndPoint(hostPort.getHost(), hostPort.getPort())), scheduled);
        roleCommand.future().addListener(roleFuture -> redundant.set(redundant(roleFuture)));
        try {
            Role role = roleCommand.execute().get(1, TimeUnit.SECONDS);
            logger.info("[isKeeperOrDead] role: {}", role.getServerRole().name());
        } catch (Throwable th) {
            logger.error("[isKeeperOrDead][failed]{}", hostPort, th);
        }

        return redundant.get();
    }

    boolean redundant(CommandFuture<Role> roleCommandFuture) throws ExecutionException, InterruptedException {
        if (context.getInfo().getClusterType().supportKeeper())
            return isKeeper(roleCommandFuture) || inactive(roleCommandFuture);

        return inactive(roleCommandFuture);
    }

    boolean isKeeper(CommandFuture<Role> roleCommandFuture) throws ExecutionException, InterruptedException {
        return roleCommandFuture.isSuccess() && Server.SERVER_ROLE.KEEPER.equals(roleCommandFuture.get().getServerRole());
    }

    boolean inactive(CommandFuture<Role> roleCommandFuture) {
        return !roleCommandFuture.isSuccess() &&
                (roleCommandFuture.cause() instanceof CommandExecutionException ||
                        roleCommandFuture.cause() instanceof CommandTimeoutException ||
                        roleCommandFuture.cause() instanceof SocketException);
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
                logger.error("[{}-{}][checkReset-slaves]{}", LOG_TITLE, sentinelMonitorName, sentinel, throwable);
                return new ArrayList<>();
            });

            if (slaves.isEmpty())
                return;

            Pair<Boolean, String> shouldResetAndReason = shouldReset(slaves, clusterId, shardId);

            if (shouldResetAndReason.getKey()) {
                logger.info("[{}-{}+{}][reset]{}, {}, {}", LOG_TITLE, clusterId, shardId, sentinelMonitorName, sentinelAddr, shouldResetAndReason.getValue());
                CatEventMonitor.DEFAULT.logEvent("Sentinel.Hello.Collector.Reset", String.format("%s,%s", sentinelAddr, shouldResetAndReason.getValue()));
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
