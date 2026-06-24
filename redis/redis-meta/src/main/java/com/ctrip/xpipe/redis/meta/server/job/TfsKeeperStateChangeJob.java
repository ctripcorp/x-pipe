package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.command.RequestResponseCommand;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.*;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeObjectPoolFromKeyed;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.meta.MetaUtils;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractKeeperCommand.KeeperSetStateCommand;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.tfs.TfsCommandConstants;
import com.ctrip.xpipe.redis.meta.server.tfs.TfsGateway;
import com.ctrip.xpipe.redis.meta.server.tfs.TfsKeeperUtils;
import com.ctrip.xpipe.redis.meta.server.tfs.TfsPrepareReleaseCommand;
import com.ctrip.xpipe.redis.meta.server.tfs.TfsShardContext;
import com.ctrip.xpipe.retry.RetryDelay;
import com.ctrip.xpipe.tuple.Pair;
import com.ctrip.xpipe.utils.StringUtil;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand.DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI;

/**
 * TFS keeper state change: lease release (serial) → ACTIVE (serial) → others (parallel).
 */
public class TfsKeeperStateChangeJob extends AbstractCommand<Void> implements RequestResponseCommand<Void> {

    private final TfsShardContext shardContext;
    private final List<KeeperMeta> keepers;
    private final KeeperMeta previousActiveKeeper;
    private final Pair<String, Integer> activeKeeperMaster;
    private final RouteMeta routeForActiveKeeper;
    private final SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool;
    private final DcMetaCache dcMetaCache;
    private final MetaServerConfig metaServerConfig;
    private final ScheduledExecutorService scheduled;
    private final Executor executor;
    private final Map<KeeperMeta, KeeperState> keeperRoles;
    private final TfsGateway tfsGateway;
    private Command<?> activeSuccessCommand;

    public TfsKeeperStateChangeJob(Long clusterDbId, Long shardDbId, List<KeeperMeta> keepers,
                                   KeeperMeta previousActiveKeeper,
                                   Pair<String, Integer> activeKeeperMaster,
                                   RouteMeta routeForActiveKeeper,
                                   SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool,
                                   DcMetaCache dcMetaCache,
                                   MetaServerConfig metaServerConfig,
                                   ScheduledExecutorService scheduled,
                                   Executor executor,
                                   Map<KeeperMeta, KeeperState> keeperRoles) {
        this(clusterDbId, shardDbId, keepers, previousActiveKeeper, activeKeeperMaster, routeForActiveKeeper,
                clientPool, dcMetaCache, metaServerConfig, scheduled, executor, keeperRoles, null);
    }

    public TfsKeeperStateChangeJob(Long clusterDbId, Long shardDbId, List<KeeperMeta> keepers,
                                   KeeperMeta previousActiveKeeper,
                                   Pair<String, Integer> activeKeeperMaster,
                                   RouteMeta routeForActiveKeeper,
                                   SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool,
                                   DcMetaCache dcMetaCache,
                                   MetaServerConfig metaServerConfig,
                                   ScheduledExecutorService scheduled,
                                   Executor executor,
                                   Map<KeeperMeta, KeeperState> keeperRoles,
                                   TfsGateway tfsGateway) {
        this.shardContext = new TfsShardContext(clusterDbId, shardDbId);
        this.keepers = new LinkedList<>(keepers);
        this.previousActiveKeeper = previousActiveKeeper;
        this.activeKeeperMaster = activeKeeperMaster;
        this.routeForActiveKeeper = routeForActiveKeeper;
        this.clientPool = clientPool;
        this.dcMetaCache = dcMetaCache;
        this.metaServerConfig = metaServerConfig;
        this.scheduled = scheduled;
        this.executor = executor;
        this.keeperRoles = keeperRoles;
        this.tfsGateway = tfsGateway;
    }

    @Override
    public String getName() {
        return "tfs keeper change job";
    }

    @Override
    protected void doExecute() throws CommandExecutionException {
        if (future().isDone()) {
            return;
        }

        KeeperMeta newActive = findNewActiveKeeper();
        if (newActive == null) {
            future().setFailure(new Exception("can not find active keeper:" + keepers));
            return;
        }

        getLogger().info("[tfsKeeperStateChange]cluster_{},shard_{},newActive={},previousActive={}",
                shardContext.getClusterDbId(), shardContext.getShardDbId(), newActive, previousActiveKeeper);

        SequenceCommandChain chain = new SequenceCommandChain(false);

        KeeperMeta oldTfsForPrepare = resolveOldTfsActiveForPrepare(newActive);
        if (oldTfsForPrepare != null) {
            chain.add(new TfsPrepareReleaseCommand(shardContext, oldTfsForPrepare, newActive, clientPool, dcMetaCache,
                    metaServerConfig, scheduled, executor, tfsGateway));
        }

        if (activeKeeperMaster != null) {
            Command<?> setActiveCommand = createKeeperSetStateCommand(newActive, activeKeeperMaster);
            addActiveCommandHook(setActiveCommand);
            chain.add(setActiveCommand);
        }

        ParallelCommandChain othersChain = new ParallelCommandChain(executor);
        for (KeeperMeta keeperMeta : keepers) {
            if (MetaUtils.same(keeperMeta, newActive)) {
                continue;
            }
            if (oldTfsForPrepare != null && MetaUtils.same(keeperMeta, oldTfsForPrepare)) {
                continue;
            }
            Command<?> roleCommand = createKeeperSetStateCommand(keeperMeta,
                    new Pair<>(newActive.getIp(), newActive.getPort()));
            othersChain.add(roleCommand);
        }
        chain.add(othersChain);

        if (future().isDone()) {
            return;
        }
        chain.execute().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if (commandFuture.isSuccess()) {
                    future().setSuccess(null);
                } else {
                    future().setFailure(commandFuture.cause());
                }
            }
        });
    }

    private KeeperMeta findNewActiveKeeper() {
        for (KeeperMeta keeperMeta : keepers) {
            if (KeeperState.ACTIVE == resolveKeeperState(keeperMeta)) {
                return keeperMeta;
            }
        }
        return null;
    }

    private KeeperMeta resolveOldTfsActiveForPrepare(KeeperMeta newActive) {
        if (previousActiveKeeper == null || MetaUtils.same(previousActiveKeeper, newActive)) {
            return null;
        }
        if (!TfsKeeperUtils.isTfsKeeper(previousActiveKeeper, dcMetaCache)) {
            return null;
        }
        if (KeeperState.PREPARE != resolveKeeperState(previousActiveKeeper)) {
            return null;
        }
        return previousActiveKeeper;
    }

    private Command<?> createKeeperSetStateCommand(KeeperMeta keeper, Pair<String, Integer> masterAddress) {
        SimpleObjectPool<NettyClient> pool = new XpipeObjectPoolFromKeyed<>(clientPool,
                new DefaultEndPoint(keeper.getIp(), keeper.getPort()));
        KeeperState keeperState = resolveKeeperState(keeper);
        KeeperSetStateCommand command = new KeeperSetStateCommand(pool, keeperState, masterAddress,
                KeeperState.ACTIVE == keeperState ? routeForActiveKeeper : null, scheduled);
        command.setCommandTimeoutMilli(TfsCommandConstants.TFS_STEP_TIMEOUT_MILLI);
        return CommandRetryWrapper.buildCountRetry(TfsCommandConstants.TFS_STEP_RETRY_TIMES,
                new RetryDelay(TfsCommandConstants.TFS_STEP_TIMEOUT_MILLI), command, scheduled);
    }

    private KeeperState resolveKeeperState(KeeperMeta keeper) {
        if (keeperRoles == null) {
            return keeper.isActive() ? KeeperState.ACTIVE : KeeperState.BACKUP;
        }
        for (Map.Entry<KeeperMeta, KeeperState> entry : keeperRoles.entrySet()) {
            if (MetaUtils.same(entry.getKey(), keeper)) {
                return entry.getValue();
            }
        }
        getLogger().warn("[resolveKeeperState][keeper not in roles map, fallback]{}", keeper);
        return keeper.isActive() ? KeeperState.ACTIVE : KeeperState.BACKUP;
    }

    public void setActiveSuccessCommand(Command<?> activeSuccessCommand) {
        this.activeSuccessCommand = activeSuccessCommand;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void addActiveCommandHook(final Command<?> setActiveCommand) {
        setActiveCommand.future().addListener(new CommandFutureListener() {
            @Override
            public void operationComplete(CommandFuture commandFuture) throws Exception {
                if (commandFuture.isSuccess() && activeSuccessCommand != null) {
                    getLogger().info("[addActiveCommandHook][set active success, execute hook]{}, {}",
                            setActiveCommand, activeSuccessCommand);
                    activeSuccessCommand.execute();
                }
            }
        });
    }

    @Override
    protected void doReset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return String.format("[%s] master: %s",
                StringUtil.join(",", keeper -> String.format("%s.%s", keeper.desc(), keeper.isActive()), keepers),
                activeKeeperMaster);
    }

    @Override
    public int getCommandTimeoutMilli() {
        return DEFAULT_REDIS_COMMAND_TIME_OUT_MILLI * 2;
    }
}
