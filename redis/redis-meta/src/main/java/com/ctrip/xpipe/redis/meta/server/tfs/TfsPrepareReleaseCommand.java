package com.ctrip.xpipe.redis.meta.server.tfs;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleKeyedObjectPool;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.CommandRetryWrapper;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeObjectPoolFromKeyed;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractKeeperCommand.KeeperSetStateCommand;
import com.ctrip.xpipe.redis.meta.server.config.MetaServerConfig;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.retry.RetryDelay;
import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Step 1 + 1b: SETSTATE PREPARE (no retry); on failure invoke ForceCloseDir (non-blocking).
 */
public class TfsPrepareReleaseCommand extends AbstractCommand<Void> {

    private final TfsShardContext shardContext;
    private final KeeperMeta oldTfsActive;
    private final KeeperMeta newActive;
    private final SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool;
    private final DcMetaCache dcMetaCache;
    private final MetaServerConfig metaServerConfig;
    private final ScheduledExecutorService scheduled;
    private final Executor executor;
    private final TfsGateway tfsGateway;

    public TfsPrepareReleaseCommand(TfsShardContext shardContext, KeeperMeta oldTfsActive, KeeperMeta newActive,
                                    SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool,
                                    DcMetaCache dcMetaCache, MetaServerConfig metaServerConfig,
                                    ScheduledExecutorService scheduled, Executor executor) {
        this(shardContext, oldTfsActive, newActive, clientPool, dcMetaCache, metaServerConfig, scheduled, executor, null);
    }

    public TfsPrepareReleaseCommand(TfsShardContext shardContext, KeeperMeta oldTfsActive, KeeperMeta newActive,
                                    SimpleKeyedObjectPool<Endpoint, NettyClient> clientPool,
                                    DcMetaCache dcMetaCache, MetaServerConfig metaServerConfig,
                                    ScheduledExecutorService scheduled, Executor executor, TfsGateway tfsGateway) {
        this.shardContext = shardContext;
        this.oldTfsActive = oldTfsActive;
        this.newActive = newActive;
        this.clientPool = clientPool;
        this.dcMetaCache = dcMetaCache;
        this.metaServerConfig = metaServerConfig;
        this.scheduled = scheduled;
        this.executor = executor;
        this.tfsGateway = tfsGateway;
    }

    @Override
    public String getName() {
        return "tfs prepare release";
    }

    @Override
    protected void doExecute() throws Exception {
        SimpleObjectPool<NettyClient> pool = new XpipeObjectPoolFromKeyed<>(clientPool,
                new DefaultEndPoint(oldTfsActive.getIp(), oldTfsActive.getPort()));
        Pair<String, Integer> newActiveAddress = new Pair<>(newActive.getIp(), newActive.getPort());
        KeeperSetStateCommand prepareCommand = new KeeperSetStateCommand(pool, KeeperState.PREPARE, newActiveAddress,
                scheduled);
        prepareCommand.setCommandTimeoutMilli(TfsCommandConstants.TFS_STEP_TIMEOUT_MILLI);
        CommandRetryWrapper.buildCountRetry(TfsCommandConstants.TFS_STEP_RETRY_TIMES,
                new RetryDelay(TfsCommandConstants.TFS_STEP_TIMEOUT_MILLI), prepareCommand, scheduled)
                .execute().addListener(new CommandFutureListener<String>() {
            @Override
            public void operationComplete(CommandFuture<String> commandFuture) throws Exception {
                if (commandFuture.isSuccess()) {
                    future().setSuccess(null);
                    return;
                }
                getLogger().warn("[prepareRelease][prepare fail, force close dir]cluster_{},shard_{},{}",
                        shardContext.getClusterDbId(), shardContext.getShardDbId(), oldTfsActive, commandFuture.cause());
                createForceCloseDirCommand().execute().addListener(new CommandFutureListener<Void>() {
                    @Override
                    public void operationComplete(CommandFuture<Void> forceCloseFuture) throws Exception {
                        future().setSuccess(null);
                    }
                });
            }
        });
    }

    private TfsForceCloseDirCommand createForceCloseDirCommand() {
        if (tfsGateway != null) {
            return new TfsForceCloseDirCommand(shardContext, oldTfsActive, dcMetaCache, metaServerConfig, scheduled,
                    executor, tfsGateway);
        }
        return new TfsForceCloseDirCommand(shardContext, oldTfsActive, dcMetaCache, metaServerConfig, scheduled,
                executor);
    }

    @Override
    protected void doReset() {
        throw new UnsupportedOperationException();
    }
}
