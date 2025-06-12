package com.ctrip.xpipe.redis.keeper.impl;


import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.exception.psync.GapAllowedSyncRdbNotContinuousRuntimeException;
import com.ctrip.xpipe.redis.keeper.exception.psync.PsyncMasterRdbOffsetNotContinuousRuntimeException;
import com.ctrip.xpipe.redis.keeper.exception.psync.RdbOnlyPsyncReplIdNotSameException;
import com.ctrip.xpipe.redis.keeper.exception.replication.UnexpectedReplIdException;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 * <p>
 * Aug 25, 2016
 */
public class RedisMasterNewRdbDumper extends AbstractRdbDumper {

    private final Logger logger = LoggerFactory.getLogger(RedisMasterNewRdbDumper.class);

    private DumpedRdbStore dumpedRdbStore;

    private final RedisMaster redisMaster;

    private RdbonlyRedisMasterReplication rdbonlyRedisMasterReplication;

    private final NioEventLoopGroup nioEventLoopGroup;

    private final ScheduledExecutorService scheduled;

    private final KeeperResourceManager resourceManager;

    private final boolean tryRordb;

    private final boolean freshRdbNeeded;

    public RedisMasterNewRdbDumper(RedisMaster redisMaster, RedisKeeperServer redisKeeperServer,
                                   boolean tryRordb, boolean freshRdbNeeded,
                                   NioEventLoopGroup nioEventLoopGroup, ScheduledExecutorService scheduled,
                                   KeeperResourceManager resourceManager) {
        super(redisKeeperServer);
        this.redisMaster = redisMaster;
        this.tryRordb = tryRordb;
        this.freshRdbNeeded = freshRdbNeeded;
        this.nioEventLoopGroup = nioEventLoopGroup;
        this.scheduled = scheduled;
        this.resourceManager = resourceManager;
    }

    @Override
    public boolean tryRordb() {
        return tryRordb;
    }

    @Override
    protected void doExecute() throws Exception {
        startRdbOnlyReplication();

        future().addListener(new CommandFutureListener<Void>() {

            @Override
            public void operationComplete(CommandFuture<Void> commandFuture) throws Exception {
                releaseResource();
                if (!commandFuture.isSuccess()) {
                    if (commandFuture.cause() instanceof PsyncMasterRdbOffsetNotContinuousRuntimeException ||
                            commandFuture.cause() instanceof  GapAllowedSyncRdbNotContinuousRuntimeException) {
                        redisKeeperServer.resetDefaultReplication();
                    }
                }
            }
        });
    }

    protected void releaseResource() {

        try {
            LifecycleHelper.stopIfPossible(rdbonlyRedisMasterReplication);
            LifecycleHelper.disposeIfPossible(rdbonlyRedisMasterReplication);
        } catch (Exception e) {
            logger.error("[releaseResource]" + rdbonlyRedisMasterReplication, e);
        }

    }

    protected void startRdbOnlyReplication() throws Exception {
        if (redisKeeperServer.gapAllowSyncEnabled()) {
            rdbonlyRedisMasterReplication = new GapAllowedRdbonlyRedisMasterReplication(redisKeeperServer, redisMaster, tryRordb, freshRdbNeeded,
                    nioEventLoopGroup, scheduled, this, resourceManager);
        } else {
            rdbonlyRedisMasterReplication = new RdbonlyRedisMasterReplication(redisKeeperServer, redisMaster, tryRordb, freshRdbNeeded,
                    nioEventLoopGroup, scheduled, this, resourceManager);
        }

        rdbonlyRedisMasterReplication.initialize();
        rdbonlyRedisMasterReplication.start();
    }

    @Override
    protected void doCancel() {
        super.doCancel();

        logger.info("[doCancel][release resource]");
        releaseResource();
    }

    @Override
    public DumpedRdbStore prepareRdbStore() throws IOException {

        dumpedRdbStore = redisMaster.getCurrentReplicationStore().prepareNewRdb();
        logger.info("[prepareRdbStore]{}", dumpedRdbStore);
        return dumpedRdbStore;
    }

    @Override
    public void beginReceiveRdbData(String replId, long masterOffset) {

        try {
            logger.info("[beginReceiveRdbData][update rdb]{}", dumpedRdbStore);
            redisMaster.getCurrentReplicationStore().checkReplId(replId);
            super.beginReceiveRdbData(replId, masterOffset);
        } catch (UnexpectedReplIdException e) {
            dumpFail(new RdbOnlyPsyncReplIdNotSameException("[beginReceiveRdbData]", e));
        } catch (Throwable th) {
            logger.error("[beginReceiveRdbData]", th);
        }
    }

    @Override
    public String toString() {
        return String.format("%s(%s)[%s]", getClass().getSimpleName(), rdbonlyRedisMasterReplication, tryRordb);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }
}
