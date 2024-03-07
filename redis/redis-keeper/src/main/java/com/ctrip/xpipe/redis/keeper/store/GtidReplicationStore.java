package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.Gtid2OffsetIndexGenerator;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.store.cmd.GtidSetCommandReaderWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * @author lishanglin
 * date 2022/5/24
 */
public class GtidReplicationStore extends DefaultReplicationStore {

    private static final Logger logger = LoggerFactory.getLogger(GtidReplicationStore.class);

    private volatile Gtid2OffsetIndexGenerator indexGenerator;

    private volatile Future indexingFuture;

    protected final Object indexingLock = new Object();

    public GtidReplicationStore(File baseDir, KeeperConfig config, String keeperRunid,
                                KeeperMonitor keeperMonitor, RedisOpParser redisOpParser) throws IOException {
        super(baseDir, config, keeperRunid,
                new GtidSetCommandReaderWriterFactory(redisOpParser, config.getCommandIndexBytesInterval()),
                keeperMonitor);
    }

    @Override
    protected CommandStore createCommandStore(File baseDir, ReplicationStoreMeta replMeta, int cmdFileSize,
                                              KeeperConfig config, CommandReaderWriterFactory cmdReaderWriterFactory,
                                              KeeperMonitor keeperMonitor) throws IOException {

        String replRdbGtidSet = replMeta.getRdbGtidSet();
        logger.info("[createCommandStore], replRdbGtidSet={}", replRdbGtidSet);
        GtidCommandStore cmdStore = new GtidCommandStore(new File(baseDir, replMeta.getCmdFilePrefix()), cmdFileSize,
                config::getReplicationStoreCommandFileKeepTimeSeconds,
                config.getReplicationStoreMinTimeMilliToGcAfterCreate(),
                config::getReplicationStoreCommandFileNumToKeep,
                config.getCommandReaderFlyingThreshold(), cmdReaderWriterFactory, keeperMonitor);

        try {
            cmdStore.initialize();
        } catch (Exception e) {
            logger.info("[createCommandStore] init fail", e);
            throw new XpipeRuntimeException("cmdStore init fail", e);
        }
        return cmdStore;
    }

    @Override
    protected RdbStore createRdbStore(File rdb, String replId, long rdbOffset, EofType eofType) throws IOException {
        return new GtidRdbStore(rdb, replId, rdbOffset, eofType, null);
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (indexingFuture != null) {
            indexingFuture.cancel(true);
            indexingFuture = null;
        }
    }

    @Override
    public DumpedRdbStore prepareNewRdb() throws IOException {
        makeSureOpen();
        return new DumpedGtidRdbStore(new File(getBaseDir(), newRdbFileName()));
    }

    @Override
    public FULLSYNC_FAIL_CAUSE fullSyncIfPossible(FullSyncListener fullSyncListener, boolean tryRordb) throws IOException {
        makeSureOpen();

        if (!fullSyncListener.supportProgress(GtidSetReplicationProgress.class)) {
            return super.fullSyncIfPossible(fullSyncListener, tryRordb);
        }

        return doFullSyncIfPossible(fullSyncListener, tryRordb);
    }

    @Override
    protected FULLSYNC_FAIL_CAUSE tryDoFullSync(FullSyncContext ctx, FullSyncListener fullSyncListener) throws IOException {
        if (!ctx.getRdbStore().isGtidSetInit()) {
            ctx.getRdbStore().decrementRefCount();
            throw new IllegalStateException("unexpected rdb with gtid not init: " + ctx.getRdbStore());
        }

        return super.tryDoFullSync(ctx, fullSyncListener);
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public FULLSYNC_FAIL_CAUSE createIndexIfPossible(ExecutorService indexingExecutors) {

        synchronized (indexingLock) {

            if (indexingFuture != null) {
                return null;
            }

            FullSyncContext ctx = lockAndCheckIfRdbFullSyncPossible();
            if (ctx.isFullSyncPossible() && !ctx.getRdbStore().isGtidSetInit()) {
                ctx.getRdbStore().decrementRefCount();
                throw new IllegalStateException("unexpected rdb with gtid not init: " + ctx.getRdbStore());
            }

            if (ctx.isFullSyncPossible()) {
                return tryCreateIndex(ctx, indexingExecutors);
            } else {
                return ctx.getCause();
            }
        }
    }


    protected FULLSYNC_FAIL_CAUSE tryCreateIndex(FullSyncContext ctx, ExecutorService indexingExecutors) {

        RdbStore rdbStore = ctx.getRdbStore();
        GtidSet fromGtidSet = new GtidSet(rdbStore.getGtidSet());
        rdbStore.decrementRefCount();

        CommandFileSegment lastSegment = cmdStore.findLastFileSegment();
        if (lastSegment != null) {
            fromGtidSet = fromGtidSet.union(lastSegment.getStartIdx().getExcludedGtidSet());
        }

        String gtidSetString = fromGtidSet.toString();

        logger.info("[tryCreateIndex] indexing from {}", gtidSetString);

        indexGenerator = new Gtid2OffsetIndexGenerator(cmdStore, new GtidSet(gtidSetString));

        indexingFuture = indexingExecutors.submit(()->{

            try {
                addCommandsListener(new GtidSetReplicationProgress(new GtidSet(gtidSetString)), indexGenerator);
            } catch (Throwable t) {
                EventMonitor.DEFAULT.logAlertEvent("[tryCreateIndex] probably creating reader error: " + this);

                logger.error("[tryCreateIndex] probably creating reader error - " + this);
                logger.error("[tryCreateIndex]", t);
            } finally {
                indexingFuture = null;
            }
        });
        return null;
    }

    @Override
    public GtidSet getBeginGtidSet() throws IOException {
        return cmdStore.getBeginGtidSet();
    }

    @Override
    public GtidSet getEndGtidSet() {
        if (indexGenerator != null) {
            return indexGenerator.getEndGtidSet();
        } else {
            return null;
        }
    }

    @Override
    public boolean supportGtidSet() {
        return getRdbStore().supportGtidSet();
    }
}
