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
import com.ctrip.xpipe.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
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
                                              KeeperMonitor keeperMonitor, RdbStore rdbStore) throws IOException {

        String replRdbGtidSet = replMeta.getRdbGtidSet();
        logger.info("[createCommandStore], replRdbGtidSet={}", replRdbGtidSet);
        GtidCommandStore cmdStore = new GtidCommandStore(new File(baseDir, replMeta.getCmdFilePrefix()), cmdFileSize,
                new GtidSet(replRdbGtidSet), config::getReplicationStoreCommandFileKeepTimeSeconds,
                config.getReplicationStoreMinTimeMilliToGcAfterCreate(), config::getReplicationStoreCommandFileNumToKeep,
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
    protected RdbStore createRdbStore(File rdb, long rdbOffset, EofType eofType) throws IOException {
        ReplicationStoreMeta meta = getMetaStore().dupReplicationStoreMeta();
        if (meta.getRdbFile().equals(rdb.getName())) {
            return new GtidRdbStore(rdb, rdbOffset, eofType, meta.getRdbGtidSet());
        } else {
            return new GtidRdbStore(rdb, rdbOffset, eofType, null);
        }
    }

    @Override
    protected RdbStoreListener createRdbStoreListener(RdbStore rdbStore) {
        return new GtidReplicationStoreRdbFileListener(rdbStore);
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
    public FULLSYNC_FAIL_CAUSE fullSyncIfPossible(FullSyncListener fullSyncListener) throws IOException {
        makeSureOpen();

        if (!fullSyncListener.supportProgress(GtidSetReplicationProgress.class)) {
            return super.fullSyncIfPossible(fullSyncListener);
        }

        FullSyncContext ctx = lockAndCheckIfFullSyncPossible();
        if (ctx.isFullSyncPossible() && StringUtil.isEmpty(((GtidRdbStore)ctx.getRdbStore()).getGtidSet())) {
            return FULLSYNC_FAIL_CAUSE.RDB_GTIDSET_NOT_READY;
        }

        if (ctx.isFullSyncPossible()) {
            return tryDoFullSync(ctx, fullSyncListener);
        } else {
            return ctx.getCause();
        }
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    public class GtidReplicationStoreRdbFileListener extends ReplicationStoreRdbFileListener implements RdbStoreListener {

        public GtidReplicationStoreRdbFileListener(RdbStore rdbStore) {
            super(rdbStore);
        }

        @Override
        public void onRdbGtidSet(String gtidSet) {
            try {
                getLogger().info("[onRdbGtidSet][update metastore] {} {}", rdbStore.getRdbFileName(), gtidSet);
                getMetaStore().attachRdbGtidSet(rdbStore.getRdbFileName(), gtidSet);

                getLogger().info("[onRdbGtidSet][update metastore] info to init first index {} - ({} - 1) = {} : {}",
                        rdbStore.rdbOffset(), getMetaStore().beginOffset(), rdbStore.rdbOffset() - (getMetaStore().beginOffset() - 1), gtidSet);
                cmdStore.setBaseIndex(gtidSet, rdbStore.rdbOffset() - (getMetaStore().beginOffset() - 1));
            } catch (IOException e) {
                getLogger().error("[onRdbGtidSet][update metastore]", e);
            }
        }

    }

    @Override
    public FULLSYNC_FAIL_CAUSE createIndexIfPossible(ExecutorService indexingExecutors) {

        synchronized (indexingLock) {

            if (indexingFuture != null) {
                return null;
            }

            FullSyncContext ctx = lockAndCheckIfFullSyncPossible();
            if (ctx.isFullSyncPossible() && StringUtil.isEmpty(ctx.getRdbStore().getGtidSet())) {
                return FULLSYNC_FAIL_CAUSE.RDB_GTIDSET_NOT_READY;
            }

            if (ctx.isFullSyncPossible()) {
                return tryCreateIndex(ctx, indexingExecutors);
            } else {
                return ctx.getCause();
            }
        }
    }


    protected FULLSYNC_FAIL_CAUSE tryCreateIndex(FullSyncContext ctx, ExecutorService indexingExecutors) {
        //TODO 1: how about gc() invoked at the same time ?
        //TODO 2: find latest index - DONE
        GtidSet gtidSet = new GtidSet(ctx.getRdbStore().getGtidSet());

        CommandFileSegment lastSegment = cmdStore.findLastFileSegment();
        if (lastSegment != null) {
            gtidSet = gtidSet.intersectionGtidSet(lastSegment.getStartIdx().getExcludedGtidSet());
        }

        String gtidSetString = gtidSet.toString();

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
}
