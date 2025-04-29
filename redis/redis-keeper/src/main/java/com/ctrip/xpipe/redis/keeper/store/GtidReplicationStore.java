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
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.store.cmd.GtidSetCommandReaderWriterFactory;
import com.ctrip.xpipe.tuple.Pair;
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
                                KeeperMonitor keeperMonitor, RedisOpParser redisOpParser, SyncRateManager syncRateManager) throws IOException {
        super(baseDir, config, keeperRunid,
                new GtidSetCommandReaderWriterFactory(redisOpParser, config.getCommandIndexBytesInterval()),
                keeperMonitor, syncRateManager, redisOpParser);
    }

    @Override
    protected Pair<RdbStore,RdbStore> recoverRdbStores(File baseDir, ReplicationStoreMeta meta) throws IOException{
        RdbStore rdbStore = null, rordbStore = null;

        if (meta != null && meta.getRdbFile() != null) {
            File rdb = new File(baseDir, meta.getRdbFile());
            if (rdb.isFile()) {
                ReplStage replStage = meta.getCurReplStage();
                ReplStage.ReplProto replProto = replStage != null ? replStage.getProto() : null;
                GtidSet gtidLost = replStage != null ? replStage.getGtidLost() : null;
                String masterUuid = replStage != null ? replStage.getMasterUuid() : null;

                rdbStore = createRdbStore(rdb, meta.getReplId(), meta.getRdbLastOffset(), initRdbEofType(meta), replProto, gtidLost, masterUuid);
                rdbStore.updateRdbType(RdbStore.Type.NORMAL);
                rdbStore.updateRdbGtidSet(null != meta.getRdbGtidSet() ? meta.getRdbGtidSet() : GtidSet.EMPTY_GTIDSET);
            }
        }

        if (meta != null && meta.getRordbFile() != null) {
            File rordb = new File(baseDir, meta.getRordbFile());
            if (rordb.isFile()) {
                ReplStage replStage = meta.getCurReplStage();
                ReplStage.ReplProto replProto = replStage != null ? replStage.getProto() : null;
                GtidSet gtidLost = replStage != null ? replStage.getGtidLost() : null;
                String masterUuid = replStage != null ? replStage.getMasterUuid() : null;

                rordbStore = createRdbStore(rordb, meta.getReplId(), meta.getRordbLastOffset(), initRordbEofType(meta), replProto, gtidLost, masterUuid);
                rordbStore.updateRdbType(RdbStore.Type.RORDB);
                rordbStore.updateRdbGtidSet(null != meta.getRordbGtidSet() ? meta.getRordbGtidSet() : GtidSet.EMPTY_GTIDSET);
            }
        }
        return new Pair<>(rdbStore,rordbStore);
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
                config.getCommandReaderFlyingThreshold(), cmdReaderWriterFactory, keeperMonitor, this.redisOpParser);
        cmdStore.attachRateLimiter(syncRateManager.generatePsyncRateLimiter());

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
        RdbStore rdbStore = new GtidRdbStore(rdb, replId, rdbOffset, eofType, null, null, null, null);
        rdbStore.attachRateLimiter(syncRateManager.generateFsyncRateLimiter());
        return rdbStore;
    }

    public RdbStore prepareRdb(String replId, long rdbOffset, EofType eofType, ReplStage.ReplProto replProto,
                               GtidSet gtidLost, String masterUuid) throws IOException {
        makeSureOpen();
        getBaseDir().mkdirs();

        getLogger().info("[makeRdb] replId:{}, rdbOffset:{}, eof:{}, replProto:{}, gtidLost:{}, masterUuid: {}",
                replId, rdbOffset, eofType, replProto, gtidLost, masterUuid);
        String rdbFile = newRdbFileName();
        return createRdbStore(new File(getBaseDir(), rdbFile), replId, rdbOffset, eofType, replProto, gtidLost, masterUuid);
    }

    protected RdbStore createRdbStore(File rdb, String replId, long rdbOffset, EofType eofType, ReplStage.ReplProto replProto,
                                      GtidSet gtidLost, String masterUuid) throws IOException {
        RdbStore rdbStore = new GtidRdbStore(rdb, replId, rdbOffset, eofType, replProto, null,
                gtidLost == null ? GtidSet.EMPTY_GTIDSET: gtidLost.toString(), masterUuid);
        rdbStore.attachRateLimiter(syncRateManager.generateFsyncRateLimiter());
        return rdbStore;
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
        DumpedRdbStore rdbStore = new DumpedGtidRdbStore(new File(getBaseDir(), newRdbFileName()));
        rdbStore.attachRateLimiter(syncRateManager.generateFsyncRateLimiter());
        return rdbStore;
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
