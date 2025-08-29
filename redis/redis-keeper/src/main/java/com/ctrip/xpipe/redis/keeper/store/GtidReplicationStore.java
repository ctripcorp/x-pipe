package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.ratelimit.SyncRateManager;
import com.ctrip.xpipe.redis.keeper.store.cmd.GtidSetCommandReaderWriterFactory;
import com.ctrip.xpipe.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/5/24
 */
public class GtidReplicationStore extends DefaultReplicationStore {

    private static final Logger logger = LoggerFactory.getLogger(GtidReplicationStore.class);

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

                rdbStore = createRdbStore(rdb, meta.getReplId(), 0, initRdbEofType(meta), replProto, gtidLost, masterUuid);
                rdbStore.setContiguousBacklogOffset(meta.getRdbContiguousBacklogOffset());
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

                rordbStore = createRdbStore(rordb, meta.getReplId(), 0, initRordbEofType(meta), replProto, gtidLost, masterUuid);
                rordbStore.setContiguousBacklogOffset(meta.getRordbContiguousBacklogOffset());
                rordbStore.updateRdbType(RdbStore.Type.RORDB);
                rordbStore.updateRdbGtidSet(null != meta.getRordbGtidSet() ? meta.getRordbGtidSet() : GtidSet.EMPTY_GTIDSET);
            }
        }
        return new Pair<>(rdbStore,rordbStore);
    }

    @Override
    protected CommandStore createCommandStore(File baseDir, ReplicationStoreMeta replMeta, int cmdFileSize,
                                              KeeperConfig config, CommandReaderWriterFactory cmdReaderWriterFactory,
                                              KeeperMonitor keeperMonitor,GtidCmdFilter filter) throws IOException {

        String replRdbGtidSet = replMeta.getRdbGtidSet();
        boolean buildIndex = true;
        if(replMeta.getCurReplStage() != null && replMeta.getCurReplStage().getProto() == ReplStage. ReplProto.PSYNC) {
            buildIndex = false;
        }
        replMeta.getCurReplStage().getProto();
        logger.info("[createCommandStore], replRdbGtidSet={}, buildIndex={}", replRdbGtidSet, buildIndex);
        GtidCommandStore cmdStore = new GtidCommandStore(new File(baseDir, replMeta.getCmdFilePrefix()), cmdFileSize,
                config::getReplicationStoreCommandFileKeepTimeSeconds,
                config.getReplicationStoreMinTimeMilliToGcAfterCreate(),
                config::getReplicationStoreCommandFileNumToKeep,
                config.getCommandReaderFlyingThreshold(), cmdReaderWriterFactory, keeperMonitor, this.redisOpParser, filter, buildIndex);
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
    public GtidSet getBeginGtidSet() throws IOException {
        if (null == cmdStore) return new GtidSet("");
        return cmdStore.getBeginGtidSet();
    }

    @Override
    public GtidSet getEndGtidSet() {
        return cmdStore.getIndexGtidSet();
    }

    @Override
    public boolean supportGtidSet() {
        return getRdbStore().supportGtidSet();
    }
}
