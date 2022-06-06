package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.store.cmd.GtidSetCommandReaderWriterFactory;
import com.ctrip.xpipe.redis.keeper.store.cmd.GtidSetReplicationProgress;
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
                                KeeperMonitor keeperMonitor, RedisOpParser redisOpParser) throws IOException {
        super(baseDir, config, keeperRunid, new GtidSetCommandReaderWriterFactory(redisOpParser), keeperMonitor);
    }

    @Override
    protected CommandStore createCommandStore(File baseDir, ReplicationStoreMeta replMeta, int cmdFileSize,
                                              KeeperConfig config, CommandReaderWriterFactory cmdReaderWriterFactory,
                                              KeeperMonitor keeperMonitor) throws IOException {

        GtidCommandStore cmdStore = new GtidCommandStore(new File(baseDir, replMeta.getCmdFilePrefix()), cmdFileSize,
                new GtidSet(replMeta.getRdbGtidSet()), config::getReplicationStoreCommandFileKeepTimeSeconds,
                config.getReplicationStoreMinTimeMilliToGcAfterCreate(), config::getReplicationStoreCommandFileNumToKeep,
                config.getCommandReaderFlyingThreshold(), cmdReaderWriterFactory, keeperMonitor);
        if (null != replMeta.getRdbGtidSet()) {
            try {
                cmdStore.initialize();
            } catch (Exception e) {
                logger.info("[createCommandStore] init fail", e);
                throw new XpipeRuntimeException("cmdStore init fail", e);
            }
        } else {
            logger.info("[createCommandStore] init after rdb gtid set ready");
        }

        return cmdStore;
    }

    @Override
    protected RdbStore createRdbStore(File rdb, long rdbOffset, EofType eofType) throws IOException {
        return new GtidRdbStore(rdb, rdbOffset, eofType);
    }

    @Override
    protected RdbStoreListener createRdbStoreListener(RdbStore rdbStore) {
        return new GtidReplicationStoreRdbFileListener(rdbStore);
    }

    @Override
    public DumpedRdbStore prepareNewRdb() throws IOException {
        makeSureOpen();
        return new DumpedGtidRdbStore(new File(getBaseDir(), newRdbFileName()));
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void addCommandsListener(long offset, CommandsListener commandsListener) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addCommandsListener(GtidSet excludedGtidSet, CommandsListener commandsListener) throws IOException {
        makeSureOpen();
        getCommandStore().addCommandsListener(new GtidSetReplicationProgress(excludedGtidSet), commandsListener);
    }

    public class GtidReplicationStoreRdbFileListener extends ReplicationStoreRdbFileListener implements RdbStoreListener {

        public GtidReplicationStoreRdbFileListener(RdbStore rdbStore) {
            super(rdbStore);
        }

        @Override
        public void onRdbGtidSet(String gtidSet) {
            try {
                getLogger().info("[onRdbGtidSet] {} {}", rdbStore.getRdbFileName(), gtidSet);
                if (getMetaStore().attachRdbGtidSet(rdbStore.getRdbFileName(), gtidSet)) {
                    ((GtidCommandStore)getCommandStore()).setBaseGtidSet(gtidSet);
                    getCommandStore().initialize();
                }
            } catch (Exception e) {
                getLogger().error("[onRdbGtidSet]", e);
            }
        }

    }

}
