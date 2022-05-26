package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.CommandReaderWriterFactory;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandsListener;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
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

        return new GtidCommandStore(new File(baseDir, replMeta.getCmdFilePrefix()), cmdFileSize,
                new GtidSet(replMeta.getRdbGtidSet()), config::getReplicationStoreCommandFileKeepTimeSeconds,
                config.getReplicationStoreMinTimeMilliToGcAfterCreate(), config::getReplicationStoreCommandFileNumToKeep,
                config.getCommandReaderFlyingThreshold(), cmdReaderWriterFactory, keeperMonitor);
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
}
