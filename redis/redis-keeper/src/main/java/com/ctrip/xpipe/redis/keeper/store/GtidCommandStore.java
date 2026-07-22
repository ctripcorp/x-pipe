package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.KeeperMonitor;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;
import com.ctrip.xpipe.redis.keeper.store.ck.CKStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

/**
 * @author lishanglin
 * date 2022/5/24
 */
public class GtidCommandStore extends DefaultCommandStore implements CommandStore {

    private static final Logger logger = LoggerFactory.getLogger(GtidCommandStore.class);

    public GtidCommandStore(CKStore ckStore, KeeperConfig keeperConfig, File file, int maxFileSize,
                            BooleanSupplier recordWrongStreamConfig, IntSupplier maxTimeSecondKeeperCmdFileAfterModified,
                            int minTimeMilliToGcAfterModified, IntSupplier fileNumToKeep, long commandReaderFlyingThreshold,
                            BooleanSupplier commandOffsetNotifyCoalescingEnabled, CommandReaderWriterFactory cmdReaderWriterFactory,
                            KeeperMonitor keeperMonitor, RedisOpParser redisOpParser, GtidCmdFilter cmdFilter, boolean buildIndex,
                            long cmdStoreStartOffset, AsyncFileSystem asyncFileSystem, IntSupplier asyncWriteMaxBytes,
                            ReplId fileSystemReplId) throws IOException {
        super(ckStore, keeperConfig, file, maxFileSize, recordWrongStreamConfig, maxTimeSecondKeeperCmdFileAfterModified,
                minTimeMilliToGcAfterModified, fileNumToKeep, commandReaderFlyingThreshold, commandOffsetNotifyCoalescingEnabled,
                cmdReaderWriterFactory, keeperMonitor, redisOpParser, cmdFilter, buildIndex, cmdStoreStartOffset,
                asyncFileSystem, asyncWriteMaxBytes, fileSystemReplId);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

}
