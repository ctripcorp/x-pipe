package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.utils.OffsetNotifier;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/4/17
 */
public class OffsetCommandReaderWriterFactory implements CommandReaderWriterFactory {

    @Override
    public CommandWriter createCmdWriter(CommandStore cmdStore,
                                         int maxFileSize, Logger delayTraceLogger) {
        return new OffsetCommandWriter(cmdStore, maxFileSize, delayTraceLogger);
    }

    @Override
    public CommandReader<ReferenceFileRegion> createCmdReader(OffsetReplicationProgress replProgress,
                                                              CommandStore cmdStore,
                                                              OffsetNotifier offsetNotifier, long commandReaderFlyingThreshold) throws IOException {
        long currentOffset = replProgress.getProgress();
        cmdStore.rotateFileIfNecessary();
        CommandFile commandFile = cmdStore.findFileForOffset(currentOffset);
        if (null == commandFile) {
            throw new IOException("File for offset " + replProgress.getProgress() + " in store " + cmdStore + " does not exist");
        }

        return new OffsetCommandReader(commandFile.getFile(), currentOffset, currentOffset - commandFile.getStartOffset(),
                cmdStore, offsetNotifier, commandReaderFlyingThreshold);
    }

    @Override
    public CommandReader<RedisOp> createCmdReader(GtidSetReplicationProgress replProgress, CommandStore cmdStore,
                                                  OffsetNotifier offsetNotifier, long commandReaderFlyingThreshold) throws IOException {
        throw new UnsupportedOperationException();
    }
}
