package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.utils.OffsetNotifier;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/4/17
 */
public class OffsetCommandReaderWriterFactory implements CommandReaderWriterFactory<OffsetReplicationProgress> {

    @Override
    public CommandWriter createCmdWriter(CommandFileContext cmdFileContext, CommandStore<OffsetReplicationProgress> cmdStore, int maxFileSize, Logger delayTraceLogger) {
        return new OffsetCommandWriter(cmdFileContext, cmdStore, maxFileSize, delayTraceLogger);
    }

    @Override
    public CommandReader createCmdReader(OffsetReplicationProgress replProgress, CommandStore<OffsetReplicationProgress> cmdStore, OffsetNotifier offsetNotifier, long commandReaderFlyingThreshold) throws IOException {
        long currentOffset = replProgress.getProgress();
        cmdStore.rotateFileIfNecessary();
        CommandFile commandFile = cmdStore.findFileForOffset(currentOffset);
        if (null == commandFile) {
            throw new IOException("File for offset " + replProgress.getProgress() + " in store " + cmdStore + " does not exist");
        }

        return new OffsetCommandReader(commandFile.getFile(), currentOffset, currentOffset - commandFile.getStartOffset(),
                cmdStore, offsetNotifier, commandReaderFlyingThreshold);
    }
}
