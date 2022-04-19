package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.utils.OffsetNotifier;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/4/15
 */
public interface CommandReaderWriterFactory<T extends ReplicationProgress<?,?>> {

    CommandWriter createCmdWriter(CommandFileContext cmdFileContext, CommandStore<T> cmdStore, int maxFileSize, Logger delayTraceLogger) throws IOException;

    CommandReader createCmdReader(T replProgress, CommandStore<T> cmdStore, OffsetNotifier offsetNotifier, long commandReaderFlyingThreshold) throws IOException;

}
