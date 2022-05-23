package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.utils.OffsetNotifier;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/4/15
 */
public interface CommandReaderWriterFactory<T extends ReplicationProgress<?,?>, R> {

    CommandWriter createCmdWriter(CommandStore<T, R> cmdStore, int maxFileSize, Logger delayTraceLogger) throws IOException;

    CommandReader<R> createCmdReader(T replProgress, CommandStore<T, R> cmdStore, OffsetNotifier offsetNotifier, long commandReaderFlyingThreshold) throws IOException;

}
