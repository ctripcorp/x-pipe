package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.store.ratelimit.ReplDelayConfig;
import com.ctrip.xpipe.utils.OffsetNotifier;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/4/15
 */
public interface CommandReaderWriterFactory {

    CommandWriter createCmdWriter(CommandStore cmdStore, int maxFileSize, Logger delayTraceLogger) throws IOException;

    CommandReader<ReferenceFileRegion> createCmdReader(OffsetReplicationProgress replProgress, CommandStore cmdStore,
                                                       OffsetNotifier offsetNotifier, ReplDelayConfig replDelayConfig, long commandReaderFlyingThreshold) throws IOException;

    CommandReader<RedisOp> createCmdReader(GtidSetReplicationProgress replProgress, CommandStore cmdStore,
                                           OffsetNotifier offsetNotifier, long commandReaderFlyingThreshold) throws IOException;

}
