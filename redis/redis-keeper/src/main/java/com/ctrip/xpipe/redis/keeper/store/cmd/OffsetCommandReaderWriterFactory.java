package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.netty.filechannel.DefaultReferenceFileRegion;
import com.ctrip.xpipe.netty.filechannel.ReferenceFileRegion;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.core.store.ratelimit.ReplDelayConfig;
import com.ctrip.xpipe.utils.OffsetNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/4/17
 */
public class OffsetCommandReaderWriterFactory implements CommandReaderWriterFactory {

    private static final Logger logger = LoggerFactory.getLogger(OffsetCommandReaderWriterFactory.class);

    @Override
    public CommandWriter createCmdWriter(CommandStore cmdStore,
                                         int maxFileSize, Logger delayTraceLogger) {
        return new OffsetCommandWriter(cmdStore, maxFileSize, delayTraceLogger);
    }

    @Override
    public CommandReader<ReferenceFileRegion> createCmdReader(ReplicationProgress<Long> replProgress,
                                                              CommandStore cmdStore, OffsetNotifier offsetNotifier,
                                                              ReplDelayConfig replDelayConfig, long commandReaderFlyingThreshold) throws IOException {
        long currentOffset = replProgress.getProgress();
        long endOffsetExcluded = -1;
        if (replProgress instanceof BacklogOffsetReplicationProgress) {
            endOffsetExcluded = ((BacklogOffsetReplicationProgress) replProgress).getEndProgressExcluded();
            if (endOffsetExcluded >= 0 && endOffsetExcluded <= currentOffset)
                throw new UnsupportedOperationException("endOffset must gt beginOffset: " + endOffsetExcluded + ":" + currentOffset);
        }

        return new OffsetCommandReader(currentOffset, endOffsetExcluded,
                cmdStore, offsetNotifier, replDelayConfig, commandReaderFlyingThreshold);
    }

    @Override
    public CommandReader<RedisOp> createCmdReader(GtidSetReplicationProgress replProgress, CommandStore cmdStore,
                                                  OffsetNotifier offsetNotifier, long commandReaderFlyingThreshold) throws IOException {
        throw new UnsupportedOperationException();
    }
}
