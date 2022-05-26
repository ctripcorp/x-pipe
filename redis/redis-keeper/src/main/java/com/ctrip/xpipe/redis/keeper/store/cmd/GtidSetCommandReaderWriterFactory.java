package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.CommandReader;
import com.ctrip.xpipe.redis.core.store.CommandReaderWriterFactory;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.CommandWriter;
import com.ctrip.xpipe.utils.OffsetNotifier;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/5/24
 */
public class GtidSetCommandReaderWriterFactory implements CommandReaderWriterFactory<GtidSetReplicationProgress, RedisOp> {

    private RedisOpParser redisOpParser;

    public GtidSetCommandReaderWriterFactory(RedisOpParser redisOpParser) {
        this.redisOpParser = redisOpParser;
    }

    @Override
    public CommandWriter createCmdWriter(CommandStore<GtidSetReplicationProgress, RedisOp> cmdStore,
                                         int maxFileSize, Logger delayTraceLogger) throws IOException {
        return new GtidSetCommandWriter(new ArrayParser(), redisOpParser, cmdStore, maxFileSize, delayTraceLogger);
    }

    @Override
    public CommandReader<RedisOp> createCmdReader(GtidSetReplicationProgress replProgress,
                                                  CommandStore<GtidSetReplicationProgress, RedisOp> cmdStore,
                                                  OffsetNotifier offsetNotifier, long commandReaderFlyingThreshold) throws IOException {
        return new GtidSetCommandReader(cmdStore, replProgress.getProgress(),
                new ArrayParser(), redisOpParser, offsetNotifier, commandReaderFlyingThreshold);
    }

}
