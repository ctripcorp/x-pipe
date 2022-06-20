package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.utils.OffsetNotifier;
import org.slf4j.Logger;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/5/24
 */
public class GtidSetCommandReaderWriterFactory extends OffsetCommandReaderWriterFactory implements CommandReaderWriterFactory {

    private RedisOpParser redisOpParser;

    public GtidSetCommandReaderWriterFactory(RedisOpParser redisOpParser) {
        this.redisOpParser = redisOpParser;
    }

    @Override
    public CommandWriter createCmdWriter(CommandStore cmdStore,
                                         int maxFileSize, Logger delayTraceLogger) {
        return new GtidSetCommandWriter(new ArrayParser(), redisOpParser, cmdStore, maxFileSize, delayTraceLogger);
    }

    @Override
    public CommandReader<RedisOp> createCmdReader(GtidSetReplicationProgress replProgress,
                                                  CommandStore cmdStore,
                                                  OffsetNotifier offsetNotifier, long commandReaderFlyingThreshold) throws IOException {
        return new GtidSetCommandReader(cmdStore, replProgress.getProgress(),
                new ArrayParser(), redisOpParser, offsetNotifier, commandReaderFlyingThreshold);
    }

}
