package com.ctrip.xpipe.redis.keeper.store.cmd;

import com.ctrip.xpipe.redis.core.protocal.protocal.ArrayParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOpParser;
import com.ctrip.xpipe.redis.core.store.CommandReader;
import com.ctrip.xpipe.redis.core.store.CommandReaderWriterFactory;
import com.ctrip.xpipe.redis.core.store.CommandStore;
import com.ctrip.xpipe.redis.core.store.GtidSetReplicationProgress;
import com.ctrip.xpipe.utils.OffsetNotifier;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/5/24
 */
public class GtidSetCommandReaderWriterFactory extends OffsetCommandReaderWriterFactory implements CommandReaderWriterFactory {

    private RedisOpParser redisOpParser;

    private int bytesBetweenIndex;

    public GtidSetCommandReaderWriterFactory(RedisOpParser redisOpParser, int bytesBetweenIndex) {
        this.redisOpParser = redisOpParser;
        this.bytesBetweenIndex = bytesBetweenIndex;
    }

    @Override
    public CommandReader<RedisOp> createCmdReader(GtidSetReplicationProgress replProgress,
                                                  CommandStore cmdStore,
                                                  OffsetNotifier offsetNotifier, long commandReaderFlyingThreshold) throws IOException {
        return new GtidSetCommandReader(cmdStore, replProgress.getProgress(),
                new ArrayParser(), redisOpParser, offsetNotifier, commandReaderFlyingThreshold);
    }

}
