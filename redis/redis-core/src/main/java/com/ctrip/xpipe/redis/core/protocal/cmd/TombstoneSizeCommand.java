package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

public class TombstoneSizeCommand extends AbstractRedisCommand<Long> {

    public TombstoneSizeCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
        super(clientPool, scheduled, commandTimeoutMilli);
    }

    protected Long format(Object payload) {
        return payloadToLong(payload);
    }

    @Override
    public String getName() {
        return "TOMBSTONESIZE";
    }

    @Override
    public ByteBuf getRequest() {
        RequestStringParser requestString = new RequestStringParser(getName());
        return requestString.format();
    }

}
