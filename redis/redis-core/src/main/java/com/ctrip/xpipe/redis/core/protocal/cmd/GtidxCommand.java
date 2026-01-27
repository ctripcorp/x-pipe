package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author TB
 * @date 2026/1/26 17:43
 */
public  abstract class GtidxCommand<T> extends AbstractRedisCommand<T>{
    protected final String CMD = "gtidx";
    protected String uuid;
    protected long startGno;
    protected long endGno;
    public GtidxCommand(SimpleObjectPool clientPool, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
    }

    protected abstract String getValue();


}
