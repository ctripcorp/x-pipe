package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Apr 09, 2018
 */
public class PublishCommand extends AbstractRedisCommand<Object> {

    private static final String PUBLISH_COMMAND_NAME = "publish";

    private String pubChannel;

    private String message;

    public PublishCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
                          String pubChannel, String message) {
        super(clientPool, scheduled);
        this.pubChannel = pubChannel;
        this.message = message;
    }

    public PublishCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli, String pubChannel, String message) {
        super(clientPool, scheduled, commandTimeoutMilli);
        this.pubChannel = pubChannel;
        this.message = message;
    }

    @Override
    protected Object format(Object payload) {
        return payload;
    }

    @Override
    public ByteBuf getRequest() {
        return new RequestStringParser(getName(), pubChannel, message).format();
    }

    @Override
    public String getName() {
        return PUBLISH_COMMAND_NAME;
    }
}
