package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

public class PsubscribeCommand extends AbstractSubscribe{

    private static final Logger logger = LoggerFactory.getLogger(PsubscribeCommand.class);

    protected PsubscribeCommand(String host, int port, ScheduledExecutorService scheduled, MESSAGE_TYPE messageType, String... subscribeChannel) {
        super(host, port, scheduled, messageType, subscribeChannel);
    }

    public PsubscribeCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli, String... channel) {
        super(clientPool, scheduled, commandTimeoutMilli, MESSAGE_TYPE.PMESSAGE, channel);
    }


    @Override
    public String getName() {
        return PSUBSCRIBE;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
