package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author chen.zhu
 * <p>
 * Apr 04, 2018
 */
public class SubscribeCommand extends AbstractSubscribe {

    private static final Logger logger = LoggerFactory.getLogger(SubscribeCommand.class);

    @VisibleForTesting
    public SubscribeCommand(String host, int port, ScheduledExecutorService scheduled, String... channel) {
        super(host, port, scheduled, MESSAGE_TYPE.MESSAGE, channel);
    }

    @VisibleForTesting
    public SubscribeCommand(Endpoint endpoint, ScheduledExecutorService scheduled, String... channel) {
        super(endpoint.getHost(), endpoint.getPort(), scheduled, MESSAGE_TYPE.MESSAGE, channel);
    }

    public SubscribeCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, String... channel) {
        super(clientPool, scheduled, MESSAGE_TYPE.MESSAGE, channel);
    }

    public SubscribeCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli, String... channel) {
        super(clientPool, scheduled, commandTimeoutMilli, MESSAGE_TYPE.MESSAGE, channel);
    }

    @Override
    public String getName() {
        return "subscribe";
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
