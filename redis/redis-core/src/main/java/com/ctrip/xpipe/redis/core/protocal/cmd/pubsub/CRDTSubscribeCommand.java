package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

public class CRDTSubscribeCommand extends AbstractSubscribe {

    private static final Logger logger = LoggerFactory.getLogger(CRDTSubscribeCommand.class);

    private static final String CRDT_SUBSCRIBE_COMMAND_NAME = "crdt.subscribe";

    public CRDTSubscribeCommand(String host, int port, ScheduledExecutorService scheduled, String... channel) {
        super(host, port, scheduled, MESSAGE_TYPE.CRDT_MESSAGE, channel);
    }

    public CRDTSubscribeCommand(Endpoint endpoint, ScheduledExecutorService scheduled, String... channel) {
        super(endpoint.getHost(), endpoint.getPort(), scheduled, MESSAGE_TYPE.CRDT_MESSAGE, channel);
    }

    public CRDTSubscribeCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, String... channel) {
        super(clientPool, scheduled, MESSAGE_TYPE.CRDT_MESSAGE, channel);
    }

    public CRDTSubscribeCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,int commandTimeoutMilli,  String... channel) {
        super(clientPool, scheduled, commandTimeoutMilli, MESSAGE_TYPE.CRDT_MESSAGE, channel);
    }

    @Override
    public String getName() {
        return CRDT_SUBSCRIBE_COMMAND_NAME;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
