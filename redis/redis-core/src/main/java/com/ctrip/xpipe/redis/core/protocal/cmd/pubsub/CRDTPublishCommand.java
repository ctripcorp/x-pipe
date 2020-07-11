package com.ctrip.xpipe.redis.core.protocal.cmd.pubsub;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

public class CRDTPublishCommand extends PublishCommand {

    private static final Logger logger = LoggerFactory.getLogger(CRDTPublishCommand.class);

    private static final String CRDT_PUBLISH_COMMAND_NAME = "crdtpublish";

    public CRDTPublishCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
                          String pubChannel, String message) {
        super(clientPool, scheduled, pubChannel, message);
    }

    public CRDTPublishCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli, String pubChannel, String message) {
        super(clientPool, scheduled, commandTimeoutMilli, pubChannel, message);
    }

    @Override
    public String getName() {
        return CRDT_PUBLISH_COMMAND_NAME;
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }

}
