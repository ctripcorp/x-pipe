package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;

import java.util.concurrent.ScheduledExecutorService;

public class CRDTInfoCommand extends InfoCommand {

    public CRDTInfoCommand(SimpleObjectPool<NettyClient> clientPool, String args, ScheduledExecutorService scheduled) {
        super(clientPool, args, scheduled);
    }

    public CRDTInfoCommand(SimpleObjectPool<NettyClient> clientPool, INFO_TYPE infoType, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
        super(clientPool, infoType, scheduled, commandTimeoutMilli);
    }

    public CRDTInfoCommand(SimpleObjectPool<NettyClient> clientPool, INFO_TYPE infoType, ScheduledExecutorService scheduled) {
        super(clientPool, infoType, scheduled);
    }

    public CRDTInfoCommand(SimpleObjectPool<NettyClient> clientPool, String args, ScheduledExecutorService scheduled,
                       int commandTimeoutMilli) {
        super(clientPool, args, scheduled, commandTimeoutMilli);
    }

    @Override
    public String getName() {
        return "crdt.info";
    }

}
