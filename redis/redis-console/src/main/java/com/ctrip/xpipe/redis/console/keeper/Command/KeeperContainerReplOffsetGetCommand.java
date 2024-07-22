package com.ctrip.xpipe.redis.console.keeper.command;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.cmd.InfoResultExtractor;

import java.util.concurrent.ScheduledExecutorService;

public class KeeperContainerReplOffsetGetCommand<V> extends AbstractKeeperCommand<Object>{

    private Endpoint keeper;

    public KeeperContainerReplOffsetGetCommand(XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled, Endpoint keeper) {
        super(keyedObjectPool, scheduled);
        this.keeper = keeper;
    }

    @Override
    public String getName() {
        return "KeeperContainerReplOffsetGetCommand";
    }

    @Override
    protected void doExecute() throws Throwable {
        this.future().setSuccess(new InfoResultExtractor(generateInfoReplicationCommand(keeper).execute().get()).getMasterReplOffset());
    }

    @Override
    protected void doReset() {

    }
}
