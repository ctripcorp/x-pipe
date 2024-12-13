package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.ScheduledExecutorService;

public class FreshRdbOnlyPsync extends RdbOnlyPsync {

    public FreshRdbOnlyPsync(SimpleObjectPool<NettyClient> clientPool, ReplicationStore store, ScheduledExecutorService scheduled) {
        super(clientPool, store, scheduled);
    }

    @Override
    protected Pair<String, Long> getRequestMasterInfo() {
        // psync ? -3
        return new Pair<>("?", KEEPER_FRESH_RDB_SYNC_OFFSET);
    }

}
