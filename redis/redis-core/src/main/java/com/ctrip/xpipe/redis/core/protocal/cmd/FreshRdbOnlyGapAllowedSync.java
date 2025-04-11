package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.tuple.Pair;

import java.util.concurrent.ScheduledExecutorService;

public class FreshRdbOnlyGapAllowedSync extends RdbOnlyGapAllowedSync {

    public FreshRdbOnlyGapAllowedSync(SimpleObjectPool<NettyClient> clientPool, ReplicationStore store, ScheduledExecutorService scheduled) {
        super(clientPool, store, scheduled);
    }

    @Override
    public SyncRequest getSyncRequest() {
        //TODO confirm how to force freshrdb sync
        PsyncRequest psync = new PsyncRequest();
        psync.setReplId("?");
        psync.setReplOff(KEEPER_FRESH_RDB_SYNC_OFFSET);
        return psync;
    }

}
