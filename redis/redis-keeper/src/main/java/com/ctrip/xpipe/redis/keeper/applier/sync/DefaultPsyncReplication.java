package com.ctrip.xpipe.redis.keeper.applier.sync;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.Sync;
import com.ctrip.xpipe.redis.core.protocal.SyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.ApplierPsync;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author hailu
 * @date 2024/4/29 19:25
 */
public class DefaultPsyncReplication extends AbstractSyncReplication implements ApplierSyncReplication, SyncObserver {

    private static final Logger logger = LoggerFactory.getLogger(DefaultPsyncReplication.class);

    @InstanceDependency
    public AtomicReference<String> replId;

    public DefaultPsyncReplication(ApplierServer applierServer) {
        this.applierServer = applierServer;
    }


    //all below are definition to invoke StubbornNetworkCommunication functionality
    //see StubbornNetworkCommunication API: connect(Endpoint, Object...), disconnect()
    @Override
    public Command<Object> connectCommand() throws Exception {
        SimpleObjectPool<NettyClient> objectPool = pool.getKeyPool(endpoint);
        Sync sync = new ApplierPsync(objectPool, replId, offsetRecorder, scheduled, applierServer.getListeningPort());
        sync.addSyncObserver(dispatcher);
        sync.addSyncObserver(this);

        this.currentSync = sync;
        return sync;
    }


    @Override
    protected void refreshStateWhenReconnect() {
        cancelReplConf();
    }

    @Override
    public void initState(Endpoint endpoint, Object... states) {
        this.endpoint = endpoint;
        cancelReplConf();
    }
}
