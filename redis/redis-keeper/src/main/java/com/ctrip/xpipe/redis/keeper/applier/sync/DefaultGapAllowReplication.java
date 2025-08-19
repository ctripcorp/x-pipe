package com.ctrip.xpipe.redis.keeper.applier.sync;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.cmd.ApplierGapAllowSync;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author hailu
 * @date 2024/4/29 19:25
 */
public class DefaultGapAllowReplication extends AbstractSyncReplication implements ApplierSyncReplication {

    private static final Logger logger = LoggerFactory.getLogger(DefaultGapAllowReplication.class);

    @InstanceDependency
    public AtomicReference<String> replId;

    @InstanceDependency
    public AtomicReference<GtidSet> execGtidSet;

    @InstanceDependency
    public AtomicReference<GtidSet> lostGtidSet;

    @InstanceDependency
    public AtomicReference<GtidSet> startGtidSet;

    @InstanceDependency
    public AtomicReference<String> replProto;


    public DefaultGapAllowReplication(ApplierServer applierServer) {
        this.applierServer = applierServer;
    }


    //all below are definition to invoke StubbornNetworkCommunication functionality
    //see StubbornNetworkCommunication API: connect(Endpoint, Object...), disconnect()
    @Override
    public Command<Object> connectCommand() throws Exception {
        SimpleObjectPool<NettyClient> objectPool = pool.getKeyPool(endpoint);
        scheduleReplconf();
        rdbParser.registerListener(dispatcher);
        if(replProto == null) {
            replProto = new AtomicReference<>();
        }
        ApplierGapAllowSync sync = new ApplierGapAllowSync(objectPool, scheduled, replId, execGtidSet, offsetRecorder, rdbParser,
                startGtidSet, lostGtidSet, replProto);
        sync.addObserver(dispatcher);
        sync.addObserver(this);
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
