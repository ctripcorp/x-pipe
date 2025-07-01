package com.ctrip.xpipe.redis.keeper.applier.sync;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.Sync;
import com.ctrip.xpipe.redis.core.protocal.SyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultXsync;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 17:20
 */
public class DefaultXsyncReplication extends AbstractSyncReplication implements ApplierSyncReplication, SyncObserver {

    private static final Logger logger = LoggerFactory.getLogger(DefaultXsyncReplication.class);

    @InstanceDependency
    public AtomicReference<GtidSet> gtid_executed;

    private GtidSet gtidSetExcluded;
    public DefaultXsyncReplication(ApplierServer applierServer) {
        this.applierServer = applierServer;
    }


    //all below are definition to invoke StubbornNetworkCommunication functionality
    //see StubbornNetworkCommunication API: connect(Endpoint, Object...), disconnect()

    @Override
    public Command<Object> connectCommand() throws Exception {

        /* simple implementation */

        SimpleObjectPool<NettyClient> objectPool = pool.getKeyPool(endpoint);

        /* TODO connect with master first, then handler notify to xsyncReplication with nettyClient, then use fixedObjectPool to create xsync */
        Sync sync = new DefaultXsync(objectPool, gtidSetExcluded, null, scheduled, applierServer.getListeningPort());
        sync.addSyncObserver(dispatcher);
        sync.addSyncObserver(this);

        this.currentSync = sync;
        return sync;
    }

    @Override
    protected void refreshStateWhenReconnect() {
        cancelReplConf();
        GtidSet gtidReceived = dispatcher.getGtidReceived();
        if (gtidReceived != null) {
            this.gtidSetExcluded = gtidReceived.union(this.gtidSetExcluded);
        }
    }

    @Override
    public void initState(Endpoint endpoint, Object... states) {
        this.endpoint = endpoint;
        if (states.length > 0) {
            this.gtidSetExcluded = (GtidSet) states[0];
        }
        cancelReplConf();
    }
}
