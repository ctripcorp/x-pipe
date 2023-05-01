package com.ctrip.xpipe.redis.keeper.applier.xsync;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.Xsync;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultXsync;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.utils.CloseState;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 17:20
 */
public class DefaultXsyncReplication extends StubbornNetworkCommunication implements ApplierXsyncReplication {

    @InstanceDependency
    public ApplierCommandDispatcher dispatcher;

    @InstanceDependency
    public AtomicReference<GtidSet> gtid_executed;

    @InstanceDependency
    public XpipeNettyClientKeyedObjectPool pool;

    @InstanceDependency
    public ScheduledExecutorService scheduled;

    private Endpoint endpoint;

    private GtidSet gtidSetExcluded;

    private Xsync currentXsync;

    private CloseState closeState = new CloseState();

    @Override
    protected void doStart() throws Exception {
        //do nothing
        //call replication.connect(Endpoint, Object...) to trigger/update xsync replication
    }

    @Override
    protected void doStop() throws Exception {
        this.closeState.setClosing();

        disconnect();

        synchronized (closeState) {
            this.closeState.setClosed();
        }
    }

    //all below are definition to invoke StubbornNetworkCommunication functionality
    //see StubbornNetworkCommunication API: connect(Endpoint, Object...), disconnect()

    @Override
    public Command<Object> connectCommand() throws Exception {

        /* simple implementation */

        SimpleObjectPool<NettyClient> objectPool = pool.getKeyPool(endpoint);

        /* TODO connect with master first, then handler notify to xsyncReplication with nettyClient, then use fixedObjectPool to create xsync */
        Xsync xsync = new DefaultXsync(objectPool, gtidSetExcluded, null, scheduled);
        xsync.addXsyncObserver(dispatcher);

        this.currentXsync = xsync;
        return xsync;
    }

    @Override
    public void doDisconnect() throws Exception {
        if (currentXsync != null) {
            currentXsync.close();
            currentXsync = null;
        }
    }

    @Override
    public Endpoint endpoint() {
        return endpoint;
    }

    @Override
    public void initState(Endpoint endpoint, Object... states) {
        this.endpoint = endpoint;
        if (states.length > 0) {
            this.gtidSetExcluded = (GtidSet) states[0];
        }
    }

    @Override
    protected void refreshStateWhenReconnect() {
        GtidSet gtidReceived = dispatcher.getGtidReceived();
        if (gtidReceived != null) {
            this.gtidSetExcluded = gtidReceived.union(this.gtidSetExcluded);
        }
    }

    @Override
    public ScheduledExecutorService scheduled() {
        return scheduled;
    }

    @Override
    public boolean closed() {
        return closeState.isClosed();
    }
}
