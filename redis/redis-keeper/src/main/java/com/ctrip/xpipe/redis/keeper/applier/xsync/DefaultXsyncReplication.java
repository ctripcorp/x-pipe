package com.ctrip.xpipe.redis.keeper.applier.xsync;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.Xsync;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultXsync;
import com.ctrip.xpipe.redis.keeper.applier.AbstractInstanceComponent;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.utils.CloseState;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 17:20
 */
public class DefaultXsyncReplication
        extends AbstractInstanceComponent implements ApplierXsyncReplication, StubbornNetworkCommunication {

    @InstanceDependency
    public ApplierCommandDispatcher dispatcher;

    @InstanceDependency
    public AtomicReference<GtidSet> gtid_executed;

    @InstanceDependency
    public XpipeNettyClientKeyedObjectPool pool;

    @InstanceDependency
    public ScheduledExecutorService scheduled;

    private Xsync currentXsync;

    private GtidSet gtidSetExcluded;

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

    public Endpoint endpoint;

    public Xsync xsync;

    @Override
    public Command<Object> connectCommand() throws Exception {

        /* simple implementation */

        SimpleObjectPool<NettyClient> objectPool = pool.getKeyPool(endpoint);
        Xsync xsync = new DefaultXsync(objectPool, gtidSetExcluded, null, scheduled);
        xsync.addXsyncObserver(dispatcher);

        this.currentXsync = xsync;
        return xsync;
    }

    @Override
    public void doDisconnect() throws Exception {
        if (isConnected()) {
            currentXsync.close();
        }
    }

    @Override
    public boolean isConnected() {
        return currentXsync != null;
    }

    @Override
    public Endpoint endpoint() {
        return endpoint;
    }

    @Override
    public void initState(Endpoint endpoint, Object... states) {
        this.endpoint = endpoint;
        this.gtidSetExcluded = (GtidSet) states[0];
    }

    @Override
    public ScheduledExecutorService scheduled() {
        return scheduled;
    }

    @Override
    public long reconnectDelayMillis() {
        return 2000;
    }

    @Override
    public boolean closed() {
        return closeState.isClosed();
    }
}
