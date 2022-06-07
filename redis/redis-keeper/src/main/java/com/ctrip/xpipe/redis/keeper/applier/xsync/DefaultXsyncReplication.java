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

import java.util.concurrent.ScheduledExecutorService;

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
    public GtidSet gtidSet;

    @InstanceDependency
    public Endpoint endpoint;

    @InstanceDependency
    public XpipeNettyClientKeyedObjectPool pool;

    @InstanceDependency
    public ScheduledExecutorService scheduled;

    @Override
    protected void doStart() throws Exception {
        connect(endpoint);
    }

    //all below are definition to invoke StubbornNetworkCommunication functionality
    //see StubbornNetworkCommunication API: connect(Endpoint), disconnect(), scheduleReconnect()

    public Xsync xsync;

    @Override
    public Command<Object> connectCommand() throws Exception {

        /* simple implementation */

        SimpleObjectPool<NettyClient> objectPool = pool.getKeyPool(endpoint);
        Xsync xsync = new DefaultXsync(objectPool, gtidSet, null, scheduled);
        xsync.addXsyncObserver(dispatcher);
        return xsync;
    }

    @Override
    public void doDisconnect() throws Exception {

    }

    @Override
    public boolean isConnected() {
        return false;
    }

    @Override
    public Endpoint endpoint() {
        return endpoint;
    }

    @Override
    public void setHostPort(Endpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public ScheduledExecutorService scheduled() {
        return scheduled;
    }

    @Override
    public long reconnectDelayMillis() {

//        long delayMillis = Math.max(0L,
//                reconnectDelayMillis() - (System.currentTimeMillis() - lastConnectedTimeMillis()));

        return 2000;
    }

    @Override
    public boolean closed() {
        return false;
    }
}
