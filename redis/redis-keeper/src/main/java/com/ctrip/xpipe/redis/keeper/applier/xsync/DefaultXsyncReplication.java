package com.ctrip.xpipe.redis.keeper.applier.xsync;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.Xsync;
import com.ctrip.xpipe.redis.core.protocal.XsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.DefaultXsync;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.utils.CloseState;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Slight
 * <p>
 * Jun 01, 2022 17:20
 */
public class DefaultXsyncReplication extends StubbornNetworkCommunication implements ApplierXsyncReplication, XsyncObserver {

    private static final Logger logger = LoggerFactory.getLogger(DefaultXsyncReplication.class);

    @InstanceDependency
    public ApplierCommandDispatcher dispatcher;

    @InstanceDependency
    public AtomicReference<GtidSet> gtid_executed;

    @InstanceDependency
    public XpipeNettyClientKeyedObjectPool pool;

    @InstanceDependency
    public ScheduledExecutorService scheduled;

    @InstanceDependency
    public AtomicLong offsetRecorder;

    private ApplierServer applierServer;

    private Endpoint endpoint;

    private GtidSet gtidSetExcluded;

    private Xsync currentXsync;

    private Future<?> replConfFuture;

    private CloseState closeState = new CloseState();

    public DefaultXsyncReplication(ApplierServer applierServer) {
        this.applierServer = applierServer;
    }

    @Override
    protected void doStart() throws Exception {
        //do nothing
        //call replication.connect(Endpoint, Object...) to trigger/update xsync replication
    }

    @Override
    protected void doStop() throws Exception {
        this.closeState.setClosing();

        cancelReplConf();
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
        Xsync xsync = new DefaultXsync(objectPool, gtidSetExcluded, null, scheduled, applierServer.getListeningPort());
        xsync.addXsyncObserver(dispatcher);
        xsync.addXsyncObserver(this);

        this.currentXsync = xsync;
        return xsync;
    }

    @Override
    public void doDisconnect() throws Exception {
        cancelReplConf();

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
        offsetRecorder.set(0);
        cancelReplConf();
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

    // only use for full sync (source is non-gtid redis, dest is gtid redis, full sync)
    private void scheduleReplconf() {

        if (logger.isInfoEnabled()) {
            logger.info("[scheduleReplconf]" + this);
        }

        cancelReplConf();

        replConfFuture = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                try {

                    getLogger().debug("[run][send ack]{}", ((DefaultXsync)currentXsync).getNettyClient().channel());

                    Command<Object> command = new Replconf(new FixedObjectPool<>(((DefaultXsync)currentXsync).getNettyClient()), Replconf.ReplConfType.ACK, scheduled, String.valueOf(offsetRecorder.get()));
                    command.execute();
                }catch (Throwable t){
                    logger.error("[scheduleReplconf] xsync {} error", currentXsync, t);
                }

            }

        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    protected void cancelReplConf() {

        if (replConfFuture != null) {
            replConfFuture.cancel(true);
            replConfFuture = null;
        }
    }

    @Override
    public void onFullSync(GtidSet rdbGtidSet, long rdbOffset) {

    }

    @Override
    public void beginReadRdb(EofType eofType, GtidSet rdbGtidSet, long rdbOffset) {

    }

    @Override
    public void onRdbData(ByteBuf rdbData) {

    }

    @Override
    public void endReadRdb(EofType eofType, GtidSet rdbGtidSet, long rdbOffset) {
        offsetRecorder.set(rdbOffset);
        scheduleReplconf();
    }

    @Override
    public void onContinue(GtidSet gtidSetExcluded, long continueOffset) {
        offsetRecorder.set(continueOffset);
        scheduleReplconf();
    }

    @Override
    public void onCommand(long commandOffset, Object[] rawCmdArgs) {

    }
}
