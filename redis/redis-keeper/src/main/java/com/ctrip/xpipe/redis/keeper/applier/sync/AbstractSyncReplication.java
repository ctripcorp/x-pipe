package com.ctrip.xpipe.redis.keeper.applier.sync;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.pool.FixedObjectPool;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.Sync;
import com.ctrip.xpipe.redis.core.protocal.SyncObserver;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractSync;
import com.ctrip.xpipe.redis.core.protocal.cmd.ApplierPsync;
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

/**
 * @author hailu
 * @date 2024/5/10 9:53
 */
public abstract class AbstractSyncReplication extends StubbornNetworkCommunication implements ApplierSyncReplication, SyncObserver {
    private static final Logger logger = LoggerFactory.getLogger(DefaultPsyncReplication.class);
    @InstanceDependency
    public ApplierCommandDispatcher dispatcher;

    @InstanceDependency
    public XpipeNettyClientKeyedObjectPool pool;

    @InstanceDependency
    public ScheduledExecutorService scheduled;

    @InstanceDependency
    public AtomicLong offsetRecorder;

    protected ApplierServer applierServer;

    protected Endpoint endpoint;

    protected Sync currentSync;

    protected Future<?> replConfFuture;

    protected CloseState closeState = new CloseState();

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


    @Override
    public void doDisconnect() throws Exception {
        cancelReplConf();

        if (currentSync != null) {
            currentSync.close();
            currentSync = null;
        }
    }

    @Override
    public Endpoint endpoint() {
        return endpoint;
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
    protected void scheduleReplconf() {

        if (logger.isInfoEnabled()) {
            logger.info("[scheduleReplconf]" + this);
        }

        cancelReplConf();

        replConfFuture = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                try {

                    getLogger().debug("[run][send ack]{}", ((AbstractSync) currentSync).getNettyClient().channel());

                    Command<Object> command = new Replconf(new FixedObjectPool<>(((AbstractSync) currentSync).getNettyClient()), Replconf.ReplConfType.ACK, scheduled, String.valueOf(offsetRecorder.get()));
                    command.execute();
                } catch (Throwable t) {
                    logger.error("[scheduleReplconf] sync {} error", currentSync, t);
                }

            }

        }, 0, 1000, TimeUnit.MILLISECONDS);
    }
}
