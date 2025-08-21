package com.ctrip.xpipe.redis.keeper.applier.sync;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.core.protocal.ApplierSyncObserver;
import com.ctrip.xpipe.redis.core.protocal.CAPA;
import com.ctrip.xpipe.redis.core.protocal.GapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.cmd.ApplierGapAllowSync;
import com.ctrip.xpipe.redis.core.protocal.cmd.Replconf;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.keeper.applier.ApplierServer;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.utils.CloseState;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author hailu
 * @date 2024/5/10 9:53
 */
public abstract class AbstractSyncReplication extends StubbornNetworkCommunication implements ApplierSyncReplication, ApplierSyncObserver {
    private static final Logger logger = LoggerFactory.getLogger(AbstractSyncReplication.class);
    @InstanceDependency
    public ApplierCommandDispatcher dispatcher;

    @InstanceDependency
    public XpipeNettyClientKeyedObjectPool pool;

    @InstanceDependency
    public ScheduledExecutorService scheduled;

    @InstanceDependency
    public AtomicLong offsetRecorder;

    @InstanceDependency
    public RdbParser<?> rdbParser;

    protected ApplierServer applierServer;

    protected Endpoint endpoint;

    protected GapAllowedSync currentSync;

    protected Future<?> replConfFuture;

    protected CloseState closeState = new CloseState();

    private boolean onContinueCommand = false;

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
            currentSync.future().cancel(true);
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

    protected void scheduleReplconf() {

        if (logger.isInfoEnabled()) {
            logger.info("[scheduleReplconf]" + this);
        }

        cancelReplConf();

        replConfFuture = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {

            @Override
            protected void doRun() throws Exception {
                try {
                    if(endpoint == null) {
                        return;
                    }
                    if(currentSync != null) {
                        getLogger().debug("[run][send ack]{}", ((ApplierGapAllowSync) currentSync).toString());
                    }

                    Command<Object> command = null;
                    // 定时发送 repl 避免处理 rdb 时间太长导致被 idle 检测给干掉了
                    if(onContinueCommand) {
                        command = new Replconf(pool.getKeyPool(endpoint), Replconf.ReplConfType.ACK, scheduled, String.valueOf(offsetRecorder.get()));
                    } else {
                        command = new Replconf(pool.getKeyPool(endpoint), Replconf.ReplConfType.CAPA, scheduled, CAPA.EOF.toString());
                    }
                    command.execute();
                } catch (Throwable t) {
                    logger.error("[scheduleReplconf] sync {} error", currentSync, t);
                }

            }

        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void doOnFullSync(String replId, long replOffset) {
        this.rdbParser.reset();
    }

    @Override
    public void doOnXFullSync(GtidSet lost, long replOffset) {
        this.rdbParser.reset();
    }

    @Override
    public void doOnXContinue(GtidSet lost, long replOffset) {
        scheduleReplconf();
        onContinueCommand = true;
    }

    @Override
    public void doOnContinue(String newReplId) {
        scheduleReplconf();
        onContinueCommand = true;
    }

    @Override
    public void doOnAppendCommand(ByteBuf byteBuf) {
    }

    @Override
    public void endReadRdb() {
        scheduleReplconf();
        onContinueCommand = true;
    }

    @Override
    public void protoChange() {
    }
}
