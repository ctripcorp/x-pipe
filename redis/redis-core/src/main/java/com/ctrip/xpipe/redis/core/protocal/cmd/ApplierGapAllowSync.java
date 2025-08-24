package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.pool.ReturnObjectException;
import com.ctrip.xpipe.redis.core.protocal.ApplierSyncObserver;
import com.ctrip.xpipe.redis.core.protocal.protocal.RdbBulkStringParser;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ApplierGapAllowSync extends AbstractGapAllowedSync {

    private static final Logger log = LoggerFactory.getLogger(ApplierGapAllowSync.class);
    private AtomicReference<String> replProto;
    private AtomicReference<String> replId;
    private AtomicLong offsetRecorder;
    private AtomicReference<GtidSet> startGtidSet;
    private AtomicReference<GtidSet> lostGtidSet;
    private AtomicReference<GtidSet> execGtidSet;
    private RdbParser<?> rdbParser;

    List<ApplierSyncObserver> observers;

    private NettyClient nettyClient;

    private int listenPort;


    public ApplierGapAllowSync(SimpleObjectPool<NettyClient> objectPool, ScheduledExecutorService scheduled, AtomicReference<String> replId,
                               AtomicReference<GtidSet> execGtidSet, AtomicLong offsetRecorder, RdbParser<?> rdbParser,
                               AtomicReference<GtidSet> startGtidSet, AtomicReference<GtidSet> lostGtidSet,
                               AtomicReference<String> proto, int listenPort) {
        super(objectPool, true, scheduled);
        this.replId = replId;
        this.execGtidSet = execGtidSet;
        this.offsetRecorder = offsetRecorder;
        this.rdbParser = rdbParser;
        observers = new ArrayList<>();
        this.startGtidSet = startGtidSet;
        this.lostGtidSet = lostGtidSet;
        this.replProto = proto;
        this.listenPort = listenPort;
    }

    public void addObserver(ApplierSyncObserver observer) {
        this.observers.add(observer);
    }

    @Override
    protected void failReadRdb(Throwable throwable) {

    }

    @Override
    protected void endReadRdb() {
        super.endReadRdb();
        for (ApplierSyncObserver observer : observers) {
            observer.endReadRdb();
        }
    }

    @Override
    public SyncRequest getSyncRequest() {
        if(replProto.get() == null || StringUtil.trimEquals(replProto.get(), PSYNC)) {
            return getPsyncRequest();
        } else {
            return getXsyncRequest();
        }
    }

    private PsyncRequest getPsyncRequest() {
        PsyncRequest request = new PsyncRequest();
        long offsetRequest = "?".equalsIgnoreCase(replId.get()) ? -1 : offsetRecorder.get() + 1;
        request.setReplId(replId.get());
        request.setReplOff(offsetRequest);
        return request;
    }

    private XsyncRequest getXsyncRequest() {
        XsyncRequest request = new XsyncRequest();
        request.setMaxGap(0);
        request.setUuidIntrested("*");
        GtidSet gtidSet = startGtidSet.get().union(lostGtidSet.get()).union(execGtidSet.get());
        request.setGtidSet(gtidSet);
        return request;
    }

    @Override
    protected void appendCommands(ByteBuf byteBuf) throws IOException {
        for(ApplierSyncObserver observer : observers) {
            observer.doOnAppendCommand(byteBuf);
        }
    }

    @Override
    protected RdbBulkStringParser createRdbReader() {
        ByteArrayOutputStreamPayload rdb = new ByteArrayOutputStreamPayload();
        return new RdbBulkStringParser(rdb, rdbParser);
    }



    @Override
    protected void doOnSwitchToXsync() throws IOException {
        super.doOnSwitchToXsync();
        if(replProto.get() != null) {
            notifyProtoChange();
        }
        replProto.set(XSYNC);
    }

    private void notifyProtoChange() {
        for(ApplierSyncObserver observer : observers) {
            observer.protoChange();
        }
    }

    @Override
    protected void doOnSwitchToPsync() throws IOException {
        super.doOnSwitchToPsync();
        if(replProto.get() != null) {
            notifyProtoChange();
        }
        replProto.set(PSYNC);
    }
    @Override
    protected void doOnFullSync() throws IOException {
        super.doOnFullSync();
        for(ApplierSyncObserver observer : observers) {
            observer.doOnFullSync(syncReply.getReplId(), syncReply.getReplOff());
        }
        if(replProto.get() == null) {
            replProto.set(PSYNC);
        }
    }

    @Override
    protected void doOnXFullSync() throws IOException {
        super.doOnXFullSync();
        XFullresyncReply repl =  (XFullresyncReply)this.syncReply;
        for (ApplierSyncObserver observer : observers) {
            observer.doOnXFullSync(repl.gtidLost, syncReply.getReplOff());
        }
        if(replProto.get() == null) {
            replProto.set(XSYNC);
        }

    }

    @Override
    protected void doOnContinue(String newReplId) {
        for (ApplierSyncObserver observer : observers) {
            observer.doOnContinue(newReplId);
        }
    }


    @Override
    protected void doOnXContinue() {
        try {
            super.doOnXContinue();
        } catch (IOException e) {
            log.error("[doOnXContinue]", e);
        }
        XContinueReply repl =  (XContinueReply) this.syncReply;
        for (ApplierSyncObserver observer : observers) {
            observer.doOnXContinue(repl.getGtidLost(), repl.getReplOff());
        }
    }

    @Override
    protected void afterCommandExecute(NettyClient nettyClient) {
        // temporary solution, handle channel evicted by channel pool

        this.nettyClient = nettyClient;

        replConfListeningPort().execute().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if (!commandFuture.isSuccess()) {
                    close();
                }
            }
        });

        nettyClient.channel().closeFuture().addListener(closeFuture -> {
            if (!future().isDone()) {
                future().setFailure(new XpipeRuntimeException("channel closed"));
            }

            try {
                getClientPool().returnObject(nettyClient);
            } catch (ReturnObjectException e) {
                getLogger().error("[afterCommandExecute]", e);
            }
        });

        future().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if (nettyClient.channel().isOpen()) {
                    nettyClient.channel().close();
                }

                if (isPoolCreated()) {
                    LifecycleHelper.stopIfPossible(getClientPool());
                    LifecycleHelper.disposeIfPossible(getClientPool());
                }
            }
        });
    }

    protected Command<Object> replConfListeningPort() {
        log.info("send replof listen port: {}", listenPort);
        return new Replconf(getClientPool(), Replconf.ReplConfType.LISTENING_PORT, scheduled,
                String.valueOf(listenPort));
    }

    @Override
    public void close() {
        super.close();
        if (nettyClient != null && nettyClient.channel() != null) {
            nettyClient.channel().close();
        }
    }

}
