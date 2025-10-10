package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.pool.ReturnObjectException;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.RedisClientProtocol;
import com.ctrip.xpipe.redis.core.protocal.Sync;
import com.ctrip.xpipe.redis.core.protocal.SyncObserver;
import com.ctrip.xpipe.redis.core.protocal.protocal.AbstractBulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.RdbBulkStringParser;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static com.ctrip.xpipe.redis.core.protocal.Sync.SYNC_STATE.READING_COMMANDS;

/**
 * @author hailu
 * @date 2024/5/10 16:37
 */
public abstract class AbstractSync extends AbstractRedisCommand<Object> implements Sync, AbstractBulkStringParser.BulkStringParserListener, InOutPayload {

    protected EofType eofType;

    protected GtidSet gitdSetExcluded;

    protected GtidSet rdbDataGtidSet = new GtidSet(GtidSet.EMPTY_GTIDSET);

    protected long rdbOffset;

    protected NettyClient nettyClient;

    protected int listeningPort;

    protected AtomicLong currentCommandOffset;

    protected RdbBulkStringParser rdbReader;

    protected List<SyncObserver> observers = new LinkedList<>();

    protected SYNC_STATE syncState = SYNC_STATE.SYNC_COMMAND_WAIT_RESPONSE;

    protected abstract void handleRedisResponse(Channel channel, String sync) throws IOException;

    public AbstractSync(String host, int port, ScheduledExecutorService scheduled){
        super(host, port, scheduled);
    }

    public AbstractSync(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
    }

    public NettyClient getNettyClient() {
        return nettyClient;
    }

    @Override
    public void addSyncObserver(SyncObserver observer) {
        this.observers.add(observer);
    }

    @Override
    public void close() {
        if (nettyClient != null && nettyClient.channel() != null) {
            nettyClient.channel().close();
        }
    }

    @Override
    protected Object doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception {

        while (byteBuf.readableBytes() > 0) {
            getLogger().debug("[doReceiveResponse][{}][{}]", syncState, this);

            switch (syncState) {

                case SYNC_COMMAND_WAIT_RESPONSE:
                    Object response = super.doReceiveResponse(channel, byteBuf);
                    if (response != null) {
                        handleRedisResponse(channel, (String) response);
                    }
                    break;

                case READING_RDB:
                    if (null == rdbReader) {
                        rdbReader = new RdbBulkStringParser(this);
                        rdbReader.setBulkStringParserListener(this);
                    }

                    RedisClientProtocol<InOutPayload> rdbPayload = null;
                    try {
                        rdbPayload = rdbReader.read(byteBuf);
                    } catch (Exception e) {
                        getLogger().error("[doReceiveResponse][READING_RDB][fail] {}", this, e);
                        throw new RuntimeException(e);
                    }
                    getLogger().debug("[doReceiveResponse][READING_RDB][rdbPayload] {}", rdbPayload);
                    if (rdbPayload != null) {
                        syncState = READING_COMMANDS;
                        getLogger().info("[doReceiveResponse][READING_RDB][end] {}", this);
                        endReadRdb();
                    }
                    break;

                case READING_COMMANDS:
                    int prevIndex = byteBuf.readerIndex();
                    Object cmdPayload = super.doReceiveResponse(channel, byteBuf);
                    currentCommandOffset.addAndGet(byteBuf.readerIndex() - prevIndex);
                    if (cmdPayload instanceof Object[]) {
                        doOnCommand(currentCommandOffset.getAndSet(0), (Object[]) cmdPayload);
                    } else if (null != cmdPayload) {
                        getLogger().info("[doReceiveResponse][{}][unknown payload] {}, {}", READING_COMMANDS, this, cmdPayload);
                        throw new RedisRuntimeException("unknown payload:" + cmdPayload);
                    }

                    break;
                default:
                    throw new IllegalStateException("unknown state:" + syncState);
            }
        }

        return null;
    }


    // reset state for next query parse
    protected void resetClient() {
        if (redisProtocolParser != null) {
            redisProtocolParser.reset();
        }

    }


    protected void doOnFullSync() {
        getLogger().debug("[doOnFullSync] {}", this);
        for (SyncObserver observer : observers) {
            try {
                observer.onFullSync(rdbDataGtidSet, rdbOffset);
            } catch (Throwable th) {
                getLogger().error("[doOnFullSync][fail] {}", observer, th);
            }
        }
        resetClient();
    }

    protected void doOnContinue(long continueOffset) {
        getLogger().debug("[doOnContinue] {}", this);
        for (SyncObserver observer : observers) {
            try {
                observer.onContinue(gitdSetExcluded, continueOffset);
            } catch (Throwable th) {
                getLogger().error("[doOnContinue][fail] {}", observer, th);
            }
        }
        resetClient();
    }

    protected void doOnCommand(long commandOffset, Object[] rawCmdArgs) {
        getLogger().debug("[doOnCommand] {}", this);
        for (SyncObserver observer : observers) {
            try {
                observer.onCommand(commandOffset, rawCmdArgs);
            } catch (Throwable th) {
                getLogger().error("[doOnCommand][fail] {}", observer, th);
            }
        }
        resetClient();
    }

    protected void beginReadRdb() {
        getLogger().debug("[beginReadRdb] {}", this);
        for (SyncObserver observer : observers) {
            try {
                observer.beginReadRdb(eofType, rdbDataGtidSet, rdbOffset);
            } catch (Throwable th) {
                getLogger().error("[beginReadRdb][fail] {}", observer, th);
            }
        }
    }

    protected void onRdbData(ByteBuf byteBuf) {
        getLogger().debug("[notifyRdbData] {}", this);
        for (SyncObserver observer : observers) {
            try {
                observer.onRdbData(byteBuf.slice());
            } catch (Throwable th) {
                getLogger().error("[notifyRdbData][fail] {}", observer, th);
                throw th;
            }
        }
    }

    protected void endReadRdb() {
        getLogger().debug("[endReadRdb] {}", this);

        for (SyncObserver observer : observers) {
            try {
                observer.endReadRdb(eofType, rdbDataGtidSet, rdbOffset);
            } catch (Throwable th) {
                getLogger().error("[notifyRdbData][fail] {}", observer, th);
            }
        }
    }

    protected Command<Object> replConfListeningPort() {

        return new Replconf(getClientPool(), Replconf.ReplConfType.LISTENING_PORT, scheduled,
                String.valueOf(listeningPort));
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

    @Override
    protected Object format(Object payload) {
        return payload;
    }

    @Override
    public void onEofType(EofType eofType) {
        this.eofType = eofType;
        beginReadRdb();
    }

    @Override
    public int getCommandTimeoutMilli() {
        return 0;
    }

    // implementation of InOutPayload
    @Override
    public void startInput() {

    }

    @Override
    public long inputSize() {
        return 0;
    }

    @Override
    public int in(ByteBuf byteBuf) throws IOException {
        getLogger().debug("[in]");
        onRdbData(byteBuf);
        return byteBuf.readableBytes();
    }

    @Override
    public void endInput() throws IOException {

    }

    @Override
    public void endInputTruncate(int reduceLen) throws IOException {

    }

    @Override
    public void startOutput() throws IOException {

    }

    @Override
    public long outSize() {
        return 0;
    }

    @Override
    public long out(WritableByteChannel writableByteChannel) throws IOException {
        return 0;
    }

    @Override
    public void endOutput() {

    }
}
