package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.*;
import com.ctrip.xpipe.redis.core.protocal.protocal.AbstractBulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.RdbBulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.core.protocal.Xsync.XSYNC_STATE.READING_COMMANDS;

/**
 * @author lishanglin
 * date 2022/2/23
 */
public class DefaultXsync extends AbstractRedisCommand<Object> implements Xsync, AbstractBulkStringParser.BulkStringParserListener, InOutPayload {

    private GtidSet gitdSetExcluded;

    private Object vectorClockExcluded;

    private EofType eofType;

    private RdbBulkStringParser rdbReader;

    private GtidSet rdbDataGtidSet;

    private List<XsyncObserver> observers = new LinkedList<>();

    protected Xsync.XSYNC_STATE xsyncState = XSYNC_STATE.XSYNC_COMMAND_WAIT_RESPONSE;

    public DefaultXsync(String host, int port, GtidSet gtidSetExcluded, Object vectorClockExcluded, ScheduledExecutorService scheduled) {
        super(host, port, scheduled);
        this.gitdSetExcluded = gtidSetExcluded;
        this.vectorClockExcluded = vectorClockExcluded;
    }

    public DefaultXsync(SimpleObjectPool<NettyClient> clientPool, GtidSet gtidSetExcluded, Object vectorClockExcluded, ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        this.gitdSetExcluded = gtidSetExcluded;
        this.vectorClockExcluded = vectorClockExcluded;
    }

    @Override
    public void addXsyncObserver(XsyncObserver observer) {
        this.observers.add(observer);
    }

    @Override
    // XSYNC <sidno interested> <gtid.set excluded> [vectorclock excluded]
    public ByteBuf getRequest() {
        String interestedSidno = String.join(SIDNO_SEPARATOR, this.gitdSetExcluded.getUUIDs());

        RequestStringParser requestString = new RequestStringParser(getName(), interestedSidno,
                gitdSetExcluded.toString(), null != vectorClockExcluded ? vectorClockExcluded.toString() : "");
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("[doRequest]{}, {}", this, StringUtil.join(" ", requestString.getPayload()));
        }
        return requestString.format();
    }

    @Override
    protected Object doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception {
        while(true) {
            switch (xsyncState) {

                case XSYNC_COMMAND_WAIT_RESPONSE:
                    Object response = super.doReceiveResponse(channel, byteBuf);
                    if (response == null) {
                        return null;
                    }
                    handleRedisResponse(channel, (String) response);
                    break;

                case READING_RDB:
                    if (null == rdbReader) {
                        rdbReader = new RdbBulkStringParser(this);
                        rdbReader.setBulkStringParserListener(this);
                    }

                    RedisClientProtocol<InOutPayload> rdbPayload = rdbReader.read(byteBuf);
                    if (rdbPayload != null) {
                        xsyncState = READING_COMMANDS;
                        endReadRdb();
                        break;
                    } else {
                        break;
                    }
                case READING_COMMANDS:
                    Object cmdPayload = super.doReceiveResponse(channel, byteBuf);
                    if (cmdPayload instanceof Object[]) {
                        doOnCommand((Object[]) cmdPayload);
                    } else {
                        getLogger().info("[doReceiveResponse][{}][unknown payload] {}, {}", READING_COMMANDS, this, cmdPayload);
                        throw new RedisRuntimeException("unknown payload:" + cmdPayload);
                    }

                    break;
                default:
                    throw new IllegalStateException("unknown state:" + xsyncState);
            }

            return null;
        }
    }

    // reset state for next query parse
    private void resetClient() {
        commandResponseState = COMMAND_RESPONSE_STATE.READING_SIGN;
    }

    // FULL: +FULLRESYNC <RDB gtid.set>
    // PARTIAL: +CONTINUE
    protected void handleRedisResponse(Channel channel, String xsync) throws IOException {
        if (getLogger().isInfoEnabled()) {
            getLogger().info("[handleRedisResponse]{}, {}, {}", ChannelUtil.getDesc(channel), this, xsync);
        }

        String[] split = splitSpace(xsync);
        if (split.length == 0) {
            throw new RedisRuntimeException("wrong reply:" + xsync);
        }

        if (split[0].equalsIgnoreCase(FULL_SYNC)) {
            if (split.length != 2) {
                throw new RedisRuntimeException("unknown reply:" + xsync);
            }
            this.rdbDataGtidSet = new GtidSet(split[1]);
            getLogger().debug("[readRedisResponse][FULL]{}, {} {}", ChannelUtil.getDesc(channel), this, rdbDataGtidSet);
            xsyncState = XSYNC_STATE.READING_RDB;
            doOnFullSync();
        } else if (split[0].equalsIgnoreCase(PARTIAL_SYNC)) {
            xsyncState = READING_COMMANDS;
            getLogger().debug("[readRedisResponse][PARTIAL]{}, {}", ChannelUtil.getDesc(channel), this);
            doOnContinue();
        } else {
            throw new RedisRuntimeException("unknown reply:" + xsync);
        }
    }

    private void doOnFullSync() {
        getLogger().debug("[doOnFullSync]");
        for (XsyncObserver observer: observers) {
            try {
                observer.onFullSync(rdbDataGtidSet);
            } catch (Throwable th) {
                getLogger().debug("[doOnFullSync][fail] {}", observer, th);
            }
        }
        resetClient();
    }

    private void doOnContinue() {
        getLogger().debug("[doOnContinue]");
        for (XsyncObserver observer: observers) {
            try {
                observer.onContinue();
            } catch (Throwable th) {
                getLogger().debug("[doOnContinue][fail] {}", observer, th);
            }
        }
        resetClient();
    }

    private void doOnCommand(Object[] rawCmdArgs) {
        getLogger().debug("[doOnCommand]");
        for (XsyncObserver observer: observers) {
            try {
                observer.onCommand(rawCmdArgs);
            } catch (Throwable th) {
                getLogger().debug("[doOnCommand][fail] {}", observer, th);
            }
        }
        resetClient();
    }

    private void beginReadRdb() {
        getLogger().debug("[beginReadRdb]");
        for (XsyncObserver observer: observers) {
            try {
                observer.beginReadRdb(eofType, rdbDataGtidSet);
            } catch (Throwable th) {
                getLogger().debug("[beginReadRdb][fail] {}", observer, th);
            }
        }
    }

    private void onRdbData(ByteBuf byteBuf) {
        getLogger().debug("[notifyRdbData]");
        for (XsyncObserver observer: observers) {
            try {
                observer.onRdbData(byteBuf.slice());
            } catch (Throwable th) {
                getLogger().debug("[notifyRdbData][fail] {}", observer, th);
            }
        }
    }

    private void endReadRdb() {
        getLogger().debug("[endReadRdb]");
        for (XsyncObserver observer: observers) {
            try {
                observer.endReadRdb(eofType, rdbDataGtidSet);
            } catch (Throwable th) {
                getLogger().debug("[notifyRdbData][fail] {}", observer, th);
            }
        }
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
    public String getName() {
        return "XSYNC";
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
