package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.command.CommandFutureListener;
import com.ctrip.xpipe.api.payload.InOutPayload;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.command.CommandExecutionException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.protocal.*;
import com.ctrip.xpipe.redis.core.protocal.protocal.AbstractBulkStringParser.BulkStringParserListener;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.RdbBulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.utils.ChannelUtil;
import com.ctrip.xpipe.utils.CloseState;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public abstract class AbstractGapAllowedSync extends AbstractRedisCommand<Object> implements GapAllowedSync, BulkStringParserListener {

    private boolean saveCommands;

    private RdbBulkStringParser rdbReader;

    protected List<PsyncObserver> observers = new LinkedList<PsyncObserver>();

    protected PSYNC_STATE syncState = PSYNC_STATE.PSYNC_COMMAND_WAITING_REPONSE;

    private final CloseState closeState = new CloseState();

    protected SyncRequest syncRequest;
    protected SyncReply syncReply;

    public AbstractGapAllowedSync(String host, int port, boolean saveCommands, ScheduledExecutorService scheduled) {
        super(host, port, scheduled);
        this.saveCommands = saveCommands;
    }

    public AbstractGapAllowedSync(SimpleObjectPool<NettyClient> clientPool, boolean saveCommands,
                                  ScheduledExecutorService scheduled) {
        super(clientPool, scheduled);
        this.saveCommands = saveCommands;
    }

    @Override
    public String getName() {
        return "gasync";
    }

    @Override
    protected void doExecute() throws CommandExecutionException {
        super.doExecute();
        addFutureListener();
    }

    //public for unit test
    public void addFutureListener() {
        future().addListener(new CommandFutureListener<Object>() {
            @Override
            public void operationComplete(CommandFuture<Object> commandFuture) throws Exception {
                if (!commandFuture.isSuccess()) {
                    failPsync(commandFuture.cause());
                }
            }
        });
    }

    protected void failPsync(Throwable throwable) {
        if (syncState == PSYNC_STATE.READING_RDB) {
            failReadRdb(throwable);
        }
    }

    protected abstract void failReadRdb(Throwable throwable);

    interface SyncRequest {
        ByteBuf format();
    }

    static class PsyncRequest implements SyncRequest {
        String replId;
        long replOff = -1;

        public String getReplId() {
            return replId;
        }

        public void setReplId(String replId) {
            this.replId = replId;
        }

        public long getReplOff() {
            return replOff;
        }

        public void setReplOff(long replOff) {
            this.replOff = replOff;
        }

        @Override
        public ByteBuf format() {
            if (replId == null) {
                replId = "?";
                replOff = -1;
            }
            RequestStringParser requestString = new RequestStringParser(PSYNC, replId,
                    String.valueOf(replOff));
            return requestString.format();
        }
    }

    static class XsyncRequest implements SyncRequest {
        String uuidIntrested;
        GtidSet gtidSet;
        long maxGap = 0;

        public String getUuidIntrested() {
            return uuidIntrested;
        }

        public void setUuidIntrested(String uuidIntrested) {
            this.uuidIntrested = uuidIntrested;
        }

        public GtidSet getGtidSet() {
            return gtidSet;
        }

        public void setGtidSet(GtidSet gtidSet) {
            this.gtidSet = gtidSet;
        }

        public long getMaxGap() {
            return maxGap;
        }

        public void setMaxGap(long maxGap) {
            this.maxGap = maxGap;
        }

        @Override
        public ByteBuf format() {
            RequestStringParser requestString = new RequestStringParser(XSYNC, uuidIntrested,
                    gtidSet.toString(), XSYNC_OPT_MAXGAP, String.valueOf(maxGap));
            return requestString.format();
        }
    }

    public abstract SyncRequest getSyncRequest();

    @Override
    public ByteBuf getRequest() {
        this.syncRequest = getSyncRequest();
        ByteBuf request = getSyncRequest().format();
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("[doRequest]{}, {}", this, request.toString(Charset.defaultCharset()));
        }
        return request;
    }

    @Override
    public void addPsyncObserver(PsyncObserver observer) {
        this.observers.add(observer);
    }

    @Override
    protected Object doReceiveResponse(Channel channel, ByteBuf byteBuf) throws Exception {
        while (true) {
            switch (syncState) {

                case PSYNC_COMMAND_WAITING_REPONSE:
                    Object response = super.doReceiveResponse(channel, byteBuf);
                    if (response == null) {
                        return null;
                    }
                    handleRedisResponse(channel, (String) response);
                    break;

                case READING_RDB:

                    if (rdbReader == null) {
                        getLogger().info("[doReceiveResponse][createRdbReader]{}", ChannelUtil.getDesc(channel));
                        rdbReader = createRdbReader();
                        rdbReader.setBulkStringParserListener(this);
                    }

                    RedisClientProtocol<InOutPayload> payload = rdbReader.read(byteBuf);
                    if (payload != null) {
                        syncState = PSYNC_STATE.READING_COMMANDS;
                        if (!saveCommands) {
                            future().setSuccess();
                        }
                        endReadRdb();
                        break;
                    } else {
                        break;
                    }
                case READING_COMMANDS:
                    if (saveCommands) {
                        synchronized (closeState) {
                            if (!closeState.isOpen()) {
                                getLogger().info("[doHandleResponse][psync already closed]{}", channel);
                                throw new IllegalStateException("psync already closed");
                            }
                            try {
                                appendCommands(byteBuf);
                            } catch (IOException e) {
                                getLogger().error("[doHandleResponse][write commands error]" + this, e);
                            }
                        }
                    }
                    break;
                default:
                    throw new IllegalStateException("unknown state:" + syncState);
            }

            return null;
        }
    }

    public interface SyncReply {
        String XSYNC_REPLY_OPT_REPLID = "replid";
        String XSYNC_REPLY_OPT_REPLOFF = "reploff";
        String XSYNC_REPLY_OPT_MASTER_UUID = "master.uuid";
        String XSYNC_REPLY_OPT_GTID_SET = "gtid.set";
        String XSYNC_REPLY_OPT_GTID_LOST = "gtid.lost";

        String getReplId();

        long getReplOff();

        SyncReply parse(String[] split);
    }

    abstract class AbstractSyncReply implements SyncReply {
        long REPLOFF_UNSET = -1;
        String replId = null;
        long replOff = REPLOFF_UNSET;

        public String getReplId() {
            return replId;
        }

        public void setReplId(String replId) {
            this.replId = replId;
        }

        public long getReplOff() {
            return replOff;
        }

        public void setReplOff(long replOff) {
            this.replOff = replOff;
        }
    }

    class ContinueReply extends AbstractSyncReply {
        public SyncReply parse(String[] split) {
            if (split.length < 1) {
                throw new RedisRuntimeException("invalid continue reply");
            }
            if (split.length >= 2) {
                replId = split[1];
                if (split.length >= 3 && StringUtils.isNumeric(split[2])) {
                    replOff = Long.parseLong(split[2]);
                }
            }
            return this;
        }
    }

    class FullresyncReply extends AbstractSyncReply {
        public SyncReply parse(String[] split) {
            if (split.length != 3) {
                throw new RedisRuntimeException("invalid fullresync reply");
            }
            replId = split[1];
            replOff = Long.parseLong(split[2]);
            return this;
        }
    }

    class XFullresyncReply extends AbstractSyncReply {
        String masterUuid;
        GtidSet gtidLost;

        public GtidSet getGtidLost() {
            return gtidLost;
        }

        public String getMasterUuid() {
            return masterUuid;
        }

        public SyncReply parse(String[] split) {
            if (split.length < 1) {
                throw new RedisRuntimeException("invalid xfullresync reply");
            }
            for (int i = 1; i+1 < split.length; i+=2) {
                String optkey = split[i];
                String optval = split[i+1];

                if (optkey.equalsIgnoreCase(XSYNC_REPLY_OPT_REPLID)) {
                    replId = optval;
                } else if (optkey.equalsIgnoreCase(XSYNC_REPLY_OPT_REPLOFF)) {
                    replOff = Long.parseLong(optval);
                } else if (optkey.equalsIgnoreCase(XSYNC_REPLY_OPT_MASTER_UUID)) {
                    masterUuid = optval;
                } else if (optkey.equalsIgnoreCase(XSYNC_REPLY_OPT_GTID_LOST)) {
                    gtidLost = new GtidSet(StringUtil.unwrap(optval,'"'));
                } else {
                    getLogger().info("[parse] ignore unrecognized option {}:{}", optkey, optval);
                }
            }
            if (replId == null) {
                throw new RedisRuntimeException("replid reply: replid unset");
            }
            if (replOff == REPLOFF_UNSET) {
                throw new RedisRuntimeException("replid reply: reploff unset");
            }
            if (masterUuid == null) {
                throw new RedisRuntimeException("replid reply: master.uuid unset");
            }
            if (gtidLost == null) {
                throw new RedisRuntimeException("replid reply: gtid.lost unset");
            }
            return this;
        }
    }

    class XContinueReply extends AbstractSyncReply {
        String masterUuid;
        GtidSet gtidCont;

        public GtidSet getGtidCont() {
            return gtidCont;
        }

        public String getMasterUuid() {
            return masterUuid;
        }

        public SyncReply parse(String[] split) {
            for (int i = 1; i+1 < split.length; i+=2) {
                String optkey = split[i];
                String optval = split[i+1];

                if (optkey.equalsIgnoreCase(XSYNC_REPLY_OPT_REPLID)) {
                    replId = optval;
                } else if (optkey.equalsIgnoreCase(XSYNC_REPLY_OPT_REPLOFF)) {
                    replOff = Long.parseLong(optval);
                } else if (optkey.equalsIgnoreCase(XSYNC_REPLY_OPT_MASTER_UUID)) {
                    masterUuid = optval;
                } else if (optkey.equalsIgnoreCase(XSYNC_REPLY_OPT_GTID_SET)) {
                    gtidCont = new GtidSet(StringUtil.unwrap(optval,'"'));
                } else {
                    getLogger().info("[parse] ignore unrecognized option {}:{}", optkey, optval);
                }
            }
            if (replId == null) {
                throw new RedisRuntimeException("replid reply: replid unset");
            }
            if (replOff == REPLOFF_UNSET) {
                throw new RedisRuntimeException("replid reply: reploff unset");
            }
            if (masterUuid == null) {
                throw new RedisRuntimeException("replid reply: masteruuid unset");
            }
            if (gtidCont == null) {
                throw new RedisRuntimeException("replid reply: gtidcont unset");
            }
            return this;
        }
    }

    SyncReply parseSyncReply(String reply) {
        String[] split = splitSpace(reply);

        if (split.length == 0) {
            throw new RedisRuntimeException("invalid reply:" + reply);
        }

        if (split[0].equalsIgnoreCase(FULL_SYNC)) {
            return new FullresyncReply().parse(split);
        } else if (split[0].equalsIgnoreCase(PARTIAL_SYNC)) {
            return new ContinueReply().parse(split);
        } else if (split[0].equalsIgnoreCase(XFULL_SYNC)) {
            return new XFullresyncReply().parse(split);
        } else if (split[0].equalsIgnoreCase(XPARTIAL_SYNC)) {
            return new XContinueReply().parse(split);
        } else {
            throw new RedisRuntimeException("invalid reply:" + reply);
        }
    }

    protected void handleRedisResponse(Channel channel, String reply) throws IOException {

        if (getLogger().isInfoEnabled()) {
            getLogger().info("[handleRedisResponse]{}, {}, {}", ChannelUtil.getDesc(channel), this, reply);
        }

        SyncReply parsedReply = parseSyncReply(reply);
        if (getLogger().isDebugEnabled()) {
            getLogger().debug("[readRedisResponse]{}, {}, {}, {}", ChannelUtil.getDesc(channel), this, parsedReply);
        }
        this.syncReply = parsedReply;

        if (parsedReply instanceof FullresyncReply) {
            syncState = PSYNC_STATE.READING_RDB;
            doOnFullSync();
        } else if (parsedReply instanceof XFullresyncReply) {
            syncState = PSYNC_STATE.READING_RDB;
            doOnXFullSync();
        } else if (parsedReply instanceof ContinueReply) {
            syncState = PSYNC_STATE.READING_COMMANDS;

            if (syncRequest instanceof PsyncRequest) {
                if (parsedReply.getReplOff() >= 0) {
                    doOnKeeperContinue(syncReply.getReplId(), syncReply.getReplOff());
                } else {
                    doOnContinue(syncReply.getReplId());
                }
            } else if (syncRequest instanceof XsyncRequest) {
                doOnSwitchToPsync();
            } else {
                throw new IllegalStateException("unexpected reuqest type:" + syncRequest);
            }
        } else if (parsedReply instanceof XContinueReply) {
            syncState = PSYNC_STATE.READING_COMMANDS;

            if (syncRequest instanceof PsyncRequest) {
                doOnSwitchToXsync();
            } else if (syncRequest instanceof XsyncRequest) {
                doOnXContinue();
            } else {
                throw new IllegalStateException("unexpected reuqest type:" + syncRequest);
            }
        } else {
            throw new IllegalStateException("invalid reply:" + reply);
        }
    }

    protected void endReadRdb() {

        getLogger().info("[endReadRdb]");
        for (PsyncObserver observer : observers) {
            try {
                observer.endWriteRdb();
            } catch (Throwable th) {
                getLogger().error("[endReadRdb]" + this, th);
            }
        }
    }

    protected abstract void appendCommands(ByteBuf byteBuf) throws IOException;

    protected abstract RdbBulkStringParser createRdbReader();

    protected void doOnFullSync() throws IOException {
        getLogger().debug("[doOnFullSync]");
        notifyFullSync();
    }

    private void notifyFullSync() {
        getLogger().debug("[notifyFullSync]");
        for (PsyncObserver observer : observers) {
            observer.onFullSync(syncReply.getReplOff());
        }
    }

    protected void doOnContinue(String newReplId) throws IOException {
        getLogger().debug("[doOnContinue]{}", newReplId);
        notifyContinue(newReplId);
    }

    private void notifyContinue(String newReplId) {
        getLogger().debug("[notifyContinue]{}", newReplId);
        for (PsyncObserver observer : observers) {
            observer.onContinue(((PsyncRequest) syncRequest).getReplId(), newReplId);
        }
    }

    protected void doOnKeeperContinue(String replId, long continueOffset) throws IOException {
        getLogger().debug("[doOnKeeperContinue]{}:{}", replId, continueOffset);
        notifyKeeperContinue(replId, continueOffset);
    }

    protected void notifyKeeperContinue(String replId, long beginOffset) {
        getLogger().debug("[notifyKeeperContinue]{}:{}", replId, beginOffset);
        for (PsyncObserver observer : observers) {
            observer.onKeeperContinue(replId, beginOffset);
        }
    }

    protected void doOnXFullSync() throws IOException {
        getLogger().debug("[doOnXFullSync]");
        notifyXFullSync();
    }

    private void notifyXFullSync() {
        XFullresyncReply reply = (XFullresyncReply) syncReply;
        getLogger().debug("[notifyXFullSync] replId:{}, replOff:{}, masterUuid:{}, gtidLost{}", reply.getReplId(),
                reply.getReplOff(), reply.getMasterUuid(), reply.getGtidLost());
        for (PsyncObserver observer : observers) {
            if (observer instanceof GapAllowedSyncObserver) {
                ((GapAllowedSyncObserver)observer).onXFullSync(reply.getReplId(), reply.getReplOff(),
                        reply.getMasterUuid(), reply.getGtidLost());
            }
        }
    }

    protected void doOnXContinue() throws IOException {
        getLogger().debug("[doOnXContinue]");
        notifyXContinue();
    }

    private void notifyXContinue() {
        XContinueReply reply = (XContinueReply) syncReply;
        getLogger().debug("[notifyXContinue] replId:{}, replOff:{}, masterUuid:{}, gtidLost{}", reply.getReplId(),
                reply.getReplOff(), reply.getMasterUuid(), reply.getGtidCont());
        for (PsyncObserver observer : observers) {
            if (observer instanceof GapAllowedSyncObserver) {
                ((GapAllowedSyncObserver) observer).onXContinue(reply.getReplId(), reply.getReplOff(),
                        reply.getMasterUuid(), reply.getGtidCont());
            }
        }
    }

    protected void doOnSwitchToPsync() throws IOException {
        getLogger().debug("[doOnSwitchToPsync]");
        notifySwitchToPsync();
    }

    private void notifySwitchToPsync() {
        ContinueReply reply = (ContinueReply) syncReply;
        getLogger().debug("[notifySwitchToPsync] replid:{}, replOff:{}", reply.getReplId(), reply.getReplOff());
        for (PsyncObserver observer : observers) {
            if (observer instanceof GapAllowedSyncObserver) {
                ((GapAllowedSyncObserver)observer).onSwitchToPsync(reply.getReplId(), reply.getReplOff());
            }
        }
    }

    protected void doOnSwitchToXsync() throws IOException {
        getLogger().debug("[doOnSwitchToXsync]");
        notifySwitchToXsync();
    }

    private void notifySwitchToXsync() {
        XContinueReply reply = (XContinueReply) syncReply;
        getLogger().debug("[notifySwitchToXsync] replid:{}, replOff:{}, masterUuid:{}", reply.getReplId(),
                reply.getReplOff(), reply.getMasterUuid());
        for (PsyncObserver observer : observers) {
            if (observer instanceof GapAllowedSyncObserver) {
                ((GapAllowedSyncObserver) observer).onSwitchToXsync(reply.getReplId(),
                        reply.getReplOff(), reply.getMasterUuid());
            }
        }
    }

    protected void notifyXsyncUpdated() {
        getLogger().debug("[notifyXsyncUpdated]");
        for (PsyncObserver observer : observers) {
            if (observer instanceof GapAllowedSyncObserver) {
                ((GapAllowedSyncObserver)observer).onUpdateXsync();
            }
        }
    }

    protected void notifyReadAuxEnd(RdbStore rdbStore, Map<String, String> auxMap) {
        getLogger().debug("[notifyReadAuxEnd] {}", auxMap);
        for (PsyncObserver observer : observers) {
            try {
                observer.readAuxEnd(rdbStore, auxMap);
            } catch (Throwable th) {
                getLogger().error("[onAuxFinish]" + this, th);
            }
        }
    }

    @Override
    public void onEofType(EofType eofType) {
        beginReadRdb(eofType);
    }

    protected void beginReadRdb(EofType eofType) {
        getLogger().info("[beginReadRdb]{}, eof:{}", this, eofType);
        for (PsyncObserver observer : observers) {
            try {
                observer.beginWriteRdb(eofType, syncReply.getReplId(), syncReply.getReplOff());
            } catch (Throwable th) {
                getLogger().error("[beginReadRdb]" + this, th);
            }
        }
    }

    protected void notifyReFullSync() {
        for (PsyncObserver observer : observers) {
            observer.reFullSync();
        }
    }

    @Override
    protected Object format(Object payload) {
        return payload;
    }

    @Override
    public int getCommandTimeoutMilli() {
        return 0;
    }

    @Override
    protected void doReset() {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public void close() {
        if (!this.closeState.isOpen()) return;

        this.closeState.setClosing();
        synchronized (closeState) {
            this.closeState.setClosed();
        }
    }
}
