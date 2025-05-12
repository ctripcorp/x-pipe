package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.protocal.GapAllowedSyncObserver;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.RdbBulkStringParser;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.IntSupplier;

public class InMemoryGapAllowedSync extends AbstractGapAllowedSync {
    private static final Logger log = LoggerFactory.getLogger(InMemoryGapAllowedSync.class);
    private SyncRequest request;

    private long replOffset = 0;

    private int fullSyncCnt = 0;

    private ByteArrayOutputStreamPayload rdb = new ByteArrayOutputStreamPayload();
    private ByteArrayOutputStream commands = new ByteArrayOutputStream();

    private GtidSet lostGtidSet;

    public InMemoryGapAllowedSync(String host, int port, boolean saveCommands, ScheduledExecutorService scheduled) {
        super(host, port, saveCommands, scheduled);
        lostGtidSet = new GtidSet("");
        addPsyncObserver(new GapAllowedSyncObserver() {
            @Override
            public void onXFullSync(String replId, long replOff, String masterUuid, GtidSet gtidLost) {
                log.info("[onXFullSync]{}, {}", replId, replOff);
                log.info(gtidLost.toString());
                replOffset = replOff;
                fullSyncCnt++;
                lostGtidSet = lostGtidSet.union(gtidLost);
            }

            @Override
            public void onXContinue(String replId, long replOff, String masterUuid, GtidSet gtidCont) {
                log.info("[onXContinue]{}, {}", replId, replOff);
                log.info(gtidCont.toString());
                replOffset = replOff;
                log.info("[onXContinue]{}, {}", replId, replOffset);
                lostGtidSet = gtidCont.subtract(((XsyncRequest)request).getGtidSet());
                log.info(((XsyncRequest)request).getGtidSet().toString());
            }

            @Override
            public void onSwitchToXsync(String replId, long replOff, String masterUuid, GtidSet gtidCont) {
                log.info("[onSwitchToXsync]{}, {}", replId, replOff);
                replOffset = replOff;
            }

            @Override
            public void onSwitchToPsync(String replId, long replOff) {
                replOffset = replOff;
                log.info("[onSwitchToPsync]{}, {}", replId, replOffset);
            }

            @Override
            public void onUpdateXsync() {
                log.info("[onUpdateXsync]{}", replOffset);
            }

            @Override
            public void onFullSync(long masterRdbOffset) {
                log.info("[onFullSync]{}, {}", replOffset, masterRdbOffset);
            }

            @Override
            public void reFullSync() {
                log.info("[reFullSync]{}", replOffset);
            }

            @Override
            public void beginWriteRdb(EofType eofType, String replId, long masterRdbOffset) throws IOException {
                log.info("[beginWriteRdb]{}, {}", replId, replOffset);
            }

            @Override
            public void readAuxEnd(RdbStore rdbStore, Map<String, String> auxMap) {
                log.info("[readAuxEnd]{}, {}", replOffset, auxMap);
            }

            @Override
            public void endWriteRdb() {
                log.info("[endWriteRdb]{}", replOffset);
            }

            @Override
            public void onContinue(String requestReplId, String responseReplId) {
                log.info("[onContinue]{}, {}", replOffset, responseReplId);
            }

            @Override
            public void onKeeperContinue(String replId, long beginOffset) {
                log.info("[onKeeperContinue]{}, {}", replOffset, beginOffset);
            }
        });
    }

    public InMemoryGapAllowedSync(String masterHost, int masterPort, String requestMasterId, long   requestMasterOffset, ScheduledExecutorService scheduled) {
        this(masterHost, masterPort, true, scheduled);
        setPsyncRequest(requestMasterId, requestMasterOffset);
    }

    public InMemoryGapAllowedSync(SimpleObjectPool<NettyClient> clientPool, String requestMasterId, long   requestMasterOffset, ScheduledExecutorService scheduled) {
        super(clientPool, true, scheduled);
        setPsyncRequest(requestMasterId, requestMasterOffset);
    }

    public void setXsyncRequest(GtidSet gtidSet, IntSupplier maxGap) {
        AbstractGapAllowedSync.XsyncRequest xsync = new AbstractGapAllowedSync.XsyncRequest();
        xsync.setGtidSet(gtidSet);
        xsync.setUuidIntrested(UUID_INSTRESTED_DEFAULT);
        xsync.setMaxGap(maxGap == null ? DEFAULT_XSYNC_MAXGAP : maxGap.getAsInt());
        this.request = xsync;
    }

    public void setPsyncFull() {
        PsyncRequest full = new PsyncRequest();
        full.setReplId("?");
        full.setReplOff(-1);
        this.request = full;
    }

    public void setPsyncRequest(String replId, long replOff) {
        PsyncRequest psync = new PsyncRequest();
        psync.setReplId(replId);
        psync.setReplOff(replOff);
        this.request = psync;
    }

    @Override
    protected void failReadRdb(Throwable throwable) {
        getLogger().error("[failReadRdb]", throwable);
    }

    @Override
    public SyncRequest getSyncRequest() {
        return request;
    }

    @Override
    protected void appendCommands(ByteBuf byteBuf) throws IOException {
        commands.write(ByteBufUtils.readToBytes(byteBuf));
    }


    @Override
    protected RdbBulkStringParser createRdbReader() {
        return new RdbBulkStringParser(rdb);
    }

    public long getReplOffset() {
        return replOffset + commands.toByteArray().length;
    }

    public int getFullSyncCnt() {
        return fullSyncCnt;
    }

    public byte[] getCommands() {
        return commands.toByteArray();
    }

	public byte[] getRdb() {
		return rdb.getBytes();
	}

    public GtidSet getLostGtidSet() {
        return lostGtidSet;
    }
}
