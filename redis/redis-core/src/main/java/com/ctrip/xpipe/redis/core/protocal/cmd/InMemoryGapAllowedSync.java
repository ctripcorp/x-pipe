package com.ctrip.xpipe.redis.core.protocal.cmd;

<<<<<<< HEAD
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.ByteBufUtils;
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

    public InMemoryGapAllowedSync(String host, int port, boolean saveCommands, ScheduledExecutorService scheduled) {
        super(host, port, saveCommands, scheduled);
        addPsyncObserver(new GapAllowedSyncObserver() {
            @Override
            public void onXFullSync(String replId, long replOff, String masterUuid, GtidSet gtidLost) {
                log.info("[onXFullSync]{}, {}", replId, replOff);
                replOffset = replOff;
                fullSyncCnt++;
            }

            @Override
            public void onXContinue(String replId, long replOff, String masterUuid, GtidSet gtidCont) {
                log.info("[onXContinue]{}, {}", replId, replOff);
                replOffset = replOff;
                log.info("[onXContinue]{}, {}", replId, replOffset);
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

    @Override
    protected void failReadRdb(Throwable throwable) {

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
=======
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.payload.ByteArrayOutputStreamPayload;
import com.ctrip.xpipe.redis.core.protocal.protocal.RdbBulkStringParser;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Aug 26, 2016
 */
public class InMemoryPsync extends AbstractPsync{
	
	private String requestMasterId;
	private Long   requestMasterOffset;
	private ByteArrayOutputStream commands = new ByteArrayOutputStream();
	private ByteArrayOutputStreamPayload rdb = new ByteArrayOutputStreamPayload();

	public InMemoryPsync(String masterHost, int masterPort, String requestMasterId, long   requestMasterOffset, ScheduledExecutorService scheduled) {
		super(masterHost, masterPort, true, scheduled);
		this.requestMasterId = requestMasterId;
		this.requestMasterOffset = requestMasterOffset;
		
	}

	public InMemoryPsync(SimpleObjectPool<NettyClient> clientPool, String requestMasterId, long   requestMasterOffset, ScheduledExecutorService scheduled) {
		super(clientPool, true, scheduled);
		this.requestMasterId = requestMasterId;
		this.requestMasterOffset = requestMasterOffset;
	}

	@Override
	protected Pair<String, Long> getRequestMasterInfo() {
		return new Pair<String, Long>(requestMasterId, requestMasterOffset);
	}

	@Override
	protected void appendCommands(ByteBuf byteBuf) throws IOException {
		
		commands.write(ByteBufUtils.readToBytes(byteBuf));
	}

	@Override
	protected RdbBulkStringParser createRdbReader() {
		return new RdbBulkStringParser(rdb);
	}
	
	@Override
	protected void failReadRdb(Throwable throwable) {
		getLogger().error("[failReadRdb]", throwable);
	}

	public byte[] getCommands() {
		return commands.toByteArray();
	}
	
	public byte[] getRdb() {
		return rdb.getBytes();
	}
>>>>>>> fix replstage shallow copy when metaDup.
}
