package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.RdbBulkStringParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.AuxOnlyRdbParser;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.IntSupplier;

public abstract class AbstractReplicationStoreGapAllowedSync extends AbstractGapAllowedSync implements RdbParseListener {

	protected volatile ReplicationStore  currentReplicationStore;

	private volatile RdbStore rdbStore;

	private volatile InOutPayloadReplicationStore inOutPayloadReplicationStore;

	private final IntSupplier maxGap;

	public AbstractReplicationStoreGapAllowedSync(SimpleObjectPool<NettyClient> clientPool, boolean saveCommands, ScheduledExecutorService scheduled, IntSupplier maxGap) {
		super(clientPool, saveCommands, scheduled);
		this.maxGap = maxGap;
	}

	protected abstract ReplicationStore getCurrentReplicationStore();

	protected  SyncRequest getReplicationStoreSyncRequest() {
		ReplStage.ReplProto proto = currentReplicationStore.getMetaStore().getCurrentReplStage().getProto();
		if (proto != ReplStage.ReplProto.XSYNC) {
			PsyncRequest psync = new PsyncRequest();
			psync.setReplId(currentReplicationStore.getMetaStore().getCurReplStageReplId());
			psync.setReplOff(currentReplicationStore.getCurReplStageReplOff() + 1);
			return psync;
		} else {
			XsyncRequest xsync = new XsyncRequest();
			Pair<GtidSet, GtidSet> gtidSets = currentReplicationStore.getGtidSet();
			GtidSet gtidSet = gtidSets.getKey().union(gtidSets.getValue());
			xsync.setGtidSet(gtidSet);
			xsync.setUuidIntrested(UUID_INSTRESTED_DEFAULT);
			xsync.setMaxGap(maxGap == null ? DEFAULT_XSYNC_MAXGAP : maxGap.getAsInt());
			return xsync;
		}
	}

	public SyncRequest getSyncRequest() {
		ReplStage.ReplProto proto = null;
		if (currentReplicationStore != null && currentReplicationStore.getMetaStore().getCurrentReplStage() != null) {
			proto = currentReplicationStore.getMetaStore().getCurrentReplStage().getProto();
		}

		if (proto == null) {
			PsyncRequest full = new PsyncRequest();
			full.setReplId("?");
			full.setReplOff(-1);
			return full;
		} else {
			return getReplicationStoreSyncRequest();
		}
	}

	@Override
	protected void doOnFullSync() throws IOException {
		if(currentReplicationStore == null || !currentReplicationStore.isFresh()){
			doWhenFullSyncToNonFreshReplicationStore(syncReply.getReplId());
		}
		super.doOnFullSync();
	}
	
	
	@Override
	protected void doOnContinue(String newReplId) throws IOException {
		if(newReplId != null){
			currentReplicationStore.psyncContinue(newReplId);
		}
		super.doOnContinue(newReplId);
	}

	@Override
	protected void doOnKeeperContinue(String replId, long beginOffset) throws IOException {
		try {
			if(currentReplicationStore == null || !currentReplicationStore.isFresh()){
				throw new IllegalStateException("keeper-continue to non fresh repl store");
			}
			currentReplicationStore.psyncContinueFrom(replId, beginOffset);
			super.doOnKeeperContinue(replId, beginOffset);
		} catch (IOException e) {
			getLogger().error("[doOnKeeperContinue]" + replId + ":" + beginOffset, e);
		}
	}

	@Override
	protected void doOnXFullSync() throws IOException {
		if(currentReplicationStore == null || !currentReplicationStore.isFresh()){
			doWhenFullSyncToNonFreshReplicationStore(syncReply.getReplId());
		}
		super.doOnXFullSync();
	}

	@Override
	protected void doOnXContinue() throws IOException {
		XContinueReply reply = (XContinueReply) syncReply;
		boolean updated = currentReplicationStore.xsyncContinue(reply.getReplId(), reply.getReplOff(),
					reply.getMasterUuid(), reply.getGtidCont());
		super.doOnXContinue();
		if (updated) {
			super.notifyXsyncUpdated();
		}
	}

	@Override
	protected void doOnSwitchToPsync() throws IOException{
		currentReplicationStore.switchToPSync(syncReply.getReplId(), syncReply.getReplOff());
		super.doOnSwitchToPsync();
	}

	@Override
	protected void doOnSwitchToXsync() throws IOException {
		XContinueReply reply = (XContinueReply) syncReply;

		if (currentReplicationStore.isFresh()) {
			currentReplicationStore.xsyncContinueFrom(reply.getReplId(), reply.getReplOff(),
					reply.getMasterUuid(), reply.getGtidCont(), reply.getGtidLost());
		} else {
			currentReplicationStore.switchToXSync(reply.getReplId(), reply.getReplOff(),
					reply.getMasterUuid(), reply.getGtidCont(), reply.getGtidLost());
		}
		super.doOnSwitchToXsync();
	}

	@Override
	protected RdbBulkStringParser createRdbReader() {
		inOutPayloadReplicationStore = new InOutPayloadReplicationStore();
		AuxOnlyRdbParser rdbParser = new AuxOnlyRdbParser();
		rdbParser.registerListener(this);
		return new RdbBulkStringParser(inOutPayloadReplicationStore, rdbParser);
	}

	@Override
	protected void beginReadRdb(EofType eofType) {
		try {
			if (syncReply instanceof FullresyncReply) {
				rdbStore = currentReplicationStore.prepareRdb(syncReply.getReplId(), syncReply.getReplOff(), eofType,
						ReplStage.ReplProto.PSYNC, null, null);
			} else if (syncReply instanceof XFullresyncReply) {
				XFullresyncReply reply = (XFullresyncReply) syncReply;
				rdbStore = currentReplicationStore.prepareRdb(syncReply.getReplId(), syncReply.getReplOff(), eofType,
						ReplStage.ReplProto.XSYNC, reply.getGtidLost(), reply.getMasterUuid());
			} else {
				throw new IllegalStateException("unexpected syncReply type:{}" + syncReply);
			}
			inOutPayloadReplicationStore.setRdbStore(rdbStore);
			super.beginReadRdb(eofType);
		} catch (IOException e) {
			getLogger().error("[beginReadRdb]" + syncReply.getReplId() + "," + syncReply.getReplOff(), e);
		}
	}
	
	@Override
	protected void failReadRdb(Throwable throwable) {
		if (rdbStore == null) {
			getLogger().info("[failRdb], rdbStore=null", throwable);
			return;
		}
		rdbStore.failRdb(throwable);
	}

	protected void appendCommands(ByteBuf byteBuf) throws IOException {
		
		currentReplicationStore.appendCommands(byteBuf);
	}

	protected abstract void doWhenFullSyncToNonFreshReplicationStore(String masterRunid) throws IOException;

	@Override
	public void onRedisOp(RedisOp redisOp) {

	}

	@Override
	public void onAux(String key, String value) {
	}

	@Override
	public void onAuxFinish(Map<String, String> auxMap) {
		getLogger().info("[onAuxFinish] aux is finish");
		notifyReadAuxEnd(rdbStore, auxMap);
	}

	@Override
	public void onFinish(RdbParser<?> parser) {

	}
}
