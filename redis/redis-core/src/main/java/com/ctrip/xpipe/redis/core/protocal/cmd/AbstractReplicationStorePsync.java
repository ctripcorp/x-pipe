package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.RdbBulkStringParser;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParser;
import com.ctrip.xpipe.redis.core.redis.rdb.parser.AuxOnlyRdbParser;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

import static com.ctrip.xpipe.redis.core.redis.rdb.RdbConstant.REDIS_RDB_AUX_KEY_GTID;

/**
 * @author marsqing
 *
 * 2016年3月24日 下午2:24:38
 */
public abstract class AbstractReplicationStorePsync extends AbstractPsync implements RdbParseListener {
	
	protected volatile ReplicationStore  	    currentReplicationStore;

	private volatile RdbStore rdbStore;
	
	private volatile InOutPayloadReplicationStore inOutPayloadReplicationStore;

	public AbstractReplicationStorePsync(SimpleObjectPool<NettyClient> clientPool, boolean saveCommands, ScheduledExecutorService scheduled) {
		super(clientPool, saveCommands, scheduled);
	}
	

	@Override
	protected Pair<String, Long> getRequestMasterInfo() {
		
		String replIdRequest = null;
		long offset = -1;
		if (useKeeperPsync()) {
			replIdRequest = "?";
			offset = KEEPER_PARTIAL_SYNC_OFFSET;
		} else if(currentReplicationStore == null){
			replIdRequest = "?";
			offset = -1;
		}else{
			replIdRequest = currentReplicationStore.getMetaStore().getReplId();
			offset = currentReplicationStore.getEndOffset() + 1;
		}
		return new Pair<String, Long>(replIdRequest, offset);
	}

	protected abstract boolean useKeeperPsync();

	protected abstract ReplicationStore getCurrentReplicationStore();
	
	@Override
	protected void doOnFullSync() throws IOException {
		
		if(currentReplicationStore == null || !currentReplicationStore.isFresh()){
			doWhenFullSyncToNonFreshReplicationStore(replId);
		}
		super.doOnFullSync();
	}
	
	
	@Override
	protected void doOnContinue(String newReplId) throws IOException {
		
		if(newReplId != null){
			currentReplicationStore.shiftReplicationId(newReplId);
		}
		super.doOnContinue(newReplId);
	}

	@Override
	protected void doOnKeeperContinue(String replId, long beginOffset) throws IOException {
		try {
			if(currentReplicationStore == null || !currentReplicationStore.isFresh()){
				throw new IllegalStateException("keeper-continue to non fresh repl store");
			}
			currentReplicationStore.continueFromOffset(replId, beginOffset);
			super.doOnKeeperContinue(replId, beginOffset);
		} catch (IOException e) {
			getLogger().error("[doOnKeeperContinue]" + replId + ":" + beginOffset, e);
		}
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
			rdbStore = currentReplicationStore.beginRdb(replId, masterRdbOffset, eofType);
			inOutPayloadReplicationStore.setRdbStore(rdbStore);
			super.beginReadRdb(eofType);
		} catch (IOException e) {
			getLogger().error("[beginReadRdb]" + replId + "," + masterRdbOffset, e);
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
		//this part should be in AbstractPsync
		if (REDIS_RDB_AUX_KEY_GTID.equalsIgnoreCase(key)) {
			readRdbGtidSet(value);
		}
	}

	protected void readRdbGtidSet(String gtidSet) {

		getLogger().info("[readRdbGtidSet]{}, gtidset:{}", this, gtidSet);

		for (PsyncObserver observer : observers) {
			try {
				observer.readRdbGtidSet(rdbStore, gtidSet);
			} catch (Throwable th) {
				getLogger().error("[readRdbGtidSet]" + this, th);
			}
		}
	}

	@Override
	public void onFinish(RdbParser<?> parser) {

	}
}
