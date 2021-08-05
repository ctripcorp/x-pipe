package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.protocal.protocal.RdbBulkStringParser;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author marsqing
 *
 * 2016年3月24日 下午2:24:38
 */
public abstract class AbstractReplicationStorePsync extends AbstractPsync {
	
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
		
		if(currentReplicationStore == null){
			replIdRequest = "?";
			offset = -1;
		}else{
			replIdRequest = currentReplicationStore.getMetaStore().getReplId();
			offset = currentReplicationStore.getEndOffset() + 1;
		}
		return new Pair<String, Long>(replIdRequest, offset);
	}


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
	protected RdbBulkStringParser createRdbReader() {
		
		inOutPayloadReplicationStore = new InOutPayloadReplicationStore();
		RdbBulkStringParser rdbReader = new RdbBulkStringParser(inOutPayloadReplicationStore);
		return rdbReader;
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
		rdbStore.failRdb(throwable);
	}

	protected void appendCommands(ByteBuf byteBuf) throws IOException {
		
		currentReplicationStore.appendCommands(byteBuf);
	}

	protected abstract void doWhenFullSyncToNonFreshReplicationStore(String masterRunid) throws IOException;
}
