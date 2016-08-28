package com.ctrip.xpipe.redis.core.protocal.cmd;



import java.io.IOException;

import org.unidal.tuple.Pair;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;

import io.netty.buffer.ByteBuf;


/**
 * @author marsqing
 *
 * 2016年3月24日 下午2:24:38
 */
public abstract class AbstractReplicationStorePsync extends AbstractPsync {
	
	
	protected volatile ReplicationStore  	    currentReplicationStore;
	
	
	private volatile RdbStore rdbStore;
	
	private volatile InOutPayloadReplicationStore inOutPayloadReplicationStore;
	
	public AbstractReplicationStorePsync(SimpleObjectPool<NettyClient> clientPool, boolean saveCommands) {
		super(clientPool, saveCommands);
	}
	

	@Override
	protected  Pair<String, Long> getRequestMasterInfo() {
		
		String masterRunidRequest = null;
		long offset = -1;
		
		if(currentReplicationStore == null){
			masterRunidRequest = "?";
			offset = -1;
		}else{
			masterRunidRequest = currentReplicationStore.getMetaStore().getMasterRunid();
			offset = currentReplicationStore.getEndOffset() + 1;
		}
		return new Pair<String, Long>(masterRunidRequest, offset);
	}


	protected abstract ReplicationStore getCurrentReplicationStore();
	
	@Override
	protected void doOnFullSync() throws IOException {
		
		if(currentReplicationStore == null || !currentReplicationStore.isFresh()){
			doWhenFullSyncToNonFreshReplicationStore(masterRunid);
		}
		super.doOnFullSync();
	}

	@Override
	protected BulkStringParser createRdbReader() {
		
		inOutPayloadReplicationStore = new InOutPayloadReplicationStore();
		BulkStringParser rdbReader = new BulkStringParser(inOutPayloadReplicationStore);
		return rdbReader;
	}

	@Override
	protected void beginReadRdb(long fileSize) {
		try {
			rdbStore = currentReplicationStore.beginRdb(masterRunid, masterRdbOffset, fileSize);
			inOutPayloadReplicationStore.setRdbStore(rdbStore);
		} catch (IOException e) {
			logger.error("[beginReadRdb]" + masterRunid + "," + masterRdbOffset, e);
		}

		super.beginReadRdb(fileSize);
	}

	@Override
	protected void endReadRdb() {
		
		logger.info("[endReadRdb]{}", this);
		try {
			rdbStore.endRdb();
		} catch (IOException e) {
			logger.error("[endReadRdb]", e);
		}
		super.endReadRdb();
	}

	protected void appendCommands(ByteBuf byteBuf) throws IOException {
		
		int n = currentReplicationStore.appendCommands(byteBuf);
		logger.debug("[appendCommands]{}", n);
	}

	protected abstract void doWhenFullSyncToNonFreshReplicationStore(String masterRunid) throws IOException;
}
