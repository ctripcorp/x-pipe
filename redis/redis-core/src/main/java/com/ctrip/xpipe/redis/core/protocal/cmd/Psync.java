package com.ctrip.xpipe.redis.core.protocal.cmd;


import java.io.IOException;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;


/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:24:38
 */
public class Psync extends AbstractPsync {
	
	private ReplicationStoreManager replicationStoreManager;
	
	private Endpoint masterEndPoint;
	
	
	public Psync(SimpleObjectPool<NettyClient> clientPool, 
			Endpoint masterEndPoint, ReplicationStoreManager replicationStoreManager) {
		super(clientPool, true);
		this.masterEndPoint = masterEndPoint;
		this.replicationStoreManager = replicationStoreManager;
		currentReplicationStore = getCurrentReplicationStore();
	}
	
	@Override
	protected ReplicationStore getCurrentReplicationStore() {
		
		try {
			return replicationStoreManager.getCurrent();
		} catch (IOException e) {
			logger.error("[doRequest]" + this + replicationStoreManager, e);
			throw new XpipeRuntimeException("[doRequest]getReplicationStore failed." + replicationStoreManager, e);
		}
	}


	@Override
	public String toString() {
		return getName() + "->"  + masterEndPoint;
	}
	
	@Override
	protected void doWhenFullSyncToNonFreshReplicationStore(String masterRunid) throws IOException {
		logger.info("[handleResponse][full sync][replication store out of time, destroy]{}, {}", this, currentReplicationStore);
		
		ReplicationStore oldStore = currentReplicationStore;
		long newKeeperBeginOffset = ReplicationStoreMeta.DEFAULT_KEEPER_BEGIN_OFFSET;
		if(oldStore != null){
			try {
				oldStore.close();
			} catch (IOException e) {
				logger.error("[handleRedisReponse]" + oldStore, e);
			}
			newKeeperBeginOffset = oldStore.nextNonOverlappingKeeperBeginOffset();
			oldStore.delete();
		}
		logger.info("[handleRedisResponse][set keepermeta]{}, {}", masterRunid, newKeeperBeginOffset);
		currentReplicationStore = createReplicationStore(masterRunid, newKeeperBeginOffset);
		notifyReFullSync();
	}
	
	private ReplicationStore createReplicationStore(String masterRunid, long keeperBeginOffset) {
		
		try {
			return replicationStoreManager.create(masterRunid, keeperBeginOffset);
		} catch (IOException e) {
			throw new XpipeRuntimeException("[createNewReplicationStore]" + replicationStoreManager, e);
		}
	}
}
