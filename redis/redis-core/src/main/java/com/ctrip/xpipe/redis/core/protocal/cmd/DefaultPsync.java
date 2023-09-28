package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * 2016年3月24日 下午2:24:38
 */
public class DefaultPsync extends AbstractReplicationStorePsync{
	
	private ReplicationStoreManager replicationStoreManager;
	
	private Endpoint masterEndPoint;

	public DefaultPsync(SimpleObjectPool<NettyClient> clientPool,
						Endpoint masterEndPoint, ReplicationStoreManager replicationStoreManager, ScheduledExecutorService scheduled) {
		super(clientPool, true, scheduled);
		this.masterEndPoint = masterEndPoint;
		this.replicationStoreManager = replicationStoreManager;
		currentReplicationStore = getCurrentReplicationStore();
	}
	
	@Override		
	protected final ReplicationStore getCurrentReplicationStore() {
		
		try {
			return replicationStoreManager.createIfNotExist();
		} catch (IOException e) {
			getLogger().error("[doRequest]" + this + replicationStoreManager, e);
			throw new XpipeRuntimeException("[doRequest]getReplicationStore failed." + replicationStoreManager, e);
		}
	}

	@Override
	public String toString() {
		return getName() + "->"  + masterEndPoint;
	}
	
	@Override
	protected void doWhenFullSyncToNonFreshReplicationStore(String replId) throws IOException {
		
		ReplicationStore oldStore = currentReplicationStore;
		if(oldStore != null){
			try {
				getLogger().info("[doWhenFullSyncToNonFreshReplicationStore][full sync][replication store out of time, destroy]{}, {}", this, currentReplicationStore);
				oldStore.close();
			} catch (Exception e) {
				getLogger().error("[handleRedisReponse]" + oldStore, e);
			}
			notifyReFullSync();
		}
		getLogger().info("[doWhenFullSyncToNonFreshReplicationStore][set keepermeta]{}", replId);
		currentReplicationStore = createReplicationStore();
	}
	
	private ReplicationStore createReplicationStore() {
		
		try {
			return replicationStoreManager.create();
		} catch (IOException e) {
			throw new XpipeRuntimeException("[createNewReplicationStore]" + replicationStoreManager, e);
		}
	}
}
