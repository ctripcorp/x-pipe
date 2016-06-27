package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.impl.RedisSlavePromotor.SlavePromotionInfo;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;



/**
 * @author wenchao.meng
 *
 * Jun 8, 2016
 */
public class RedisKeeperServerStateActive extends AbstractRedisKeeperServerState{
	
	private PROMOTION_STATE promotionState = PROMOTION_STATE.NORMAL;

	public RedisKeeperServerStateActive(RedisKeeperServer redisKeeperServer) {
		super(redisKeeperServer);
	}
	
	public RedisKeeperServerStateActive(RedisKeeperServer redisKeeperServer, ShardStatus shardStatus) {
		super(redisKeeperServer, shardStatus);
	}

	
	@Override
	protected Endpoint doGetMaster(ShardStatus shardStatus) {
		
		if(shardStatus.getRedisMaster() != null){
			return new DefaultEndPoint(shardStatus.getRedisMaster().getIp(), shardStatus.getRedisMaster().getPort());
		}
		
		if(shardStatus.getUpstreamKeeper() != null){
			return new DefaultEndPoint(shardStatus.getUpstreamKeeper().getIp(), shardStatus.getUpstreamKeeper().getPort());
		}
		
		return null;
	}
	
	
	@Override
	protected void becomeBackup() {
		//active changed!
		try {
			logger.info("[becomeBackup][active->backup] {}", this);
			redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateBackup(redisKeeperServer, getShardStatus()));
			redisKeeperServer.getReplicationStore().changeMetaToKeeper();
			reconnectMaster();
		} catch (IOException e) {
			logger.error("[becomeBackup]" + this, e);
		}
		
	}

	@Override
	protected void becomeActive() {
		//nothing to do
		logger.info("[becomeActive][nothing to do]");
	}

	
	@Override
	protected void keeperMasterChanged() {

		reconnectMaster();
	}



	
	
	@Override
	public void setPromotionState(PROMOTION_STATE promotionState, Object info) {
		
		@SuppressWarnings("unused")
		PROMOTION_STATE oldState = this.promotionState;
		this.promotionState = promotionState;
		
		logger.info("[setKeeperServerState]{},{}" ,promotionState, info);

		switch(promotionState){
			case NORMAL:
				break;
			case BEGIN_PROMOTE_SLAVE:
				redisKeeperServer.stopAndDisposeMaster();
				break;
			case SLAVE_PROMTED:
				SlavePromotionInfo promotionInfo = (SlavePromotionInfo) info;
				RedisMeta newMaster = masterChanged(promotionInfo.getKeeperOffset(), promotionInfo.getNewMasterEndpoint()
							, promotionInfo.getNewMasterRunid(), promotionInfo.getNewMasterReplOffset());
				this.promotionState = PROMOTION_STATE.REPLICATION_META_EXCHANGED;
				
				//TODO should be called!!
				this.getShardStatus().setRedisMaster(newMaster);
				reconnectMaster();
				break;
			case REPLICATION_META_EXCHANGED:
				throw new IllegalStateException("state should no be changed outside:" + promotionState);
			default:
				throw new IllegalStateException("unkonow state:" + promotionState);
		}
	}
	
	public RedisMeta masterChanged(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid, long newMasterReplOffset) {
		
		ReplicationStore replicationStore = redisKeeperServer.getReplicationStore();
		ReplicationStoreMeta meta = replicationStore.getReplicationStoreMeta();
		long delta = (meta.getKeeperBeginOffset() - meta.getBeginOffset()) + newMasterReplOffset - keeperOffset;
		replicationStore.masterChanged(newMasterEndpoint, newMasterRunid, delta);
		
		RedisMeta redisMeta = new RedisMeta();
		redisMeta.setIp(newMasterEndpoint.getHost());
		redisMeta.setPort(newMasterEndpoint.getPort());
		redisMeta.setMaster(true);
		
		return redisMeta;
	}

	@Override
	public boolean psync(RedisClient redisClient, String []args) {
		return true;
	}
	
	@Override
	protected void reconnectMaster() {
		
		if(promotionState == PROMOTION_STATE.NORMAL || promotionState == PROMOTION_STATE.REPLICATION_META_EXCHANGED){
			super.reconnectMaster();
		}else{
			logger.warn("[reconnectMaster][can reconnect][promotioning...]{}, {}", promotionState, this.redisKeeperServer);
		}
	}

	@Override
	public boolean isActive() {
		return true;
	}

}
