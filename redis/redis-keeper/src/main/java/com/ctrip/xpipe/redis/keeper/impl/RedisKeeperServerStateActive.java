package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.impl.RedisPromotor.SlavePromotionInfo;

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
	
	public RedisKeeperServerStateActive(RedisKeeperServer redisKeeperServer, InetSocketAddress masterAddress) {
		super(redisKeeperServer, masterAddress);
	}

	@Override
	public void becomeBackup(InetSocketAddress masterAddress) {
		//active changed!
		try {
			logger.info("[becomeBackup][active->backup] {}", this);
			redisKeeperServer.getReplicationStore().changeMetaToKeeper();
			redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateBackup(redisKeeperServer, masterAddress));
			reconnectMaster();
		} catch (IOException e) {
			logger.error("[becomeBackup]" + this, e);
		}
		
	}

	@Override
	public void becomeActive(InetSocketAddress masterAddress) {
		
		setMasterAddress(masterAddress);
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
				
				setMasterAddress(new InetSocketAddress(newMaster.getIp(), newMaster.getPort()));
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
			logger.warn("[reconnectMaster][can  not reconnect][promotioning...]{}, {}", promotionState, this.redisKeeperServer);
		}
	}

	@Override
	public boolean isActive() {
		return true;
	}

	@Override
	public void initPromotionState() {
		this.promotionState = PROMOTION_STATE.NORMAL;
	}

	@Override
	public KeeperState keeperState() {
		return KeeperState.ACTIVE;
	}

}
