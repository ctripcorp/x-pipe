package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;
import com.ctrip.xpipe.redis.keeper.impl.RedisPromotor.SlavePromotionInfo;

import java.io.IOException;
import java.net.InetSocketAddress;

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
	public void becomeBackup(InetSocketAddress masterAddress){
		
		logger.info("[becomeBackup]{}", masterAddress);
		doBecomeBackup(masterAddress);
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
	public void setPromotionState(PROMOTION_STATE promotionState, Object info) throws IOException {
		
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
				
				InetSocketAddress newMasterAddress = null;
				if(info instanceof SlavePromotionInfo){
					SlavePromotionInfo promotionInfo = (SlavePromotionInfo) info;
					RedisMeta newMaster = masterChanged(promotionInfo.getKeeperOffset(), promotionInfo.getNewMasterEndpoint()
								, promotionInfo.getNewMasterRunid(), promotionInfo.getNewMasterReplOffset());
					newMasterAddress = new InetSocketAddress(newMaster.getIp(), newMaster.getPort());
				}else if (info instanceof InetSocketAddress){
					newMasterAddress = (InetSocketAddress) info;
				}else{
					throw new IllegalStateException("unknown info:" + info);
				}
				this.promotionState = PROMOTION_STATE.REPLICATION_META_EXCHANGED;
				setMasterAddress(newMasterAddress);
				break;
			case REPLICATION_META_EXCHANGED:
				throw new IllegalStateException("state should no be changed outside:" + promotionState);
			default:
				throw new IllegalStateException("unkonow state:" + promotionState);
		}
	}
	
	@SuppressWarnings("deprecation")
	public RedisMeta masterChanged(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid, long newMasterReplOffset) throws IOException {
		
		ReplicationStore replicationStore = redisKeeperServer.getReplicationStore();
		replicationStore.getMetaStore().masterChanged(keeperOffset, newMasterEndpoint, newMasterRunid, newMasterReplOffset);
		
		RedisMeta redisMeta = new RedisMeta();
		redisMeta.setIp(newMasterEndpoint.getHost());
		redisMeta.setPort(newMasterEndpoint.getPort());
		
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
	public void initPromotionState() {
		this.promotionState = PROMOTION_STATE.NORMAL;
	}

	@Override
	public KeeperState keeperState() {
		return KeeperState.ACTIVE;
	}


	@Override
	public boolean handleSlaveOf() {
		return true;
	}
}
