package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.ReplicationStoreMeta;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;
import com.ctrip.xpipe.redis.keeper.entity.KeeperMeta;
import com.ctrip.xpipe.redis.keeper.entity.RedisMeta;
import com.ctrip.xpipe.redis.keeper.handler.SlaveOfCommandHandler.SlavePromotionInfo;

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
	
	public RedisKeeperServerStateActive(RedisKeeperServer redisKeeperServer, KeeperMeta  activeKeeperMeta, RedisMeta masterRedisMeta) {
		super(redisKeeperServer, activeKeeperMeta, masterRedisMeta);
	}

	@Override
	public Endpoint getMaster() {

		RedisMeta meta = getMasterRedisMeta();
		if(meta == null){
			return null;
		}
		return new DefaultEndPoint(meta.getIp(), meta.getPort());
	}

	@Override
	protected void becomeBackup() {
		//active changed!
		try {
			logger.info("[becomeBackup][active->backup] {}", this);
			redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateBackup(redisKeeperServer, getActiveKeeperMeta(), getMasterRedisMeta()));
			redisKeeperServer.getReplicationStore().changeMetaToKeeper();
			redisKeeperServer.reconnectMaster();
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
	protected void masterRedisMetaChanged() {
		redisKeeperServer.reconnectMaster();
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
			case COMMANDS_SEND_FINISH:
				break;
			case SLAVE_PROMTED:
				SlavePromotionInfo promotionInfo = (SlavePromotionInfo) info;
				masterChanged(promotionInfo.getKeeperOffset(), promotionInfo.getNewMasterEndpoint()
							, promotionInfo.getNewMasterRunid(), promotionInfo.getNewMasterReplOffset());
				break;
			default:
				throw new IllegalStateException("unkonow state:" + promotionState);
		}
		
	}
	
	public void masterChanged(long keeperOffset, DefaultEndPoint newMasterEndpoint, String newMasterRunid, long newMasterReplOffset) {
		
		ReplicationStore replicationStore = redisKeeperServer.getReplicationStore();
		ReplicationStoreMeta meta = replicationStore.getReplicationStoreMeta();
		long delta = (meta.getKeeperBeginOffset() - meta.getBeginOffset()) + newMasterReplOffset - keeperOffset;
		replicationStore.masterChanged(newMasterEndpoint, newMasterRunid, delta);
		
		RedisMeta redisMeta = new RedisMeta();
		redisMeta.setIp(newMasterEndpoint.getHost());
		redisMeta.setPort(newMasterEndpoint.getPort());
		redisMeta.setMaster(true);
		
		this.setRedisMasterMeta(redisMeta);//invoke reconnect with master
	}

	@Override
	public boolean psync(RedisClient redisClient, String []args) {
		return true;
	}
}
