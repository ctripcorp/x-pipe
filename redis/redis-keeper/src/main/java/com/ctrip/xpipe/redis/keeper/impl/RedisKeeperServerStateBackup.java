package com.ctrip.xpipe.redis.keeper.impl;


import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;
import com.ctrip.xpipe.redis.keeper.handler.PsyncHandler;

/**
 * @author wenchao.meng
 *
 * Jun 8, 2016
 */
public class RedisKeeperServerStateBackup extends AbstractRedisKeeperServerState{

	public RedisKeeperServerStateBackup(RedisKeeperServer redisKeeperServer) {
		super(redisKeeperServer);
	}
	
	public RedisKeeperServerStateBackup(RedisKeeperServer redisKeeperServer, ShardStatus shardStatus) {
		super(redisKeeperServer, shardStatus);
	}


	
	@Override
	protected Endpoint doGetMaster(ShardStatus shardStatus) {

		KeeperMeta meta = shardStatus.getActiveKeeper();
		if(meta == null){
			return null;
		}
		return new DefaultEndPoint(meta.getIp(), meta.getPort());
	}

	@Override
	protected void becomeBackup() {
		//keeper master changed
		reconnectMaster();
	}

	@Override
	protected void becomeActive() {
		
		try {
			ReplicationStore replicationStore = redisKeeperServer.getReplicationStore();
			replicationStore.changeMetaTo(DefaultRedisKeeperServer.BACKUP_REPLICATION_STORE_REDIS_MASTER_META_NAME);
			redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer, getShardStatus()));
			reconnectMaster();
		} catch (IOException e) {
			logger.error("[becomeActive]" + this, e);
		}
	}


	@Override
	protected void keeperMasterChanged() {
		
		//nothing to do
		logger.info("[keeperMasterChanged][nothing to do]");
	}
	
	
	@Override
	public void setPromotionState(PROMOTION_STATE promotionState, Object promitionInfo) {
		throw new IllegalStateException("state backup, promotion unsupported!");
	}

	
	@Override
	public boolean sendKinfo() {
		return true;
	}

	@Override
	public boolean psync(RedisClient redisClient, String []args) {
		
		logger.info("[psync][server state backup, ask slave to wait]{}, {}", redisClient, this);
		
		redisClient.sendMessage(RedisProtocol.CRLF.getBytes());
		redisKeeperServer.addObserver(new PsyncKeeperServerStateObserver(args, redisClient));
		return false;
	}
	
	public static class PsyncKeeperServerStateObserver implements Observer, Releasable{
		
		private static Logger logger = LoggerFactory.getLogger(PsyncKeeperServerStateObserver.class);
		
		private String []args;
		private RedisClient redisClient;
		
		public PsyncKeeperServerStateObserver(String []args, RedisClient redisClient) {
			
			this.args = args;
			this.redisClient = redisClient;
		}
		
		@Override
		public void update(Object updateArgs, Observable observable) {
			
			logger.info("[update]{},{},{}", redisClient, updateArgs, observable);
			
			if(updateArgs instanceof KeeperServerStateChanged){
				new PsyncHandler().handle(args, redisClient);
				try {
					release();
				} catch (Exception e) {
					logger.error("[update]" + updateArgs+ "," + observable + "," + redisClient, e);
				}
			}
		}

		@Override
		public void release() throws Exception {
			this.redisClient.getRedisKeeperServer().removeObserver(this);
		}
	}

	@Override
	public boolean isActive() {
		return false;
	}
}
