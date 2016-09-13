package com.ctrip.xpipe.redis.keeper.impl;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.observer.Observable;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
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
	
	public RedisKeeperServerStateBackup(RedisKeeperServer redisKeeperServer, InetSocketAddress masterAddress) {
		super(redisKeeperServer, masterAddress);
	}



	@Override
	public void becomeBackup(InetSocketAddress masterAddress) {
		setMasterAddress(masterAddress);
	}

	@Override
	public void becomeActive(InetSocketAddress masterAddress) {
		
		logger.info("[becomeActive]{}", masterAddress);
		backupToActive(masterAddress);
	}

	@Override
	protected void keeperMasterChanged() {
		reconnectMaster();
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
	public KeeperState keeperState() {
		return KeeperState.BACKUP;
	}
}
