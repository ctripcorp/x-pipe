package com.ctrip.xpipe.redis.keeper.impl;


import java.io.IOException;
import java.net.InetSocketAddress;

import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;

/**
 * @author wenchao.meng
 *
 * Aug 22, 2016
 */
public class RedisKeeperServerStatePreActive extends AbstractRedisKeeperServerState{

	//do not set master address
	public RedisKeeperServerStatePreActive(RedisKeeperServer redisKeeperServer) {
		super(redisKeeperServer);
	}

	@Override
	public void becomeActive(InetSocketAddress masterAddress) {
		logger.info("[becomeActive]{}", masterAddress);
		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer, masterAddress));
		reconnectMaster();
	}

	@Override
	public void becomeBackup(InetSocketAddress masterAddress){
		
		logger.info("[becomeBackup]{}", masterAddress);
		activeToBackup(masterAddress);
	}

	@Override
	public void setPromotionState(PROMOTION_STATE promotionState, Object promitionInfo) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean psync(RedisClient redisClient, String[] args) {
		throw new UnsupportedOperationException();
	}

	@Override
	public KeeperState keeperState() {
		return KeeperState.PRE_ACTIVE;
	}

	@Override
	protected void keeperMasterChanged() {
		reconnectMaster();
	}

}
