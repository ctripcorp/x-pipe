package com.ctrip.xpipe.redis.keeper;


import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.meta.ShardStatus;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author wenchao.meng
 *
 * Jun 8, 2016
 */
public interface RedisKeeperServerState{
	
	void becomeActive(InetSocketAddress masterAddress);
	
	void becomeBackup(InetSocketAddress masterAddress);
	
	void setShardStatus(ShardStatus shardStatus) throws IOException;
	
	Endpoint getMaster();
	
	RedisKeeperServer getRedisKeeperServer();
	
	void setPromotionState(PROMOTION_STATE promotionState, Object promitionInfo) throws IOException;

	void setPromotionState(PROMOTION_STATE promotionState) throws IOException;
	
	void initPromotionState();
	
	boolean psync(RedisClient redisClient, String []args) throws Exception;
	
	KeeperState keeperState();

	void setMasterAddress(InetSocketAddress masterAddress);

	boolean handleSlaveOf();
	
}
