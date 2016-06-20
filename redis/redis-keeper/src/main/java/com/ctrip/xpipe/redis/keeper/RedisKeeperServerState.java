package com.ctrip.xpipe.redis.keeper;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.observer.Observer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;

/**
 * @author wenchao.meng
 *
 * Jun 8, 2016
 */
public interface RedisKeeperServerState extends Observer{
	
	Endpoint getMaster();
	
	RedisKeeperServer getRedisKeeperServer();
	
	void setPromotionState(PROMOTION_STATE promotionState, Object promitionInfo);

	void setPromotionState(PROMOTION_STATE promotionState);
	
	boolean sendKinfo();
	
	boolean psync(RedisClient redisClient, String []args);
	
	boolean isActive();
}
