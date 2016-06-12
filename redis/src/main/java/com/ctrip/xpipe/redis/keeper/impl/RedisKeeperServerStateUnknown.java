package com.ctrip.xpipe.redis.keeper.impl;

import java.io.IOException;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;

/**
 * @author wenchao.meng
 *
 * Jun 8, 2016
 */
public class RedisKeeperServerStateUnknown extends AbstractRedisKeeperServerState{

	public RedisKeeperServerStateUnknown(RedisKeeperServer redisKeeperServer) {
		super(redisKeeperServer);
	}

	@Override
	public Endpoint getMaster() {
		return null;
	}

	@Override
	protected void becomeBackup() {
		
		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateBackup(redisKeeperServer, getActiveKeeperMeta(), getMasterRedisMeta()));
		redisKeeperServer.reconnectMaster();
	}

	@Override
	protected void becomeActive() {
		
		redisKeeperServer.setRedisKeeperServerState(new RedisKeeperServerStateActive(redisKeeperServer, getActiveKeeperMeta(), getMasterRedisMeta()));
		redisKeeperServer.reconnectMaster();
	}

	@Override
	protected void masterRedisMetaChanged() {
		//nothing to do
		logger.info("[masterRedisMetaChanged][nothing to do]");
	}

	@Override
	public void setPromotionState(PROMOTION_STATE promotionState, Object promitionInfo) {
		throw new IllegalStateException("state unknown, promotion unsupported!");
	}

	@Override
	public boolean psync(RedisClient redisClient, String[] args) {
		
		logger.error("[psync][server state unknown, close connection.]" + redisClient + "," + this);
		try {
			redisClient.close();
		} catch (IOException e) {
			logger.error("[doHandle][close redisClient]" + redisClient, e);
		}
		
		return false;
	}

}
