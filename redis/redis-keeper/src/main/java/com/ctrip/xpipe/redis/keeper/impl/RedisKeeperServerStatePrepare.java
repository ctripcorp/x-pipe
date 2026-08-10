package com.ctrip.xpipe.redis.keeper.impl;


import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.error.NoMasterlinkRedisError;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer.PROMOTION_STATE;

import java.io.IOException;

/**
 * PREPARE: store lease released; no master reconnect / no PSYNC serve.
 * Re-enter ACTIVE/BACKUP via {@link RedisKeeperServer#doReenterFromPrepare} (T-R.9).
 *
 * @see com.ctrip.xpipe.redis.core.meta.KeeperState#PREPARE
 */
public class RedisKeeperServerStatePrepare extends AbstractRedisKeeperServerState {

	public RedisKeeperServerStatePrepare(RedisKeeperServer redisKeeperServer) {
		super(redisKeeperServer);
	}

	public RedisKeeperServerStatePrepare(RedisKeeperServer redisKeeperServer, Endpoint masterAddress) {
		super(redisKeeperServer, masterAddress);
	}

	@Override
	public void becomePrepare(Endpoint masterAddress) {
		logger.info("[becomePrepare][already prepare]{}", masterAddress);
		this.masterAddress = masterAddress;
	}

	@Override
	public void becomeActive(Endpoint masterAddress) {
		logger.info("[becomeActive][prepare->active]{}", masterAddress);
		redisKeeperServer.doReenterFromPrepare(masterAddress, true);
	}

	@Override
	public void becomeBackup(Endpoint masterAddress) {
		logger.info("[becomeBackup][prepare->backup]{}", masterAddress);
		redisKeeperServer.doReenterFromPrepare(masterAddress, false);
	}

	@Override
	public void setPromotionState(PROMOTION_STATE promotionState, Object promitionInfo) throws IOException {
		throw new IllegalStateException("state prepare, promotion unsupported!");
	}

	@Override
	public boolean psync(RedisClient redisClient, String[] args) throws Exception {
		throw new NoMasterlinkRedisError("keeper state :" + keeperState());
	}

	@Override
	public KeeperState keeperState() {
		return KeeperState.PREPARE;
	}

	@Override
	protected void keeperMasterChanged() {
		logger.info("[keeperMasterChanged][prepare][no reconnect]{}", masterAddress);
	}
}
