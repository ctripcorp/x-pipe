package com.ctrip.xpipe.redis.keeper.impl;

import java.io.File;
import java.util.concurrent.ExecutionException;

import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;

/**
 * @author wenchao.meng
 *
 * Aug 25, 2016
 */
public class RedisMasterReplicationRdbDumper extends AbstractRdbDumper{

	public RedisMasterReplicationRdbDumper(RedisKeeperServer redisKeeperServer) {
		super(redisKeeperServer);
	}

	@Override
	protected void doExecute() throws Exception {
		//nothing to do
	}

	@Override
	protected void doReset() throws InterruptedException, ExecutionException {
		
	}
	
	@Override
	protected void doCancel() {
		super.doCancel();
		throw new IllegalStateException("can not cancel");
	}

	@Override
	public File prepareRdbFile() {
		throw new UnsupportedOperationException();
	}
}
