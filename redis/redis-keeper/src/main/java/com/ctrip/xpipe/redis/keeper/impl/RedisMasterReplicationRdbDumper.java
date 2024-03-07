package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMasterReplication;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 * Aug 25, 2016
 */
public class RedisMasterReplicationRdbDumper extends AbstractRdbDumper{

	private RedisMasterReplication redisMasterReplication;
	public RedisMasterReplicationRdbDumper(RedisMasterReplication redisMasterReplication,
										   RedisKeeperServer redisKeeperServer, KeeperResourceManager resourceManager) {
		super(redisKeeperServer);
		this.redisMasterReplication = redisMasterReplication;
	}

	@Override
	public boolean tryRordb() {
		return redisMasterReplication.tryRordb();
	}

	@Override
	protected void doExecute() throws Exception {
		//nothing to do
	}

	@Override
	protected void doCancel() {
		super.doCancel();
		throw new IllegalStateException("can not cancel");
	}

	@Override
	public DumpedRdbStore prepareRdbStore() throws IOException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public String toString() {
		return String.format("%s", redisMasterReplication);
	}

}
