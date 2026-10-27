/**
 * 
 */
package com.ctrip.xpipe.redis.keeper.handler.keeper;


import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.protocal.protocal.CommandBulkStringParser;
import com.ctrip.xpipe.redis.core.protocal.protocal.RedisErrorParser;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;

/**
 * @author marsqing
 *
 *         Jun 1, 2016 11:08:30 AM
 */
public class KinfoCommandHandler extends AbstractCommandHandler {

	@Override
	public String[] getCommands() {
		return new String[] { "kinfo" };
	}

	@Override
	protected void doHandle(String[] args, RedisClient redisClient) {
		RedisKeeperServer keeper = (RedisKeeperServer) redisClient.getRedisServer();

		// get semantics (m6 spec §9.3): never constructs a store
		ReplicationStore replicationStore = keeper.getReplicationStore();
		if (replicationStore == null) {
			logger.info("[doHandle][{}]{}", keeper.getReplId(), RedisKeeperServer.ERR_REPLICATION_STORE_NOT_OPENED);
			redisClient.sendMessage(new RedisErrorParser(RedisKeeperServer.ERR_REPLICATION_STORE_NOT_OPENED).format());
			return;
		}
		String result = Codec.DEFAULT.encode(replicationStore.getMetaStore().dupReplicationStoreMeta());

		logger.info("[doHandle]{}", result);
		redisClient.sendMessage(new CommandBulkStringParser(result).format());
	}

	@Override
	public boolean support(RedisServer server) {
		return server instanceof RedisKeeperServer;
	}

}
