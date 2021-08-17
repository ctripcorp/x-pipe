/**
 * 
 */
package com.ctrip.xpipe.redis.keeper.handler;


import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.protocal.protocal.CommandBulkStringParser;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;

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
		RedisKeeperServer keeper = redisClient.getRedisKeeperServer();

		String result = Codec.DEFAULT.encode(keeper.getReplicationStore().getMetaStore().dupReplicationStoreMeta());

		logger.info("[doHandle]{}", result);
		redisClient.sendMessage(new CommandBulkStringParser(result).format());
	}

}
