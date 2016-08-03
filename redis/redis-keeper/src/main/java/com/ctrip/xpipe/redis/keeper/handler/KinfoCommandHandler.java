/**
 * 
 */
package com.ctrip.xpipe.redis.keeper.handler;


import com.alibaba.fastjson.JSON;
import com.ctrip.xpipe.redis.core.protocal.protocal.BulkStringParser;
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

		String result = JSON.toJSONString(keeper.getReplicationStore().getMetaStore().dupReplicationStoreMeta());

		redisClient.sendMessage(new BulkStringParser(result).format());
	}

}
