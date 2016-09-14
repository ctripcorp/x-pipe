package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.core.protocal.protocal.ParserManager;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;

/**
 * @author wenchao.meng
 *
 * Sep 14, 2016
 */
public class RoleCommandHandler extends AbstractCommandHandler{

	@Override
	public String[] getCommands() {
		return new String[]{"role"};
	}

	@Override
	protected void doHandle(String[] args, RedisClient redisClient) {
		
		RedisKeeperServer redisKeeperServer = redisClient.getRedisKeeperServer();
		RedisMaster redisMaster = redisKeeperServer.getRedisMaster();
		
		Object [] result = new Object[5];
		result[0] = redisKeeperServer.role().toString();
		result[1] = redisMaster.masterEndPoint().getHost();
		result[2] = redisMaster.masterEndPoint().getPort();
		result[3] = redisMaster.getMasterState().getDesc();
		result[4] = redisKeeperServer.getReplicationStore().getEndOffset();
		redisClient.sendMessage(ParserManager.parse(result));
	}

}
