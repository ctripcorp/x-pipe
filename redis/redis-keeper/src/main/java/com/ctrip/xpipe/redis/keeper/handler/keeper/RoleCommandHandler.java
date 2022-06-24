package com.ctrip.xpipe.redis.keeper.handler.keeper;


import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.protocal.protocal.ParserManager;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import com.ctrip.xpipe.redis.keeper.handler.AbstractCommandHandler;

/**
 * @author wenchao.meng
 *
 * Sep 14, 2016
 */
public class RoleCommandHandler extends AbstractCommandHandler {

	@Override
	public String[] getCommands() {
		return new String[]{"role"};
	}

	@Override
	protected void doHandle(String[] args, RedisClient<?> redisClient) {
		
		RedisKeeperServer redisKeeperServer = (RedisKeeperServer) redisClient.getRedisServer();
		ReplicationStore replicationStore = redisKeeperServer.getReplicationStore();
		RedisMaster redisMaster = redisKeeperServer.getRedisMaster();
		Endpoint    masterEndPoint = null;
		
		if(redisMaster != null){
			masterEndPoint = redisMaster.masterEndPoint();
		}
		
		Object [] result = new Object[5];
		result[0] = redisKeeperServer.role().toString();
		result[1] = masterEndPoint == null ? "0.0.0.0": masterEndPoint.getHost();
		result[2] = masterEndPoint == null ? "0": masterEndPoint.getPort();
		result[3] = redisMaster == null ? MASTER_STATE.REDIS_REPL_NONE.getDesc(): redisMaster.getMasterState().getDesc();
		result[4] = replicationStore == null ? -1L: replicationStore.getEndOffset();
		redisClient.sendMessage(ParserManager.parse(result));
	}

	@Override
	public boolean support(RedisServer server) {
		return server instanceof RedisKeeperServer;
	}

}
