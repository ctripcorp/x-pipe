package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.keeper.RedisClient;

/**
 * @author wenchao.meng
 *
 * 2016年4月25日 下午4:45:08
 */
public class PingCommandHandler extends AbstractCommandHandler{

	@Override
	public String[] getCommands() {
		return new String[]{"ping"};
	}

	@Override
	protected void doHandle(String[] args, RedisClient redisClient) {
		
		redisClient.sendMessage("+PONG\r\n".getBytes());
	}

}
