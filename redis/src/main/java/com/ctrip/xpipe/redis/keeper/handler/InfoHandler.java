package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.keeper.RedisClient;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 下午3:51:33
 */
public class InfoHandler extends AbstractCommandHandler{

	@Override
	public String[] getCommands() {
		return new String[]{"info"};
	}

	@Override
	protected void doHandle(String[] args, RedisClient redisClient) {
		
	}

}
