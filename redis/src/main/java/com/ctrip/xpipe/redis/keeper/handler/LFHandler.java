package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisClient.CLIENT_ROLE;

/**
 * @author wenchao.meng
 *
 * 2016年4月26日 上午10:51:52
 */
public class LFHandler extends AbstractCommandHandler{

	@Override
	public String[] getCommands() {
		return new String[]{"\n"};
	}

	@Override
	protected void doHandle(String[] args, RedisClient redisClient) {
		
		if(redisClient.getClientRole() == CLIENT_ROLE.SLAVE){
			
			if(logger.isDebugEnabled()){
				logger.debug("[doHandle][\\n get]" + redisClient);
			}
			redisClient.ack(null);
		}
	}
}
