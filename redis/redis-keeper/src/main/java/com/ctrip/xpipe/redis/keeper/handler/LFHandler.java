package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisSlave;

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
	protected void doHandle(String[] args, RedisClient<?> redisClient) {
		
		if(redisClient instanceof RedisSlave){
			
			if(logger.isDebugEnabled()){
				logger.debug("[doHandle][\\n get]" + redisClient);
			}
			RedisSlave redisSlave = (RedisSlave) redisClient;
			redisSlave.ack(null);
		}
	}

	@Override
	public boolean isLog(String[] args) {
		return false;
	}
}
