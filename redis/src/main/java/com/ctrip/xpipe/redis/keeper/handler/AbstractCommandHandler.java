package com.ctrip.xpipe.redis.keeper.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.ctrip.xpipe.redis.keeper.CommandHandler;
import com.ctrip.xpipe.redis.keeper.RedisClient;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:47:27
 */
public abstract class AbstractCommandHandler implements CommandHandler{
	
	protected Logger logger = LogManager.getLogger(getClass());

	@Override
	public void handle(String[] args, RedisClient redisClient) {
		
		doHandle(args, redisClient);
	}

	protected abstract void doHandle(String[] args, RedisClient redisClient);
}
