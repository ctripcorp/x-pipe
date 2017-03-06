package com.ctrip.xpipe.redis.keeper.handler;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.ctrip.xpipe.redis.keeper.CommandHandler;
import com.ctrip.xpipe.redis.keeper.RedisClient;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:47:27
 */
public abstract class AbstractCommandHandler implements CommandHandler{
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void handle(String[] args, RedisClient redisClient) throws Exception {
		doHandle(args, redisClient);
	}

	protected abstract void doHandle(String[] args, RedisClient redisClient) throws Exception;
	
	@Override
	public boolean isLog(String[] args) {
		return true;
	}
}
