package com.ctrip.xpipe.redis.keeper.handler;

import com.ctrip.xpipe.redis.keeper.CommandHandler;
import com.ctrip.xpipe.redis.keeper.RedisClient;
import com.ctrip.xpipe.redis.keeper.RedisServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wenchao.meng
 *
 * 2016年4月22日 上午11:47:27
 */
public abstract class AbstractCommandHandler implements CommandHandler {
	
	protected Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void handle(String[] args, RedisClient<?> redisClient) throws Exception {
		doHandle(args, redisClient);
	}

	/**
	 * when implements this method, should not block or sleep
	 * @param args
	 * @param redisClient
	 * @throws Exception
	 */
	protected abstract void doHandle(String[] args, RedisClient<?> redisClient) throws Exception;
	
	@Override
	public boolean isLog(String[] args) {
		return true;
	}

	@Override
	public boolean support(RedisServer server) {
		return true;
	}
}
