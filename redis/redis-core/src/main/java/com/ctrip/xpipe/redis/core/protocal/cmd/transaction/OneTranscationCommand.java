package com.ctrip.xpipe.redis.core.protocal.cmd.transaction;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.RedisCommand;
import com.ctrip.xpipe.redis.core.protocal.cmd.AbstractRedisCommand;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Dec 8, 2016
 */
@SuppressWarnings("rawtypes") 
public class OneTranscationCommand extends AbstractRedisCommand<String>{

	public static final String SUCCESS_STRING = "QUEUED";

	private RedisCommand redisCommand;
	
	public OneTranscationCommand(SimpleObjectPool<NettyClient> clientPool, RedisCommand redisCommand, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
		this.redisCommand = redisCommand;
	}


	@Override
	public String getName() {
		return String.format("transaction(%s)", redisCommand);
	}

	@Override
	protected String format(Object payload) {
		
		return payloadToString(payload);
	}

	@Override
	public ByteBuf getRequest() {
		return redisCommand.getRequest();
	}
	

}
