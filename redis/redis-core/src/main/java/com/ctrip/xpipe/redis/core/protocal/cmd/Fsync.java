package com.ctrip.xpipe.redis.core.protocal.cmd;


import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author marsqing
 *
 *         May 16, 2016 6:46:15 PM
 */
public class Fsync extends AbstractRedisCommand<String> {
	
	public static String SUCCESS_STRING = "CONTINUE";
	
	public Fsync(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
	}

	@Override
	public String getName() {
		return "fsync";
	}

	@Override
	public ByteBuf getRequest() {
		RequestStringParser requestString = new RequestStringParser(getName());
		return requestString.format();
	}

	@Override
	protected String format(Object payload) {
		
		return payloadToString(payload);
	}
}