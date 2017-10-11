package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Nov 30, 2016
 */
public class PingCommand extends AbstractRedisCommand<String>{
	
	public static int DEFAULT_PINT_TIME_OUT_MILLI = 500;
	
	public static final String PING = "PING";

	public static final String PONG = "PONG";

	public PingCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
	}

	@Override
	public String getName() {
		return "ping cmd";
	}

	@Override
	protected String format(Object payload) {
		return payloadToString(payload);
	}

	@Override
	public ByteBuf getRequest() {
		
		return new RequestStringParser(PING).format();
	}
	
	@Override
	public int getCommandTimeoutMilli() {
		return DEFAULT_PINT_TIME_OUT_MILLI;
	}

}
