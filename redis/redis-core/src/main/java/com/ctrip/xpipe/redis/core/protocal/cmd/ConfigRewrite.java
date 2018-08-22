package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Dec 9, 2016
 */
public class ConfigRewrite extends AbstractConfigCommand<String>{

	public ConfigRewrite(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
	}

	public ConfigRewrite(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int timeoutMilli) {
		super(clientPool, scheduled, timeoutMilli);
	}

	@Override
	protected String format(Object payload) {
		return payloadToString(payload);
	}

	@Override
	public ByteBuf getRequest() {
		return new RequestStringParser(CONFIG, REDIS_CONFIG_TYPE.REWRITE.getConfigName()).format();
	}
}
