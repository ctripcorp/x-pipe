package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * Dec 2, 2016
 */
public abstract class ConfigSetCommand<T> extends AbstractConfigCommand<T>{
	
	
	public ConfigSetCommand(SimpleObjectPool<NettyClient> clientPool) {
		super(clientPool);
	}

	@Override
	public String getName() {
		return getClass().getSimpleName();
	}

	@Override
	public ByteBuf getRequest() {
		return new RequestStringParser("config set " + getConfigName() + " " + getValue()).format();
	}
	
	protected abstract String getValue();

	protected abstract String getConfigName();


	public static class ConfigSetMinSlavesToWrite extends ConfigSetCommand<Boolean>{

		private int minSlavesToWrite;
		
		public ConfigSetMinSlavesToWrite(SimpleObjectPool<NettyClient> clientPool, int minSlavesToWrite) {
			super(clientPool);
			this.minSlavesToWrite = minSlavesToWrite;
		}

		@Override
		protected String getConfigName() {
			return REDIS_CONFIG_TYPE.MIN_SLAVES_TO_WRITE.getConfigName();
		}

		@Override
		protected Boolean format(Object payload) {
			
			String response = payloadToString(payload);
			return RedisProtocol.OK.equalsIgnoreCase(response);
		}
		
		@Override
		public String getName() {
			return String.format("%s %d", super.getName(), minSlavesToWrite);
		}

		@Override
		protected String getValue() {
			return String.valueOf(minSlavesToWrite);
		}

	}

}
