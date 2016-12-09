package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;

import io.netty.buffer.ByteBuf;

/**
 * @author wenchao.meng
 *
 * Dec 2, 2016
 */
public abstract class ConfigGetCommand<T> extends AbstractConfigCommand<T>{
	
	
	public ConfigGetCommand(SimpleObjectPool<NettyClient> clientPool) {
		super(clientPool);
	}

	@Override
	public String getName() {
		return getClass().getSimpleName();
	}
	
	@Override
	protected T format(Object payload) {
		
		return doFormat((Object[])payload);
	}

	protected abstract T doFormat(Object[] payload);

	@Override
	public ByteBuf getRequest() {
		return new RequestStringParser("config get " + getConfigName()).format();
	}
	
	protected abstract String getConfigName();


	public static class ConfigGetMinSlavesToWrite extends ConfigGetCommand<Integer>{

		public ConfigGetMinSlavesToWrite(SimpleObjectPool<NettyClient> clientPool) {
			super(clientPool);
		}

		@Override
		protected Integer doFormat(Object[] payload) {
			
			if(payload.length < 2){
				throw new IllegalStateException(getName() + " result length not right:" + payload.length);
			}
			return (Integer) payloadToInteger(payload[1]);
		}


		@Override
		protected String getConfigName() {
			return REDIS_CONFIG_TYPE.MIN_SLAVES_TO_WRITE.getConfigName();
		}


	}

}
