package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;

/**
 * @author wenchao.meng
 *
 * Dec 2, 2016
 */
public abstract class AbstractConfigCommand<T> extends AbstractRedisCommand<T>{

	public AbstractConfigCommand(SimpleObjectPool<NettyClient> clientPool) {
		super(clientPool);
	}
	
	public static enum REDIS_CONFIG_TYPE{
		
		MIN_SLAVES_TO_WRITE("min-slaves-to-write")
		;
		
		private String configName;
		
		REDIS_CONFIG_TYPE(String configName){
			this.configName = configName;
		}
		
		public String getConfigName() {
			return configName;
		}
		
	}

}
