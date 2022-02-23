package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Dec 2, 2016
 */
public abstract class ConfigSetCommand<T> extends AbstractConfigCommand<T>{
	
	public ConfigSetCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
	}

	@Override
	public String getName() {
		return getClass().getSimpleName();
	}

	@Override
	public ByteBuf getRequest() {
		return new RequestStringParser(CONFIG, " set " + getConfigName() + " " + getValue()).format();
	}
	
	protected abstract String getValue();

	protected abstract String getConfigName();


	public static class ConfigSetMinSlavesToWrite extends ConfigSetCommand<Boolean>{

		private int minSlavesToWrite;

		public ConfigSetMinSlavesToWrite(SimpleObjectPool<NettyClient> clientPool, int minSlavesToWrite, ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
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

	public static class ConfigSetSlaveReadOnly extends ConfigSetCommand<Boolean>{

		private boolean readonly = false;

		public ConfigSetSlaveReadOnly(boolean readonly, SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
			this.readonly = readonly;
		}

		@Override
		protected String getValue() {
			return RedisProtocol.booleanToString(readonly);
		}

		public void setReadonly(boolean readonly) {
			this.readonly = readonly;
		}

		@Override
		protected String getConfigName() {
			return REDIS_CONFIG_TYPE.SLAVE_READONLY.getConfigName();
		}

		@Override
		protected Boolean format(Object payload) {
			String response = payloadToString(payload);
			return RedisProtocol.OK.equalsIgnoreCase(response);
		}
	}

	public static class ConfigSetReplAll extends ConfigSetCommand<Boolean>{

		private boolean replall = false;

		public ConfigSetReplAll(boolean replall, SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
			this.replall  = replall;
		}

		@Override
		protected String getValue() {
			return RedisProtocol.booleanToString(replall);
		}

		public void setReplall(boolean replall) {
			this.replall = replall;
		}

		@Override
		protected String getConfigName() {
			return REDIS_CONFIG_TYPE.SLAVE_REPL_ALL.getConfigName();
		}

		@Override
		protected Boolean format(Object payload) {
			String response = payloadToString(payload);
			return RedisProtocol.OK.equalsIgnoreCase(response);
		}
	}


}
