package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author wenchao.meng
 *
 * Dec 2, 2016
 */
public abstract class ConfigGetCommand<T> extends AbstractConfigCommand<T>{

	public ConfigGetCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
	}

	public ConfigGetCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, int commandTimeoutMilli) {
		super(clientPool, scheduled, commandTimeoutMilli);
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
		return new RequestStringParser(CONFIG, "get", getConfigName()).format();
	}
	
	protected abstract String getConfigName();


	public static class ConfigGetMinSlavesToWrite extends ConfigGetCommand<Integer>{

		public ConfigGetMinSlavesToWrite(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
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

	public static abstract class ConfigGetBool extends ConfigGetCommand<Boolean> {

		public ConfigGetBool(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
		}

		public ConfigGetBool(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
								  int commandTimeoutMilli) {
			super(clientPool, scheduled, commandTimeoutMilli);
		}

		protected Boolean defaultValue() {
			return null;
		}

		@Override
		protected Boolean doFormat(Object[] payload) {

			if(payload.length < 2){
				if (null != defaultValue()) return defaultValue();
				throw new IllegalStateException(getName() + " result length not right:" + payload.length);
			}
			String result = payloadToString(payload[1]);
			if(result.equalsIgnoreCase("yes")){
				return true;
			}
			if(result.equalsIgnoreCase("no")){
				return false;
			}
			if (null != defaultValue()) return defaultValue();
			throw new IllegalStateException("expected yes or no, but:" + result);
		}

	}

	public static class ConfigGetRordbSync extends ConfigGetBool {

		public ConfigGetRordbSync(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
		}

		public ConfigGetRordbSync(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
									 int commandTimeoutMilli) {
			super(clientPool, scheduled, commandTimeoutMilli);
		}

		@Override
		protected Boolean defaultValue() {
			return false;
		}

		@Override
		protected String getConfigName() {
			return REDIS_CONFIG_TYPE.RORDB_SYNC.getConfigName();
		}

	}

	public static class ConfigGetDisklessSync extends ConfigGetBool {

		public ConfigGetDisklessSync(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
		}

		public ConfigGetDisklessSync(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled,
									 int commandTimeoutMilli) {
			super(clientPool, scheduled, commandTimeoutMilli);
		}

		@Override
		protected String getConfigName() {
			return REDIS_CONFIG_TYPE.DISKLESS_SYNC.getConfigName();
		}
	}

	public static class ConfigGetDisklessSyncDelay extends ConfigGetCommand<Integer>{

		public ConfigGetDisklessSyncDelay(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
		}

		@Override
		protected Integer doFormat(Object[] payload) {
			
			if(payload.length < 2){
				throw new IllegalStateException(getName() + " result length not right:" + payload.length);
			}
			return payloadToInteger(payload[1]);
		}


		@Override
		protected String getConfigName() {
			return REDIS_CONFIG_TYPE.DISKLESS_SYNC_DELAY.getConfigName();
		}
	}

	public static class ConfigGetAnyCommand extends ConfigGetCommand<String> {

		private String args;

		public ConfigGetAnyCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled, String args) {
			super(clientPool, scheduled);
			this.args = args;
		}


		@Override
		protected String doFormat(Object[] payload) {
			if(payload.length < 2){
				throw new IllegalStateException(getName() + " result length not right:" + payload.length);
			}
			return payloadToString(payload[1]);
		}

		@Override
		protected String getConfigName() {
			return args;
		}

		@Override
		public String getName() {
			return super.getName() + args;
		}
	}
}
