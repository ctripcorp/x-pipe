package com.ctrip.xpipe.redis.core.protocal.cmd;


import java.net.InetSocketAddress;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.RedisProtocol;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;

import io.netty.buffer.ByteBuf;

/**
 * @author marsqing
 *
 *         May 16, 2016 6:46:15 PM
 */
public abstract class AbstractKeeperCommand<T> extends AbstractRedisCommand<T> {
		
	public AbstractKeeperCommand(SimpleObjectPool<NettyClient> clientPool) {
		super(clientPool);
	}

	@Override
	public String getName() {
		return "keeper";
	}
	
	public static class KeeperGetStateCommand extends AbstractKeeperCommand<KeeperState>{

		public KeeperGetStateCommand(SimpleObjectPool<NettyClient> clientPool) {
			super(clientPool);
		}

		@Override
		protected KeeperState format(Object payload) {
			return KeeperState.valueOf(payloadToString(payload));
		}

		@Override
		protected ByteBuf getRequest() {
			return new RequestStringParser(getName(), "getstate").format();
		}
	}
	
	public static class KeeperSetStateCommand extends AbstractKeeperCommand<Boolean>{

		private KeeperState state;
		private InetSocketAddress masterAddress;
		
		public KeeperSetStateCommand(SimpleObjectPool<NettyClient> clientPool, KeeperState state, InetSocketAddress masterAddress) {
			super(clientPool);
			this.state = state;
			this.masterAddress = masterAddress;
		}

		@Override
		protected Boolean format(Object payload) {
			
			String ret = payloadToString(payload);
			if(ret.equalsIgnoreCase(RedisProtocol.OK) || ret.startsWith(RedisProtocol.OK)){
				return true;
			}
			return false;
		}

		@Override
		protected ByteBuf getRequest() {
			return new RequestStringParser(getName(), state.toString(), masterAddress.getHostName(), String.valueOf(masterAddress.getPort())).format();
		}
		
		
		@Override
		public String toString() {
			return String.format("%s %s %s", getName(), state.toString(), masterAddress.toString());
		}
	}
}