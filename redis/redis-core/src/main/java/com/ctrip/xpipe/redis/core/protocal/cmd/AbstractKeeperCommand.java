package com.ctrip.xpipe.redis.core.protocal.cmd;

import com.ctrip.xpipe.api.pool.SimpleObjectPool;
import com.ctrip.xpipe.netty.commands.NettyClient;
import com.ctrip.xpipe.proxy.ProxyEndpoint;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.RouteMeta;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.protocal.RequestStringParser;
import com.ctrip.xpipe.tuple.Pair;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author marsqing
 *
 *         May 16, 2016 6:46:15 PM
 */
public abstract class AbstractKeeperCommand<T> extends AbstractRedisCommand<T> {
	
	public static String GET_STATE = "getstate";

	public static String SET_STATE = "setstate";

	
	public AbstractKeeperCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
		super(clientPool, scheduled);
	}

	public AbstractKeeperCommand(KeeperMeta keeperMeta, ScheduledExecutorService scheduled){
		super(keeperMeta.getIp(), keeperMeta.getPort(), scheduled);
	}
	
	@Override
	public String getName() {
		return "keeper";
	}
	
	public static class KeeperGetStateCommand extends AbstractKeeperCommand<KeeperState>{

		public KeeperGetStateCommand(SimpleObjectPool<NettyClient> clientPool, ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
		}
		
		public KeeperGetStateCommand(KeeperMeta keeperMeta, ScheduledExecutorService scheduled) {
			super(keeperMeta, scheduled);
		}

		@Override
		protected KeeperState format(Object payload) {
			return KeeperState.valueOf(payloadToString(payload));
		}

		@Override
		public ByteBuf getRequest() {
			return new RequestStringParser(getName(), GET_STATE).format();
		}
	}
	
	public static class KeeperSetStateCommand extends AbstractKeeperCommand<String>{

		private KeeperState state;
		private Pair<String, Integer> masterAddress;
		private RouteMeta routeMeta;

		public KeeperSetStateCommand(SimpleObjectPool<NettyClient> clientPool,
									 KeeperState state,
									 Pair<String, Integer> masterAddress,
									 ScheduledExecutorService scheduled) {
			this(clientPool, state, masterAddress, null, scheduled);
		}

		public KeeperSetStateCommand(SimpleObjectPool<NettyClient> clientPool,
									 KeeperState state,
									 Pair<String, Integer> masterAddress,
									 RouteMeta routeMeta,
									 ScheduledExecutorService scheduled) {
			super(clientPool, scheduled);
			this.state = state;
			this.masterAddress = masterAddress;
			this.routeMeta = routeMeta;
		}

		public KeeperSetStateCommand(KeeperMeta keeperMeta,
									 KeeperState state,
									 Pair<String, Integer> masterAddress,
									 ScheduledExecutorService scheduled) {
			this(keeperMeta, state, masterAddress, null, scheduled);
		}

		public KeeperSetStateCommand(KeeperMeta keeperMeta,
									 KeeperState state,
									 Pair<String, Integer> masterAddress,
									 RouteMeta routeMeta,
									 ScheduledExecutorService scheduled) {
			super(keeperMeta, scheduled);
			this.state = state;
			this.masterAddress = masterAddress;
			this.routeMeta = routeMeta;
		}


		@Override
		protected String format(Object payload) {
			
			return payloadToString(payload);
		}

		@Override
		public ByteBuf getRequest() {
			return new RequestStringParser(
					getName(),
					SET_STATE,
					state.toString(),
					masterAddress.getKey(), String.valueOf(masterAddress.getValue()),
					routeMeta == null?"":(routeMeta.routeProtocol() + " " + ProxyEndpoint.PROXY_SCHEME.TCP.name())
			).format();
		}
		
		
		@Override
		public String toString() {
			return String.format("(to:%s) %s %s %s %s %s", getClientPool().desc(), getName(), SET_STATE, state.toString(), masterAddress.getKey(), masterAddress.getValue());
		}
	}
}