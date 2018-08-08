
package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.proxy.ProxyResourceManager;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisMasterReplication;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
/**
 * @author wenchao.meng
 *
 *         May 22, 2016 6:36:21 PM
 */
public class DefaultRedisMaster extends AbstractLifecycle implements RedisMaster {

	private RedisKeeperServer redisKeeperServer;

	private ReplicationStoreManager replicationStoreManager;

	private Endpoint endpoint;
	
	private AtomicBoolean isKeeper = new AtomicBoolean(false);
	
	private ScheduledExecutorService scheduled;
	
	private MASTER_STATE masterState = MASTER_STATE.REDIS_REPL_NONE;
	
	private RedisMasterReplication redisMasterReplication;

	private NioEventLoopGroup nioEventLoopGroup;

	private ProxyResourceManager endpointManager;

	public DefaultRedisMaster(RedisKeeperServer redisKeeperServer, DefaultEndPoint endpoint, NioEventLoopGroup nioEventLoopGroup,
							  ReplicationStoreManager replicationStoreManager, ScheduledExecutorService scheduled,
							  ProxyResourceManager proxyResourceManager) {

		this.redisKeeperServer = redisKeeperServer;
		this.replicationStoreManager = replicationStoreManager;
		this.nioEventLoopGroup = nioEventLoopGroup;
		this.endpoint = endpoint;
		this.scheduled = scheduled;
		this.endpointManager = proxyResourceManager;
		redisMasterReplication = new DefaultRedisMasterReplication(this, this.redisKeeperServer, nioEventLoopGroup,
				this.scheduled, proxyResourceManager);
	}
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		redisMasterReplication.initialize();
	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		redisMasterReplication.start();
	}

	@Override
	protected void doStop() throws Exception {
		redisMasterReplication.stop();
		super.doStop();
	}
	
	@Override
	protected void doDispose() throws Exception {
		
		redisMasterReplication.dispose();
		super.doDispose();
	}

	@Override
	public ReplicationStoreManager getReplicationStoreManager() {
		return replicationStoreManager;
	}
	
	@Override
	public ReplicationStore getCurrentReplicationStore() {

		return redisKeeperServer.getReplicationStore();
	}


	@Override
	public Endpoint masterEndPoint() {
		return this.endpoint;
	}

	@Override
	public PARTIAL_STATE partialState() {
		return redisMasterReplication.partialState();
	}

	
	@Override
	public RdbDumper createRdbDumper() throws CreateRdbDumperException {
		
		if(masterState != MASTER_STATE.REDIS_REPL_CONNECTED){
			logger.info("[createRdbDumper][master state not connected, dumper not allowed]{}", redisMasterReplication);
			throw new CreateRdbDumperException(this, "master state not connected, dumper not allowed:" + masterState);
		}
		return new RedisMasterNewRdbDumper(this, redisKeeperServer, nioEventLoopGroup, scheduled, endpointManager);
	}
	
	public MASTER_STATE getMasterState() {
		return masterState;
	}
	
	public void setMasterState(MASTER_STATE masterState) {
		
		logger.info("[setMasterState]{}, {}", this, masterState);
		this.masterState = masterState;
	}

	@Override
	public String metaInfo() {
		return String.format("%s(%s:%d)", roleDesc(), masterEndPoint().getHost(), masterEndPoint().getPort());
	}

	@Override
	public String toString() {
		return String.format("%s", endpoint);
	}

	@Override
	public boolean isKeeper() {
		return isKeeper.get();
	}

	@Override
	public void setKeeper() {
		isKeeper.set(true);
		logger.info("[setKeeper]{}", this);
	}
}
