
package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.*;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
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

	private NioEventLoopGroup masterEventLoopGroup;

	private NioEventLoopGroup rdbOnlyEventLoopGroup;

	private KeeperResourceManager keeperResourceManager;

	public DefaultRedisMaster(RedisKeeperServer redisKeeperServer, DefaultEndPoint endpoint, NioEventLoopGroup masterEventLoopGroup,
							  NioEventLoopGroup rdbOnlyEventLoopGroup, ReplicationStoreManager replicationStoreManager, ScheduledExecutorService scheduled,
							  KeeperResourceManager resourceManager) {

		this.redisKeeperServer = redisKeeperServer;
		this.replicationStoreManager = replicationStoreManager;
		this.masterEventLoopGroup = masterEventLoopGroup;
		this.rdbOnlyEventLoopGroup = rdbOnlyEventLoopGroup;
		this.endpoint = endpoint;
		this.scheduled = scheduled;
		this.keeperResourceManager = resourceManager;
		this.redisMasterReplication = new DefaultRedisMasterReplication(this, this.redisKeeperServer, masterEventLoopGroup,
				this.scheduled, resourceManager);
	}
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		redisMasterReplication.initialize();
		//init we treat is as redis
		redisKeeperServer.getKeeperMonitor().getMasterStats().setMasterRole(endpoint, SERVER_TYPE.REDIS);

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
	public void reconnect() {
		redisMasterReplication.reconnectMaster();
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
		return new RedisMasterNewRdbDumper(this, redisKeeperServer, rdbOnlyEventLoopGroup, scheduled, keeperResourceManager);
	}
	
	public MASTER_STATE getMasterState() {
		return masterState;
	}
	
	public void setMasterState(MASTER_STATE masterState) {
		
		logger.info("[setMasterState]{}, {}", this, masterState);
		this.masterState = masterState;

		//for monitor
		redisKeeperServer.getKeeperMonitor().getMasterStats().setMasterState(masterState);
		redisKeeperServer.getKeeperMonitor().getReplicationStoreStats().setMasterState(masterState);
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
		//for monitor
		redisKeeperServer.getKeeperMonitor().getMasterStats().setMasterRole(endpoint, SERVER_TYPE.KEEPER);
		logger.info("[setKeeper]{}", this);
	}
}
