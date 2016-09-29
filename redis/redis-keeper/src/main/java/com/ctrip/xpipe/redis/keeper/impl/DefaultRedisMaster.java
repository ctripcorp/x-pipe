package com.ctrip.xpipe.redis.keeper.impl;


import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;


import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.RedisMasterReplication;


/**
 * @author wenchao.meng
 *
 *         May 22, 2016 6:36:21 PM
 */
public class DefaultRedisMaster extends AbstractLifecycle implements RedisMaster {

	private RedisKeeperServer redisKeeperServer;

	private ReplicationStoreManager replicationStoreManager;

	private DefaultEndPoint endpoint;
	
	private ScheduledExecutorService scheduled;
	
	private MASTER_STATE masterState = MASTER_STATE.REDIS_REPL_NONE;
	
	private RedisMasterReplication redisMasterReplication;

	public DefaultRedisMaster(RedisKeeperServer redisKeeperServer, DefaultEndPoint endpoint, ReplicationStoreManager replicationStoreManager,
			ScheduledExecutorService scheduled) {

		this.redisKeeperServer = redisKeeperServer;
		this.replicationStoreManager = replicationStoreManager;
		this.endpoint = endpoint;
		this.scheduled = scheduled;
		redisMasterReplication = new DefaultRedisMasterReplication(this, this.redisKeeperServer, this.scheduled);
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

		try {
			ReplicationStore replicationStore = replicationStoreManager.createIfNotExist();
			return replicationStore;
		} catch (IOException e) {
			logger.error("[getCurrentReplicationStore]" + this, e);
			throw new XpipeRuntimeException("[getCurrentReplicationStore]" + this, e);
		}
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
	public RdbDumper createRdbDumper() {
		return new RedisMasterNewRdbDumper(this, redisKeeperServer);
	}
	
	public MASTER_STATE getMasterState() {
		return masterState;
	}
	
	public void setMasterState(MASTER_STATE masterState) {
		this.masterState = masterState;
	}
}
