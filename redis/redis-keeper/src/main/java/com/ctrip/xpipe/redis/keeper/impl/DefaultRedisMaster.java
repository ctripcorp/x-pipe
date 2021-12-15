
package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.*;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wenchao.meng
 *
 *         May 22, 2016 6:36:21 PM
 */
public class DefaultRedisMaster extends AbstractLifecycle implements RedisMaster {

	private RedisKeeperServer redisKeeperServer;

	private ReplicationStoreManager replicationStoreManager;

	private volatile Endpoint endpoint;
	
	private AtomicBoolean isKeeper = new AtomicBoolean(false);
	
	private ScheduledExecutorService scheduled;
	
	private MASTER_STATE masterState = MASTER_STATE.REDIS_REPL_NONE;

	private RedisMasterReplication redisMasterReplication;

	private NioEventLoopGroup nioEventLoopGroup;

	private KeeperResourceManager keeperResourceManager;

	private AtomicInteger adjustMasterWaitRound = new AtomicInteger(0);

	private ScheduledFuture<?> scheduledFuture;

	protected static final String KEY_ADJUST_MASTER_REPL_INTERVAL_MILLI = "KEY_ADJUST_MASTER_REPL_INTERVAL_MILLI";
	protected static final String KEY_ADJUST_MASTER_REPL_MAX_WAIT_TIME = "KEY_ADJUST_MASTER_REPL_MAX_WAIT_TIME";

	public static int ADJUST_MASTER_REPL_INTERVAL_MILLI = Integer.parseInt(System.getProperty(KEY_ADJUST_MASTER_REPL_INTERVAL_MILLI, "100"));
	public static int ADJUST_MASTER_REPL_MAX_WAIT_TIME = Integer.parseInt(System.getProperty(KEY_ADJUST_MASTER_REPL_MAX_WAIT_TIME, "15000"));

	public DefaultRedisMaster(RedisKeeperServer redisKeeperServer, DefaultEndPoint endpoint, NioEventLoopGroup nioEventLoopGroup,
							  ReplicationStoreManager replicationStoreManager, ScheduledExecutorService scheduled,
							  KeeperResourceManager resourceManager) {

		this.redisKeeperServer = redisKeeperServer;
		this.replicationStoreManager = replicationStoreManager;
		this.nioEventLoopGroup = nioEventLoopGroup;
		this.endpoint = endpoint;
		this.scheduled = scheduled;
		this.keeperResourceManager = resourceManager;
		this.redisMasterReplication = new DefaultRedisMasterReplication(this, this.redisKeeperServer, nioEventLoopGroup,
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
	public synchronized void changeReplAddress(Endpoint address) {
		this.endpoint = address;
		this.adjustMasterWaitRound.set(0);
		this.startCheckAndAdjustTask();
	}

	private void startCheckAndAdjustTask() {
		if (null == scheduledFuture || scheduledFuture.isDone()) {
			logger.info("[startCheckAndAdjustTask][start adjust task] {}", this);
			this.scheduledFuture = scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
				@Override
				protected void doRun() throws Exception {
					checkAndAdjustReplication();
				}
			}, 0, ADJUST_MASTER_REPL_INTERVAL_MILLI, TimeUnit.MILLISECONDS);
		} else {
			logger.info("[startCheckAndAdjustTask][already start adjust task, skip] {}", this);
		}
	}

	protected synchronized void checkAndAdjustReplication() throws Exception {
		logger.debug("[checkAndAdjustReplication][begin] {}", this);
		if (!this.endpoint.equals(redisMasterReplication.masterEndpoint())) {
			logger.info("[checkAndAdjustReplication][wrong master] {}->{}", redisMasterReplication.masterEndpoint(), this.endpoint);
			tryStopAndDisposeReplication();

			int executeTime = adjustMasterWaitRound.getAndIncrement();
			if (redisMasterReplication.getLifecycleState().isDisposed() && redisMasterReplication.isReplStopCompletely()
					|| (executeTime * ADJUST_MASTER_REPL_INTERVAL_MILLI >= ADJUST_MASTER_REPL_MAX_WAIT_TIME)) {
				logger.info("[checkAndAdjustReplication][replace repl] exec time:{}", executeTime);
				redisMasterReplication = new DefaultRedisMasterReplication(this,
						this.redisKeeperServer, nioEventLoopGroup, this.scheduled, keeperResourceManager);
			} else {
				logger.info("[checkAndAdjustReplication][not ready] exec time:{}", executeTime);
				return;
			}
		}

		syncMasterAndReplState();
		if (this.getLifecycleState().getPhaseName().equals(redisMasterReplication.getLifecycleState().getPhaseName())) {
			logger.info("[checkAndAdjustReplication][success]");
			this.scheduledFuture.cancel(false);
			this.scheduledFuture = null;
		} else {
			logger.info("[checkAndAdjustReplication][state desync] master:{}, repl:{}", this.getLifecycleState().getPhaseName(),
					redisMasterReplication.getLifecycleState().getPhaseName());
		}
	}

	private void syncMasterAndReplState() throws Exception {
		logger.debug("[syncMasterAndReplState][begin]");
		if (this.getLifecycleState().isInitialized()) LifecycleHelper.initializeIfPossible(redisMasterReplication);
		if (this.getLifecycleState().isStarted()) LifecycleHelper.startIfPossible(redisMasterReplication);
		if (this.getLifecycleState().isStopped()) LifecycleHelper.stopIfPossible(redisMasterReplication);
		if (this.getLifecycleState().isDisposed()) LifecycleHelper.disposeIfPossible(redisMasterReplication);
	}

	private void tryStopAndDisposeReplication() {
		try {
			LifecycleHelper.stopIfPossible(redisMasterReplication);
			LifecycleHelper.disposeIfPossible(redisMasterReplication);
		} catch (Throwable th) {
			logger.error("[tryStopAndDisposeReplication][error] {}", redisMasterReplication, th);
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
	public RdbDumper createRdbDumper() throws CreateRdbDumperException {
		
		if(masterState != MASTER_STATE.REDIS_REPL_CONNECTED){
			logger.info("[createRdbDumper][master state not connected, dumper not allowed]{}", redisMasterReplication);
			throw new CreateRdbDumperException(this, "master state not connected, dumper not allowed:" + masterState);
		}
		return new RedisMasterNewRdbDumper(this, redisKeeperServer, nioEventLoopGroup, scheduled, keeperResourceManager);
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
