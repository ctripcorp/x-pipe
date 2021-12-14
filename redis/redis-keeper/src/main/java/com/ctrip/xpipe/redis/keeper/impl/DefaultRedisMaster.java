
package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.endpoint.Endpoint;
import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.lifecycle.LifecycleHelper;
import com.ctrip.xpipe.redis.core.protocal.MASTER_STATE;
import com.ctrip.xpipe.redis.core.store.ReplicationStore;
import com.ctrip.xpipe.redis.core.store.ReplicationStoreManager;
import com.ctrip.xpipe.redis.keeper.*;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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

	private AtomicReference<RedisMasterReplication> redisMasterReplicationRef = new AtomicReference<>();

	private NioEventLoopGroup nioEventLoopGroup;

	private KeeperResourceManager keeperResourceManager;

	protected static final String KEY_CLOSE_REPL_COMPLETELY_TIMEOUT_MILLIS = "KEY_CLOSE_REPL_COMPLETELY_TIMEOUT_MILLIS";

	public static int CLOSE_REPL_COMPLETELY_TIMEOUT_MILLIS = Integer.parseInt(System.getProperty(KEY_CLOSE_REPL_COMPLETELY_TIMEOUT_MILLIS, "15000"));

	public DefaultRedisMaster(RedisKeeperServer redisKeeperServer, DefaultEndPoint endpoint, NioEventLoopGroup nioEventLoopGroup,
							  ReplicationStoreManager replicationStoreManager, ScheduledExecutorService scheduled,
							  KeeperResourceManager resourceManager) {

		this.redisKeeperServer = redisKeeperServer;
		this.replicationStoreManager = replicationStoreManager;
		this.nioEventLoopGroup = nioEventLoopGroup;
		this.endpoint = endpoint;
		this.scheduled = scheduled;
		this.keeperResourceManager = resourceManager;
		this.redisMasterReplicationRef.set(new DefaultRedisMasterReplication(this, this.redisKeeperServer, nioEventLoopGroup,
				this.scheduled, resourceManager));
	}
	
	@Override
	protected void doInitialize() throws Exception {
		super.doInitialize();
		redisMasterReplication().initialize();
		//init we treat is as redis
		redisKeeperServer.getKeeperMonitor().getMasterStats().setMasterRole(endpoint, SERVER_TYPE.REDIS);

	}

	@Override
	protected void doStart() throws Exception {
		super.doStart();
		redisMasterReplication().start();
	}

	@Override
	protected void doStop() throws Exception {
		RedisMasterReplication repl = redisMasterReplication();
		CommandFuture<Void> closeCompletelyFuture = repl.waitReplStopCompletely();
		stopReplication(repl);
		try {
			closeCompletelyFuture.get(CLOSE_REPL_COMPLETELY_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
		} catch (Throwable th) {
			logger.info("[doStop][wait repl close timeout] {}", repl);
		}
		super.doStop();
	}
	
	@Override
	protected void doDispose() throws Exception {
		disposeReplication(redisMasterReplication());
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
		redisMasterReplication().reconnectMaster();
	}

	@Override
	public void changeReplAddress(Endpoint address) {
		this.endpoint = address;
		RedisMasterReplication originRepl = redisMasterReplication();
		CommandFuture<Void> closeCompletelyFuture = originRepl.waitReplStopCompletely();
		stopReplication(originRepl);

		ScheduledFuture<?> timeoutFuture = scheduled.schedule(() -> {
			if (!closeCompletelyFuture.isDone()) {
				logger.info("[changeReplAddress][wait close completely timeout] {}", this);
				disposeReplication(originRepl);
				initAndStartReplication(originRepl);
			}
		}, CLOSE_REPL_COMPLETELY_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

		closeCompletelyFuture.addListener(future -> {
			if (timeoutFuture.isDone()) {
				logger.info("[changeReplAddress][close completely but timeout] {}", this);
				return;
			} else {
				try {
					closeCompletelyFuture.cancel(true);
				} catch (Throwable th) {
					logger.info("[changeReplAddress][cancel timeout fail] {}", this);
				}
			}

			logger.info("[changeReplAddress][close completely, start new repl] {}", this);
			disposeReplication(originRepl);
			initAndStartReplication(originRepl);
		});
	}

	private void stopReplication(RedisMasterReplication repl) {
		try {
			LifecycleHelper.stopIfPossible(repl);
		} catch (Throwable th) {
			logger.info("[stopReplication][stop repl fail] {}", repl, th);
		}
	}

	private void disposeReplication(RedisMasterReplication repl) {
		try {
			LifecycleHelper.disposeIfPossible(repl);
		} catch (Throwable th) {
			logger.info("[disposeReplication][dispose repl fail] {}", repl, th);
		}
	}

	private void initAndStartReplication(RedisMasterReplication originRepl) {
		try {
			if(getLifecycleState().isStopping() || getLifecycleState().isStopped()){
				logger.info("[initAndStartReplication][stopped, exit]{}, {}", endpoint, this);
				return;
			}

			RedisMasterReplication newRepl = new DefaultRedisMasterReplication(this,
					this.redisKeeperServer, nioEventLoopGroup, this.scheduled, keeperResourceManager);
			if (!redisMasterReplicationRef.compareAndSet(originRepl, newRepl)) {
				logger.info("[initAndStartReplication][repl already change, exit] expt:{} but:{}", originRepl, redisMasterReplication());
			} else {
				LifecycleHelper.initializeIfPossible(newRepl);
				LifecycleHelper.startIfPossible(newRepl);
			}
		} catch (Exception e) {
			logger.error("[initAndStartReplication] {}, origin {}", this, originRepl, e);
		}
	}

	private RedisMasterReplication redisMasterReplication() {
		return redisMasterReplicationRef.get();
	}

	@Override
	public Endpoint masterEndPoint() {
		return this.endpoint;
	}

	@Override
	public PARTIAL_STATE partialState() {
		return redisMasterReplication().partialState();
	}

	
	@Override
	public RdbDumper createRdbDumper() throws CreateRdbDumperException {
		
		if(masterState != MASTER_STATE.REDIS_REPL_CONNECTED){
			logger.info("[createRdbDumper][master state not connected, dumper not allowed]{}", redisMasterReplication());
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
