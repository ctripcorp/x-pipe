package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.store.*;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.SLAVE_STATE;
import com.ctrip.xpipe.redis.keeper.store.DefaultFullSyncListener;
import com.ctrip.xpipe.utils.VisibleForTesting;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.ctrip.xpipe.redis.core.store.RdbDumpState.WAIT_DUMPPING;

/**
 * @author wenchao.meng
 *
 *         Aug 25, 2016
 */
public abstract class AbstractRdbDumper extends AbstractCommand<Void> implements RdbDumper {

	private volatile RdbDumpState rdbDumpState = WAIT_DUMPPING;

	protected RedisKeeperServer redisKeeperServer;

	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	public AbstractRdbDumper(RedisKeeperServer redisKeeperServer) {
		this.redisKeeperServer = redisKeeperServer;
	}

	@Override
	public String getName() {
		return getClass().getSimpleName();
	}

	public void setRdbDumpState(RdbDumpState rdbDumpState) {
		lock.writeLock().lock();
		try {
			this.rdbDumpState = rdbDumpState;
			switch (rdbDumpState) {
				case DUMPING:
					doWhenDumping();
					break;
				case FAIL:
					doWhenDumpFailed();
					redisKeeperServer.clearRdbDumper(this);
					break;
				case NORMAL:
					// clear dumper
					redisKeeperServer.clearRdbDumper(this);
					;
					break;
				case WAIT_DUMPPING:
					break;
			}
		} finally {
		    lock.writeLock().unlock();
		}
	}

	private void doWhenDumpFailed() {

		for (final RedisSlave redisSlave : redisKeeperServer.slaves()) {
			if (redisSlave.getSlaveState() == SLAVE_STATE.REDIS_REPL_WAIT_RDB_DUMPING) {
				getLogger().info("[doWhenDumping][slave waiting for rdb, close]{}", redisSlave);
				try {
					redisSlave.close();
				} catch (IOException e) {
					getLogger().error("[doWhenDumpFailed][close slave]", e);
				}
			}
		}
	}

	private void doWhenDumping() {

		for (final RedisSlave redisSlave : redisKeeperServer.slaves()) {
			if (redisSlave.getSlaveState() == SLAVE_STATE.REDIS_REPL_WAIT_RDB_DUMPING) {
				getLogger().info("[doWhenDumping][slave waiting for rdb, resume]{}", redisSlave);
				continueSlaveFsync(redisSlave);
			}
		}
	}

	@Override
	public void rdbGtidSetParsed() {
		for (final RedisSlave redisSlave : redisKeeperServer.slaves()) {
			if (redisSlave.getSlaveState() == SLAVE_STATE.REDIS_REPL_WAIT_RDB_GTIDSET) {
				getLogger().info("[doWhenDumping][slave waiting for rdb, resume]{}", redisSlave);
				continueSlaveFsync(redisSlave);
			}
		}
	}

	private void continueSlaveFsync(final RedisSlave redisSlave) {
		try {
			redisSlave.processPsyncSequentially(new Runnable() {
				@Override
				public void run() {
					try {
						redisKeeperServer.fullSyncToSlave(redisSlave);
					} catch (Throwable th) {
						try {
							getLogger().error(String.format("fullsync to slave:%s", redisSlave), th);
							if(redisSlave.isOpen()){
								redisSlave.close();
							}
						} catch (IOException e) {
							getLogger().error("[run][close]" + redisSlave, th);
						}
					}
				}
			});
		} catch (Throwable th) {
			getLogger().info("[continueSlaveFsync][fail]{}", redisSlave, th);
		}
	}

	@Override
	public void tryFullSync(RedisSlave redisSlave) throws IOException {
		RdbDumpState state;
		lock.readLock().lock();
		try {
			state = rdbDumpState;
			if (state == WAIT_DUMPPING) {
				getLogger().info("[tryFullSync][make slave waiting]{}", redisSlave);
				redisSlave.waitForRdbDumping();
				return;
			}
		} finally {
			lock.readLock().unlock();
		}
		switch (state) {
			case DUMPING:
				doFullSyncOrGiveUp(redisSlave);
				break;
			case FAIL:
			case NORMAL:
				getLogger().warn("[tryFullSync]{}", redisSlave);
				redisKeeperServer.clearRdbDumper(this);
				redisKeeperServer.fullSyncToSlave(redisSlave);
				break;
		}

	}

	@VisibleForTesting void doFullSyncOrGiveUp(RedisSlave redisSlave) throws IOException {
		FullSyncListener fullSyncListener = new DefaultFullSyncListener(redisSlave);
		FULLSYNC_FAIL_CAUSE failCause = redisKeeperServer.getReplicationStore().fullSyncIfPossible(fullSyncListener);
		if (null != failCause) {
			throw new IllegalStateException("[tryFullSync][rdb dumping, but can not full sync] " + failCause);
		}
	}

	@Override
	public void beginReceiveRdbData(String replId, long masterOffset) {
		getLogger().info("[beginReceiveRdbData]{}", this);
		setRdbDumpState(RdbDumpState.DUMPING);

	}

	@Override
	public void dumpFinished() {
		getLogger().info("[dumpFinished]{}", this);
		setRdbDumpState(RdbDumpState.NORMAL);
		future().setSuccess();
	}

	@Override
	public void dumpFail(Throwable th) {

		if (future().isDone()) {
			getLogger().info("[dumpFail][already done]{}, {}, {}", this, th.getMessage(), future().isSuccess());
			return;
		}

		getLogger().info("[dumpFail]{}, {}", this, th.getMessage());
		setRdbDumpState(RdbDumpState.FAIL);
		future().setFailure(th);
	}

	@Override
	public void exception(Throwable th) {
		dumpFail(th);
	}

	@Override
	protected void doReset() {

	}
}
