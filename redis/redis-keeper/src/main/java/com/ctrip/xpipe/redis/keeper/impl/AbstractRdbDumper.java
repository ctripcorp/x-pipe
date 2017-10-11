package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.redis.core.store.FullSyncListener;
import com.ctrip.xpipe.redis.core.store.RdbDumpState;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.SLAVE_STATE;
import com.ctrip.xpipe.redis.keeper.store.DefaultFullSyncListener;

import java.io.IOException;

/**
 * @author wenchao.meng
 *
 *         Aug 25, 2016
 */
public abstract class AbstractRdbDumper extends AbstractCommand<Void> implements RdbDumper {

	private volatile RdbDumpState rdbDumpState = RdbDumpState.WAIT_DUMPPING;

	protected RedisKeeperServer redisKeeperServer;

	public AbstractRdbDumper(RedisKeeperServer redisKeeperServer) {
		this.redisKeeperServer = redisKeeperServer;
	}

	@Override
	public String getName() {
		return getClass().getSimpleName();
	}

	public void setRdbDumpState(RdbDumpState rdbDumpState) {

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
	}

	private void doWhenDumpFailed() {

		for (final RedisSlave redisSlave : redisKeeperServer.slaves()) {
			if (redisSlave.getSlaveState() == SLAVE_STATE.REDIS_REPL_WAIT_RDB_DUMPING) {
				logger.info("[doWhenDumping][slave waiting for rdb, close]{}", redisSlave);
				try {
					redisSlave.close();
				} catch (IOException e) {
					logger.error("[doWhenDumpFailed][close slave]", e);
				}
			}
		}
	}

	private void doWhenDumping() {

		for (final RedisSlave redisSlave : redisKeeperServer.slaves()) {
			if (redisSlave.getSlaveState() == SLAVE_STATE.REDIS_REPL_WAIT_RDB_DUMPING) {
				logger.info("[doWhenDumping][slave waiting for rdb, resume]{}", redisSlave);
				redisSlave.processPsyncSequentially(new Runnable() {
					@Override
					public void run() {
						try {
							redisKeeperServer.fullSyncToSlave(redisSlave);
						} catch (Exception e) {
							logger.error(String.format("fullsync to slave:%s", redisSlave), e);
						}
					}
				});
			}
		}
	}

	@Override
	public void tryFullSync(RedisSlave redisSlave) throws IOException {

		switch (rdbDumpState) {

		case DUMPING:
			FullSyncListener fullSyncListener = new DefaultFullSyncListener(redisSlave);
			if (!redisKeeperServer.getReplicationStore().fullSyncIfPossible(fullSyncListener)) {
				throw new IllegalStateException("[tryFullSync][rdb dumping, but can not full synn]");
			}
			break;
		case FAIL:
		case NORMAL:
			logger.warn("[tryFullSync]{}", redisSlave);
			redisKeeperServer.clearRdbDumper(this);
			redisKeeperServer.fullSyncToSlave(redisSlave);
			break;
		case WAIT_DUMPPING:
			logger.info("[tryFullSync][make slave waiting]{}", redisSlave);
			redisSlave.waitForRdbDumping();
			break;
		}
	}

	@Override
	public void beginReceiveRdbData(long masterOffset) {
		logger.info("[beginReceiveRdbData]{}", this);
		setRdbDumpState(RdbDumpState.DUMPING);

	}

	@Override
	public void dumpFinished() {
		logger.info("[dumpFinished]{}", this);
		setRdbDumpState(RdbDumpState.NORMAL);
		future().setSuccess();
	}

	@Override
	public void dumpFail(Throwable th) {

		if (future().isDone()) {
			logger.info("[dumpFail][already done]{}, {}, {}", this, th.getMessage(), future().isSuccess());
			return;
		}

		logger.info("[dumpFail]{}, {}", this, th.getMessage());
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
