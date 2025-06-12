package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.cmd.FreshRdbOnlyGapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.cmd.RdbOnlyGapAllowedSync;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.UPDATE_RDB_RESULT;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.exception.psync.GapAllowedSyncRdbNotContinuousRuntimeException;
import com.ctrip.xpipe.redis.keeper.exception.psync.KeeperTolerantClosePsyncException;
import com.ctrip.xpipe.redis.keeper.exception.replication.KeeperReplicationStoreRuntimeException;
import com.ctrip.xpipe.redis.keeper.store.RdbOnlyReplicationStore;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.ScheduledExecutorService;

public class GapAllowedRdbonlyRedisMasterReplication extends RdbonlyRedisMasterReplication{

	public GapAllowedRdbonlyRedisMasterReplication(RedisKeeperServer redisKeeperServer, RedisMaster redisMaster,
                                                   boolean tryRordb, boolean freshRdbNeeded,
                                                   NioEventLoopGroup nioEventLoopGroup, ScheduledExecutorService scheduled,
                                                   RdbDumper rdbDumper, KeeperResourceManager resourceManager) {
		super(redisKeeperServer, redisMaster, tryRordb, freshRdbNeeded, nioEventLoopGroup, scheduled, rdbDumper, resourceManager);
	}

	@Override
	protected Psync doCreatePsync(RdbOnlyReplicationStore rdbOnlyReplicationStore) {
		Psync psync;

		if (state.equals(REPL_STATE.FRESH_SYNC)) {
			psync = new FreshRdbOnlyGapAllowedSync(clientPool, rdbOnlyReplicationStore, scheduled);
		} else {
			psync = new RdbOnlyGapAllowedSync(clientPool, rdbOnlyReplicationStore, scheduled);
		}
		return psync;
	}

	@Override
	protected void doRdbTypeConfirm(RdbStore rdbStore) {
		int checkCount = 0;
		boolean checkAgain;

		do {
			checkAgain = false;

			UPDATE_RDB_RESULT result = null;
			try {
				result = redisMaster.getCurrentReplicationStore().checkReplIdAndUpdateRdbGapAllowed(rdbStore);
			} catch (Throwable th) {
				logger.error("[doRdbTypeConfirm][checkReplIdAndUpdateRdbGapAllowed]", th);
				dumpFail(th);
				throw new KeeperReplicationStoreRuntimeException(th.getMessage());
			}
			checkCount++;

			switch (result)	{
				case OK:
					break;
				case REPLSTAGE_NOT_MATCH:
				case REPLOFF_OUT_RANGE:
				case LACK_BACKLOG:
					if (state.equals(REPL_STATE.NORMAL_SYNC)) {
						state = REPL_STATE.FAIL_FOR_NOT_CONTINUE;
						try {
							logger.info("[retryOnceForRdbNotContinue][resetRdbStore]{},{}", result, dumpedRdbStore);
							currentPsync.get().future().setFailure(new KeeperTolerantClosePsyncException(
									new GapAllowedSyncRdbNotContinuousRuntimeException(result.toString())));
							disconnectWithMaster();
							resetReplicationStore();
							getRdbDumper().waitRetry();
						} catch (Exception e) {
							logger.info("[doOnFullSync][retryForNotContinue] fail", e);
							dumpFail(new GapAllowedSyncRdbNotContinuousRuntimeException(result.toString()));
						}
					} else {
						dumpFail(new GapAllowedSyncRdbNotContinuousRuntimeException(result.toString()));
					}
					break;
				case RDB_MORE_RECENT:
					if (checkCount < 5) {
						checkAgain = true;
						logger.info("[doRdbTypeConfirm] rdb more recent, wait and check again, checkCount:{}", checkCount);
						try {
							Thread.sleep(500);
						} catch (InterruptedException e) {
							logger.error("[doRdbTypeConfirm] wait interrupted.");
						}
					} else {
						logger.info("[doRdbTypeConfirm] rdb more recent, no more check, checkCount:{}", checkCount);
						dumpFail(new IllegalStateException("checkReplIdAndUpdateRdbGapAllowed fail: RDB_MORE_RECENT too many times"));
					}
					break;
				case REPLID_NOT_MATCH:
				case MASTER_UUID_NOT_MATCH:
				case GTID_SET_NOT_MATCH:
				default:
					dumpFail(new IllegalStateException("checkReplIdAndUpdateRdbGapAllowed fail:" + result));
					break;
			}
		} while (checkAgain);
	}

	@Override
	protected void doOnFullSync(long masterRdbOffset) {
	}


	protected void doOnXFullSync(String replId, long replOff, String masterUuid, GtidSet gtidLost) {
	}

	@Override
	protected String getSimpleName() {
		return "GARdbRep";
	}
}
