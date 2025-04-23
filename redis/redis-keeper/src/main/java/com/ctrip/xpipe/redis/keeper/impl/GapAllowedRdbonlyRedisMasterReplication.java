package com.ctrip.xpipe.redis.keeper.impl;

import com.ctrip.xpipe.api.server.PARTIAL_STATE;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.Psync;
import com.ctrip.xpipe.redis.core.protocal.cmd.FreshRdbOnlyGapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.cmd.FreshRdbOnlyPsync;
import com.ctrip.xpipe.redis.core.protocal.cmd.RdbOnlyGapAllowedSync;
import com.ctrip.xpipe.redis.core.protocal.cmd.RdbOnlyPsync;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.UPDATE_RDB_RESULT;
import com.ctrip.xpipe.redis.keeper.RdbDumper;
import com.ctrip.xpipe.redis.keeper.RedisKeeperServer;
import com.ctrip.xpipe.redis.keeper.RedisMaster;
import com.ctrip.xpipe.redis.keeper.config.KeeperResourceManager;
import com.ctrip.xpipe.redis.keeper.exception.psync.KeeperTolerantClosePsyncException;
import com.ctrip.xpipe.redis.keeper.exception.psync.PsyncConnectMasterFailException;
import com.ctrip.xpipe.redis.keeper.exception.psync.PsyncMasterRdbOffsetNotContinuousRuntimeException;
import com.ctrip.xpipe.redis.keeper.exception.psync.PsyncRuntimeException;
import com.ctrip.xpipe.redis.keeper.exception.replication.UnexpectedReplIdException;
import com.ctrip.xpipe.redis.keeper.store.RdbOnlyReplicationStore;
import com.ctrip.xpipe.utils.VisibleForTesting;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

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

			try {
				UPDATE_RDB_RESULT result = redisMaster.getCurrentReplicationStore().checkReplIdAndUpdateRdbGapAllowed(rdbStore);
				checkCount++;

				switch (result)	{
					case OK:
						break;
					case LACK_BACKLOG:
						if (state.equals(REPL_STATE.NORMAL_SYNC)) {
							state = REPL_STATE.FAIL_FOR_NOT_CONTINUE;
							try {
								logger.info("[retryOnceForRdbNotContinue][resetRdbStore]{}", dumpedRdbStore);
								currentPsync.get().future().setFailure(new KeeperTolerantClosePsyncException(
										new PsyncMasterRdbOffsetNotContinuousRuntimeException(0, 0)));
								resetReplicationStore();
								disconnectWithMaster();
							} catch (Exception e) {
								logger.info("[doOnFullSync][retryForNotContinue] fail", e);
								dumpFail(new PsyncMasterRdbOffsetNotContinuousRuntimeException(0, 0));
							}
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
						}
						break;
					case REPLSTAGE_NOT_MATCH:
					case REPLID_NOT_MATCH:
					case REPLOFF_OUT_RANGE:
					case MASTER_UUID_NOT_MATCH:
					case GTID_SET_NOT_MATCH:
					default:
						dumpFail(new XpipeRuntimeException("checkReplIdAndUpdateRdbGapAllowed fail:" + result));
						break;
				}
			} catch (Throwable th) {
				logger.info("[doRdbTypeConfirm][checkReplIdAndUpdateRdb] fail", th);
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
