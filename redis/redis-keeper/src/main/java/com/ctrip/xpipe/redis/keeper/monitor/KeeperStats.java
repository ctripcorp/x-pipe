package com.ctrip.xpipe.redis.keeper.monitor;

/**
 * @author wenchao.meng
 *
 *         Feb 20, 2017
 */
public interface KeeperStats {

	void increaseFullSync();

	long getFullSyncCount();

	void increatePartialSync();

	long getPartialSyncCount();

	void increatePartialSyncError();

	long getPartialSyncErrorCount();

	long increaseWaitOffsetSucceed();

	long increasWaitOffsetFail();

	long getWaitOffsetSucceed();

	long getWaitOffsetFail();

}
