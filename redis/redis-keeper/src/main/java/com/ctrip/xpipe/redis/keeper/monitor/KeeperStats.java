package com.ctrip.xpipe.redis.keeper.monitor;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @author wenchao.meng
 *
 *         Feb 20, 2017
 */
public interface KeeperStats extends Startable,Stoppable {

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

	long getInputInstantaneousBPS();

	long getOutputInstantaneousBPS();

	void increaseInputBytes(long bytes);

	void increaseOutputBytes(long bytes);

	long getInputBytes();

	long getOutputBytes();

}
