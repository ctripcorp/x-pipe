/**
 *
 */
package com.ctrip.xpipe.api.cluster;

import com.ctrip.xpipe.cluster.ElectContext;

import java.util.function.Supplier;

/**
 * @author marsqing
 *
 *         Jun 17, 2016 4:49:11 PM
 */
public interface LeaderElectorManager {

	/** The context is read on every (re)start, so a stop/start round re-creates the latch data. */
	LeaderElector createLeaderElector(Supplier<ElectContext> ctxSupplier);

}
