package com.ctrip.xpipe.api.cluster;

import com.ctrip.xpipe.api.lifecycle.Lifecycle;

/**
 * @author marsqing
 *
 *         Jun 17, 2016 4:51:59 PM
 */
public interface LeaderElector extends Lifecycle {

	void elect() throws Exception;

	boolean hasLeaderShip();

}
