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

	/**
	 * Publish the latest election context into this elector's existing ephemeral node.
	 * The node path and sequence stay unchanged. Returns false when the node is not
	 * ready or ZooKeeper rejects the update.
	 */
	default boolean publishElectionData() {
		return false;
	}

}
