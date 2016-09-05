package com.ctrip.xpipe.cluster;

import com.ctrip.xpipe.api.codec.Codec;

/**
 * @author marsqing
 *
 *         May 25, 2016 5:48:16 PM
 */
public class ElectContext {

	private String leaderElectionZKPath;

	private String leaderElectionID;

	public ElectContext(String leaderElectionZKPath, String leaderElectionID) {
		this.leaderElectionZKPath = leaderElectionZKPath;
		this.leaderElectionID = leaderElectionID;
	}

	public String getLeaderElectionZKPath() {
		return leaderElectionZKPath;
	}

	public String getLeaderElectionID() {
		return leaderElectionID;
	}
	
	@Override
	public String toString() {
		return Codec.DEFAULT.encode(this);
	}

}
